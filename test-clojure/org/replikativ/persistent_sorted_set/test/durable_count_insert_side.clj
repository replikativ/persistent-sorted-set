(ns org.replikativ.persistent-sorted-set.test.durable-count-insert-side
  "The INSERT twin of `durable-count-read-side`: what `conj` on a cold tree does to the
   counts it writes.

   A branch on the insert path took its new count as `own - oldChild + newChildren`. On a
   cold tree that arithmetic is unavailable more often than not: a child that just SPLIT
   returns two halves whose counts come from a walk over children still on disk, so both
   answer -1, and the parent — which knew its own total — threw it away. The unknown then
   climbed to the root, was SERIALIZED there, and every later restore started from a root
   that could not answer `count` (datahike #1108: `has-subtree-counts?` false for the
   store from then on, so the query estimator fell back to heuristics).

   The parent needs none of that arithmetic. `add` reports a redundant insert as UNCHANGED
   before any count is touched, so on every other return exactly one element was added
   below this node, and a node that knows its total knows the new one: old + 1. (Not with
   a leaf processor, which may compact or expand; those keep the old arithmetic.)

   What stays unknown, deliberately: the two halves of a branch that split while its
   children were on disk. Dividing a total needs per-child counts, and fetching them is the
   whole-subtree restore 04499a0 removed. When ONE half is resident the other is derived."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [clojure.edn :as edn])
  (:import [org.replikativ.persistent_sorted_set Settings IStorage ANode Branch Leaf]))

(set! *warn-on-reflection* true)

;; PERSISTS :subtree-count, the way datahike's storage does.
(defrecord CountingStorage [*disk *restores ^Settings settings]
  IStorage
  (store [_ node]
    (let [^ANode node node
          addr (str (java.util.UUID/randomUUID))
          br?  (instance? Branch node)]
      (swap! *disk assoc addr
             (pr-str {:level         (.level node)
                      :keys          (vec (.keys node))
                      :addresses     (when br? (vec (.addresses ^Branch node)))
                      :subtree-count (when br? (.subtreeCount ^Branch node))}))
      addr))
  (accessed [_ _] nil)
  (restore [_ address]
    (swap! *restores inc)
    (let [{:keys [level ^java.util.List keys ^java.util.List addresses subtree-count]}
          (edn/read-string (@*disk address))]
      (if addresses
        (let [b (Branch. (int level) keys addresses settings)]
          (set! (.-_subtreeCount b) (long (or subtree-count -1)))
          b)
        (Leaf. keys settings)))))

(defn- storage [disk restores bf]
  (->CountingStorage disk restores (Settings. (int bf) nil nil nil (int 0))))

;; Baseline pinned: this storage does not persist diff-buf slots, and the suite runs with
;; -Dpss.diffBufSize=256. Unpinned, the buffered inserts would simply be lost on store.
(defn- opts-for [bf] {:comparator compare :branching-factor bf :diff-buf-size 0})

(defn- root-count [disk addr]
  (:subtree-count (edn/read-string (@disk addr))))

(defn- generations
  "`gens` rounds of (cold restore, insert `per` NEW scattered keys and re-insert `per`
   keys that are already there, store). Returns {:addr :expected :unknown-roots}.
   `add` is the insert: persistent conj, or a transient round. `pattern` is where the new
   keys land: :scattered over the whole range, or :append past the maximum — the shape of
   datahike's EAVT under fresh entity ids, and the one that splits the right spine all the
   way up."
  [disk bf n gens per pattern add]
  (let [opts (opts-for bf)
        w    (atom 0)
        ;; EVEN keys, conj-built: nodes at minimum occupancy, the state a real store is in.
        s0   (reduce #(set/conj %1 %2 compare) (set/sorted-set* opts) (range 0 (* 2 n) 2))
        a0   (set/store s0 (storage disk w bf))]
    (loop [addr a0 gen 0 expected (long n) unknown-roots 0 live-unknown 0]
      (if (= gen gens)
        {:addr addr :expected expected :unknown-roots unknown-roots :live-unknown live-unknown}
        (let [cold (set/restore-by compare addr (storage disk w bf) opts)
              ;; ODD keys spread over the whole range: every insert is new and lands in a
              ;; different part of the tree, so leaves and branches split all over it.
              news (if (= pattern :append)
                     (range (+ (* 2 n) (* gen per)) (+ (* 2 n) (* (inc gen) per)))
                     (distinct (map #(inc (* 2 (mod (* 7919 (+ % (* gen per))) n))) (range per))))
              olds (map #(* 2 (mod (* 104729 (+ % gen)) n)) (range per))
              {:keys [s lost]} (add cold (interleave news olds))
              addr' (set/store s (storage disk w bf))]
          (recur addr' (inc gen) (+ expected (count news))
                 (cond-> unknown-roots (neg? (long (or (root-count disk addr') -1))) inc)
                 (+ live-unknown (long lost))))))))

(defn- conj-each [s ks]
  (reduce (fn [{:keys [s lost]} k]
            (let [s' (set/conj s k compare)]
              {:s s' :lost (cond-> lost (not (set/has-subtree-counts? s')) inc)}))
          {:s s :lost 0} ks))

(defn- conj-transient [s ks]
  {:s (persistent! (reduce conj! (transient s) ks)) :lost 0})

(deftest the-root-keeps-its-count-through-cold-inserts
  (testing "no insert on a cold tree may leave the root without a count, in memory or on
            disk — redundant inserts included, which must change nothing"
    (doseq [[bf n] [[8 5000] [16 5000] [64 20000]]
            pattern    [:scattered :append]
            [mode add] [[:persistent conj-each] [:transient conj-transient]]
            :let [label (str mode " " pattern)]]
      (let [disk (atom {})
            {:keys [addr expected unknown-roots live-unknown]} (generations disk bf n 6 300 pattern add)
            r    (atom 0)
            cold (set/restore-by compare addr (storage disk r bf)
                                 (opts-for bf))]
        (is (zero? live-unknown)
            (str label " bf=" bf ": " live-unknown " inserts left the live root without a count"))
        (is (zero? unknown-roots)
            (str label " bf=" bf ": " unknown-roots " of 6 stored roots carry an unknown count"))
        (is (set/has-subtree-counts? cold)
            (str label " bf=" bf ": a cold reader's root knows its count"))
        (reset! r 0)
        (is (= expected (count cold))
            (str label " bf=" bf ": the count is exact (redundant inserts added nothing)"))
        (is (<= @r 1)
            (str label " bf=" bf ": a cold reader answers count from the root, paid " @r " restores"))
        (is (= expected (count (seq cold)))
            (str label " bf=" bf ": and it agrees with the elements"))))))

(deftest a-redundant-insert-on-a-cold-tree-changes-no-count
  (testing "an existing key: same set back, nothing restored beyond the path, count intact"
    (let [bf 16 n 6000 disk (atom {}) w (atom 0)
          opts (opts-for bf)
          s0   (reduce #(set/conj %1 %2 compare) (set/sorted-set* opts) (range n))
          addr (set/store s0 (storage disk w bf))
          cold (set/restore-by compare addr (storage disk w bf) opts)
          same (reduce #(set/conj %1 %2 compare) cold (range 0 n 97))]
      (is (identical? cold same))
      (is (set/has-subtree-counts? same))
      (is (= n (count same))))))

;; ---- removal, and removal mixed with insert ----
;;
;; `Branch.remove` already kept a node's own count by delta (991bdcf, and the joins after
;; it). What it could not keep is the count of the node that BECOMES the root: when the old
;; root is left with one child, that child — a join — is promoted, and a join is unknown as
;; soon as either side was. A borrow on a cold pair leaves both halves unknown, so a few
;; deletes later the promoted node had no count and neither did the set.

(defn- removal-generations
  "`gens` rounds of (cold restore, apply ops, store). pattern picks what is deleted:
   :prefix / :scattered from the live keys, or :mixed deletes interleaved with appends.
   Every round also deletes keys that are NOT there, which must change nothing."
  [disk bf n gens per pattern transient?]
  (let [opts (opts-for bf)
        w    (atom 0)
        s0   (reduce #(set/conj %1 %2 compare) (set/sorted-set* opts) (range n))]
    (loop [addr (set/store s0 (storage disk w bf)) gen 0 live (set (range n)) unknown-roots 0]
      (if (= gen gens)
        {:addr addr :live live :unknown-roots unknown-roots}
        (let [cold   (set/restore-by compare addr (storage disk w bf) opts)
              sorted (vec (sort live))
              dels   (case pattern
                       :prefix    (take per sorted)
                       :scattered (take per (take-nth (max 1 (quot (count sorted) per)) sorted))
                       :mixed     (take (quot per 2) (take-nth 5 sorted)))
              adds   (if (= pattern :mixed)
                       (map #(+ 100000 % (* gen 1000)) (range (quot per 2)))
                       [])
              absent (map #(- -1 %) (range 20))
              ops    (concat (map vector (repeat :del) absent)
                             (interleave (map vector (repeat :del) dels)
                                         (concat (map vector (repeat :add) adds) (repeat [:del -1]))))
              s      (if transient?
                       (persistent! (reduce (fn [s [op k]] (if (= op :del) (disj! s k) (conj! s k)))
                                            (transient cold) ops))
                       (reduce (fn [s [op k]] (if (= op :del) (set/disj s k compare) (set/conj s k compare)))
                               cold ops))
              live'  (reduce conj (reduce disj live dels) adds)
              addr'  (set/store s (storage disk w bf))
              root   (edn/read-string (@disk addr'))]
          (recur addr' (inc gen) live'
                 (cond-> unknown-roots
                   (and (:addresses root) (neg? (long (or (:subtree-count root) -1)))) inc)))))))

(deftest the-root-keeps-its-count-through-cold-removals
  (testing "removals on a cold tree — through root collapses, borrows and joins, with
            absent keys and with inserts mixed in — never store a root without a count"
    (doseq [[bf n per] [[8 3000 450] [16 3000 450] [64 20000 3000]]
            pattern    [:prefix :scattered :mixed]
            transient? [false true]
            :let [label (str (if transient? :transient :persistent) " " pattern " bf=" bf)]]
      (let [disk (atom {})
            {:keys [addr live unknown-roots]} (removal-generations disk bf n 6 per pattern transient?)
            r    (atom 0)
            cold (set/restore-by compare addr (storage disk r bf) (opts-for bf))]
        (is (zero? unknown-roots)
            (str label ": " unknown-roots " of 6 stored roots carry an unknown count"))
        (reset! r 0)
        (is (= (count live) (count cold))
            (str label ": the count is exact (absent keys removed nothing)"))
        (is (<= @r 1)
            (str label ": a cold reader answers count from the root, paid " @r " restores"))
        (is (= (sort live) (seq cold))
            (str label ": and the elements are the right ones"))))))
