(ns org.replikativ.persistent-sorted-set.impl.delta
  (:refer-clojure :exclude [await])
  #?(:cljs (:require-macros [org.replikativ.persistent-sorted-set.impl.tree-macros :refer [read-field]]
                            [org.replikativ.persistent-sorted-set.macros :refer [async+sync]]))
  (:require #?(:clj [org.replikativ.persistent-sorted-set.impl.tree-macros :refer [read-field]])
            #?(:clj [org.replikativ.persistent-sorted-set.macros :refer [async+sync]])
            [is.simm.partial-cps.async #?@(:cljs [:refer [await] :refer-macros [async]]
                                           :clj [:refer [async await]])]
            [org.replikativ.persistent-sorted-set.arrays :as arrays]
            [org.replikativ.persistent-sorted-set.branch :as branch]
            [org.replikativ.persistent-sorted-set.impl.node :as node]))

(defn child-refs
  "Frontier entries for every child of `branches` (already materialized). Pure."
  [branches]
  (into []
        (mapcat (fn [[b]]
                  (let [addrs (read-field b :addresses)
                        ;; diff-buf: a branch buffers a child's changes in its OWN
                        ;; slots and leaves the child's ADDRESS untouched, so for a
                        ;; buffered child an address match no longer proves the
                        ;; subtrees are equal. The guard is per CHILD: a nil slot
                        ;; means that child has no buffered diff. A non-nil slot is
                        ;; unprunable whatever its shape — for a BRANCH child :diff
                        ;; is nil and the real diff lives in the subtree, so a nil
                        ;; :diff does not mean no change.
                        slots (read-field b :_slots)]
                    (map (fn [i]
                           [nil b i
                            (when addrs (arrays/aget addrs i))
                            (or (nil? slots) (nil? (arrays/aget slots i)))])
                         (range (node/len b))))))
        branches))

(defn prune
  "Drop from each frontier the entries the other side holds at the same address —
   identical subtrees, which cannot contain a difference. Pure, no IO."
  [fa fb]
  (let [addrs   (fn [f] (into #{} (keep (fn [[_ _ _ addr prunable?]]
                                          (when (and (some? addr) prunable?) addr)))
                              f))
        sa      (addrs fa)
        sb      (addrs fb)
        shared? (fn [other] (fn [[_ _ _ addr prunable?]]
                              (and (some? addr) prunable? (contains? other addr))))]
    ;; an entry the OTHER side marked unprunable never entered its address set, so
    ;; neither side prunes against a buffered branch.
    [(into [] (remove (shared? sb)) fa)
     (into [] (remove (shared? sa)) fb)]))

;; ---- diff -------------------------------------------------------------------
;; Mirrors the JVM frontier walk in persistent_sorted_set.clj: same pruning rule,
;; same candidate differencing, so a diff computed here agrees with one computed
;; there on the same pair of trees.
;;
;; A frontier entry names a node WITHOUT loading it:
;;
;;     [node parent idx addr prunable?]
;;
;; `node` is nil until materialized, `addr` is the stored address (nil for a node
;; never stored), and `prunable?` says whether that address can be trusted to
;; stand for the contents.
;;
;; Each round intersects the two frontiers' addresses, drops what both sides
;; hold, and loads only the remainder. The addresses come from the parents, which
;; are already loaded, so pruning itself costs no IO. That shape matters more
;; here than on the JVM: every load is an async round trip, so the count of them
;; is what the caller feels, and it is bounded by the CHANGE rather than by the
;; tree — measured on the JVM at 3-4 node reads for a two-element delta whether
;; the set holds a thousand elements or a hundred thousand.

(defn- diff-sorted
  "Elements of `xs` absent from `ys`. Both ascending under `cmp`; O(n+m), no IO."
  [cmp xs ys]
  (loop [xs (seq xs) ys (seq ys) out (transient [])]
    (cond
      (nil? xs) (persistent! out)
      (nil? ys) (persistent! (reduce conj! out xs))
      :else     (let [c (cmp (first xs) (first ys))]
                  (cond
                    (neg? c) (recur (next xs) ys (conj! out (first xs)))
                    (pos? c) (recur xs (next ys) out)
                    :else    (recur (next xs) (next ys) out))))))

(defn- diff-descend
  "One level down. At level 0 the entries are leaves and contribute their keys to
   `cand`; above it they contribute their children to the next frontier.

   Loads are sequential rather than overlapped: a frontier holds only the nodes
   that actually changed, so there is little to overlap, and issuing a whole
   frontier at once would make fan-out unbounded on a large delta."
  [storage refs level cand {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [n (count refs)]
                 (loop [i 0 nodes []]
                   (if (< i n)
                     (let [[node parent idx :as ref] (nth refs i)]
                       (if (some? node)
                         (recur (inc i) (conj nodes ref))
                         (recur (inc i)
                                (conj nodes (assoc ref 0 (await (branch/child parent storage idx opts)))))))
                     (if (zero? level)
                       [[] (reduce (fn [c [nd]] (reduce conj c (.-keys nd))) cand nodes)]
                       [(child-refs nodes) cand])))))))

(defn diff
  "Keys added and removed between two sets that SHARE STRUCTURE. See the JVM
   `org.replikativ.persistent-sorted-set/diff` for the full contract; this is the
   same algorithm with the loads awaited.

   Returns `{:added [...] :removed [...]}`, or a continuation yielding it when
   `{:sync? false}`."
  [root-of a b storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [addr-a (.-address a)
                     addr-b (.-address b)]
                 (if (and (some? addr-a) (= addr-a addr-b))
                   {:added [] :removed []}          ; same root: zero reads
                   (let [cmp    (.-comparator b)
                         ;; roots go through root-of, which stamps the projection
                         ;; comparator that diff-buf needs on descent
                         root-a (await (root-of a opts))
                         root-b (await (root-of b opts))]
                     (loop [fa [[root-a nil nil addr-a true]] la (node/level root-a)
                            fb [[root-b nil nil addr-b true]] lb (node/level root-b)
                            ca [] cb []]
                       (let [la (if (seq fa) la -1)
                             lb (if (seq fb) lb -1)]
                         (if (and (neg? la) (neg? lb))
                           {:added (diff-sorted cmp cb ca) :removed (diff-sorted cmp ca cb)}
                           ;; addresses only mean the same thing at the same level,
                           ;; and a shared node keeps its level, so pruning across
                           ;; unequal levels would find nothing. Walk the deeper
                           ;; side down until they meet.
                           (let [[fa fb]   (if (== la lb) (prune fa fb) [fa fb])
                                 la        (if (seq fa) la -1)
                                 lb        (if (seq fb) lb -1)
                                 down-a?   (and (>= la 0) (>= la lb))
                                 down-b?   (and (>= lb 0) (>= lb la))
                                 [fa' ca'] (if down-a?
                                             (await (diff-descend storage fa la ca opts))
                                             [fa ca])
                                 [fb' cb'] (if down-b?
                                             (await (diff-descend storage fb lb cb opts))
                                             [fb cb])]
                             (recur fa' (if down-a? (dec la) la)
                                    fb' (if down-b? (dec lb) lb)
                                    ca' cb')))))))))))

(defn- walk-delta-descend
  "Materialize one frontier and return its children without retaining keys.
   `on-address` is nil on the old side and observational on the new side."
  [storage refs level on-address {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [n (count refs)
                     nodes
                     (loop [i 0 nodes []]
                       (if (< i n)
                         (let [[nd parent idx :as ref] (nth refs i)]
                           (if (some? nd)
                             (recur (inc i) (conj nodes ref))
                             (recur (inc i)
                                    (conj nodes
                                          (assoc ref 0
                                                 (await (branch/child parent storage idx opts)))))))
                         nodes))]
                 (when on-address
                   (doseq [[_ _ _ address] nodes]
                     (when (some? address)
                       (on-address address))))
                 (if (zero? level) [] (child-refs nodes))))))

(defn walk-delta
  "Restore and visit the stored nodes in `b` that cannot be pruned as subtrees
   shared with `a`.

   Uses the same paired-frontier and diff-buffer-aware pruning rules as `diff`,
   but does not retain or compare elements. `on-address` observes successfully
   materialized new-side addresses; its return value is ignored so callback return
   values cannot accidentally prune an incomplete hydration. Old-side restores
   are not reported.

   Returns nil when synchronous, or a continuation yielding nil under
   `{:sync? false}`."
  [root-of a b storage on-address {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [addr-a (.-address a)
                     addr-b (.-address b)]
                 (when-not (and (some? addr-a) (= addr-a addr-b))
                   (let [root-a (await (root-of a opts))
                         root-b (await (root-of b opts))]
                     (loop [fa [[root-a nil nil addr-a true]] la (node/level root-a)
                            fb [[root-b nil nil addr-b true]] lb (node/level root-b)]
                       (let [la (if (seq fa) la -1)
                             lb (if (seq fb) lb -1)]
                         (when-not (and (neg? la) (neg? lb))
                           (let [[fa fb] (if (== la lb) (prune fa fb) [fa fb])
                                 la      (if (seq fa) la -1)
                                 lb      (if (seq fb) lb -1)
                                 down-a? (and (>= la 0) (>= la lb))
                                 down-b? (and (>= lb 0) (>= lb la))
                                 fa'     (if down-a?
                                           (await (walk-delta-descend storage fa la nil opts))
                                           fa)
                                 fb'     (if down-b?
                                           (await (walk-delta-descend storage fb lb on-address opts))
                                           fb)]
                             (recur fa' (if down-a? (dec la) la)
                                    fb' (if down-b? (dec lb) lb))))))))))))
