(ns org.replikativ.persistent-sorted-set.test.diff-buf-portable
  "Cross-platform diff-buf invariant regression guards (the 'C' items). Each invariant holds in
   the current implementation; these pin it so a future change can't silently break it. Where an
   assertion needs node/storage internals, a thin per-platform shim bridges JVM Branch/Slot and
   the cljs Branch — the TEST LOGIC is shared. See doc/diff-buffering.md.

   C2 separator/max-key repair · C3 budget Σ≤B + biggest-first eviction · C4 I0 (b=0 ⇒ no slots)
   C5 structural identity (writer == reconstructed) · C6 forward-compat (read slot-less old data)."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [org.replikativ.persistent-sorted-set :as set]
            #?(:clj  [org.replikativ.persistent-sorted-set.test.storage :as tstore])
            #?(:cljs [org.replikativ.persistent-sorted-set.test.storage.util :as util])
            #?(:cljs [org.replikativ.persistent-sorted-set.branch :refer [Branch] :as branch]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set Settings Branch ANode PersistentSortedSet]
                   [java.lang.ref Reference])))

;; elements are [k v]; the set order keys on the FIRST element only, so [k 0] and [k 7] are
;; cmp-equal (a content-only replace) yet order-distinct from neighbours.
(def cmp (fn [a b] (compare (first a) (first b))))

;; ---- per-platform shims (the only platform-specific code) -------------------------------------
#?(:clj
   (do
     (defn make-storage [disk bf b] (tstore/->Storage (atom {}) disk (Settings. (int bf) nil nil nil (int b))))
     (defn restore* [addr storage opts] (set/restore-by cmp addr storage opts))
     (defn- deref-c [c] (cond (instance? ANode c) c (instance? Reference c) (.get ^Reference c) :else c))
     (defn- kids [^Branch b] (when-let [ch (.-_children b)] (->> (range (.-_len b)) (map #(deref-c (aget ^objects ch %))) (remove nil?))))
     (defn- root-of [s] (.root ^PersistentSortedSet s))
     (defn- branch? [n] (instance? Branch n))
     (defn- blevel [b] (.-_level ^Branch b))
     (defn- blen   [b] (.-_len ^Branch b))
     (defn- slots-of [b] (.slotsForStorage ^Branch b))))
#?(:cljs
   (do
     (defn make-storage [disk bf b] (util/storage (atom {}) disk {:branching-factor bf :diff-buf-size b :comparator cmp}))
     (defn restore* [addr storage opts] (set/restore addr storage opts))
     (defn- kids [b] (remove nil? (array-seq (.-children b))))
     (defn- root-of [s] (.-root s))
     (defn- branch? [n] (instance? Branch n))
     (defn- blevel [b] (.-level b))
     (defn- blen   [b] (alength (.-keys b)))
     (defn- slots-of [b] (branch/slots-for-storage b))))

;; ---- shared helpers ---------------------------------------------------------------------------
(defn- all-branches [s]
  (dorun (seq s))                                          ; materialize the lazy tree before walking
  ((fn w [n] (when (branch? n) (cons n (mapcat w (kids n))))) (root-of s)))

(defn- tree-shape [s]                                      ; multiset of (level,fanout) over all branches
  (frequencies (map (fn [b] [(blevel b) (blen b)]) (all-branches s))))

;; entry count in a serialized slot's diff. childLevel = level of the node the diff belongs to:
;; 0 ⇒ leaf-diff {:absent :present}; >=1 ⇒ branch-diff {idx -> {:diff …}} summed at childLevel-1.
(defn- nentries [diff child-level]
  (if-not (map? diff) 0
    (if (zero? child-level)
      (+ (count (:absent diff)) (count (:present diff)))
      (reduce + 0 (map (fn [e] (nentries (:diff (val e)) (dec child-level))) diff)))))

(defn- blob-embedded [blob-str]                            ; buffered entries embedded in a written branch blob
  (let [{:keys [level slots]} (edn/read-string blob-str)]
    (reduce + 0 (map (fn [e] (nentries (:diff (val e)) (dec level))) (or slots {})))))

(def opts {:branching-factor 8 :diff-buf-size 256 :comparator cmp})
(defn- build [n o] (reduce (fn [s i] (conj s [i 0])) (set/sorted-set* o) (range n)))

;; ---- C2: separator / max-key repair -----------------------------------------------------------
(deftest separator-repair
  (testing "deleting elements that are child max-keys (incl. the global max) repairs separators ⇒
            no phantom routes after a cold restore"
    (let [bf 4 b 256 disk (atom {})
          o {:branching-factor bf :diff-buf-size b :comparator cmp}
          l1 (restore* (set/store (build 60 o) (make-storage disk bf b)) (make-storage disk bf b) o)
          ;; victims include the global max (59) and a spread that hits interior leaf boundaries
          victims (conj (set (range 5 60 7)) 59)
          s1 (reduce (fn [s k] (disj s [k 0])) l1 victims)
          l2 (restore* (set/store s1 (make-storage disk bf b)) (make-storage disk bf b) o)
          model (apply sorted-set-by cmp (map #(vector % 0) (remove victims (range 60))))]
      (is (= (vec model) (vec (seq l2))) "content exact after deleting max-keys + cold restore")
      ;; full routing scan: every key resolves like the model (a phantom separator would mis-route)
      (is (every? #(= (contains? model [% 0]) (contains? l2 [% 0])) (range -1 62)) "no phantom routes")
      (is (= [58 0] (last (seq l2))) "global max correctly shrank (59 deleted ⇒ 58)"))))

;; ---- C3: budget Σ≤B strictly enforced + biggest-first eviction ---------------------------------
(deftest budget-and-biggest-first
  (testing "every written branch embeds ≤ B entries, and when two children overflow the budget the
            LARGER diff is flushed while the smaller stays buffered (biggest-first)"
    (let [bf 8 b 6 disk (atom {})
          o {:branching-factor bf :diff-buf-size b :comparator cmp}
          l1 (restore* (set/store (build 200 o) (make-storage disk bf b)) (make-storage disk bf b) o)
          _  (dorun (seq l1))
          ;; content-only replaces (cmp-equal ⇒ buffered Present): 20 in a low subtree, 3 in a mid one
          s1 (as-> l1 s (reduce (fn [s i] (set/replace s [i 0] [i 1])) s (range 0 20))
                        (reduce (fn [s i] (set/replace s [i 0] [i 1])) s (range 100 103)))
          addr (set/store s1 (make-storage disk bf b))]
      ;; budget bound on EVERY written branch blob
      (is (every? #(<= (blob-embedded %) b) (filter #(re-find #":slots" %) (vals @disk)))
          "every written branch satisfies Σ embedded ≤ B")
      ;; biggest-first: the root embeds the SMALL (3-entry) subtree; the 20-entry one was flushed
      (is (= 3 (blob-embedded (get @disk addr)))
          "root buffered the small diff and flushed the large one (biggest-first, not naive fill)"))))

;; ---- C4: I0 — diff-buf-size 0 buffers nothing --------------------------------------------------
(deftest i0-no-slots-at-zero
  (testing "at diff-buf-size 0 no node carries slots (byte-identical-to-baseline path)"
    (let [bf 8 disk (atom {})
          o {:branching-factor bf :diff-buf-size 0 :comparator cmp}
          s0 (build 100 o)
          l1 (restore* (set/store s0 (make-storage disk bf 0)) (make-storage disk bf 0) o)
          _  (dorun (seq l1))
          s1 (reduce (fn [s i] (set/replace s [i 0] [i 1])) l1 (range 0 10))]  ; would buffer if b>0
      (set/store s1 (make-storage disk bf 0))
      (is (every? #(nil? (slots-of %)) (all-branches s1)) "b=0 ⇒ slotsForStorage nil on every branch")
      (is (= (vec (map #(vector % (if (< % 10) 1 0)) (range 100))) (vec (seq s1))) "content correct"))))

;; ---- C5: structural identity (writer tree == reconstructed tree, not merely content-equal) -----
(deftest structural-identity
  (testing "after a rebalancing (split+merge) commit, the cold-restored tree is structurally
            identical to the writer (depth + per-branch fanout), not just content-equal"
    (let [bf 4 b 64 disk (atom {})
          o {:branching-factor bf :diff-buf-size b :comparator cmp}
          l1 (restore* (set/store (build 60 o) (make-storage disk bf b)) (make-storage disk bf b) o)
          s1 (as-> l1 s (reduce (fn [s i] (conj s [i 0])) s (range 60 90))   ; splits
                        (reduce (fn [s i] (disj s [i 0])) s (range 10 30)))      ; merges
          l2 (restore* (set/store s1 (make-storage disk bf b)) (make-storage disk bf b) o)]
      (is (= (vec (seq s1)) (vec (seq l2))) "content exact")
      (is (= (tree-shape s1) (tree-shape l2)) "writer and reconstructed trees are structurally identical"))))

;; ---- C6: forward-compat — read slot-less (baseline) data with diff-buf ON ----------------------
(deftest forward-compat-slotless
  (testing "data written at diff-buf-size 0 (no :slots) reads exactly under diff-buf ON and then
            mutates + round-trips cleanly into the slot-bearing format"
    (let [bf 8 disk (atom {})
          o0 {:branching-factor bf :diff-buf-size 0 :comparator cmp}
          o256 {:branching-factor bf :diff-buf-size 256 :comparator cmp}
          addr (set/store (build 100 o0) (make-storage disk bf 0))            ; old format
          l1   (restore* addr (make-storage disk bf 256) o256)               ; read with diff-buf ON
          s1   (reduce (fn [s i] (set/replace s [i 0] [i 7])) l1 (range 0 10))
          l2   (restore* (set/store s1 (make-storage disk bf 256)) (make-storage disk bf 256) o256)
          model (apply sorted-set-by cmp (map #(vector % (if (< % 10) 7 0)) (range 100)))]
      (is (= (vec (map #(vector % 0) (range 100))) (vec (seq l1))) "slot-less old data reads exactly under diff-buf ON")
      (is (= (vec model) (vec (seq l2))) "mutate + store + restore transitions cleanly to slot-bearing"))))
