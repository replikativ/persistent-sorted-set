(ns org.replikativ.persistent-sorted-set.test.proj-cmp-sharing
  "One set's comparator must not decide another set's leaf order.

   `_projCmp` is the comparator `projectLeaf` rebuilds a buffered leaf's key array with. It
   lives as a plain FIELD on the node, stamped by `PersistentSortedSet.root()` and by
   `Branch.child()` — and it was stamped UNCONDITIONALLY, on the object the `IStorage`
   returned. A caching storage (datahike's `CachedStorage`; the in-tree test storage) hands
   the same object to every set opened at that address, so two sets whose comparators order
   ties differently overwrote each other's stamp, and whichever ran last won.

   Measured before the fix, two sets over one storage reading interleaved, bf 8 / diff-buf
   64 / n 60:

       root object shared   true
       count 129            (correct — every count-based oracle passes)
       seq sorted under A's comparator   FALSE, first disorder at index 18
       contains? false for [8 1] [9 0] [14 1] [14 2] [15 0] — all present in seq

   `count` and `seq` AGREE, which is why nothing in the suite noticed.

   And it does not stay in memory. One `conj` into the mis-sorted cached leaf, one `store`,
   and a cold reload through a fresh cache leaves an element PERMANENTLY unfindable: present
   in `seq`, `contains?` false, because the branch separator no longer bounds it. Same shape
   as the `separatorMoved` class of defect, reached through a different door.

   ## How a caller gets here without doing anything exotic

   `restore` hard-codes `RT/DEFAULT_COMPARATOR` (`persistent_sorted_set.clj`), so calling
   `restore` where `restore-by` was meant, on a store built with `sorted-set-by`, is enough —
   as is opening the same root twice with two different comparators from a tooling path.
   Neither docstring warned.

   ## Why copy rather than refuse

   Refusing on a differing stamp would be cheaper, but two SEMANTICALLY IDENTICAL comparators
   are routinely distinct objects — any caller building `(fn [a b] ...)` per restore — and
   those order the leaf identically. Throwing there would break correct code. Copying is
   silent and right in both cases; it costs nothing on the single-comparator path, which is
   every normal use."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set Settings PersistentSortedSet Branch]))

(set! *warn-on-reflection* true)

;; Same key order, ties REVERSED — so the two disagree only where a leaf holds several
;; elements sharing a first component, which is exactly what projectLeaf reorders.
(defn- cmp1 [[k1 v1] [k2 v2]]
  (let [c (compare k1 k2)] (if-not (zero? c) c (compare v1 v2))))
(defn- cmp2 [[k1 v1] [k2 v2]]
  (let [c (compare k1 k2)] (if-not (zero? c) c (compare v2 v1))))

(defn- sorted-under? [cmp xs]
  (every? neg? (map (fn [[x y]] (cmp x y)) (partition 2 1 xs))))

(defn- scenario
  "Build a diff-buf tree whose leaf-parents carry leaf diffs, open it under BOTH comparators
   over ONE caching storage, and interleave the reads so B's root() lands between two steps
   of A's lazy seq."
  [{:keys [bf dbs n]}]
  (let [st   (ts/storage-with-settings (Settings. (int bf) nil nil nil (int dbs)))
        opts {:comparator cmp1 :branching-factor bf :diff-buf-size dbs
              :ref-type :strong :storage st}
        base (into (ss/sorted-set* opts) (mapcat (fn [k] [[k 0] [k 1]]) (range n)))
        _    (ss/store base st)
        ;; second generation: content-only inserts, so leaf-parents buffer leaf diffs
        s1   (persistent! (reduce (fn [t k] (conj! t [k 2])) (transient base) (range 0 n 7)))
        addr (ss/store s1 st)
        truth (vec (seq s1))
        A    (ss/restore-by cmp1 addr st opts)
        B    (ss/restore-by cmp2 addr st (assoc opts :comparator cmp2))
        sA   (seq A)
        _    (first sA)                        ; A materialises its first leaf under cmp1
        rootA (.root ^PersistentSortedSet A)
        rootB (.root ^PersistentSortedSet B)   ; B's root() must not re-stamp A's node
        ;; Read the stamp HERE, immediately after B's read. Every later `A` operation calls
        ;; A.root() again, which re-seeds the field to cmp1 and REPAIRS the damage — so
        ;; capturing it in the returned map (evaluated after the `store` below) observed the
        ;; repaired value and passed against the unfixed build. An adversarial review caught
        ;; that; the binding order is the assertion here.
        proj-after-B (.-_projCmp ^Branch rootA)
        got  (vec sA)                          ; A finishes its walk
        ;; durability: mutate A and store, then read back through a FRESH cache
        dk   (or (ffirst (keep (fn [[x y]] (when (pos? (cmp1 x y)) [x y]))
                               (partition 2 1 got)))
                 [-1 0])
        addr2 (ss/store (ss/conj A [(first dk) 9] cmp1) st)
        st2  (assoc st :*memory (atom {}))
        back (ss/restore-by cmp1 addr2 st2 (assoc opts :comparator cmp1 :storage st2))
        bseq (vec (seq back))]
    {:root-shared    (identical? rootA rootB)
     :proj-cmp-of-A  proj-after-B
     :seq            got
     :truth          truth
     :count-A        (count A)
     :unfindable     (into [] (comp (remove #(contains? A %)) (take 5)) truth)
     :reloaded       bseq
     :reloaded-unfindable (into [] (comp (remove #(contains? back %)) (take 5)) truth)}))

(def ^:private configs [{:bf 8 :dbs 64 :n 60} {:bf 8 :dbs 256 :n 200} {:bf 16 :dbs 128 :n 300}])

(deftest a-second-set-does-not-resteal-the-first-sets-projection-comparator
  (testing "reading through B must leave A projecting under A's own comparator"
    (doseq [{:keys [bf dbs n] :as cfg} configs]
      (let [{:keys [root-shared proj-cmp-of-A]} (scenario cfg)
            label (str "bf=" bf " dbs=" dbs " n=" n)]
        (is (false? root-shared)
            (str label ": the two sets must not share one root object, since a node carries "
                 "a single projection comparator"))
        (is (identical? cmp1 proj-cmp-of-A)
            (str label ": A's root must still project under cmp1"))))))

(deftest the-first-sets-contents-stay-correct-and-findable
  (testing "count agreeing is not enough — the ORDER and the lookups have to hold too"
    (doseq [{:keys [bf dbs n] :as cfg} configs]
      (let [{:keys [seq truth count-A unfindable]} (scenario cfg)
            label (str "bf=" bf " dbs=" dbs " n=" n)]
        (is (= (clojure.core/count truth) count-A) (str label ": count"))
        (is (sorted-under? cmp1 seq) (str label ": A's seq must be sorted under cmp1"))
        (is (= truth seq) (str label ": A's seq must be exactly what was stored"))
        (is (= [] unfindable)
            (str label ": every stored element must be findable — these were in seq but "
                 "contains? answered false: " unfindable))))))

(deftest and-nothing-mis-ordered-reaches-the-disk
  (testing "the part that made this more than an in-memory nuisance: a dirty leaf is written
            wholesale, so a mis-ordering survives store and a cold reload through a fresh
            cache — one element per run was permanently unfindable"
    (doseq [{:keys [bf dbs n] :as cfg} configs]
      (let [{:keys [reloaded reloaded-unfindable]} (scenario cfg)
            label (str "bf=" bf " dbs=" dbs " n=" n)]
        (is (sorted-under? cmp1 reloaded) (str label ": the reloaded seq must be sorted"))
        (is (= [] reloaded-unfindable)
            (str label ": unfindable after a cold reload: " reloaded-unfindable))))))
