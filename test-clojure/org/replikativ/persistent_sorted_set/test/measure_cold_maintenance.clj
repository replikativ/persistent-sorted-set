(ns org.replikativ.persistent-sorted-set.test.measure-cold-maintenance
  "What a WRITE costs the next reader of `:measure`, on a tree that is still mostly on disk.

   The measure twin of `durable-count-read-side`, and the same argument: the counts and the
   measures both stay exact whatever happens here, so every contents assertion in the suite
   passes either way, and the bill lands entirely on a later reader.

   ## The asymmetry this closes

   `_subtreeCount` is DELTA-maintained — `Branch.add` does `_subtreeCount += 1` and
   `Branch.remove` does `-= 1`, with no reference to the siblings. `_measure` was not: every
   mutation arm recomputed it with `tryComputeMeasure`, which folds the measures of ALL `_len`
   children and returns `null` the moment one of them is not resident in memory.

   On a lazily restored tree that is the normal case — only the descent path is in memory — so
   a single `conj` erased the cached measure at every level from the touched leaf's parent up
   to the root. The value was never WRONG, it was absent; and the next reader restored it with
   `forceComputeMeasure`, which descends through `child(storage, i)` and pulls back every child
   of every erased branch.

   Measured before the fix with an exact long measure, bf 64, 200 000 elements, `:ref-type
   :weak`, against a storage that counts blob reads — five `conj` that read 4 blobs:

       cold `measure`                 1 blob   (the root's own, restored alongside it)
       5 conj                         4 blobs  root _measure -> NIL
       the `measure` right after     79 blobs  = 39 branches and 40 LEAVES

   and it did not amortise. Under `:weak` the pulled children are dropped again, so the next
   round paid the same 79: 399 blob reads over five write-then-read rounds on a 6451-blob tree.
   At bf 512 the same shape pulled 194 of 196 blobs — the whole tree — because the cost is
   `fanout x depth`, not a function of how much was written.

   Why that is worse than it sounds: the 40 are LEAVES, and a consumer whose measure is worth
   configuring is usually one whose leaves are fat. stratum carries a column chunk inline per
   leaf entry, ~4MB a leaf, so a `stats` query read hundreds of MB to produce numbers that were
   already cached in the branches it walked past.

   ## What is asserted

   Not blob counts as a performance number — the reader's BILL, the same thing
   `durable-count-read-side` asserts: a write must not erase an aggregate the tree already
   held, and reading it back must not restore the subtree.

   `remove` is deliberately NOT covered by the fix, and the last deftest records that as a
   decision rather than an oversight — see its docstring."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
  (:import [org.replikativ.persistent_sorted_set
            IMeasure IStorage ANode Branch Leaf PersistentSortedSet]))

(set! *warn-on-reflection* true)

;; An EXACT measure: longs only, so a disagreement here is a tree defect and never the
;; floating-point drift that 641a109 documents (and that made `validate-full`'s measure check
;; opt-in). `weight` is present so the same ops can drive `get-nth`.
(deftype SumOps []
  IMeasure
  (identity [_] {:n 0 :sum 0})
  (extract  [_ k] {:n 1 :sum (long k)})
  (merge    [_ a b] {:n   (+ (long (:n a)) (long (:n b)))
                     :sum (+ (long (:sum a)) (long (:sum b)))})
  (remove   [_ cur k _] {:n (dec (long (:n cur))) :sum (- (long (:sum cur)) (long k))})
  (weight   [_ m] (long (:n m))))

(def ^:private ops (SumOps.))

;; PERSISTS the measure, via the shipped content projection — the same one every codec module
;; goes through. Against a storage that drops `:measure` every restored node carries nil
;; regardless and this namespace would measure nothing, exactly as `durable-count-read-side`
;; needs a storage that persists `:subtree-count`.
(def ^:private ctx (nodes/reader-context {:measure-ops ops :default-bf 64}))

(defrecord MeasureStorage [*disk *reads *leaf-reads]
  IStorage
  (store [_ node]
    (let [addr (str (java.util.UUID/randomUUID))]
      (swap! *disk assoc addr (nodes/node->blob node))
      addr))
  (accessed [_ _addr] nil)
  (restore [_ addr]
    (swap! *reads inc)
    (let [blob (get @*disk addr)]
      (if (:addresses blob)
        (nodes/blob->branch ctx blob)
        (do (swap! *leaf-reads inc) (nodes/blob->leaf ctx blob)))))
  (markFreed [_ _addr] nil)
  (isFreed [_ _addr] false)
  (freedInfo [_ _addr] nil))

(defn- opts [bf ref-type]
  {:comparator compare :branching-factor bf :measure ops :ref-type ref-type})

(defn- truth [s]
  (let [ks (map long (seq s))]
    {:n (count ks) :sum (reduce + 0 ks)}))

(defn- stored
  "Build, store, and hand back a COLD set over a fresh storage — nothing in memory but the
   root, which is what makes `tryComputeMeasure`'s all-or-nothing answer reachable at all."
  [bf n ref-type]
  (let [o    (opts bf ref-type)
        st0  (->MeasureStorage (atom {}) (atom 0) (atom 0))
        s0   (reduce #(set/conj %1 %2 compare) (set/sorted-set* o) (range n))
        addr (set/store s0 st0)
        st   (->MeasureStorage (atom @(:*disk st0)) (atom 0) (atom 0))]
    {:storage st
     :blobs   (count @(:*disk st0))
     :set     (set/restore-by compare addr st (assoc o :storage st))}))

;; Keys above the built range, so they land in the last leaf and no interior node splits.
;; A split is the one arm that has no insert delta (a split PARTITIONS a measure, and the
;; monoid has no operation that divides one), so mixing splits in would test two things at once
;; — `every-arm-keeps-the-aggregate` below covers the scattered case where splits do happen.
(defn- appended [n cnt] (range (+ n 1000) (+ n 1000 cnt)))

(deftest a-write-does-not-erase-the-aggregate
  (testing "the root still KNOWS its measure after a conj into a cold tree — before the fix
            `tryComputeMeasure` returned nil for every branch on the path and the aggregate
            the tree had just restored from disk was thrown away"
    (doseq [bf [8 64 512], ref-type [:strong :weak]]
      (let [{:keys [set]} (stored bf 5000 ref-type)
            s' (reduce #(set/conj %1 %2 compare) set (appended 5000 5))
            lbl (str "bf=" bf " ref-type=" ref-type)]
        (is (some? (.-_measure ^ANode (.root ^PersistentSortedSet s')))
            (str lbl ": root measure erased by the write"))
        (is (= (truth s') (set/measure s'))
            (str lbl ": measure disagrees with the set's contents"))))))

(deftest reading-the-measure-after-a-write-does-not-restore-the-tree
  (testing "the reader's bill. Before the fix this cost fanout x depth blob reads, most of
            them LEAVES, and under :weak it repeated at every round instead of amortising"
    (doseq [bf [8 64 512]]
      (let [{:keys [set storage blobs]} (stored bf 5000 :weak)
            s' (reduce #(set/conj %1 %2 compare) set (appended 5000 5))
            _  (reset! (:*reads storage) 0)
            _  (reset! (:*leaf-reads storage) 0)
            m  (set/measure s')
            ;; SNAPSHOT the counters here. `truth` seqs the whole set, which restores the
            ;; whole tree — asserting the answer first made every read assertion below count
            ;; the oracle's own walk (bf 8: 1248 leaves) instead of the measure's.
            leaf-reads @(:*leaf-reads storage)
            reads      @(:*reads storage)
            lbl (str "bf=" bf " (" blobs " blobs)")]
        (is (zero? leaf-reads)
            (str lbl ": reading the measure restored " leaf-reads " leaves"))
        (is (zero? reads)
            (str lbl ": reading the measure restored " reads " blobs"))
        (is (= (truth s') m) (str lbl ": measure wrong"))))))

(deftest repeated-write-then-read-rounds-stay-flat
  (testing ":weak drops whatever forceComputeMeasure pulled, so before the fix every round
            paid the full bill again — 399 blob reads over five rounds where the writes
            themselves read 4. The measure must be maintained, not rebuilt."
    (let [{:keys [set storage]} (stored 64 20000 :weak)]
      (loop [r 0, s set]
        (when (< r 5)
          (let [s' (reduce #(set/conj %1 %2 compare) s (appended (+ 20000 (* r 100)) 5))
                before @(:*reads storage)
                m (set/measure s')
                after @(:*reads storage)]           ; before `truth`, which walks everything
            (is (= before after)
                (str "round " r ": reading the measure cost " (- after before) " blob reads"))
            (is (= (truth s') m) (str "round " r ": measure wrong"))
            (recur (inc r) s')))))))

(deftest every-arm-keeps-the-aggregate
  (testing "scattered inserts, so the absorb and split arms run too, on both the editable
            (transient) and the persistent return paths. A split has no delta and may still
            leave one node measureless — the ANSWER must be right regardless, which is what
            forceComputeMeasure is for."
    (doseq [bf [8 64], transient? [false true]]
      (let [{:keys [set]} (stored bf 5000 :weak)
            ks (map #(+ 100000 (* 37 %)) (range 200))
            s' (if transient?
                 (persistent! (reduce conj! (clojure.core/transient set) ks))
                 (reduce #(set/conj %1 %2 compare) set ks))
            lbl (str "bf=" bf (if transient? " transient" " persistent"))]
        (is (= (truth s') (set/measure s')) (str lbl ": measure disagrees with contents"))))))

(deftest remove-does-not-take-the-delta
  (testing "SCOPE, recorded as a decision. `Branch.remove` still recomputes-or-nulls, so a
            disj on a cold tree can erase the aggregate and the next read still forces. Two
            reasons, both about correctness rather than effort: the delta would have to
            subtract the element the LEAF actually removed rather than the caller's search key
            (`removedOut` is threaded only at level 1, and only under diff-buf), and
            `IMeasure/remove` is invertible only for measures that are — a min/max measure has
            to consult the remaining children, which is the very IO the insert delta avoids.
            What must hold either way is the ANSWER."
    (doseq [bf [8 64]]
      (let [{:keys [set]} (stored bf 5000 :weak)
            s' (reduce #(set/disj %1 %2 compare) set (range 0 5000 97))]
        (is (= (truth s') (set/measure s'))
            (str "bf=" bf ": measure disagrees with contents after disj"))))))
