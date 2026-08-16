(ns org.replikativ.persistent-sorted-set.test.measure-cold-maintenance
  "What a WRITE costs the next reader of `:measure`, on a tree that is still mostly on disk.

   The measure twin of `durable-count-read-side`, and the same argument: the counts and the
   measures both stay exact whatever happens here, so every contents assertion in the suite
   passes either way, and the bill lands entirely on a later reader.

   ## The asymmetry this closes

   `_subtreeCount` is DELTA-maintained, with no reference to the siblings — `+= 1` at the
   EARLY_EXIT arm, and `old - oldChildCount + newChildrenCount` at the others, MEASURED from
   the nodes the child actually returned. `_measure` was not: every mutation arm recomputed it
   with `tryComputeMeasure`, which folds the measures of ALL `_len` children and returns `null`
   the moment one of them is not resident in memory.

   The two deltas are not peers, and the difference is why the insert delta needs a guard the
   count delta does not: a returned child reports its count, so the count delta is measured,
   while nothing reports a child's measure, so the measure delta must ASSUME one element was
   inserted. `Branch.measureAfterInsert` refuses whenever that assumption is not safe.

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

;; An ORDER-SENSITIVE measure: the elements themselves, concatenated. `merge` is associative
;; but not commutative, so it declines the insert delta and the tree must postpone instead.
(deftype ConcatOps []
  IMeasure
  (identity [_] [])
  (extract  [_ k] [k])
  (merge    [_ a b] (into a b))
  (remove   [_ _cur _k recompute] (when recompute (.get ^java.util.function.Supplier recompute)))
  (commutativeMerge [_] false))

(def ^:private concat-ops (ConcatOps.))

;; PERSISTS the measure, via the shipped content projection — the same one every codec module
;; goes through. Against a storage that drops `:measure` every restored node carries nil
;; regardless and this namespace would measure nothing, exactly as `durable-count-read-side`
;; needs a storage that persists `:subtree-count`.
;; The reader-context carries the measure-ops that a RESTORED node's Settings will hold, so it
;; must be the same ops the set was built with — a restored branch whose settings say SumOps
;; while its blob carries a ConcatOps value NPEs on the first delta. Hence a field, not a
;; module-level constant.
(def ^:private ctx      (nodes/reader-context {:measure-ops ops         :default-bf 64}))
(def ^:private concat-ctx (nodes/reader-context {:measure-ops concat-ops :default-bf 64}))

(defrecord MeasureStorage [*disk *reads *leaf-reads ctx]
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
        st0  (->MeasureStorage (atom {}) (atom 0) (atom 0) ctx)
        s0   (reduce #(set/conj %1 %2 compare) (set/sorted-set* o) (range n))
        addr (set/store s0 st0)
        st   (->MeasureStorage (atom @(:*disk st0)) (atom 0) (atom 0) ctx)]
    {:storage st
     :blobs   (count @(:*disk st0))
     :set     (set/restore-by compare addr st (assoc o :storage st))}))

;; Keys above the built range, so they land in the rightmost leaf.
;;
;; This does NOT mean "no splits", which an earlier version of this comment claimed. Measured
;; at bf 8, these five conj take the leaf count 2075 -> 2077: two LEAF splits, absorbed by the
;; parent, so the arm exercised there is `absorb` rather than the same-len arm. At bf 64 and
;; 512 the leaf count is unchanged and it really is the same-len arm. No BRANCH split occurs at
;; any of the three, which is what matters for these two deftests: a branch split is the one
;; arm with no insert delta, and it would leave a measureless node and muddle the assertion.
;; `scattered` below is the one that deliberately mixes all the arms.
(defn- appended [n cnt] (range (+ n 1000) (+ n 1000 cnt)))

;; INTERLEAVED into the built range: `(range n)` is dense, so 2k+1 for k < n/2 falls strictly
;; between two existing elements. That routes inserts to interior leaves all over the tree,
;; which is what reaches the branch EARLY_EXIT arm (`appended` never does — appending changes
;; the rightmost separator, so the child returns a node rather than EARLY_EXIT).
(defn- scattered [n cnt]
  (let [step (max 1 (quot (quot n 2) cnt))]
    (map #(inc (* 2 (* step %))) (range 1 (inc cnt)))))

;; Every node's cached measure must equal a recomputation from its own content. The root-level
;; answer alone is not enough: an ancestor's delta keeps the root non-nil and correct even when
;; a descendant is measureless or wrong, which is precisely the shape a bad delta would take.
;; Returns {:wrong [...] :measureless [...]}; walking materialises the tree, so callers must
;; snapshot any read counters BEFORE calling this.
(defn- audit [^PersistentSortedSet s]
  (let [storage (.-_storage s)
        acc     (volatile! {:wrong [] :measureless []})]
    (letfn [(walk [^ANode node]
              (let [t (if (instance? Leaf node)
                        (let [ks (map long (take (.-_len ^Leaf node) (.-_keys ^Leaf node)))]
                          {:n (count ks) :sum (reduce + 0 ks)})
                        (let [^Branch b node]
                          (reduce (fn [t i]
                                    (let [ct (walk (.child b ^IStorage storage (int i)))]
                                      {:n (+ (:n t) (:n ct)) :sum (+ (:sum t) (:sum ct))}))
                                  {:n 0 :sum 0}
                                  (range (.-_len b)))))
                    cached (.-_measure node)]
                (cond
                  (nil? cached)      (vswap! acc update :measureless conj (.level node))
                  (not= cached t)    (vswap! acc update :wrong conj [(.level node) cached t]))
                t))]
      (walk (.root s)))
    @acc))

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
            themselves read 4. The measure must be maintained, not rebuilt.

            NOTHING here may seq the set. An earlier version called `truth` inside the loop to
            check the answer, and that walk made the whole tree resident IN PROCESS — so
            against the unfixed build round 0 cost 65 reads and rounds 1-4 cost 0, and the
            deftest demonstrated flatness that the fix had nothing to do with. The expected
            measure is therefore computed ARITHMETICALLY from the keys, and the contents are
            checked once at the end, after the last measurement."
    (let [n 20000
          {:keys [set storage]} (stored 64 n :weak)]
      (loop [r 0, s set, expect {:n n :sum (reduce + 0 (range n))}]
        (if (>= r 5)
          (is (= (truth s) (set/measure s))
              "final contents check, after every read count has been taken")
          (let [ks (appended (+ n (* r 100)) 5)
                s' (reduce #(set/conj %1 %2 compare) s ks)
                expect' {:n (+ (:n expect) (count ks)) :sum (+ (:sum expect) (reduce + 0 ks))}
                before @(:*reads storage)
                m (set/measure s')
                after @(:*reads storage)]
            (is (= before after)
                (str "round " r ": reading the measure cost " (- after before) " blob reads"))
            (is (= expect' m) (str "round " r ": measure wrong"))
            (recur (inc r) s' expect')))))))

(deftest every-arm-keeps-the-aggregate
  (testing "inserts INTERLEAVED into the existing range, so the interior arms run — including
            branch EARLY_EXIT, which `appended` never reaches — on both the editable
            (transient) and the persistent return paths.

            Asserted PER NODE, not just at the root. The root answer alone passes against the
            unfixed build too (forceComputeMeasure repairs it) and would also pass with a
            delta that is wrong deeper down, because an ancestor's own delta keeps the root
            right. A wrong cached measure anywhere is the failure this deftest exists for.

            A branch split has no delta and may legitimately leave a node measureless, so
            `:measureless` is reported but not failed — `remove-does-not-take-the-delta` and
            the split-arm comment in Branch.java cover why. `:wrong` is never acceptable."
    (doseq [bf [8 64], transient? [false true]]
      (let [{:keys [set]} (stored bf 5000 :weak)
            ks (scattered 5000 200)
            s' (if transient?
                 (persistent! (reduce conj! (clojure.core/transient set) ks))
                 (reduce #(set/conj %1 %2 compare) set ks))
            lbl (str "bf=" bf (if transient? " transient" " persistent"))
            a  (audit s')]
        (is (empty? (:wrong a))
            (str lbl ": " (count (:wrong a)) " node(s) cache a measure that disagrees with "
                 "their own subtree, first " (first (:wrong a))))
        (is (= (truth s') (set/measure s')) (str lbl ": measure disagrees with contents"))))))

(deftest an-order-sensitive-measure-can-decline-the-delta
  (testing "IMeasure/commutativeMerge. The delta folds the inserted key in at the END of the
            node's measure rather than at its sorted position, which is exact for a commutative
            merge and WRONG for an order-sensitive one. Measured against a concatenating
            measure before the opt-out existed: a cold-restored set of (range 0 400 2) with
            37 39 41 43 45 conj'd ended with three nodes holding their elements in insertion
            order. Declining restores the postpone-and-force behaviour, which is exact."
    (let [o    {:comparator compare :branching-factor 8 :measure concat-ops :ref-type :weak}
          st0  (->MeasureStorage (atom {}) (atom 0) (atom 0) concat-ctx)
          s0   (reduce #(set/conj %1 %2 compare) (set/sorted-set* o) (range 0 400 2))
          addr (set/store s0 st0)
          st   (->MeasureStorage (atom @(:*disk st0)) (atom 0) (atom 0) concat-ctx)
          s    (set/restore-by compare addr st (assoc o :storage st))
          s'   (reduce #(set/conj %1 %2 compare) s [37 39 41 43 45])]
      (is (= (vec (seq s')) (vec (set/measure s')))
          "a declining measure must read back in KEY order, not insertion order"))))

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
