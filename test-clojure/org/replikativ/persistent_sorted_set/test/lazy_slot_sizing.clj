(ns org.replikativ.persistent-sorted-set.test.lazy-slot-sizing
  "`store()` Pass 1 must RESOLVE a slot's entry count, not read the field raw.

   Pass 1 asks the same question twice, one line apart, and used two different
   ways to answer it:

       if (sl != null) passthrough += slotBE(sl);   // clean arm  — resolves
       ...
       csz[i] = (int) sl.bufEntries;                // dirty arm — raw

   A slot reconstructed from storage carries `Slot.LAZY` (-2) — that is what the
   4-arg `Slot` constructor is for, and `slotBE` exists to resolve it from the
   diff blob without IO. The guard above the dirty arm rejects `BUF_WRITE` (-1)
   but not `LAZY`, so a restored slot on a dirty child reached the raw read.

   -2 is not a slightly-wrong size, it is a NEGATIVE one:

     * `bufferable.sort` by `csz` puts it first,
     * the budget test `embedded + csz[i] <= budget` can no longer fail,
     * `embedded += csz[i]` moves the running total BACKWARDS,

   so the per-node budget stops bounding the node's blob, and -2 is written back
   into the slot as its settled size.

   Measured at budget 1 on a level-2 root whose slot 0 carries a real restored
   diff, with that child made dirty:

       before   child written? false   addresses[idx] == anchor? true   slot -2
       after    child written? true    addresses[idx] == anchor? false

   ## This is a latent inconsistency, not a live defect

   Zero hits over 38,813,363 `child(int,ANode)` calls and 3,899 projections
   across the diff-buf namespaces, because every path that dirties a child also
   re-deposits its slot with a computed size. The state has to be built directly,
   which is what this test does — and the reason to close it anyway is that two
   arms of the same loop must not disagree about how to read the same field."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set
            ANode Branch Slot IStorage Settings PersistentSortedSet RefType]))

(set! *warn-on-reflection* true)

(def ^:private BF 16)

(defn- seeded-disk
  "A disk holding a tree that really does carry diff-buf slots. Returns [disk addr]."
  []
  (let [st   (tstore/storage-with-settings
              (Settings. (int BF) RefType/STRONG nil nil (int 256)))
        opts {:comparator compare :branching-factor BF :ref-type :strong
              :diff-buf-size 256}
        s0   (s/from-sorted-array compare (object-array (range 0 3000 10)) 300
                                  (assoc opts :storage st))
        a0   (s/store s0 st)
        cold (s/restore-by compare a0 st opts)
        ;; the SECOND store is what deposits slots
        s1   (reduce #(s/conj %1 %2 compare) cold [5 15 25 35])]
    [(:*disk st) (s/store s1 st)]))

(deftest a-restored-slot-on-a-dirty-child-is-sized-by-its-diff
  (testing "with a budget of 1, a child carrying a real restored diff must be
            FLUSHED. Reading Slot.LAZY raw sized it as -2, which passes any
            budget test, so it was buffered instead and -2 became its recorded
            size."
    (let [[disk addr] (seeded-disk)
          budget   1
          ns1      (Settings. (int BF) RefType/STRONG nil nil (int budget))
          written  (atom [])
          ^IStorage inner (tstore/->Storage (atom {}) disk ns1)
          counting (reify IStorage
                     (store [_ node]
                       (let [a (.store inner node)]
                         (swap! written conj [(.level ^ANode node) a]) a))
                     (accessed [_ a] (.accessed inner a))
                     (restore [_ a] (.restore inner a))
                     (markFreed [_ _] nil)
                     (isFreed [_ _] false)
                     (freedInfo [_ _] nil))
          back     (s/restore-by compare addr counting
                                 {:comparator compare :branching-factor BF
                                  :ref-type :strong :diff-buf-size budget})
          ^Branch root (.root ^PersistentSortedSet back)
          slots    (aget ^objects (.addressesAndSlots root) 1)
          idx      (first (for [i (range (.len root))
                                :let [^Slot sl (when slots (aget ^objects slots i))]
                                :when (and sl (.-anchor sl) (.-diff sl))]
                            i))]
      (is (= 2 (.level ^ANode root))
          "precondition: a level-2 root, so its slots describe branch children")
      (is (some? idx)
          "precondition: some slot carries a restored diff with an anchor. Without
           one there is nothing to size and the assertions below are vacuous.")
      (let [^Slot sl (aget ^objects slots idx)]
        (is (= Slot/LAZY (.-bufEntries sl))
            "precondition: the restored slot really is LAZY — this is the value
             the raw read mistook for a size")
        ;; Build the node directly rather than calling `child(int,ANode)` on this
        ;; shared root — that writes in place and is owner-thread-only (it now
        ;; asserts `editable()`). Constructing the state is also more honest about
        ;; what is being tested: a dirty child whose restored LAZY slot survived.
        (let [len      (.len root)
              kid      (.child root ^IStorage counting (int idx))   ; resident, bare
              addrs    (object-array (seq (aget ^objects (.addressesAndSlots root) 0)))
              children (object-array len)
              slots2   (object-array (seq slots))
              _        (aset addrs idx nil)               ; dirty this index
              _        (aset children idx kid)            ; bare resident ANode
              ^Branch dirty (Branch. (int (.level ^ANode root)) (int len)
                                     (object-array (seq (.keys ^ANode root)))
                                     addrs children (long -1) nil compare ns1)
              _        (.installSlots dirty slots2 Branch/BUF_LAZY)
              _        (reset! written [])
              _        (.store dirty counting)
              addr-after (aget ^objects (aget ^objects (.addressesAndSlots dirty) 0) idx)
              child-lvl  (dec (.level ^ANode root))]
          (is (some #(= child-lvl (first %)) @written)
              (str "the child's diff exceeds the budget of " budget
                   ", so it must be flushed — a raw -2 buffers it instead"))
          (is (not= addr-after (.-anchor sl))
              "and its address must be the freshly written one, not the anchor it
               would be re-pointed to if it had been buffered"))))))
