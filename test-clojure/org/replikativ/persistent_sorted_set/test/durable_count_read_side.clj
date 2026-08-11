(ns org.replikativ.persistent-sorted-set.test.durable-count-read-side
  "What a storage that PERSISTS `:subtree-count` gets back when it reads.

   `impl.nodes/node->map` serializes `subtreeCount()` raw, so whatever a mutation leaves on a
   node goes to DISK. 04499a0 stopped the in-memory probe restoring subtrees just to count
   them — right in itself, that walk made one `disj` materialise a whole cold tree — but it
   made -1 the probe's answer far more often, and every one of those unknowns became durable.
   The cost lands on READERS, which is why nothing caught it: the counts stay exact, so every
   contents assertion in the suite passes either way.

   The gap this namespace closes is that the whole argument was conducted on WRITE-side
   numbers. There was no test anywhere for what a later reader pays.

   ## Why one `disj` reaches the whole spine

   An INCREMENTALLY BUILT tree sits at MINIMUM OCCUPANCY — splitting fills nodes to bf/2 —
   so removing a single element underflows a leaf, which merges, which drops its parent to
   minimum-1, which merges in turn: a JOIN cascade to the root. Measured at bf 16 over
   10000 elements, the leftmost spine after one `disj` went 8/8/8 children to 15/15/9.

   `from-sorted-array` is the opposite and must NOT be used here: it packs nodes FULL, so
   nothing underflows, no join fires, and every assertion below passes against any build.
   Both deftests were green against the unfixed code until this was corrected.

   Every one of those joins used to take its count from a walk over children that were still
   on disk (only idx-1/idx/idx+1 are ever materialised), so every one answered -1:

       after restore   level 4 count 10000 | level 3 4096 | level 2 512 | level 1 64
       after one disj  level 4 count  9999 | level 3   -1 | level 2  -1 | level 1 -1

   A join needs no walk. The two nodes being joined are both in hand and both know their
   totals, and the join holds exactly their sum minus the one deleted element. See
   `Branch.remove`.

   ## What is asserted

   Not blob counts or node shapes — those move with unrelated tuning. The reader's bill:
   a fresh cold reader asking for `count` must not have to rebuild the tree to answer."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [clojure.edn :as edn])
  (:import [org.replikativ.persistent_sorted_set Settings IStorage ANode Branch Leaf
            PersistentSortedSet]))

(set! *warn-on-reflection* true)

;; PERSISTS :subtree-count, the way datahike's storage does. Against one that drops it every
;; restored branch carries -1 regardless, and this whole namespace would measure nothing.
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

(defn- unknown-count-blobs [disk]
  (count (filter (fn [[_ s]]
                   (let [m (edn/read-string s)]
                     (and (:addresses m) (neg? (long (or (:subtree-count m) -1))))))
                 @disk)))

(deftest the-join-cascade-does-not-erase-the-counts-it-writes
  (testing "store -> cold restore -> ONE disj -> store: of the branch blobs that disj WROTE,
            most must still carry a count.

            Scoped to the blobs the disj wrote, and that scoping is the whole test. Counting
            unknowns across the WHOLE DISK makes this vacuous: the disk still holds the
            original tree's ~1400 intact blobs, so 4 erased ones are lost in the ratio and
            the assertion passes against any build. The first version of this test made
            exactly that mistake and was green against the unfixed code.

            Nor can it assert on the ROOT, which 991bdcf already fixed by taking the delta on
            remove — the root keeps its count either way. What the join cascade erased is
            everything BETWEEN the root and the leaves.

            Measured, bf 16 / n 10000, the counts written by one disj:
              before   [-1 -1 -1 -1 9999]   4 of 5 unknown
              after    [-1 -1 127 1023 9999]  2 of 5"
    (doseq [[bf n] [[8 2500] [16 10000] [32 10000]]]
      (let [disk     (atom {})
            restores (atom 0)
            opts     {:comparator compare :branching-factor bf}
            ;; CONJ-built, and that is load-bearing. `from-sorted-array` packs nodes FULL,
            ;; so removing an element never underflows, no join fires, and this test is green
            ;; against any build — the first version of it made exactly that mistake.
            ;; Incremental conj leaves nodes at MINIMUM occupancy, which is also the state a
            ;; cold restore reproduces, and is what makes one delete cascade.
            s0       (reduce #(set/conj %1 %2 compare) (set/sorted-set* opts) (range n))
            addr0    (set/store s0 (storage disk restores bf))]
        ;; PRECONDITION: the storage must really be writing counts. Against one that drops
        ;; :subtree-count every branch restores at -1 and this measures the storage, not the
        ;; tree — the in-tree test Storage does exactly that, which is why it is not used here.
        (is (zero? (unknown-count-blobs disk))
            (str "bf=" bf ": precondition — a freshly stored tree has no unknown counts"))
        (let [cold    (set/restore-by compare addr0 (storage disk restores bf) opts)
              seen    (set (keys @disk))
              after   (set/disj cold 0 compare)
              _       (set/store after (storage disk restores bf))
              written (remove (fn [[a _]] (seen a)) @disk)
              wbranch (filter (fn [[_ s]] (:addresses (edn/read-string s))) written)
              unknown (count (filter (fn [[_ s]] (neg? (long (or (:subtree-count (edn/read-string s)) -1))))
                                     wbranch))]
          (is (= (dec n) (count after))
              (str "bf=" bf ": precondition — the disj took effect"))
          (is (pos? (count wbranch))
              (str "bf=" bf ": precondition — the disj wrote branch blobs to judge"))
          (is (<= unknown (quot (count wbranch) 2))
              (str "bf=" bf ": one disj must not erase most of the counts it writes — "
                   unknown " unknown of " (count wbranch) " branch blobs written")))))))

(deftest a-cold-reader-does-not-rebuild-the-tree-to-count-it
  (testing "the reader's bill, across GENERATIONS — which is the only way it shows.

            One disj is not enough: the root keeps its count (991bdcf), so a reader answers
            from the root for 1 restore either way. The damage COMPOUNDS — each generation
            restores a tree whose interior counts the last one erased, and erases more — until
            the root itself can no longer be maintained. Measured at bf 16, a fresh reader's
            `count` after 25 generations of (cold restore, 20 disj, store): 256 restores
            before the join delta, 15 after."
    (doseq [[bf n] [[16 10000] [32 10000]]]
      (let [opts  {:comparator compare :branching-factor bf}
            disk  (atom {})
            w     (atom 0)
            s0    (reduce #(set/conj %1 %2 compare) (set/sorted-set* opts) (range n))
            ;; SCATTERED removals — every 7th key — not a contiguous low run. Deleting
            ;; 0..19 then 20..39 keeps hitting the same leftmost spine, so the damage never
            ;; spreads and a reader still answers from the root for 1 restore on ANY build.
            ;; That is the second way the first version of this test managed to be vacuous.
            addr  (loop [addr (set/store s0 (storage disk w bf))
                         gen  0]
                    (if (= gen 25)
                      addr
                      (let [cold (set/restore-by compare addr (storage disk w bf) opts)
                            ks   (take 20 (drop (* gen 20) (range 0 n 7)))
                            s    (reduce #(set/disj %1 %2 compare) cold ks)]
                        (recur (set/store s (storage disk w bf)) (inc gen)))))
            r     (atom 0)
            cold2 (set/restore-by compare addr (storage disk r bf) opts)
            _     (reset! r 0)
            c     (count cold2)
            paid  @r]
        (is (= (- n 500) c)
            (str "bf=" bf ": precondition — 25 generations x 20 removals, count exact"))
        (is (<= paid 60)
            (str "bf=" bf ": a cold reader must not rebuild the tree to answer count — "
                 "paid " paid " restores"))))))
