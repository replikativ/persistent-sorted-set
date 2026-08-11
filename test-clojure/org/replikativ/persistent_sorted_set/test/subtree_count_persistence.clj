(ns org.replikativ.persistent-sorted-set.test.subtree-count-persistence
  "A storage that persists `:subtree-count` must get usable counts back.

   `impl.nodes/node->map` serializes `subtreeCount()` RAW, so whatever `remove` leaves on a
   successor goes to disk. `remove` used to ask the in-memory probe
   `tryComputeSubtreeCountFromChildren`, which bails to -1 at the first child that is not
   resident — and after 04499a0 (which correctly stopped it restoring subtrees just to count
   them) that bail became far more common. The result was a write-side saving paid for by a
   permanently degraded on-disk count: every later reader of such a blob has to walk the
   subtree to recover the number.

   That is not hypothetical. Datahike persists `:subtree-count` and reads it back
   (`impl.nodes` write side, and `blob->branch` assigning `_subtreeCount` directly), and it
   calls `count` on `:eavt` on every `(count db)` and on every bulk import.

   `remove` deletes exactly ONE element and — in both arms this test exercises — no child
   leaves the node, so the successor's total is exactly one less. Taking the delta instead of
   recomputing is exact, needs no IO, and is what ClojureScript has always done. A census over
   2259 nodes found 93 disagreeing on `:subtree-count`, 93 of 93 being \"JVM -1, cljs exact\" —
   never two different real values.

   Measured here, ten generations of (cold restore, 20 disj, store) against a count-persisting
   storage, before -> after:

       bf 16   a fresh reader's `count` cost 46 restores -> 1
       bf 32                                 54 restores -> 1

   ## What this does NOT fix

   The merge/borrow arms, where children move BETWEEN nodes, still recompute and can still
   write -1: unknown-count blobs went 40 -> 30 (bf 16) and 30 -> 20 (bf 32). A delta cannot
   apportion a total across a split. So the test bounds the READER's cost — which is what
   users feel — rather than asserting zero unknown blobs, which would be a stronger claim
   than the fix supports.

   ## The leafProcessor exception

   The delta holds only WITHOUT a leaf processor. A processor may compact or expand a leaf, so
   removing one key need not change the element count by one; `PersistentSortedSet.disjoin`
   makes the same distinction. Asserting the delta unconditionally produced
   `:subtree-count-mismatch {:branch-count 139, :children-sum 137}` in
   `leaf_processor/test-mixed-processor`, so that case falls back to the probe."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.edn :as edn]
            [org.replikativ.persistent-sorted-set :as ss])
  (:import [org.replikativ.persistent_sorted_set Settings IStorage PersistentSortedSet
            Branch ANode Leaf]))

(set! *warn-on-reflection* true)

(def ^:private *restores (atom 0))

;; A storage that PERSISTS :subtree-count and feeds it back — datahike's class. The whole
;; point is lost against a storage that drops the field, so this must not be the test-tree
;; Storage (which does drop it).
(defrecord CountingStorage [*disk ^Settings settings]
  IStorage
  (store [_ node]
    (let [^ANode node node
          addr (str (java.util.UUID/randomUUID))]
      (swap! *disk assoc addr
             (pr-str {:level         (.level node)
                      :keys          (.keys node)
                      :addresses     (when (instance? Branch node) (.addresses ^Branch node))
                      :subtree-count (when (instance? Branch node) (.subtreeCount ^Branch node))}))
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

(defn- storage [disk bf] (->CountingStorage disk (Settings. (int bf) nil nil nil (int 0))))

(defn- generations
  "Cold restore, 20 disj, store — `gens` times. Returns the final address."
  [disk bf opts a0 gens]
  (loop [g 0 addr a0]
    (if (>= g gens)
      addr
      (let [c (ss/restore-by compare addr (storage disk bf) opts)
            _ (dorun (take 1 (seq c)))
            v (reduce #(ss/disj %1 %2 compare) c (range (* g 20) (+ (* g 20) 20)))]
        (recur (inc g) (ss/store v (storage disk bf)))))))

(deftest a-fresh-reader-does-not-pay-to-recover-the-count
  (testing "the count the storage persisted must still be usable ten generations later"
    (doseq [[bf n] [[16 20000] [32 20000]]]
      (let [disk (atom {})
            opts {:comparator compare :branching-factor bf}
            s0   (reduce #(ss/conj %1 %2 compare) (ss/sorted-set* opts) (range n))
            a0   (ss/store s0 (storage disk bf))
            addr (generations disk bf opts a0 10)
            lbl  (str "bf=" bf)
            ;; a reader that has seen nothing of this tree and only wants the count
            _    (reset! *restores 0)
            cold (ss/restore-by compare addr (storage disk bf) opts)
            cnt  (count cold)
            reads @*restores]
        (is (= (- n 200) cnt) (str lbl ": the count must be exact"))
        (is (<= reads 2)
            (str lbl ": a fresh reader asking only for `count` restored " reads
                 " blobs. It should read the root and take the persisted number; anything"
                 " more means -1 was written and the count is being recomputed by walking."))))))

(deftest the-persisted-counts-stay-exact
  (testing "the delta must not drift from reality — it is exact or it is nothing. Checked
            against the tree's own contents rather than against another cached number."
    (let [bf 16 n 4000
          disk (atom {})
          opts {:comparator compare :branching-factor bf}
          s0   (reduce #(ss/conj %1 %2 compare) (ss/sorted-set* opts) (range n))
          a0   (ss/store s0 (storage disk bf))
          addr (generations disk bf opts a0 5)
          cold (ss/restore-by compare addr (storage disk bf) opts)]
      (is (= (- n 100) (count cold)) "count")
      (is (= (- n 100) (count (seq cold))) "and it agrees with the seq")
      (is (= (remove (set (range 100)) (range n)) (seq cold)) "contents exact"))))
