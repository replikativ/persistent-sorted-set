(ns org.replikativ.persistent-sorted-set.test.diff
  "`diff` — what changed between two versions of a set that share structure.

   The claim is not just \"the right elements\": `clojure.set/difference` gets
   that too. The claim is that the COST is proportional to the change rather
   than to the set, because two versions share every node they have in common
   and a subtree present in both cannot contain a difference. In cljs every one
   of those reads is an async round trip, so the read counts below are the whole
   point of the function.

   The storage SERIALIZES. A storage that keeps node objects and hands the same
   instance back on restore leaves the tree fully resident, so descending it
   costs nothing and every read-count assertion passes no matter what the walk
   does — which is exactly how an earlier version of the JVM test certified a
   diff that read all 392 nodes of a 100 000-element set for a two-element
   delta."
  (:require [cljs.test :refer-macros [deftest testing is]]
            [clojure.set :as set]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.test.storage.util :as u]))

(defn- build [storage xs]
  (let [st (reduce #(s/conj %1 %2 compare) (s/sorted-set-by compare) xs)]
    (s/store st storage)
    st))

(defn- naive
  "What the answer must be, computed the expensive way."
  [a b]
  {:added   (vec (sort (set/difference (set (s/seq b)) (set (s/seq a)))))
   :removed (vec (sort (set/difference (set (s/seq a)) (set (s/seq b)))))})

(defn- reads [] (:reads @u/*stats))
(defn- reset-reads! [] (swap! u/*stats assoc :reads 0))

(defn- delta-scenario
  "Two stored sets `delta` elements apart, handed back on a COLD storage handle
   (same disk, empty node cache) so a restore really goes to storage."
  [n delta]
  (let [disk (atom {})
        w    (u/storage disk)
        a0   (reduce #(s/conj %1 %2 compare) (s/sorted-set-by compare) (range n))
        ra   (s/store a0 w)
        b0   (reduce #(s/conj %1 %2 compare) a0 (range (inc n) (+ n 1 delta)))
        rb   (s/store b0 w)
        c    (u/storage disk)]
    {:a (s/restore ra c {:comparator compare})
     :b (s/restore rb c {:comparator compare})
     :storage c
     :on-disk (count @disk)}))

;; ---------------------------------------------------------------------------

(deftest diff-agrees-with-set-difference
  (testing "against the obvious implementation, over shapes that exercise leaf
            splits, merges and multi-level rebalancing"
    (doseq [n [0 1 2 511 512 513 5000]]
      (let [storage (u/storage)
            a (build storage (range n))
            b (-> (reduce #(s/conj %1 %2 compare) a (range n (+ n 7)))
                  (as-> x (reduce #(s/disj %1 %2 compare) x (take 3 (range n))))
                  (doto (s/store storage)))]
        (is (= (naive a b) (s/diff a b storage))
            (str "n=" n))))))

(defn- build-bf
  "Like `build` but with an explicit fanout, so a test can force a MULTI-LEVEL
   tree. At the default branching factor a few hundred elements are ONE LEAF, and
   a diff over one leaf per side exercises none of the level-synchronised walk."
  [storage bf xs]
  (let [st (reduce #(s/conj %1 %2 compare)
                   (s/sorted-set* {:comparator compare :branching-factor bf}) xs)]
    (s/store st storage)
    st))

(deftest diff-of-a-set-with-itself-is-empty
  (testing "the SHORT-CIRCUIT: identical root addresses answer without touching
            storage at all"
    (let [storage (u/storage)
          a (build storage (range 1000))]
      (reset-reads!)
      (is (= {:added [] :removed []} (s/diff a a storage)))
      (is (zero? (reads)) "same root address must cost zero reads")))

  (testing "and the WALK, which the case above never reaches. `(diff a a)` returns
            on the identical-address early-out before the algorithm starts, so on
            its own it asserts nothing about differencing equal content.

            Two INDEPENDENTLY built sets with identical contents get distinct root
            addresses, restored on a COLD handle so descending them really reads."
    (let [disk (atom {})
          w    (u/storage disk)
          ra   (s/store (build-bf w 8 (range 1000)) w)
          rb   (s/store (build-bf w 8 (range 1000)) w)
          c    (u/storage disk)
          a    (s/restore ra c {:comparator compare :branching-factor 8})
          b    (s/restore rb c {:comparator compare :branching-factor 8})]
      (is (not= ra rb)
          "precondition: distinct roots, or this is the short-circuit again")
      (reset-reads!)
      (is (= {:added [] :removed []} (s/diff a b c)))
      (is (pos? (reads)) "precondition: it really walked"))))

(deftest diff-handles-empty-on-either-side
  (let [storage (u/storage)
        e (build storage [])
        a (build storage (range 100))]
    (is (= (vec (range 100)) (:added (s/diff e a storage))))
    (is (= (vec (range 100)) (:removed (s/diff a e storage))))
    (is (= {:added [] :removed []} (s/diff e e storage)))))

(deftest diff-of-unrelated-sets-is-correct-if-not-cheap
  (testing "no shared structure means nothing prunes. The answer must still be
            right — a caller who diffs unrelated sets gets a slow correct result,
            not a wrong fast one.

            Measured on the JVM twin of this test: 100 elements at the default
            branching factor is ONE LEAF per side (`:nodes-on-disk 1,
            :root-level 0`). It compared two leaves and never descended, never
            pruned, and never met the level-synchronisation the name is about."
    (let [storage (u/storage)
          a (build-bf storage 8 (range 0 4000 2))
          b (build-bf storage 8 (range 1 4001 2))]
      (is (= (naive a b) (s/diff a b storage)))))

  (testing "unrelated sets of DIFFERENT depth, so the walk must bring the two
            frontiers to a common level before any address comparison means
            anything — the branch equal-depth cases never exercise"
    (let [storage (u/storage)
          a (build-bf storage 8 (range 0 4000 2))
          b (build-bf storage 8 (range 1 41 2))]
      (is (= (naive a b) (s/diff a b storage))))))

(deftest diff-reads-only-the-nodes-that-changed
  (testing "THE point of the function. A two-element delta must not read the tree."
    (doseq [n [1000 10000 100000]]
      (let [{:keys [a b storage on-disk]} (delta-scenario n 2)]
        (reset-reads!)
        (let [d (s/diff a b storage)
              r (reads)]
          (is (= [(+ n 1) (+ n 2)] (:added d)) (str "n=" n))
          (is (= [] (:removed d)))
          ;; the changed leaf and the spine above it, on both sides — a small
          ;; multiple of the DEPTH, and nothing to do with the node count
          (is (<= r 8)
              (str "n=" n ": read " r " nodes of " on-disk
                   " for a 2-element delta")))))))

(deftest diff-cost-does-not-grow-with-set-size
  (testing "the same delta against sets two orders of magnitude apart reads the
            same number of nodes, while the trees themselves differ 80-fold."
    (let [run (fn [n] (let [{:keys [a b storage on-disk]} (delta-scenario n 1)]
                        (reset-reads!)
                        (s/diff a b storage)
                        [(reads) on-disk]))
          [small small-nodes] (run 1000)
          [big big-nodes]     (run 100000)]
      (is (< (* 8 small-nodes) big-nodes)
          "the two trees must actually differ in size for this to prove anything")
      (is (<= big (+ small 2))
          (str "1k set read " small " nodes, 100k set read " big
               " — cost is tracking set size, not delta")))))

(deftest diff-of-identical-roots-reads-nothing
  (testing "the same stored root on both sides is answered without touching
            storage at all"
    (let [disk (atom {})
          w    (u/storage disk)
          a    (build w (range 10000))
          ra   (s/store a w)
          c    (u/storage disk)]
      (reset-reads!)
      (is (= {:added [] :removed []}
             (s/diff (s/restore ra c {:comparator compare})
                     (s/restore ra c {:comparator compare})
                     c)))
      (is (zero? (reads))))))

(deftest the-async-arm-gives-the-same-answer
  (testing "`async+sync` emits both arms from one source, so they must agree —
            same elements AND the same number of reads, since a divergence in
            the walk would show up as a different read count long before it
            showed up as a wrong answer.

            The storages assert their own mode, so neither arm can pass by
            accidentally running the other."
    (cljs.test/async
     done
     (let [n     5000
           disk  (atom {})
           w     (u/storage disk)
           a0    (reduce #(s/conj %1 %2 compare) (s/sorted-set-by compare) (range n))
           ra    (s/store a0 w)
           b0    (reduce #(s/conj %1 %2 compare) a0 [(+ n 1) (+ n 2)])
           rb    (s/store b0 w)
           sync-storage  (u/storage disk)
           async-storage (u/async-storage disk)
           sa    (s/restore ra sync-storage {:comparator compare})
           sb    (s/restore rb sync-storage {:comparator compare})
           aa    (s/restore ra async-storage {:comparator compare})
           ab    (s/restore rb async-storage {:comparator compare})]
       (reset-reads!)
       (let [sync-result (s/diff sa sb sync-storage)
             sync-reads  (reads)]
         (reset-reads!)
         ((s/diff aa ab async-storage {:sync? false})
          (fn [async-result]
            (is (= [(+ n 1) (+ n 2)] (:added sync-result)) "the sync arm is right")
            (is (= sync-result async-result)
                "both arms found the same delta")
            (is (= sync-reads (reads))
                (str "both arms read the same nodes: sync " sync-reads
                     ", async " (reads)))
            (done))
          (fn [e]
            (is false (str "async arm failed: " e))
            (done))))))))
