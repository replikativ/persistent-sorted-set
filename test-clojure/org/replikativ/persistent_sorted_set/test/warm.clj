(ns org.replikativ.persistent-sorted-set.test.warm
  "The breadth-first warm, on the JVM.

   The oracle is the storage's own read counter: a warm is real exactly when
   the reads it reports match the reads the storage performed, and when the
   scan that follows performs none. Asserting only the report would pass a walk
   that fetches nothing and counts optimistically; asserting only the follow-up
   scan would pass a tree that was never cold. Both are asserted.

   TRAP, inherited from `count-reads.cljs`: `storage`'s `*memory` atom caches
   by address and `:reads` counts only cache MISSES. Reusing the storage a set
   was written with means every node is already resident and every warm looks
   perfect. Every restore below therefore gets a FRESH memory cache over the
   same `*disk`."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.warm :as warm]
            [org.replikativ.persistent-sorted-set.test.storage :as st])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet Settings]))

(defn- stored
  "Build a set of 0..n-1 at branching factor `bf`, store it, and hand back
   [disk address] — enough to restore arbitrarily many COLD copies."
  [n bf]
  (let [settings (Settings. (int bf))
        s        (into (set/sorted-set* {:branching-factor bf}) (range n))
        disk     (atom {})
        storage  (st/->Storage (atom {}) disk settings)
        addr     (set/store s storage)]
    [disk addr settings]))

(defn- cold
  "A cold restore over `disk`: fresh memory cache, fresh counters. Returns
   [set storage]."
  [disk addr settings]
  (let [storage (st/->Storage (atom {}) disk settings)]
    [(set/restore addr storage) storage]))

(defn- reads [] (:reads @st/*stats))

(deftest a-full-warm-makes-the-following-scan-free
  (let [[disk addr settings] (stored 2000 8)]
    (st/with-stats
      (let [[s _] (cold disk addr settings)
            r     (warm/warm! s {:depth :with-leaves :budget 100000})
            after-warm (reads)]
        (is (pos? (:fetched r)) "a cold tree has something to fetch")
        (is (= (:fetched r) (dec after-warm))
            "the report's count is the storage's count (minus the root restore)")
        (is (= 2000 (count (vec s))) "the scan still answers correctly")
        (is (= after-warm (reads))
            "and performs ZERO further reads — that is what a full warm means")))))

(deftest interior-stops-exactly-at-the-leaf-boundary
  (let [[disk addr settings] (stored 2000 8)]
    (st/with-stats
      (let [[s _] (cold disk addr settings)
            r     (warm/warm! s {:depth :interior :budget 100000})]
        (is (pos? (:fetched r)))
        (is (false? (:budget-exhausted? r)))
        ;; leaves were NOT fetched: the scan still has reads to do, and the
        ;; number it has left is the leaf count.
        (let [before (reads)
              _      (vec s)
              leaf-reads (- (reads) before)]
          (is (pos? leaf-reads) "an :interior warm leaves the leaves cold")
          ;; interior + leaves is the whole tree
          (st/with-stats
            (let [[s2 _] (cold disk addr settings)
                  full   (warm/warm! s2 {:depth :with-leaves :budget 100000})]
              (is (= (:fetched full) (+ (:fetched r) leaf-reads))
                  "interior fetches + leaf reads = the whole tree"))))))))

(deftest the-budget-is-a-hard-ceiling
  (let [[disk addr settings] (stored 2000 8)]
    (st/with-stats
      (let [[s _] (cold disk addr settings)
            r     (warm/warm! s {:depth :with-leaves :budget 7})]
        (is (= 7 (:fetched r)) "exactly the budget, not one more")
        (is (true? (:budget-exhausted? r)))
        (is (zero? (:budget-left r)))
        (is (= 8 (reads)) "the storage agrees: budget + the root restore")))))

(deftest a-range-scoped-warm-is-proportional-to-the-range
  (let [[disk addr settings] (stored 4000 8)]
    (st/with-stats
      (let [[s _]  (cold disk addr settings)
            narrow (warm/warm! s {:depth :with-leaves :budget 100000
                                  :from 100 :to 140})]
        (st/with-stats
          (let [[s2 _] (cold disk addr settings)
                full   (warm/warm! s2 {:depth :with-leaves :budget 100000})]
            (is (< (:fetched narrow) (/ (:fetched full) 10))
                "a 1% key range warms a small fraction of the tree, not most of it")))
        ;; and the scan the range was FOR is free: slice within [from to]
        (st/with-stats
          (let [[s3 storage] (cold disk addr settings)
                _  (warm/warm! s3 {:depth :with-leaves :budget 100000
                                   :from 100 :to 140})
                before (reads)]
            (is (= (range 100 141) (seq (set/slice s3 100 140)))
                "the slice answers correctly")
            (is (= before (reads))
                "and pays zero reads — the warm covered exactly its range")))))))

(deftest several-trees-share-one-budget-fairly
  (let [[disk-a addr-a settings] (stored 2000 8)
        [disk-b addr-b _]        (stored 2000 8)]
    (st/with-stats
      (let [[a _] (cold disk-a addr-a settings)
            [b _] (cold disk-b addr-b settings)
            r     (warm/warm-trees! [{:key :a :set a} {:key :b :set b}]
                                    {:depth :with-leaves :budget 40})]
        (is (= 40 (:fetched r)))
        (is (true? (:budget-exhausted? r)))
        (let [{:keys [a b]} (:by-index r)]
          (is (and (pos? a) (pos? b)) "both trees were warmed")
          (is (<= (abs (- a b)) 2)
              (str "round-robin splits the budget evenly, got a=" a " b=" b)))))))

(deftest the-clamp-and-the-empty-cases
  (let [[disk addr settings] (stored 500 8)]
    (st/with-stats
      (let [[s _] (cold disk addr settings)
            r     (warm/warm! s {:depth :with-leaves :budget 1000 :cache-size 100})]
        (is (true? (:budget-clamped? r)))
        (is (<= (:fetched r) 80) "0.8x the cache is the effective budget"))))
  (testing "no trees at all"
    (let [r (warm/warm-trees! [] {})]
      (is (zero? (:fetched r)))
      (is (false? (:budget-exhausted? r)))))
  (testing ":sync? false is refused on the JVM, not emulated"
    (let [[disk addr settings] (stored 10 8)
          [s _] (cold disk addr settings)]
      (is (thrown? IllegalArgumentException
                   (warm/warm! s {:sync? false}))))))
