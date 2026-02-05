(ns me.tonsky.persistent-sorted-set.test.stats
  (:require
    [me.tonsky.persistent-sorted-set :as set]
    [clojure.test :as t :refer [is are deftest testing]])
  (:import
    [me.tonsky.persistent_sorted_set ANode NumericStats NumericStatsOps]))

(set! *warn-on-reflection* true)

(def stats-ops (NumericStatsOps/instance))

(defn get-root-stats [^me.tonsky.persistent_sorted_set.PersistentSortedSet s]
  (when-let [root (.root s)]
    (.-_stats ^ANode root)))

(deftest test-from-sorted-array-stats
  (testing "stats computed for small set"
    (let [arr (object-array [1 2 3 4 5])
          s   (set/from-sorted-array compare arr 5 {:stats stats-ops})
          ^NumericStats stats (get-root-stats s)]
      (is (some? stats))
      (is (= 5 (.count stats)))
      (is (= 15.0 (.sum stats)))
      (is (= 55.0 (.sumSq stats)))
      (is (= 1 (.min stats)))
      (is (= 5 (.max stats)))
      (is (= 3.0 (.mean stats)))
      (is (= 2.0 (.variance stats)))))

  (testing "stats computed for larger set (with branches)"
    (let [n    1000
          arr  (object-array (range n))
          s    (set/from-sorted-array compare arr n {:stats stats-ops :branching-factor 64})
          ^NumericStats stats (get-root-stats s)]
      (is (some? stats))
      (is (= n (.count stats)))
      ;; sum of 0..999 = n*(n-1)/2 = 499500
      (is (= 499500.0 (.sum stats)))
      (is (= 0 (.min stats)))
      (is (= 999 (.max stats))))))

(deftest test-conj-maintains-stats
  (testing "stats updated after conj"
    (let [arr (object-array [1 2 3])
          s1  (set/from-sorted-array compare arr 3 {:stats stats-ops})
          s2  (conj s1 4)
          s3  (conj s2 5)
          ^NumericStats stats1 (get-root-stats s1)
          ^NumericStats stats2 (get-root-stats s2)
          ^NumericStats stats3 (get-root-stats s3)]
      ;; s1 has [1 2 3]
      (is (= 3 (.count stats1)))
      (is (= 6.0 (.sum stats1)))
      (is (= 3 (.max stats1)))

      ;; s2 has [1 2 3 4]
      (is (= 4 (.count stats2)))
      (is (= 10.0 (.sum stats2)))
      (is (= 4 (.max stats2)))

      ;; s3 has [1 2 3 4 5]
      (is (= 5 (.count stats3)))
      (is (= 15.0 (.sum stats3)))
      (is (= 5 (.max stats3))))))

(deftest test-disj-maintains-stats
  (testing "stats updated after disj"
    (let [arr (object-array [1 2 3 4 5])
          s1  (set/from-sorted-array compare arr 5 {:stats stats-ops})
          s2  (disj s1 5)
          s3  (disj s2 1)
          ^NumericStats stats1 (get-root-stats s1)
          ^NumericStats stats2 (get-root-stats s2)
          ^NumericStats stats3 (get-root-stats s3)]
      ;; s1 has [1 2 3 4 5]
      (is (= 5 (.count stats1)))
      (is (= 15.0 (.sum stats1)))
      (is (= 1 (.min stats1)))
      (is (= 5 (.max stats1)))

      ;; s2 has [1 2 3 4]
      (is (= 4 (.count stats2)))
      (is (= 10.0 (.sum stats2)))
      (is (= 1 (.min stats2)))
      (is (= 4 (.max stats2)))

      ;; s3 has [2 3 4]
      (is (= 3 (.count stats3)))
      (is (= 9.0 (.sum stats3)))
      (is (= 2 (.min stats3)))
      (is (= 4 (.max stats3))))))

(deftest test-transient-maintains-stats
  (testing "stats updated for transient operations"
    (let [arr   (object-array [1 2 3])
          base  (set/from-sorted-array compare arr 3 {:stats stats-ops})
          s     (-> (transient base)
                    (conj! 4)
                    (conj! 5)
                    persistent!)
          ^NumericStats stats (get-root-stats s)]
      (is (= 5 (.count stats)))
      (is (= 15.0 (.sum stats)))
      (is (= 1 (.min stats)))
      (is (= 5 (.max stats))))))

(deftest test-no-stats-when-not-configured
  (testing "no stats computed when stats not configured"
    (let [arr (object-array [1 2 3])
          s   (set/from-sorted-array compare arr 3 {})
          stats (get-root-stats s)]
      (is (nil? stats)))))

(deftest test-numeric-stats-operations
  (testing "NumericStats merge"
    (let [s1 (NumericStats/of 1)
          s2 (NumericStats/of 2)
          merged (.merge s1 s2)]
      (is (= 2 (.count merged)))
      (is (= 3.0 (.sum merged)))
      (is (= 1 (.min merged)))
      (is (= 2 (.max merged)))))

  (testing "NumericStats identity"
    (let [identity NumericStats/IDENTITY]
      (is (= 0 (.count identity)))
      (is (= 0.0 (.sum identity))))))
