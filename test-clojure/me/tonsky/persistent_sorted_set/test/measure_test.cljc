(ns me.tonsky.persistent-sorted-set.test.measure-test
  "Tests for measure (monoid) framework and subtree-count tracking."
  (:require
   [me.tonsky.persistent-sorted-set :as set]
   [clojure.test :as t :refer [is deftest testing]]
   #?(:cljs [me.tonsky.persistent-sorted-set.impl.numeric-stats :as numeric-stats]))
  #?(:clj (:import [me.tonsky.persistent_sorted_set NumericStats NumericStatsOps])))

#?(:clj (set! *warn-on-reflection* true))

(def stats-ops
  #?(:clj  (NumericStatsOps/instance)
     :cljs numeric-stats/numeric-stats-ops))

(defn stats->map [s]
  (if (nil? s)
    {:cnt 0 :sum 0.0 :min-val nil :max-val nil}
    #?(:clj  {:cnt (.-count ^NumericStats s)
              :sum (.-sum ^NumericStats s)
              :min-val (.-min ^NumericStats s)
              :max-val (.-max ^NumericStats s)}
       :cljs {:cnt (:cnt s) :sum (double (:sum s)) :min-val (:min-val s) :max-val (:max-val s)})))

(defn expected-stats
  "Compute the expected stats for a collection of numbers."
  [coll]
  (if (empty? coll)
    {:cnt 0 :sum 0.0 :min-val nil :max-val nil}
    {:cnt (count coll)
     :sum (double (reduce + 0 coll))
     :min-val (apply min coll)
     :max-val (apply max coll)}))

;; ---------------------------------------------------------------------------
;; 1a. branch $add with nil _measure should not use identity fallback
;; ---------------------------------------------------------------------------
(deftest test-add-measure-correctness
  (testing "Adding elements to a set with measure produces correct aggregate"
    ;; Build a set large enough to have multi-level tree (branches)
    (let [s (reduce conj
                    (set/sorted-set* {:measure stats-ops :branching-factor 8})
                    (range 100))
          stats (stats->map (set/measure s))]
      (is (= 100 (:cnt stats)))
      (is (= (double (reduce + (range 100))) (:sum stats)))
      (is (= 0 (long (:min-val stats))))
      (is (= 99 (long (:max-val stats))))))

  (testing "Adding one more element after initial build gives correct measure"
    (let [s (reduce conj
                    (set/sorted-set* {:measure stats-ops :branching-factor 8})
                    (range 100))
          s2 (conj s 100)
          stats (stats->map (set/measure s2))]
      (is (= 101 (:cnt stats)))
      (is (= (double (reduce + (range 101))) (:sum stats)))
      (is (= 0 (long (:min-val stats))))
      (is (= 100 (long (:max-val stats)))))))

;; ---------------------------------------------------------------------------
;; 1b. branch $remove with nil _measure should not eagerly compute
;; ---------------------------------------------------------------------------
(deftest test-remove-measure-correctness
  (testing "Removing elements preserves correct measure"
    (let [s (reduce conj
                    (set/sorted-set* {:measure stats-ops :branching-factor 8})
                    (range 100))
          s2 (disj s 50)
          stats (stats->map (set/measure s2))
          expected (expected-stats (remove #{50} (range 100)))]
      (is (= (:cnt expected) (:cnt stats)))
      (is (= (:sum expected) (:sum stats)))
      (is (= (long (:min-val expected)) (long (:min-val stats))))
      (is (= (long (:max-val expected)) (long (:max-val stats))))))

  (testing "Removing min/max triggers recomputation"
    (let [s (reduce conj
                    (set/sorted-set* {:measure stats-ops :branching-factor 8})
                    (range 100))
          s2 (disj s 0)
          s3 (disj s 99)
          stats-no-min (stats->map (set/measure s2))
          stats-no-max (stats->map (set/measure s3))]
      (is (= 1 (long (:min-val stats-no-min))))
      (is (= 98 (long (:max-val stats-no-max)))))))

;; ---------------------------------------------------------------------------
;; 1c. Leaf remove in transient with nil _measure
;; ---------------------------------------------------------------------------
(deftest test-leaf-remove-transient-nil-measure
  (testing "Transient remove on leaf with measure is consistent"
    (let [s (reduce conj
                    (set/sorted-set* {:measure stats-ops})
                    (range 10))
          ;; Transient add + remove cycle
          t (transient s)
          t2 (disj! t 5)
          s2 (persistent! t2)
          stats (stats->map (set/measure s2))
          expected (expected-stats (remove #{5} (range 10)))]
      (is (= (:cnt expected) (:cnt stats)))
      (is (= (:sum expected) (:sum stats))))))

;; ---------------------------------------------------------------------------
;; 1e. count-slice functional correctness
;; ---------------------------------------------------------------------------
(deftest test-count-slice-correctness
  (testing "count-slice on set with known counts"
    (let [s (reduce conj
                    (set/sorted-set* {:measure stats-ops :branching-factor 8})
                    (range 1000))]
      (is (= 1000 (set/count-slice s nil nil)))
      (is (= 501 (set/count-slice s nil 500)))
      (is (= 500 (set/count-slice s 500 999)))
      (is (= 1 (set/count-slice s 500 500)))
      (is (= 0 (set/count-slice s 1000 2000))))))

;; ---------------------------------------------------------------------------
;; 1f. get-nth correctness
;; ---------------------------------------------------------------------------
(deftest test-get-nth-correctness
  (testing "get-nth returns correct elements"
    (let [s (reduce conj
                    (set/sorted-set* {:measure stats-ops :branching-factor 8})
                    (range 100))]
      (is (= [0 0] (set/get-nth s 0)))
      (is (= [50 0] (set/get-nth s 50)))
      (is (= [99 0] (set/get-nth s 99)))
      (is (nil? (set/get-nth s 100)))
      (is (nil? (set/get-nth s -1)))))

  (testing "get-nth after multiple operations"
    (let [s (-> (reduce conj
                        (set/sorted-set* {:measure stats-ops :branching-factor 8})
                        (range 100))
                (disj 50)
                (conj 200))]
      ;; 100 elements: 0..49, 51..99, 200
      (is (= [0 0] (set/get-nth s 0)))
      (is (= [49 0] (set/get-nth s 49)))
      (is (= [51 0] (set/get-nth s 50)))
      (is (= [200 0] (set/get-nth s 99)))
      (is (nil? (set/get-nth s 100))))))

;; ---------------------------------------------------------------------------
;; 1g. measure consistency through mixed operations
;; ---------------------------------------------------------------------------
(deftest test-measure-consistency-through-operations
  (testing "Measure stays correct through conj/disj/replace sequence"
    (let [s0 (reduce conj
                     (set/sorted-set* {:measure stats-ops :branching-factor 8})
                     (range 50))
          ;; Add elements
          s1 (reduce conj s0 (range 50 80))
          ;; Remove some
          s2 (reduce disj s1 (range 10 20))
          ;; Replace some (identity replacement with default compare)
          s3 (reduce (fn [s k] (set/replace s k k)) s2 (range 30 40))]

      ;; Verify after each step
      (let [expected-1 (expected-stats (range 80))
            actual-1 (stats->map (set/measure s1))]
        (is (= (:cnt expected-1) (:cnt actual-1)))
        (is (= (:sum expected-1) (:sum actual-1))))

      (let [remaining-2 (remove (set (range 10 20)) (range 80))
            expected-2 (expected-stats remaining-2)
            actual-2 (stats->map (set/measure s2))]
        (is (= (:cnt expected-2) (:cnt actual-2)))
        (is (= (:sum expected-2) (:sum actual-2))))

      ;; Identity replace should not change stats
      (let [remaining-3 (remove (set (range 10 20)) (range 80))
            expected-3 (expected-stats remaining-3)
            actual-3 (stats->map (set/measure s3))]
        (is (= (:cnt expected-3) (:cnt actual-3)))
        (is (= (:sum expected-3) (:sum actual-3)))))))

;; ---------------------------------------------------------------------------
;; 1h. subtree count propagation
;; ---------------------------------------------------------------------------
(deftest test-subtree-count-propagation
  (testing "Count stays correct through operations"
    (let [s0 (reduce conj
                     (set/sorted-set* {:measure stats-ops :branching-factor 8})
                     (range 100))]
      (is (= 100 (count s0)))
      ;; Add
      (let [s1 (conj s0 200)]
        (is (= 101 (count s1))))
      ;; Remove
      (let [s2 (disj s0 50)]
        (is (= 99 (count s2))))
      ;; Replace should not change count
      (let [s3 (set/replace s0 50 50)]
        (is (= 100 (count s3))))))

  (testing "Count correct after many removals (triggers rebalancing)"
    (let [s (reduce conj
                    (set/sorted-set* {:measure stats-ops :branching-factor 8})
                    (range 200))
          ;; Remove half the elements to trigger merges/borrows
          s2 (reduce disj s (range 0 200 2))]
      (is (= 100 (count s2)))
      (is (= (vec (range 1 200 2)) (vec (seq s2)))))))

;; ---------------------------------------------------------------------------
;; measure-slice correctness
;; ---------------------------------------------------------------------------
(deftest test-measure-slice-correctness
  (testing "measure-slice computes correct sub-range aggregates"
    (let [s (reduce conj
                    (set/sorted-set* {:measure stats-ops :branching-factor 8})
                    (range 100))]
      ;; Full range
      (let [m (stats->map (set/measure-slice s nil nil))]
        (is (= 100 (:cnt m)))
        (is (= (double (reduce + (range 100))) (:sum m))))
      ;; Prefix
      (let [m (stats->map (set/measure-slice s nil 49))]
        (is (= 50 (:cnt m)))
        (is (= (double (reduce + (range 50))) (:sum m))))
      ;; Suffix
      (let [m (stats->map (set/measure-slice s 50 nil))]
        (is (= 50 (:cnt m)))
        (is (= (double (reduce + (range 50 100))) (:sum m))))
      ;; Middle slice
      (let [m (stats->map (set/measure-slice s 25 74))]
        (is (= 50 (:cnt m)))
        (is (= (double (reduce + (range 25 75))) (:sum m)))))))

;; ---------------------------------------------------------------------------
;; Large tree stress: build, verify, mutate, verify
;; ---------------------------------------------------------------------------
(deftest test-large-tree-measure-stress
  (testing "Large tree: measure correct after build and mutations"
    (let [n 2000
          s (reduce conj
                    (set/sorted-set* {:measure stats-ops :branching-factor 16})
                    (range n))
          ;; Verify initial
          _ (let [m (stats->map (set/measure s))]
              (is (= n (:cnt m)))
              (is (= (double (reduce + (range n))) (:sum m))))
          ;; Add elements
          s2 (reduce conj s (range n (* 2 n)))
          _ (let [m (stats->map (set/measure s2))]
              (is (= (* 2 n) (:cnt m)))
              (is (= (double (reduce + (range (* 2 n)))) (:sum m))))
          ;; Remove every other element
          s3 (reduce disj s2 (range 0 (* 2 n) 2))
          remaining (set (range 1 (* 2 n) 2))]
      (is (= (count remaining) (count s3)))
      (let [m (stats->map (set/measure s3))]
        (is (= (count remaining) (:cnt m)))
        (is (= (double (reduce + remaining)) (:sum m)))))))
