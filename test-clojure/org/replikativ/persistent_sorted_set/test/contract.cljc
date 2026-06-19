(ns org.replikativ.persistent-sorted-set.test.contract
  "Drop-in-replacement contract guards (the 'D' items): the ways this set must behave like
   clojure.core/sorted-set (and the one documented difference — it rejects nil). Cross-platform.
   D1 nil rejection · D2 =/hash · D3 with-meta · D4 comparator-driven order · D5 empty-set ops
   · D6 measure (incl. non-invertible min/max) after rebalances. See the audit reassessment."
  (:require
   [org.replikativ.persistent-sorted-set :as set]
   [clojure.test :refer [is deftest testing]]
   #?(:cljs [org.replikativ.persistent-sorted-set.impl.numeric-stats :as numeric-stats]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set NumericStats NumericStatsOps])))

;; ---- D1: nil is not a storable value (the one documented difference from core) ---------------
(deftest nil-rejected
  (testing "every insertion path rejects nil (matches upstream / the documented contract)"
    (is (thrown? #?(:clj IllegalArgumentException :cljs js/Error) (conj (set/sorted-set) nil)))
    (is (thrown? #?(:clj IllegalArgumentException :cljs js/Error) (conj (set/sorted-set 1 2) nil)))
    (is (thrown? #?(:clj IllegalArgumentException :cljs js/Error) (set/sorted-set 1 nil 2)))
    (is (thrown? #?(:clj IllegalArgumentException :cljs js/Error) (set/sorted-set-by compare 1 nil)))
    (is (thrown? #?(:clj IllegalArgumentException :cljs js/Error) (into (set/sorted-set) [1 nil 2])))
    (is (thrown? #?(:clj IllegalArgumentException :cljs js/Error) (persistent! (conj! (transient (set/sorted-set 1 2)) nil))))
    ;; disj of nil is a harmless no-op (nil can never be present)
    (is (= (vec [1 2]) (vec (disj (set/sorted-set 1 2) nil))))))

;; ---- D2: equality + hash, both directions, vs core sorted-set and hash-set -------------------
(deftest equality-and-hash
  (testing "= and hash satisfy the IPersistentSet contract against core collections"
    (let [p (into (set/sorted-set) [3 1 2])]
      (is (= p (sorted-set 1 2 3)))
      (is (= (sorted-set 1 2 3) p))
      (is (= p #{1 2 3}))
      (is (= #{1 2 3} p))
      (is (= (hash p) (hash (sorted-set 1 2 3)) (hash #{1 2 3})))
      (is (not= p (set/sorted-set 1 2)))
      (is (= (set/sorted-set) (sorted-set)))
      (is (= (hash (set/sorted-set)) (hash (sorted-set))))
      ;; built differently but equal
      (is (= (into (set/sorted-set) [2 3 1]) (into (set/sorted-set) [1 2 3]))))))

;; ---- D3: metadata is preserved across operations ---------------------------------------------
(deftest with-meta-preserved
  (testing "with-meta survives conj/disj/transient and on the empty set"
    (let [m {:a 1} s (with-meta (set/sorted-set 1 2 3) m)]
      (is (= m (meta s)))
      (is (= m (meta (conj s 4))))
      (is (= m (meta (disj s 1))))
      (is (= m (meta (persistent! (conj! (transient s) 9)))))
      (is (= m (meta (with-meta (set/sorted-set) m)))))))

;; ---- D4: ordering / first / slice follow the comparator (not hardcoded <) ---------------------
(deftest comparator-driven-order
  (testing "a reverse comparator orders the set descending, matching sorted-set-by"
    (let [rev (fn [a b] (compare b a))
          xs  [3 1 4 1 5 9 2 6]
          p   (into (set/sorted-set-by rev) xs)
          ref (into (sorted-set-by rev) xs)]
      (is (= (vec ref) (vec p)) "elements in comparator order")
      (is (= 9 (first p)) "first is the comparator-min (numeric max)")
      (is (= (vec (subseq ref >= 6 <= 2)) (vec (set/slice p 6 2))) "slice follows comparator direction"))))

;; ---- D5: operations on the empty set are graceful --------------------------------------------
(deftest empty-set-ops
  (testing "empty-set queries return nil/empty, never throw (incl. seek on an empty seq)"
    (let [e (set/sorted-set)]
      (is (nil? (seq e)))
      (is (nil? (rseq e)))
      (is (nil? (first e)))
      (is (= [] (vec (disj e 1))))
      (is (= [] (vec (set/slice e 1 5))))
      (is (nil? (set/seek (seq e) 5)) "seek on an empty (nil) seq returns nil, not an NPE"))))

;; ---- D6: measure correctness, incl. NON-invertible min/max, after rebalances ------------------
(def stats-ops
  #?(:clj (NumericStatsOps/instance) :cljs numeric-stats/numeric-stats-ops))
(defn- stats->map [s]
  (if (nil? s) {:cnt 0 :min-val nil :max-val nil}
      #?(:clj  {:cnt (.-count ^NumericStats s) :min-val (.-min ^NumericStats s) :max-val (.-max ^NumericStats s)}
         :cljs {:cnt (:cnt s) :min-val (:min-val s) :max-val (:max-val s)})))
(defn- expected [coll]
  {:cnt (count coll) :min-val (apply min coll) :max-val (apply max coll)})

(deftest measure-min-max-after-rebalance
  (testing "removing the current min and max (non-invertible measure ⇒ recompute) through
            borrows/merges keeps min/max/count exact"
    (let [s  (into (set/sorted-set* {:measure stats-ops :branching-factor 4}) (range 40))
          ;; delete a contiguous low chunk (forces merges + new min) and a high chunk (new max)
          s2 (reduce disj s (concat (range 0 12) (range 28 40)))
          remaining (range 12 28)]
      (is (= (expected remaining) (stats->map (set/measure s2))) "min/max/count correct after rebalances")
      ;; and after one more removal of the new min
      (is (= (expected (range 13 28)) (stats->map (set/measure (disj s2 12))))))))
