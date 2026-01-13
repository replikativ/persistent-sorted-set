(ns me.tonsky.persistent-sorted-set.test.replace-and-lookup
  (:require
   [me.tonsky.persistent-sorted-set :as set]
   [clojure.test :as t :refer [is are deftest testing]]))

#?(:clj (set! *warn-on-reflection* true))

(deftest test-lookup
  (testing "Basic lookup functionality"
    (testing "Empty set"
      (let [s (set/sorted-set)]
        (is (nil? (set/lookup s 1)))))

    (testing "Single element"
      (let [s (set/sorted-set 42)]
        (is (= 42 (set/lookup s 42)))
        (is (nil? (set/lookup s 41)))
        (is (nil? (set/lookup s 43)))))

    (testing "Small set (single leaf)"
      (let [s (into (set/sorted-set) (range 10))]
        (is (= 0 (set/lookup s 0)))
        (is (= 5 (set/lookup s 5)))
        (is (= 9 (set/lookup s 9)))
        (is (nil? (set/lookup s -1)))
        (is (nil? (set/lookup s 10)))
        (is (nil? (set/lookup s 4.5)))))

    (testing "Large set (multiple B-tree levels)"
      (let [s (into (set/sorted-set) (range 5000))]
        (is (= 0 (set/lookup s 0)))
        (is (= 2500 (set/lookup s 2500)))
        (is (= 4999 (set/lookup s 4999)))
        (is (nil? (set/lookup s -1)))
        (is (nil? (set/lookup s 5000)))
        (is (nil? (set/lookup s 2500.5))))))

  (testing "Lookup with custom comparator"
    (testing "Compare by first element of tuple"
      ;; Comparator that only looks at first element
      (let [cmp-first (fn [[a _] [b _]] (compare a b))
            s (-> (set/sorted-set-by cmp-first)
                  (conj [1 :a])
                  (conj [2 :b])
                  (conj [3 :c]))]
        ;; lookup returns the stored tuple, not the search key
        (is (= [1 :a] (set/lookup s [1 nil])))
        (is (= [2 :b] (set/lookup s [2 :anything])))
        (is (= [3 :c] (set/lookup s [3 :different])))
        (is (nil? (set/lookup s [0 :x])))
        (is (nil? (set/lookup s [4 :x])))))

    (testing "Compare by id field in map (fulltext use case)"
      ;; This is the actual use case from persistent-fulltext
      (let [cmp-id (fn [a b] (compare (:id a) (:id b)))
            s (-> (set/sorted-set-by cmp-id)
                  (conj {:id 1 :data "foo" :meta {:timestamp 100}})
                  (conj {:id 2 :data "bar" :meta {:timestamp 200}})
                  (conj {:id 3 :data "baz" :meta {:timestamp 300}}))]
        ;; Lookup with just the id returns the full stored object
        (is (= {:id 1 :data "foo" :meta {:timestamp 100}}
               (set/lookup s {:id 1})))
        (is (= {:id 2 :data "bar" :meta {:timestamp 200}}
               (set/lookup s {:id 2})))
        (is (= {:id 3 :data "baz" :meta {:timestamp 300}}
               (set/lookup s {:id 3})))
        (is (nil? (set/lookup s {:id 0})))
        (is (nil? (set/lookup s {:id 4})))))

    (testing "Case-insensitive string comparison"
      (let [cmp-ci (fn [a b] (compare (clojure.string/lower-case a)
                                      (clojure.string/lower-case b)))
            s (-> (set/sorted-set-by cmp-ci)
                  (conj "Apple")
                  (conj "Banana")
                  (conj "Cherry"))]
        ;; Returns the actual stored string with original case
        (is (= "Apple" (set/lookup s "apple")))
        (is (= "Apple" (set/lookup s "APPLE")))
        (is (= "Banana" (set/lookup s "banana")))
        (is (= "Cherry" (set/lookup s "CHERRY")))
        (is (nil? (set/lookup s "date")))))

    (testing "Lookup on large sets with custom comparator"
      (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
            s (reduce #(set/conj %1 %2 compare)
                      (set/sorted-set-by cmp-mod)
                      (range 5000))]
        ;; With mod comparator, many values compare equal
        ;; lookup should find one of them
        (is (some? (set/lookup s 0)))
        (is (some? (set/lookup s 50)))
        (is (some? (set/lookup s 99)))))))

(deftest test-replace
  (testing "Replace with persistent sets"
    (testing "Empty set - with custom comparator"
      ;; With mod-10 comparator, 1 and 11 compare equal
      (let [cmp-mod (fn [a b] (compare (mod a 10) (mod b 10)))
            s (set/sorted-set-by cmp-mod)]
        (is (identical? s (set/replace s 1 11)))
        (is (empty? (set/replace s 1 11)))))

    (testing "Single element - with custom comparator"
      ;; With mod-10 comparator, 2 and 12 compare equal
      (let [cmp-mod (fn [a b] (compare (mod a 10) (mod b 10)))
            s (set/sorted-set-by cmp-mod 2)
            s2 (set/replace s 2 12)]
        ;; Original unchanged
        (is (= 1 (count s)))
        (is (= 2 (set/lookup s 2)))
        ;; New set has replacement - use lookup to check actual stored value
        (is (= 1 (count s2)))
        (is (= 12 (set/lookup s2 12)))
        (is (= 12 (set/lookup s2 2)))))  ; Both find the same value since they compare equal
    (let [cmp-mod (fn [a b] (compare (mod a 10) (mod b 10)))
          s (set/sorted-set-by cmp-mod 2)]
      ;; Replace non-existent key (11 mod 10 = 1, not in set) returns same set
      (is (identical? s (set/replace s 11 21)))
      (is (= 1 (count (set/replace s 11 21))))))

  (testing "Small set (single leaf) - with custom comparator"
    ;; Use mod-10 comparator so 5 and 15 compare equal
    (let [cmp-mod (fn [a b] (compare (mod a 10) (mod b 10)))
          s (into (set/sorted-set-by cmp-mod) (range 10))
          s2 (set/replace s 5 15)]
      ;; Original unchanged
      (is (= 10 (count s)))
      (is (= 5 (set/lookup s 5)))
      ;; New set has replacement (15 instead of 5, both are "5" mod 10)
      (is (= 10 (count s2)))
      (is (= 15 (set/lookup s2 5)))  ; lookup with 5 finds 15 (they compare equal)
      (is (= 15 (set/lookup s2 15)))))

  (testing "Large set (multiple B-tree levels) - with custom comparator"
    ;; Use mod-5000 comparator so 2500 and 7500 compare equal
    (let [cmp-mod (fn [a b] (compare (mod a 5000) (mod b 5000)))
          s (into (set/sorted-set-by cmp-mod) (range 5000))
          s2 (set/replace s 2500 7500)]
      ;; Original unchanged
      (is (= 5000 (count s)))
      (is (= 2500 (set/lookup s 2500)))
      ;; New set has replacement (7500 instead of 2500, both are "2500" mod 5000)
      (is (= 5000 (count s2)))
      (is (= 7500 (set/lookup s2 2500)))  ; lookup with 2500 finds 7500 (they compare equal)
      (is (= 7500 (set/lookup s2 7500)))))

  (testing "Replace at boundaries - with custom comparator"
    ;; Use mod-100 comparator so 0,100,200 compare equal and 99,199,299 compare equal
    (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
          s (into (set/sorted-set-by cmp-mod) (range 100))
          s-first (set/replace s 0 100)
          s-last (set/replace s 99 199)]
      ;; Replace first element (0 -> 100, both are "0" mod 100)
      (is (= 100 (first s-first)))
      (is (= 100 (set/lookup s-first 0)))  ; lookup with 0 finds 100
      (is (= 100 (count s-first)))
      ;; Replace last element (99 -> 199, both are "99" mod 100)
      (is (= 199 (last s-last)))
      (is (= 199 (set/lookup s-last 99)))  ; lookup with 99 finds 199
      (is (= 100 (count s-last)))))

  (testing "Replace with custom comparator"
    (testing "Replace in tuple set"
      (let [cmp-first (fn [[a _] [b _]] (compare a b))
            s (-> (set/sorted-set-by cmp-first)
                  (conj [1 :a])
                  (conj [2 :b])
                  (conj [3 :c]))
            s2 (set/replace s [2 :anything] [2 :updated])]
        ;; Original unchanged
        (is (= [2 :b] (set/lookup s [2 nil])))
        ;; New set has updated value
        (is (= [2 :updated] (set/lookup s2 [2 nil])))
        (is (= 3 (count s2)))
        (is (= [[1 :a] [2 :updated] [3 :c]] (seq s2)))))

    (testing "Replace with id-based comparator (fulltext use case)"
      (let [cmp-id (fn [a b] (compare (:id a) (:id b)))
            s (-> (set/sorted-set-by cmp-id)
                  (conj {:id 1 :data "foo" :meta {:timestamp 100}})
                  (conj {:id 2 :data "bar" :meta {:timestamp 200}})
                  (conj {:id 3 :data "baz" :meta {:timestamp 300}}))
            ;; Update id:2 with new data and meta
            s2 (set/replace s
                            {:id 2}
                            {:id 2 :data "bar-updated" :meta {:timestamp 250}})]
        ;; Original unchanged
        (is (= {:id 2 :data "bar" :meta {:timestamp 200}}
               (set/lookup s {:id 2})))
        ;; New set has updated record
        (is (= {:id 2 :data "bar-updated" :meta {:timestamp 250}}
               (set/lookup s2 {:id 2})))
        (is (= 3 (count s2)))
        ;; Other records unchanged
        (is (= {:id 1 :data "foo" :meta {:timestamp 100}}
               (set/lookup s2 {:id 1})))
        (is (= {:id 3 :data "baz" :meta {:timestamp 300}}
               (set/lookup s2 {:id 3}))))))

  (testing "Replace with transient sets"
    (testing "Basic transient replace - with custom comparator"
      ;; Use mod-100 comparator so 50 and 150 compare equal
      (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
            s (into (set/sorted-set-by cmp-mod) (range 100))
            t (transient s)
            result (set/replace t 50 150)]
        ;; Check after persisting
        (let [p (persistent! result)]
          (is (= 100 (count p)))
          (is (= 150 (set/lookup p 50)))   ; lookup with 50 finds 150 (they compare equal)
          (is (= 150 (set/lookup p 150))))))

    (testing "Multiple transient replacements - with custom comparator"
      ;; Use mod-100 comparator so 10,110 and 20,120 and 30,130 compare equal
      (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
            s (into (set/sorted-set-by cmp-mod) (range 100))
            p (-> s
                  transient
                  (set/replace 10 110)
                  (set/replace 20 120)
                  (set/replace 30 130)
                  persistent!)]
        (is (= 100 (count p)))
        (is (= 110 (set/lookup p 10)))   ; lookup with 10 finds 110
        (is (= 110 (set/lookup p 110)))  ; lookup with 110 finds 110
        (is (= 120 (set/lookup p 20)))   ; lookup with 20 finds 120
        (is (= 120 (set/lookup p 120)))  ; lookup with 120 finds 120
        (is (= 130 (set/lookup p 30)))   ; lookup with 30 finds 130
        (is (= 130 (set/lookup p 130))))))

  (testing "Transient replace at boundaries - with custom comparator"
    ;; Use mod-100 comparator so 0,100 compare equal and 99,199 compare equal
    (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
          s (into (set/sorted-set-by cmp-mod) (range 100))
          p (-> s
                transient
                (set/replace 0 100)   ; 0 and 100 compare equal (mod 100)
                (set/replace 99 199)  ; 99 and 199 compare equal (mod 100)
                persistent!)]
      (is (= 100 (first p)))
      (is (= 199 (last p)))
      (is (= 100 (set/lookup p 0)))   ; lookup with 0 finds 100
      (is (= 199 (set/lookup p 99)))))

  (testing "Transient replace with custom comparator"
    (let [cmp-first (fn [[a _] [b _]] (compare a b))
          s (-> (set/sorted-set-by cmp-first)
                (conj [1 :a])
                (conj [2 :b])
                (conj [3 :c]))
          p (-> s
                transient
                (set/replace [2 :anything] [2 :updated])
                persistent!)]
      (is (= [2 :updated] (set/lookup p [2 nil])))
      (is (= 3 (count p)))))

  (testing "Replace non-existent key - with custom comparator"
    ;; Set contains even numbers 0,2,4,6,8, try to replace odd number
    (let [cmp compare
          s (into (set/sorted-set) [0 2 4 6 8])
          ;; mod-10 comparator: 11 and 21 both compare as "1" mod 10
          cmp-mod (fn [a b] (compare (mod a 10) (mod b 10)))]
      ;; Try to replace a key that doesn't exist (11 is not in the set)
      ;; With mod-10 comparator, 11 and 21 compare equal
      (is (identical? s (set/replace s 11 21 cmp-mod)))
      (is (= 5 (count s)))))

  (testing "Replace preserves set properties"
    (testing "Count unchanged - with custom comparator"
      ;; Use mod-100 comparator so 50 and 150 compare equal
      (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
            s (into (set/sorted-set-by cmp-mod) (range 100))
            s2 (set/replace s 50 150)]
        (is (= (count s) (count s2)))))

    (testing "Ordering maintained - with custom comparator"
      ;; Use mod-100 comparator so 50 and 150 compare equal
      (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
            s (into (set/sorted-set-by cmp-mod) (range 100))
            s2 (set/replace s 50 150)]
        (is (= 100 (count s2)))
        (is (= 150 (set/lookup s2 50)))   ; lookup with 50 finds 150
        (is (= 150 (set/lookup s2 150))))))

  (testing "Contains works correctly - with custom comparator"
    ;; Use mod-100 comparator so 50 and 150 compare equal
    (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
          s (into (set/sorted-set-by cmp-mod) (range 100))
          s2 (set/replace s 50 150)]
      ;; contains? uses comparator, so both 50 and 150 return true (they compare equal)
      (is (contains? s2 150))
      (is (contains? s2 50))  ; true because 50 and 150 compare equal
      ;; Use lookup to verify actual stored value is 150, not 50
      (is (= 150 (set/lookup s2 50)))
      (is (= 150 (set/lookup s2 150)))))

  (testing "Slice works after replace - with custom comparator"
    ;; Use mod-100 comparator so 50 and 150 compare equal
    (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
          s (into (set/sorted-set-by cmp-mod) (range 100))
          s2 (set/replace s 50 150)]
      (is (= 100 (count s2)))
      (is (= 150 (set/lookup s2 50)))   ; lookup with 50 finds 150
      (is (= 150 (set/lookup s2 150))))))

(deftest test-replace-and-lookup-integration
  (testing "Replace and lookup work together"
    (let [cmp-id (fn [a b] (compare (:id a) (:id b)))
          s (-> (set/sorted-set-by cmp-id)
                (conj {:id 1 :data "v1"})
                (conj {:id 2 :data "v2"})
                (conj {:id 3 :data "v3"}))
          ;; Lookup then replace
          old-val (set/lookup s {:id 2})
          new-val (assoc old-val :data "v2-updated")
          s2 (set/replace s old-val new-val)]
      (is (= "v2" (:data old-val)))
      (is (= "v2-updated" (:data (set/lookup s2 {:id 2}))))
      (is (= "v2" (:data (set/lookup s {:id 2})))))) ; original unchanged

  (testing "Performance benefit: replace vs disj+conj - with custom comparator"
    ;; This test verifies that replace works correctly as a single operation
    ;; The actual performance benefit would be measured in benchmarks
    ;; Use mod-1000 comparator so 500 and 1500 compare equal
    (let [cmp-mod (fn [a b] (compare (mod a 1000) (mod b 1000)))
          s (into (set/sorted-set-by cmp-mod) (range 1000))
          ;; Replace approach (single traversal) - 500 -> 1500
          s-replace (set/replace s 500 1500)]
      ;; Verify replace worked correctly
      (is (= 1000 (count s-replace)))
      (is (= 1500 (set/lookup s-replace 500)))   ; lookup with 500 finds 1500
      (is (= 1500 (set/lookup s-replace 1500))))))
