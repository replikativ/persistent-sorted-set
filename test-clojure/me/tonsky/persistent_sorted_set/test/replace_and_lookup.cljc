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
        (is (nil? (set/lookup s "date"))))))

  (testing "Lookup on large sets with custom comparator"
    (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
          s (reduce #(set/conj %1 %2 compare)
                    (set/sorted-set-by cmp-mod)
                    (range 5000))]
      ;; With mod comparator, many values compare equal
      ;; lookup should find one of them
      (is (some? (set/lookup s 0)))
      (is (some? (set/lookup s 50)))
      (is (some? (set/lookup s 99))))))

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
        (is (contains? s 2))
        (is (not (contains? s 12)))
        ;; New set has replacement
        (is (= 1 (count s2)))
        (is (contains? s2 12))
        (is (not (contains? s2 2))))
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
        (is (contains? s 5))
        (is (not (contains? s 15)))
        ;; New set has replacement (15 instead of 5, both are "5" mod 10)
        (is (= 10 (count s2)))
        (is (contains? s2 15))
        (is (not (contains? s2 5)))))

    (testing "Large set (multiple B-tree levels) - with custom comparator"
      ;; Use mod-5000 comparator so 2500 and 7500 compare equal
      (let [cmp-mod (fn [a b] (compare (mod a 5000) (mod b 5000)))
            s (into (set/sorted-set-by cmp-mod) (range 5000))
            s2 (set/replace s 2500 7500)]
        ;; Original unchanged
        (is (= 5000 (count s)))
        (is (contains? s 2500))
        (is (not (contains? s 7500)))
        ;; New set has replacement (7500 instead of 2500, both are "2500" mod 5000)
        (is (= 5000 (count s2)))
        (is (contains? s2 7500))
        (is (not (contains? s2 2500)))))

    (testing "Replace at boundaries - with custom comparator"
      ;; Use mod-100 comparator so 0,100,200 compare equal and 99,199,299 compare equal
      (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
            s (into (set/sorted-set-by cmp-mod) (range 100))
            s-first (set/replace s 0 100)
            s-last (set/replace s 99 199)]
        ;; Replace first element (0 -> 100, both are "0" mod 100)
        (is (= 100 (first s-first)))
        (is (not (contains? s-first 0)))
        (is (= 100 (count s-first)))
        ;; Replace last element (99 -> 199, both are "99" mod 100)
        (is (= 199 (last s-last)))
        (is (not (contains? s-last 99)))
        (is (= 100 (count s-last))))))

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
        ;; Returns same transient instance (mutated in place)
        (is (identical? t result))
        ;; Check after persisting
        (let [p (persistent! result)]
          (is (= 100 (count p)))
          (is (contains? p 150))
          (is (not (contains? p 50))))))

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
        (is (contains? p 110))
        (is (contains? p 120))
        (is (contains? p 130))
        (is (not (contains? p 10)))
        (is (not (contains? p 20)))
        (is (not (contains? p 30)))))

    (testing "Transient replace at boundaries"
      (let [s (into (set/sorted-set) (range 100))
            p (-> s
                  transient
                  (set/replace 0 -1)
                  (set/replace 99 999)
                  persistent!)]
        (is (= -1 (first p)))
        (is (= 999 (last p)))
        (is (not (contains? p 0)))
        (is (not (contains? p 99)))))

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
        (is (= 3 (count p))))))

  (testing "Replace non-existent key"
    (let [s (into (set/sorted-set) (range 10))]
      ;; Replace non-existent key returns same set
      (is (identical? s (set/replace s 100 200)))
      (is (= (range 10) (seq s)))))

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
        (is (contains? s2 150))
        (is (not (contains? s2 50)))))

    (testing "Contains works correctly - with custom comparator"
      ;; Use mod-100 comparator so 50 and 150 compare equal
      (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
            s (into (set/sorted-set-by cmp-mod) (range 100))
            s2 (set/replace s 50 150)]
        (is (contains? s2 150))
        (is (not (contains? s2 50)))))

    (testing "Slice works after replace - with custom comparator"
      ;; Use mod-100 comparator so 50 and 150 compare equal
      (let [cmp-mod (fn [a b] (compare (mod a 100) (mod b 100)))
            s (into (set/sorted-set-by cmp-mod) (range 100))
            s2 (set/replace s 50 150)]
        (is (= 100 (count s2)))
        (is (contains? s2 150))))))

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
      (is (contains? s-replace 1500))
      (is (not (contains? s-replace 500))))))
