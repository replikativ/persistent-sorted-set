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
    (testing "Empty set"
      (let [s (set/sorted-set)]
        (is (identical? s (set/replace s 1 2)))
        (is (= #{} (set/replace s 1 2)))))

    (testing "Single element"
      (let [s (set/sorted-set 42)
            s2 (set/replace s 42 43)]
        ;; Original unchanged
        (is (= #{42} s))
        (is (contains? s 42))
        (is (not (contains? s 43)))
        ;; New set has replacement
        (is (= #{43} s2))
        (is (contains? s2 43))
        (is (not (contains? s2 42))))
      (let [s (set/sorted-set 42)]
        ;; Replace non-existent key returns same set
        (is (identical? s (set/replace s 41 43)))
        (is (= #{42} (set/replace s 41 43)))))

    (testing "Small set (single leaf)"
      (let [s (into (set/sorted-set) (range 10))
            s2 (set/replace s 5 55)]
        ;; Original unchanged
        (is (= (range 10) (seq s)))
        (is (contains? s 5))
        (is (not (contains? s 55)))
        ;; New set has replacement
        (is (= [0 1 2 3 4 55 6 7 8 9] (seq s2)))
        (is (contains? s2 55))
        (is (not (contains? s2 5)))))

    (testing "Large set (multiple B-tree levels)"
      (let [s (into (set/sorted-set) (range 5000))
            s2 (set/replace s 2500 25000)]
        ;; Original unchanged
        (is (= 5000 (count s)))
        (is (contains? s 2500))
        (is (not (contains? s 25000)))
        ;; New set has replacement
        (is (= 5000 (count s2)))
        (is (contains? s2 25000))
        (is (not (contains? s2 2500)))
        (is (= 2499 (nth (seq s2) 2499)))
        (is (= 2501 (nth (seq s2) 2500)))
        (is (= 25000 (last s2)))))

    (testing "Replace at boundaries"
      (let [s (into (set/sorted-set) (range 100))
            s-first (set/replace s 0 -1)
            s-last (set/replace s 99 999)]
        ;; Replace first element
        (is (= -1 (first s-first)))
        (is (not (contains? s-first 0)))
        (is (= 99 (count s-first)))
        ;; Replace last element
        (is (= 999 (last s-last)))
        (is (not (contains? s-last 99)))
        (is (= 99 (count s-last))))))

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
    (testing "Basic transient replace"
      (let [s (into (set/sorted-set) (range 100))
            t (transient s)
            result (set/replace t 50 500)]
        ;; Returns same transient instance (mutated in place)
        (is (identical? t result))
        ;; Check after persisting
        (let [p (persistent! result)]
          (is (= 100 (count p)))
          (is (contains? p 500))
          (is (not (contains? p 50))))))

    (testing "Multiple transient replacements"
      (let [s (into (set/sorted-set) (range 100))
            p (-> s
                  transient
                  (set/replace 10 1000)
                  (set/replace 20 2000)
                  (set/replace 30 3000)
                  persistent!)]
        (is (= 100 (count p)))
        (is (contains? p 1000))
        (is (contains? p 2000))
        (is (contains? p 3000))
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
    (testing "Count unchanged"
      (let [s (into (set/sorted-set) (range 100))
            s2 (set/replace s 50 500)]
        (is (= (count s) (count s2)))))

    (testing "Ordering maintained"
      (let [s (into (set/sorted-set) (range 100))
            s2 (set/replace s 50 55)]
        (is (= (concat (range 50) [55] (range 51 100))
               (seq s2)))))

    (testing "Contains works correctly"
      (let [s (into (set/sorted-set) (range 100))
            s2 (set/replace s 50 500)]
        (is (contains? s2 500))
        (is (not (contains? s2 50)))))

    (testing "Slice works after replace"
      (let [s (into (set/sorted-set) (range 100))
            s2 (set/replace s 50 55)]
        (is (= [49 55 51] (vec (set/slice s2 49 51))))))))

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

  (testing "Performance benefit: replace vs disj+conj"
    ;; This test verifies that replace works correctly as a single operation
    ;; The actual performance benefit would be measured in benchmarks
    (let [s (into (set/sorted-set) (range 1000))
          ;; Replace approach (single traversal)
          s-replace (set/replace s 500 5000)
          ;; Disj+conj approach (two traversals)
          s-disj-conj (-> s (disj 500) (conj 5000))]
      ;; Both approaches should produce the same result
      (is (= s-replace s-disj-conj))
      (is (= (seq s-replace) (seq s-disj-conj))))))
