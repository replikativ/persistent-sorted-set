(ns org.replikativ.persistent-sorted-set.test.resident-api
  (:require [clojure.test :refer [deftest is]]
            [org.replikativ.persistent-sorted-set :as pss]))

(deftest resident-public-api
  (doseq [order [compare #(compare %2 %1)]]
    (let [cmp (fn [a b] (order (first a) (first b)))
          empty-tree (pss/sorted-set* {:branching-factor 4 :comparator cmp :meta {:index :test}})
          versions (reductions #(pss/conj %1 %2 cmp) empty-tree
                               (map (fn [i] [(mod (* i 7) 31) i]) (range 31)))
          tree (last versions)
          expected (sort cmp (map (fn [i] [(mod (* i 7) 31) i]) (range 31)))]
      (is (= {:index :test} (meta tree)))
      (is (thrown? #?(:cljs js/Error :clj Throwable) (pss/conj tree nil cmp)))
      (is (= (range 32) (map count versions)))
      (is (= expected (seq tree)))
      (is (identical? tree (pss/conj tree [0 :duplicate] cmp)))
      (is (= [7 1] (pss/lookup tree [7 :probe])))
      (is (nil? (pss/lookup tree [31] cmp)))
      (doseq [lo [nil [-1] [7] [32]]
              hi [nil [0] [7] [30]]
              :when (or (nil? lo) (nil? hi) (not (pos? (cmp lo hi))))]
        (let [xs (seq (filter #(and (or (nil? lo) (not (neg? (cmp % lo))))
                                    (or (nil? hi) (not (pos? (cmp % hi)))))
                              expected))]
          (is (= xs (pss/slice tree lo hi cmp)))))
      (is (= [[0 0]] (vec (pss/slice (second versions) nil nil)))))))
