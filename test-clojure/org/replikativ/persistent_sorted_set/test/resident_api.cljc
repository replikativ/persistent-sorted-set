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

(deftest public-replace-and-disj
  (let [by-id (fn [a b] (compare (first a) (first b)))
        entries (mapv #(vector % :original) (range 40))
        a (reduce #(pss/conj %1 %2 compare) (pss/sorted-set* {:branching-factor 4 :comparator compare}) entries)
        b (pss/replace a [20 :probe] [20 :changed] by-id)
        c (pss/disj b [2 :probe] by-id)]
    (is (= entries (seq a)))
    (is (= [20 :changed] (pss/lookup b [20 :probe] by-id)))
    (is (= 39 (count c)))
    (is (= (remove #(= 2 (first %)) (assoc entries 20 [20 :changed])) (seq c)))
    (is (identical? c (pss/disj c [2 :probe] by-id)))
    (is (identical? c (pss/replace c [2 :probe] [2 :missing] by-id)))))

(deftest multi-leaf-reverse-slice
  (let [items (mapv #(vector (quot % 8) %) (range 80))
        old (reduce #(pss/conj %1 %2 compare)
                    (pss/sorted-set* {:branching-factor 4 :comparator compare}) items)
        current (pss/disj old [5 42] compare)
        by-group #(compare (first %1) (first %2))]
    (doseq [[tree values] [[old items] [current (remove #{[5 42]} items)]]
            [lo hi] [[nil nil] [[3] [6]] [[5] [5]] [nil [0]] [[9] nil]
                     [[11] nil] [nil [-1]]]]
      (let [expected (filter #(and (or (nil? lo) (not (neg? (by-group % lo))))
                                   (or (nil? hi) (not (pos? (by-group % hi))))) values)]
        (is (= (seq expected) (pss/slice tree lo hi by-group)))
        (is (= (seq (reverse expected)) (pss/rslice tree hi lo by-group)))))))

(defn check-retained-versions
  "Run the same mixed updates on a public set or directly on the portable nodes."
  [empty-tree insert remove-key slice rslice lookup]
  (doseq [seed [13 47 101]]
    (let [states (reductions
                  (fn [[tree model] x]
                    (let [k (mod (quot x 4) 127)]
                      (if (odd? x)
                        [(insert tree k) (conj model k)]
                        [(remove-key tree k) (disj model k)])))
                  [empty-tree #{}]
                  (take 256 (iterate #(mod (+ (* 137 %) 187) 4093) seed)))
          ;; Finish all writes before inspecting old roots.
          retained (vec (take-nth 16 states))]
      (doseq [[tree model] retained]
        (let [expected (sort (filter #(<= 21 % 89) model))]
          (is (= (seq (sort model)) (slice tree nil nil)))
          (is (= (seq expected) (slice tree 21 89)))
          (is (= (seq (reverse expected)) (rslice tree 89 21)))
          (is (= (get model 42) (lookup tree 42))))))))

(deftest mixed-updates-keep-older-versions
  (check-retained-versions
   (pss/sorted-set* {:branching-factor 4 :diff-buf-size 0 :comparator compare})
   #(pss/conj %1 %2 compare) #(pss/disj %1 %2 compare)
   #(pss/slice %1 %2 %3 compare) #(pss/rslice %1 %2 %3 compare)
   #(pss/lookup %1 %2 compare)))
