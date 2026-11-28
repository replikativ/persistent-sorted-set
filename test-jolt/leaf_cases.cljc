(ns leaf-cases
  #?(:jolt (:require [org.replikativ.persistent-sorted-set.leaf :as leaf]
                     [org.replikativ.persistent-sorted-set.impl.node :as node]
                     [org.replikativ.persistent-sorted-set.impl.measure :as m])
     :clj (:import [org.replikativ.persistent_sorted_set Leaf IMeasure Settings RefType])))

;; A noncommutative measure exposes ordering mistakes that counts and sums hide.
(def ordered
  #?(:jolt (reify m/IMeasure
             (identity-measure [_] [])
             (extract [_ k] [k])
             (merge-measure [_ a b] (into a b))
             (remove-measure [_ _ _ recompute] (recompute))
             (weight [_ v] (count v)))
     :clj (reify IMeasure
            (identity [_] [])
            (extract [_ k] [k])
            (merge [_ a b] (into a b))
            (commutativeMerge [_] false)
            (remove [_ _ _ recompute] (.get ^java.util.function.Supplier recompute))
            (weight [_ v] (count v)))))

(defn snapshot [leaf]
  #?(:jolt [(vec (.-keys leaf)) (node/measure leaf) (node/max-key leaf) (node/subtree-count leaf)]
     :clj [(vec (.keys ^Leaf leaf)) (.measure ^Leaf leaf) (.maxKey ^Leaf leaf) (.subtreeCount ^Leaf leaf)]))

#?(:jolt
   (defn- result [sync? value]
     (if sync? value (value identity (fn [err] (throw err))))))

(defn exercise [bf keys k cmp warm? sync?]
  (let [opts {:sync? sync?}
        settings #?(:jolt {:branching-factor bf :measure ordered}
                    :clj (Settings. bf RefType/STRONG ordered))
        source #?(:jolt (leaf/->Leaf (object-array keys) settings nil)
                  :clj (Leaf. keys settings))
        _ (when warm?
            #?(:jolt (result sync? (node/force-compute-measure source nil ordered opts))
               :clj (.forceComputeMeasure ^Leaf source nil)))
        before (snapshot source)
        added #?(:jolt (result sync? (node/add source nil k cmp opts))
                 :clj (.add ^Leaf source nil k cmp settings))
        result (mapv snapshot added)]
    (assert (= before (snapshot source)))
    result))

(defn -main [& _]
  (doseq [bf [4 5 8]
          descending? [false true]
          warm? [false true]
          sync? [true false]
          size [(dec bf) bf]
          :let [cmp (if descending? #(compare %2 %1) compare)
                keys (vec (sort cmp (map #(* 2 %) (range size))))]
          k (concat [-1] (range (* 2 size)))]
    (let [result (exercise bf keys k cmp warm? sync?)
          sorted-keys (vec (sort cmp (conj keys k)))
          split (quot (count sorted-keys) 2)
          expected (cond (some #{k} keys) []
                         (< size bf) [sorted-keys]
                         :else [(subvec sorted-keys 0 split) (subvec sorted-keys split)])]
      (assert (= (mapv (fn [ks] [ks (when warm? ks) (peek ks) (count ks)]) expected)
                 result) (pr-str [bf keys k warm? sync? result]))
      (prn [bf descending? warm? sync? size k result]))))
