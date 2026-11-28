(ns branch-cases
  (:require [leaf-cases :refer [ordered]]
            #?@(:jolt [[org.replikativ.persistent-sorted-set.branch :as branch]
                       [org.replikativ.persistent-sorted-set.leaf :as leaf]
                       [org.replikativ.persistent-sorted-set.impl.node :as node]]
                :clj [[org.replikativ.persistent-sorted-set :as pss]]))
  #?@(:jolt [] :clj [(:import [org.replikativ.persistent_sorted_set ANode Branch PersistentSortedSet])]))

(def settings {:branching-factor 4 :measure ordered :diff-buf-size 0})

#?(:jolt
   (defn- result [opts value]
     (if (:sync? opts) value (value identity (fn [e] (throw e))))))

(defn- empty-tree [cmp]
  #?(:jolt (leaf/->Leaf (object-array 0) settings [])
     :clj (let [s (pss/sorted-set* (assoc settings :comparator cmp))]
            (.forceComputeMeasure (.root ^PersistentSortedSet s) nil)
            s)))

(defn- insert [tree k cmp opts]
  #?(:jolt (let [nodes (result opts (node/add tree nil k cmp opts))]
             (cond (nil? nodes) tree
                   (= 1 (alength nodes)) (aget nodes 0)
                   :else (branch/promote-root nodes settings cmp -1)))
     :clj (conj tree k)))

(defn- snapshot [n]
  (let [level #?(:jolt (node/level n) :clj (.level ^ANode n))
        keys #?(:jolt (vec (.-keys n)) :clj (vec (.keys ^ANode n)))]
    [level keys
     #?(:jolt (node/subtree-count n) :clj (.subtreeCount n))
     #?(:jolt (node/measure n) :clj (.measure ^ANode n))
     (when (pos? level)
       (mapv (fn [idx]
               (snapshot #?(:jolt (branch/child n nil idx {:sync? true})
                            :clj (.child ^Branch n nil (int idx)))))
             (range (count keys))))]))

(defn- tree-snapshot [tree]
  (snapshot #?(:jolt tree :clj (.root ^PersistentSortedSet tree))))

(defn- lookup [tree k cmp opts]
  #?(:jolt (result opts (node/lookup tree nil k cmp opts))
     :clj (.lookup ^PersistentSortedSet tree k cmp)))

(defn -main [& _]
  (let [ascending (range 22)
        shuffled (map #(mod (* 7 %) 22) ascending)]
    (doseq [ks [ascending (reverse ascending) shuffled]
            cmp [compare #(compare %2 %1)]
            sync? [true false]]
      (let [opts {:sync? sync?}
            versions (vec (reductions #(insert %1 %2 cmp opts) (empty-tree cmp) ks))
            final (peek versions)]
        (assert (>= (first (tree-snapshot final)) 2))
        (assert (identical? final (insert final (first ks) cmp opts)))
        ;; Inspect every retained root after later versions have finished growing.
        (doseq [[i tree] (map-indexed vector versions)]
          (let [expected (set (take i ks))
                snap (tree-snapshot tree)]
            (assert (= (vec (sort cmp expected)) (nth snap 3)) (pr-str [i ks snap]))
            (assert (= (count expected) (nth snap 2)))
            (assert (= (mapv expected ascending) (mapv #(lookup tree % cmp opts) ascending)))
            (prn snap)))))))
