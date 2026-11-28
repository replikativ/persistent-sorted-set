(ns org.replikativ.persistent-sorted-set.test.portable-storage
  (:require [org.replikativ.persistent-sorted-set.branch :as branch]
            [org.replikativ.persistent-sorted-set.impl.node :as node]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage]
            [org.replikativ.persistent-sorted-set.leaf :as leaf]
            #?(:clj [org.replikativ.persistent-sorted-set.impl.tree-macros :refer [read-field]]))
  #?(:cljs (:require-macros [org.replikativ.persistent-sorted-set.impl.tree-macros :refer [read-field]])))

(defn backend [disk reads]
  (reify storage/IStorage
    (store [_ n _]
      (let [address (inc (count @disk))]
        (swap! disk assoc address
               {:level (node/level n) :keys (vec (seq (.-keys n))) :settings (.-settings n)
                :subtree-count (node/subtree-count n)
                :measure (node/measure n)
                :child-measures (when (pos? (node/level n))
                                  (some-> (read-field n :_childMeasures) vec))
                :addresses (when (pos? (node/level n)) (vec (seq (read-field n :addresses))))})
        address))
    (restore [_ address opts]
      (swap! reads conj [address opts])
      (let [{:keys [level keys addresses settings measure] :as m} (@disk address)]
        (if (zero? level)
          (leaf/->Leaf (to-array keys) settings measure)
          (branch/from-map (assoc m :keys (to-array keys) :addresses (to-array addresses))))))
    (accessed [_ _])
    (markFreed [_ _])))

(defn tree-shape [s backend]
  (let [opts {:sync? true}
        root (or (.-root s) (storage/restore backend (.-address s) opts))]
    (letfn [(walk [n]
              (let [level (node/level n)
                    len (node/len n)]
                (cond-> {:level level :len len}
                  (pos? level) (assoc :children (mapv #(walk (branch/child n backend % opts)) (range len))))))]
      (walk root))))
