(ns storage-delta
  (:require [clojure.test :refer [deftest is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.branch :as branch]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage]
            [org.replikativ.persistent-sorted-set.impl.node :as node]
            [org.replikativ.persistent-sorted-set.leaf :as leaf]))

(defn- backend [disk reads]
  (reify storage/IStorage
    (store [_ n _]
      (let [address (inc (count @disk))]
        (swap! disk assoc address
               {:level (node/level n) :keys (vec (seq (.-keys n))) :settings (.-settings n)
                :subtree-count (node/subtree-count n)
                :addresses (when (pos? (node/level n)) (vec (seq (.-addresses n))))})
        address))
    (restore [_ address opts]
      (swap! reads conj [address opts])
      (let [{:keys [level keys addresses settings] :as m} (@disk address)]
        (if (zero? level)
          (leaf/->Leaf (to-array keys) settings nil)
          (branch/from-map (assoc m :keys (to-array keys) :addresses (to-array addresses))))))
    (accessed [_ _])
    (markFreed [_ _])))

(deftest cold-delta
  (let [disk (atom {}) reads (atom []) storage (backend disk reads)
        a (reduce s/conj (s/sorted-set* {:branching-factor 4 :storage storage}) (range 80))
        ra (s/store a)
        previous (set (keys @disk))
        b (s/conj (s/restore ra storage) 80)
        rb (s/store b)
        x (s/restore ra storage)
        y (s/restore rb storage)
        visited (atom [])
        opts {:sync? true :await-read-through? true}]
    (reset! reads [])
    (is (nil? (s/walk-delta x (s/restore ra storage) #(swap! visited conj %))))
    (is (empty? @reads))
    (is (empty? @visited))
    (is (nil? (s/walk-delta x y storage #(do (swap! visited conj %) nil) opts)))
    (is (= (set (remove previous (keys @disk))) (set @visited)))
    (is (< 1 (count @visited) (count previous)))
    (is (< (count @reads) (count previous)))
    (is (every? #(= opts (second %)) @reads))
    (is (= (range 80) (seq x)))
    (is (= (range 81) (seq y)))
    (is (= 81 (count y)))
    (is (= [77 78 79 80] (vec (s/slice y 77 nil))))
    (is (thrown? Throwable (s/walk-delta x y storage identity {:sync? false})))))
