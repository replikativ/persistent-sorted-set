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

(deftest cold-mutations
  (let [disk (atom {}) storage (backend disk (atom []))
        cmp (fn [a b] (compare (first a) (first b)))
        items (mapv #(vector % :old) (range 64))
        original (reduce s/conj (s/sorted-set* {:branching-factor 4 :storage storage}) items)
        open #(s/restore % storage {:meta {:index :test}})
        a (open (s/store original))
        b (s/replace a [31 :probe] [31 :new] cmp)
        c (s/disj (open (s/store b)) [15 :probe] cmp {:sync? true})
        restored (open (s/store c))
        empty-set (reduce #(s/disj %1 %2 {:sync? true}) restored (seq restored))]
    (is (identical? a (s/disj a [70 :old])))
    (is (identical? a (s/replace a [70 :old] [70 :old] {:sync? true})))
    (is (thrown? Throwable (s/replace a [31 :old] nil)))
    (is (thrown? Throwable (s/disj a [31 :old] {:sync? false})))
    (is (= (meta a) (meta c)))
    (is (= items (seq (open (s/store a)))))
    (is (= (assoc items 31 [31 :new]) (seq (open (s/store b)))))
    (is (= (remove #(= 15 (first %)) (assoc items 31 [31 :new])) (seq restored)))
    (is (= 63 (count restored)))
    (is (empty? (seq (open (s/store empty-set)))))
    (is (zero? (count empty-set)))))
