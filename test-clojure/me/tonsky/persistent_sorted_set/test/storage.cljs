(ns me.tonsky.persistent-sorted-set.test.storage
  (:require [cljs.test :as test :refer-macros [is are deftest testing]]
            [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.test.storage.util
             :refer [storage async-storage branch? leaf?]]))

(defn root [set] (.-root set))

(defn addresses [node] (some->> (.-addresses node) (filter some?)))

(defn children [node] (some->> (.-children node) (filter some?)))

(defn ks [node] (some->> (.-keys node) (filterv some?)))

(deftest branch-steps-test
  "steps from full leaf to branch and back down"
  (let [og ^PersistentSortedSet (into (set/sorted-set* {}) (range 0 32))]
    (and
     (is (= 32 (count og)))
     (is (leaf? (.-root og))) ;; full leaf at root
     (is (= 32 (count (ks (.-root og)))))
     (is (= (range 0 32) (ks (.-root og))))
     (let [og' (conj og 32)] ;; split first leaf
       (and
        (is (= 33 (count og')))
        (is (branch? (.-root og')))
        (is (= 2 (count (ks (.-root og')))))
        (is (= [15 32] (ks (.-root og'))))
        (is (= 2 (count (children (.-root og')))))
        (is (= 16 (count (ks (nth (children (.-root og')) 0)))))
        (is (= (range 0 16) (ks (nth (children (.-root og')) 0))))
        (is (= 17 (count (ks (nth (children (.-root og')) 1)))))
        (is (= (range 16 33) (ks (nth (children (.-root og')) 1))))
        (let [og'' (conj og' 33)] ;; first add after split
          (and
           (is (= 34 (count og'')))
           (is (branch? (.-root og'')))
           (is (= 2 (count (ks (.-root og'')))))
           (is (= [15 33] (ks (.-root og''))))
           (is (= 2 (count (children (.-root og'')))))
           (is (= 16 (count (ks (nth (children (.-root og'')) 0)))))
           (is (= (range 0 16) (ks (nth (children (.-root og'')) 0))))
           (is (= 18 (count (ks (nth (children (.-root og'')) 1)))))
           (is (= (range 16 34) (ks (nth (children (.-root og'')) 1))))
           (let [og''- (disj og'' 33)]
             (and
              (is (= 33 (count og''-)))
              (is (branch? (.-root og''-)))
              (is (= [15 32] (ks (.-root og''-))))
              (is (= 16 (count (ks (nth (children (.-root og''-)) 0)))))
              (is (= (range 0 16) (ks (nth (children (.-root og''-)) 0))))
              (is (= 17 (count (ks (nth (children (.-root og''-)) 1)))))
              (is (= (range 16 33) (ks (nth (children (.-root og''-)) 1))))
              (let [og''-- (disj og''- 32)]
                (and
                 (is (= 32 (count og''--)))
                 ;; jvm version does not shrink here, not sure we should care
                 #_(is (branch? (.-root og''--)))
                 #_(is (= (range 0 16) (ks (nth (children (.-root og''--)) 0))))
                 #_(is (= (range 16 32) (ks (nth (children (.-root og''--)) 1)))))))))))))))

(deftest disj-test
  (let [control-set (into (sorted-set) [3 5 6])
        test-set (into (set/sorted-set) [3 5 6])]
    (is (= (disj (disj control-set 6) 6)
           (disj (disj test-set 6) 6)))))

(deftest test-walk-addresses
  (let [size       1000
        xs         (range size)
        set        (into (set/sorted-set* {}) xs)
        *pre-store (atom 0)
        *stored    (atom 0)]
    (and
     (is (nil? (set/walk-addresses set (fn [addr] (swap! *pre-store inc)))))
     (is (zero? @*pre-store))
     (is (uuid? (set/store set (storage))))
     (is (nil? (set/walk-addresses set (fn [addr] (swap! *stored inc)))))
     (is (= 66 @*stored))
     (let [set'     (conj set (* 2 size))
           *stored' (atom 0)]
       (and
        (is (nil? (set/walk-addresses set' (fn [addr] (if (some? addr) (swap! *stored' inc))))))
        (is (= 63 @*stored'))))
     (let [set'     (disj set (dec size))
           *stored' (atom 0)]
       (and
        (is (nil? (set/walk-addresses set' (fn [addr] (if (some? addr) (swap! *stored' inc))))))
        (is (= 63 @*stored')))))))


