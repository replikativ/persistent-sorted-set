(ns me.tonsky.persistent-sorted-set.test.storage
  (:require-macros [me.tonsky.persistent-sorted-set.test.macros :refer [testing-group]])
  (:require
   [await-cps :refer [await run-async] :refer-macros [async] :rename {run-async run}]
   ; [is.simm.lean-cps.async :refer [await run] :refer-macros [async]]
   [cljs.test :as test :refer [is are deftest testing]]
   [clojure.edn :as edn]
   [clojure.string :as str]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.impl.storage :refer [IStorage]]
   [me.tonsky.persistent-sorted-set.btset :refer [BTSet]]
   [me.tonsky.persistent-sorted-set.leaf :refer [Leaf] :as leaf]
   [me.tonsky.persistent-sorted-set.branch :refer [Branch] :as branch]
   [me.tonsky.persistent-sorted-set.test.storage.util
    :refer [storage async-storage branch? leaf?]]))

(defn children [node] (some->> (.-children node) (filter some?)))

(defn ks [node] (some->> (.-keys node) (filterv some?)))

#!------------------------------------------------------------------------------

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

(deftest test-small-sync-restoration
  (and
   (testing "seq"
     (let [storage (storage)
           stored (into (set/sorted-set) (range 10 20))
           address (set/store stored storage)]
       (and
        (testing-group "control"
         (is (= (range 10 20) (seq (into (set/sorted-set) (range 10 20)))) "control"))
        (testing-group "flushed"
         (is (= (range 10 20) (seq stored)) "seq works on set that just flushed"))
        (testing-group "restored"
         (is (= (range 10 20) (seq (set/restore address storage))) "seq works on unrealized lazy state")))))
   (testing "slice"
     (let [storage (storage)
           stored  (into (set/sorted-set) (range 0 40))
           address (set/store stored storage)]
       (and
        (testing-group "control"
         (is (= (range 10 20) (set/slice (into (set/sorted-set) (range 0 40)) 10 19)) "control"))
        (testing-group "flushed"
         (is (= (range 10 20) (set/slice stored 10 19)) "slice works on set that just flushed"))
        (testing-group "restored"
         (is (= (range 10 20) (set/slice (set/restore address storage) 10 19)) "slice works on unrealized lazy state")))))
   (testing "slice reversed order"
     (let [storage (storage)
           stored  (into (set/sorted-set) (reverse (range 0 100)))
           address (set/store stored storage)]
       (and
        (testing-group "control"
          (is (= (range 10 20)
                 (set/slice (into (set/sorted-set) (reverse (range 0 100))) 10 19))
              "control"))
        (testing-group "flushed"
          (is (= (range 10 20) (set/slice stored 10 19))
              "slice works on set that just flushed"))
        (testing-group "restored"
          (is (= (range 10 20)
                 (set/slice (set/restore address storage) 10 19))
              "slice works on unrealized lazy state")))))
   (testing "rseq"
     (let [storage (storage)
           stored  (into (set/sorted-set) (range 10 20))
           address (set/store stored storage)]
       (and
        (testing-group "control"
          (is (= (range 19 9 -1) (rseq (into (set/sorted-set) (range 10 20)))) "control"))
        (testing-group "flushed"
          (is (= (range 19 9 -1) (rseq stored)) "rseq works on set that just flushed"))
        (testing-group "restored"
          (is (= (range 19 9 -1) (rseq (set/restore address storage))) "rseq works on unrealized lazy state")))))
   (testing "rslice"
     (let [storage (storage)
           stored  (into (set/sorted-set) (range 0 40))
           address (set/store stored storage)]
       (and
        (testing-group "control"
          (is (= (range 19 9 -1)
                 (set/rslice (into (set/sorted-set) (range 0 40)) 19 10))
              "control"))
        (testing-group "flushed"
          (is (= (range 19 9 -1) (set/rslice stored 19 10))
              "rslice works on set that just flushed"))
        (testing-group "restored"
          (is (= (range 19 9 -1) (set/rslice (set/restore address storage) 19 10))
              "rslice works on unrealized lazy state")))))
   (testing "reversing rseq is original sort"
     (let [storage (storage)
           stored  (into (set/sorted-set) (shuffle (range 0 5001)))
           address (set/store stored storage)]
       (and
        (testing-group "control"
         (let [x (set/rslice (into (set/sorted-set) (shuffle (range 0 5001))) 5000 nil)]
           (is (= x (some-> x rseq reverse)) "control")))
        (testing-group "flushed"
         (let [x (set/rslice stored 5000 nil)]
           (is (= x (some-> x rseq reverse)) "rseq reversibility on flushed set")))
        (testing-group "restored"
         (let [x (set/rslice (set/restore address storage) 5000 nil)]
           (is (= x (some-> x rseq reverse)) "rseq reversibility on restored set"))))))))



; (deftest sync-storage-test
;   (testing "Full integration test with sync storage - 10k elements"
;     (let [storage (utils/make-sync-storage)
;           s0 (set/sorted-set* {:storage storage})
;           n 10000
;           nums (shuffle (range n))
;           s-final (reduce set/conj s0 nums)]
;       (and
;        (is (= n (count s-final)))
;        (is (= 0 (first s-final)))
;        (is (= 9999 (last s-final)))
;        (testing-group "slicing on original set"
;                       (let [slice (set/slice s-final 2500 2525)]
;                         (is (= (range 2500 2526) (vec slice)))))
;        (testing-group "reverse slicing: from key-from to key-to"
;                       (let [rslice (set/rslice s-final 7525 7500)]
;                         (and
;                          (is (some? rslice))
;                          (is (= (reverse (range 7500 7526)) (vec rslice))))))
;        (testing-group "Store to storage"
;                       (let [store-info (set/store s-final {:sync? true})]
;                         (and
;                          (is (some? store-info))
;                          (is (map? store-info))
;                          (is (:root-address store-info))
;                          (and
;                           (testing  "Check storage size"
;                             (let [storage-data @(:*store storage)]
;                               (is (> 100 (count storage-data)) "Should have many nodes stored")))
;                           (testing "Restore from storage"
;                             (let [restored (set/restore store-info storage {:sync? true})]
;                               (and
;                                (is (= n (count restored)))
;                                (is (= 0 (first restored)))
;                                (is (= 9999 (last restored)))
;                                (testing "Check a sample of elements using contains?"
;                                  (loop [indices (range 0 10000 1000)]
;                                    (let [i (first indices)]
;                                      (if (nil? i)
;                                        true
;                                        (if-not (is (contains? restored i) (str "Missing element: " i))
;                                          false
;                                          (recur (rest indices)))))))
;                                (testing "Verify slices on restored set"
;                                  (and
;                                   (is (= (range 100) (vec (set/slice restored 0 99))))
;                                   (is (= (range 5000 5100) (vec (set/slice restored 5000 5099))))
;                                   (is (= (range 9900 10000) (vec (set/slice restored 9900 9999))))))
;                                (testing "conj on restored set"
;                                  (let [restored-with-new (set/conj restored 10000)]
;                                    (and
;                                     (is (= (inc n) (count restored-with-new)))
;                                     (is (contains? restored-with-new 10000)))))
;                                (testing "disj on restored set"
;                                  (let [restored-without (set/disj restored 5000)]
;                                    (and
;                                     (is (= (dec n) (count restored-without)))
;                                     (is (not (contains? restored-without 5000))))))
;                                (testing "iteration over restored set"
;                                  (and
;                                   (is (= (range 100) (take 100 restored)))
;                                   (is (= (range 9900 10000) (take 100 (drop 9900 restored)))))))))))))))))

