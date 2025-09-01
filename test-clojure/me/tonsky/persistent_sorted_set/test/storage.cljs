(ns me.tonsky.persistent-sorted-set.test.storage
  (:require-macros [me.tonsky.persistent-sorted-set.test.macros :refer [testing-group]])
  (:require
   [await-cps :refer [await run-async] :refer-macros [async] :rename {run-async run}]
   ; [is.simm.lean-cps.async :refer [await run] :refer-macros [async]]
   [cljs.test :as test :refer [is are deftest testing]]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.test.storage.util
    :refer [storage async-storage]]))

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

(defn do-seek-slice-test []
  (async
   (and
    (testing "slicing together with seek"
      (testing "A"
        (and
         (testing "sync-control"
           (is (= (range 5000 7501) (-> (set/slice (apply set/sorted-set (range 10000)) 2500 7500)
                                      (set/seek 5000)))))
         (testing "async-control"
           (let [s  (apply set/sorted-set (range 10000))
                 sl (await (set/slice s 2500 7500 {:sync? false}))
                 sk (await (set/seek sl 5000 {:sync? false}))]
             (is (set/equiv-sequential? sk (range 5000 7501) {:sync? false}))))
         (testing "async restored"
           (let [s        (apply set/sorted-set (range 10000))
                 storage  (async-storage)
                 addr     (await (set/store s storage {:sync? false}))
                 restored (set/restore addr storage)
                 sl       (await (set/slice restored 2500 7500 {:sync? false}))
                 sk       (await (set/seek sl 5000 {:sync? false}))]
             (is (set/equiv-sequential? sk (range 5000 7501) {:sync? false}))))))
      (testing "B"
        (and
         (testing "sync-control"
           (is (= (list 7500)
                  (-> (set/slice (apply set/sorted-set (range 10000)) 2500 7500)
                    (set/seek 5000)
                    (set/seek 7500)))))
         (testing "async-control"
           (let [s   (apply set/sorted-set (range 10000))
                 sl  (await (set/slice s 2500 7500 {:sync? false}))
                 sk1 (await (set/seek sl 5000 {:sync? false}))
                 sk2 (await (set/seek sk1 7500 {:sync? false}))]
             (is (set/equiv-sequential? sk2 (list 7500) {:sync? false}))))
         (testing "async restored"
           (let [s        (apply set/sorted-set (range 10000))
                 storage  (async-storage)
                 addr     (await (set/store s storage {:sync? false}))
                 restored (set/restore addr storage)
                 sl       (await (set/slice restored 2500 7500 {:sync? false}))
                 sk1      (await (set/seek sl 5000 {:sync? false}))
                 sk2      (await (set/seek sk1 7500 {:sync? false}))]
             (is (set/equiv-sequential? sk2 (list 7500) {:sync? false}))))))
      (testing "C"
        (and
         (testing "sync-control"
           (is (= (range 5000 2499 -1)
                  (-> (set/rslice (apply set/sorted-set (range 10000)) 7500 2500)
                    (set/seek 5000)))))
         (testing "async-control"
           (let [s   (apply set/sorted-set (range 10000))
                 rs  (await (set/rslice s 7500 2500 {:sync? false}))
                 sk  (await (set/seek rs 5000 {:sync? false}))]
             (is (set/equiv-sequential? sk (range 5000 2499 -1) {:sync? false}))))
         (testing "async restored"
           (let [s        (apply set/sorted-set (range 10000))
                 storage  (async-storage)
                 addr     (await (set/store s storage {:sync? false}))
                 restored (set/restore addr storage)
                 rs       (await (set/rslice restored 7500 2500 {:sync? false}))
                 sk       (await (set/seek rs 5000 {:sync? false}))]
             (is (set/equiv-sequential? sk (range 5000 2499 -1) {:sync? false}))))))
      (testing "D"
        (and
         (testing "sync-control"
           (is (= (list 2500)
                  (-> (set/rslice (apply set/sorted-set (range 10000)) 7500 2500)
                    (set/seek 5000)
                    (set/seek 2500)))))
         (testing "async-control"
           (let [s   (apply set/sorted-set (range 10000))
                 rs  (await (set/rslice s 7500 2500 {:sync? false}))
                 sk1 (await (set/seek rs 5000 {:sync? false}))
                 _(assert (some? sk1))
                 sk2 (await (set/seek sk1 2500 {:sync? false}))]
             (is (set/equiv-sequential? sk2 (list 2500) {:sync? false}))))
         (testing "async restored"
           (let [s        (apply set/sorted-set (range 10000))
                 storage  (async-storage)
                 addr     (await (set/store s storage {:sync? false}))
                 restored (set/restore addr storage)
                 rs       (await (set/rslice restored 7500 2500 {:sync? false}))
                 sk1      (await (set/seek rs 5000 {:sync? false}))
                 sk2      (await (set/seek sk1 2500 {:sync? false}))]
             (is (set/equiv-sequential? sk2 (list 2500) {:sync? false}))))))))))

(deftest seek-slice-test
  (test/async done
    (run (do-seek-slice-test)
      (fn [_] (done))
      (fn [err]
        (js/console.warn "seek-slice-test failed")
        (is (nil? err))
        (js/console.error err)
        (done)))))

; (testing "simple async seek on forward async seq"
;   (loop [i 0]
;     (if (>= i 10)
;       true
;       (let [seek-loc (* 100 i)
;             after    (await (set/seek aseq seek-loc compare {:sync? false}))
;             ok       (await (set/equiv-sequential? after (range seek-loc size) {:sync? false}))]
;         (if (true? ok)
;           (do
;             (is (true? ok))
;             (recur (inc i)))
;           (is (true? ok) (str "i:" i ", seek-loc:" seek-loc)))))))

; (testing "multiple async seek chaining"
;   (let [s1 (await (set/seek aseq 250 compare {:sync? false}))
;         s2 (await (set/seek s1   500 compare {:sync? false}))
;         ok (await (set/equiv-sequential? s2 (range 500 size) {:sync? false}))]
;     (is (true? ok))))

; (testing "async seq behaviour after seek"
;   (let [tail (await (set/seek aseq 500 compare {:sync? false}))
;         ok   (await (set/equiv-sequential? tail (range 500 size) {:sync? false}))]
;     (is (true? ok))))

; (testing "async slicing together with seek"
;   (let [slc (await (set/slice (apply set/sorted-set (range 10000)) 2500 7500 {:sync? false}))
;         a1  (await (set/seek slc 5000 compare {:sync? false}))
;         a2  (await (set/seek a1  7500 compare {:sync? false}))
;         ok1 (await (set/equiv-sequential? a1 (range 5000 7501) {:sync? false}))
;         ok2 (await (set/equiv-sequential? a2 (list 7500)       {:sync? false}))]
;     (and (is (true? ok1))
;          (is (true? ok2)))))