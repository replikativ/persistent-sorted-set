(ns org.replikativ.persistent-sorted-set.test.storage.slice-parity
  (:require [cljs.test :as test :refer-macros [is deftest testing]]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.test.storage.util
             :refer [storage async-storage]]))

(defn ->map [iterlike]
  {:left (.-left iterlike)
   :right (.-right iterlike)
   :keys (vec (.-keys iterlike))
   :idx (.-idx iterlike)})

(defn do-slice-parity-test []
  (async
   (let [s (into (set/sorted-set) (range 1024))]
     (testing "slice parity"
       (and
        (is (= (->map (set/slice s 0 15))
               (->map (await (set/slice s 0 15 {:sync? false})))
               (let [storage  (async-storage)
                     s        (into (set/sorted-set) (range 1024))
                     addr     (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/slice restored 0 15 {:sync? false}))))))
        (is (= (->map (set/slice s 0 31))
               (->map (await (set/slice s 0 31 {:sync? false})))
               (let [storage  (async-storage)
                     s        (into (set/sorted-set) (range 1024))
                     addr     (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/slice restored 0 31 {:sync? false}))))))
        (is (= (->map (set/slice s 0 1023))
               (->map (await (set/slice s 0 1023 {:sync? false})))
               (let [storage  (async-storage)
                     s        (into (set/sorted-set) (range 1024))
                     addr     (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/slice restored 0 1023 {:sync? false}))))))
        (is (= (->map (set/slice s 0 1024))
               (->map (await (set/slice s 0 1024 {:sync? false})))
               (let [storage  (async-storage)
                     s        (into (set/sorted-set) (range 1024))
                     addr     (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/slice restored 0 1024 {:sync? false}))))))
        (is (= (->map (set/slice s 64 768))
               (->map (await (set/slice s 64 768 {:sync? false})))
               (let [storage  (async-storage)
                     s        (into (set/sorted-set) (range 1024))
                     addr     (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/slice restored 64 768 {:sync? false}))))))
        (is (= (->map (set/slice s 511 1024))
               (->map (await (set/slice s 511 1024 {:sync? false})))
               (let [storage  (async-storage)
                     s        (into (set/sorted-set) (range 1024))
                     addr     (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/slice restored 511 1024 {:sync? false}))))))
        (is (= (->map (set/slice s 1023 1024))
               (->map (await (set/slice s 1023 1024 {:sync? false})))
               (let [storage  (async-storage)
                     s        (into (set/sorted-set) (range 1024))
                     addr     (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/slice restored 1023 1024 {:sync? false}))))))
        (is (= (nil? (set/slice s 1024 1025))
               (nil? (await (set/slice s 1024 1025 {:sync? false})))
               (let [storage  (async-storage)
                     s        (into (set/sorted-set) (range 1024))
                     addr     (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (nil? (await (set/slice restored 1024 1025 {:sync? false}))))))
        (is (= (nil? (set/slice s 900 800))
               (nil? (await (set/slice s 900 800 {:sync? false})))
               (let [storage  (async-storage)
                     s        (into (set/sorted-set) (range 1024))
                     addr     (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (nil? (await (set/slice restored 900 800 {:sync? false})))))))))))

(deftest slice-parity-test
  (test/async done
              ((do-slice-parity-test)
               (fn [_] (done))
               (fn [err]
                 (js/console.warn "slice-parity-test failed")
                 (is (nil? err))
                 (js/console.error err)
                 (done)))))

(defn do-rslice-parity-test []
  (async
   (let [s (into (set/sorted-set) (range 1024))]
     (testing "rslice parity"
       (and
        (is (= (->map (set/rslice s 15 0))
               (->map (await (set/rslice s 15 0 {:sync? false})))
               (let [storage (async-storage)
                     s       (into (set/sorted-set) (range 1024))
                     addr    (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/rslice restored 15 0 {:sync? false}))))))
        (is (= (->map (set/rslice s 1023 0))
               (->map (await (set/rslice s 1023 0 {:sync? false})))
               (let [storage (async-storage)
                     s       (into (set/sorted-set) (range 1024))
                     addr    (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/rslice restored 1023 0 {:sync? false}))))))
        (is (= (->map (set/rslice s 1024 0))
               (->map (await (set/rslice s 1024 0 {:sync? false})))
               (let [storage (async-storage)
                     s       (into (set/sorted-set) (range 1024))
                     addr    (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/rslice restored 1024 0 {:sync? false}))))))
        (is (= (->map (set/rslice s 768 64))
               (->map (await (set/rslice s 768 64 {:sync? false})))
               (let [storage (async-storage)
                     s       (into (set/sorted-set) (range 1024))
                     addr    (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/rslice restored 768 64 {:sync? false}))))))
        (is (= (->map (set/rslice s 1024 511))
               (->map (await (set/rslice s 1024 511 {:sync? false})))
               (let [storage (async-storage)
                     s       (into (set/sorted-set) (range 1024))
                     addr    (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/rslice restored 1024 511 {:sync? false}))))))
        (is (= (->map (set/rslice s 1024 1023))
               (->map (await (set/rslice s 1024 1023 {:sync? false})))
               (let [storage (async-storage)
                     s       (into (set/sorted-set) (range 1024))
                     addr    (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/rslice restored 1024 1023 {:sync? false}))))))
        (is (= (nil? (set/rslice s 1025 1024))
               (nil? (await (set/rslice s 1025 1024 {:sync? false})))
               (let [storage (async-storage)
                     s       (into (set/sorted-set) (range 1024))
                     addr    (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (nil? (await (set/rslice restored 1025 1024 {:sync? false}))))))
        (is (= (nil? (set/rslice s 800 900))
               (nil? (await (set/rslice s 800 900 {:sync? false})))
               (let [storage (async-storage)
                     s       (into (set/sorted-set) (range 1024))
                     addr    (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (nil? (await (set/rslice restored 800 900 {:sync? false}))))))
        (is (= (->map (set/rslice s 0 -1))
               (->map (await (set/rslice s 0 -1 {:sync? false})))
               (let [storage (async-storage)
                     s       (into (set/sorted-set) (range 1024))
                     addr    (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (->map (await (set/rslice restored 0 -1 {:sync? false}))))))
        (is (= (nil? (set/rslice s -1 -2))
               (nil? (await (set/rslice s -1 -2 {:sync? false})))
               (let [storage (async-storage)
                     s       (into (set/sorted-set) (range 1024))
                     addr    (await (set/store s storage {:sync? false}))
                     restored (set/restore addr storage)]
                 (nil? (await (set/rslice restored -1 -2 {:sync? false})))))))))))

(deftest rslice-parity-test
  (test/async done
              ((do-rslice-parity-test)
               (fn [_] (done))
               (fn [err]
                 (js/console.warn "slice-parity-test failed")
                 (is (nil? err))
                 (js/console.error err)
                 (done)))))

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
                 _ (assert (some? sk1))
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
              ((do-seek-slice-test)
               (fn [_] (done))
               (fn [err]
                 (js/console.warn "seek-slice-test failed")
                 (is (nil? err))
                 (js/console.error err)
                 (done)))))