(ns me.tonsky.persistent-sorted-set.test.storage.slice-parity
  (:require-macros [me.tonsky.persistent-sorted-set.test.macros :refer [testing-group]])
  (:require
   [await-cps :refer [await run-async] :refer-macros [async] :rename {run-async run}]
   ; [is.simm.lean-cps.async :refer [await run] :refer-macros [async]]
   [cljs.test :as test :refer [is are deftest testing]]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.test.storage.util
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
    (run (do-slice-parity-test)
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
    (run (do-rslice-parity-test)
      (fn [_] (done))
      (fn [err]
        (js/console.warn "slice-parity-test failed")
        (is (nil? err))
        (js/console.error err)
        (done)))))