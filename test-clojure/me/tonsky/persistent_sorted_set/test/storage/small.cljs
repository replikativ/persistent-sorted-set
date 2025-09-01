(ns me.tonsky.persistent-sorted-set.test.storage.small
  (:require-macros [me.tonsky.persistent-sorted-set.test.macros :refer [testing-group]])
  (:require
   [await-cps :refer [await run-async] :refer-macros [async] :rename {run-async run}]
   ; [is.simm.lean-cps.async :refer [await run] :refer-macros [async]]
   [cljs.test :as test :refer [is are deftest testing]]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.test.storage.util
    :refer [storage async-storage]]))

(deftest test-small-sync-restoration
  (and
   (testing "seq"
     (let [storage (storage)
           stored (into (set/sorted-set) (range 10 20))
           address (set/store stored storage)]
       (and
        (testing "control"
         (is (= (range 10 20) (seq (into (set/sorted-set) (range 10 20)))) "control"))
        (testing "flushed"
         (is (= (range 10 20) (seq stored)) "seq works on set that just flushed"))
        (testing "restored"
         (is (= (range 10 20) (seq (set/restore address storage))) "seq works on unrealized lazy state")))))
   (testing "slice"
     (let [storage (storage)
           stored  (into (set/sorted-set) (range 0 40))
           address (set/store stored storage)]
       (and
        (testing "control"
         (is (= (range 10 20) (set/slice (into (set/sorted-set) (range 0 40)) 10 19)) "control"))
        (testing "flushed"
         (is (= (range 10 20) (set/slice stored 10 19)) "slice works on set that just flushed"))
        (testing "restored"
         (is (= (range 10 20) (set/slice (set/restore address storage) 10 19)) "slice works on unrealized lazy state")))))
   (testing "slice reversed order"
     (let [storage (storage)
           stored  (into (set/sorted-set) (reverse (range 0 100)))
           address (set/store stored storage)]
       (and
        (testing "control"
          (is (= (range 10 20)
                 (set/slice (into (set/sorted-set) (reverse (range 0 100))) 10 19))
              "control"))
        (testing "flushed"
          (is (= (range 10 20) (set/slice stored 10 19))
              "slice works on set that just flushed"))
        (testing "restored"
          (is (= (range 10 20)
                 (set/slice (set/restore address storage) 10 19))
              "slice works on unrealized lazy state")))))
   (testing "rseq"
     (let [storage (storage)
           stored  (into (set/sorted-set) (range 10 20))
           address (set/store stored storage)]
       (and
        (testing "control"
          (is (= (range 19 9 -1) (rseq (into (set/sorted-set) (range 10 20)))) "control"))
        (testing "flushed"
          (is (= (range 19 9 -1) (rseq stored)) "rseq works on set that just flushed"))
        (testing "restored"
          (is (= (range 19 9 -1) (rseq (set/restore address storage))) "rseq works on unrealized lazy state")))))
   (testing "rslice"
     (let [storage (storage)
           stored  (into (set/sorted-set) (range 0 40))
           address (set/store stored storage)]
       (and
        (testing "control"
          (is (= (range 19 9 -1)
                 (set/rslice (into (set/sorted-set) (range 0 40)) 19 10))
              "control"))
        (testing "flushed"
          (is (= (range 19 9 -1) (set/rslice stored 19 10))
              "rslice works on set that just flushed"))
        (testing "restored"
          (is (= (range 19 9 -1) (set/rslice (set/restore address storage) 19 10))
              "rslice works on unrealized lazy state")))))
   (testing "reversing rseq is original sort"
     (let [storage (storage)
           stored  (into (set/sorted-set) (shuffle (range 0 5001)))
           address (set/store stored storage)]
       (and
        (testing "control"
         (let [x (set/rslice (into (set/sorted-set) (shuffle (range 0 5001))) 5000 nil)]
           (is (= x (some-> x rseq reverse)) "control")))
        (testing "flushed"
         (let [x (set/rslice stored 5000 nil)]
           (is (= x (some-> x rseq reverse)) "rseq reversibility on flushed set")))
        (testing "restored"
         (let [x (set/rslice (set/restore address storage) 5000 nil)]
           (is (= x (some-> x rseq reverse)) "rseq reversibility on restored set"))))))))

(defn do-test-small-async-restoration []
  (async
   (and
    (testing "async (equiv-sequential? btset, seq)"
      (let [storage (async-storage)
            stored (into (set/sorted-set) (range 10 20))
            address (await (set/store stored storage {:sync? false}))]
        (and
         (testing "async-control"
           (is (true? (await (set/equiv-sequential? (await (set/async-into (set/sorted-set) (range 10 20))) (range 10 20) {:sync? false})))))
         (testing "flushed"
           (is (true? (await (set/equiv-sequential? stored (range 10 20) {:sync? false})))))
         (testing "restored"
           (is (true? (await (set/equiv-sequential? (set/restore address storage) (range 10 20) {:sync? false}))))))))
    (testing "async slice"
      (let [storage (async-storage)
            stored  (into (set/sorted-set) (range 0 40))
            address (await (set/store stored storage {:sync? false}))]
        (and
         (testing "async-control"
           (let [s (await (set/slice (into (set/sorted-set) (range 0 40)) 10 19 {:sync? false}))]
             (is (true? (await (set/equiv-sequential? s (range 10 20) {:sync? false}))))))
         (testing "flushed"
           (let [s (await (set/slice stored 10 19 {:sync? false}))
                 b (await (set/equiv-sequential? s (range 10 20) {:sync? false}))]
             (is (true? b) "slice works on set that just flushed")))
         (testing "restored"
           (let [s (await (set/slice (set/restore address storage) 10 19 {:sync? false}))
                 b (await (set/equiv-sequential? s (range 10 20) {:sync? false}))]
             (is (true? b) "slice works on unrealized lazy state"))))))
    (testing "async slice reversed order"
      (let [storage (async-storage)
            stored  (into (set/sorted-set) (reverse (range 0 100)))
            address (await (set/store stored storage {:sync? false}))]
        (and
         (testing "async control"
           (let [s (await (set/slice (into (set/sorted-set) (reverse (range 0 100))) 10 19 {:sync? false}))
                 b (await (set/equiv-sequential? s (range 10 20) {:sync? false}))]
             (is (true? b))))
         (testing "flushed"
           (let [s (await (set/slice stored 10 19 {:sync? false}))
                 b (await (set/equiv-sequential? s (range 10 20) {:sync? false}))]
             (is (true? b) "slice works on set that just flushed")))
         (testing "restored"
           (let [s (await (set/slice (set/restore address storage) 10 19 {:sync? false}))
                 b (await (set/equiv-sequential? s (range 10 20) {:sync? false}))]
             (is (true? b) "slice works on unrealized lazy state"))))))
    (testing "very small rseq"
      (let [storage (async-storage)
            stored  (into (set/sorted-set) (range 0 5))
            address (await (set/store stored storage {:sync? false}))]
        (and
         (testing "sync control"
           (let [s (into (set/sorted-set) (range 0 5))]
             (is (= (range 4 -1 -1) (set/rseq s)) "control")))
         (testing "async control"
           (let [s (into (set/sorted-set) (range 0 5))
                 rs (await (set/rseq s {:sync? false}))
                 b  (await (set/equiv-sequential? rs (range 4 -1 -1) {:sync? false}))]
             (is (true? b))))
         (testing "flushed"
           (let [rs (await (set/rseq stored {:sync? false}))
                 b  (await (set/equiv-sequential? rs (range 4 -1 -1) {:sync? false}))]
             (is (true? b) "rseq works on set that just flushed")))
         (testing "restored"
           (let [rs (await (set/rseq (set/restore address storage) {:sync? false}))
                 b  (await (set/equiv-sequential? rs (range 4 -1 -1) {:sync? false}))]
             (is (true? b) "rseq works on unrealized lazy state"))))))
    (testing "small rseq"
      (let [storage (async-storage)
            stored  (into (set/sorted-set) (range 10 20))
            address (await (set/store stored storage {:sync? false}))]
        (and
         (testing "sync control"
           (let [s (into (set/sorted-set) (range 10 20))]
             (is (= (range 19 9 -1) (set/rseq s)) "control")))
         (testing "async control"
           (let [s (into (set/sorted-set) (range 10 20))
                 rs (await (set/rseq s {:sync? false}))
                 b  (await (set/equiv-sequential? rs (range 19 9 -1) {:sync? false}))]
             (is (true? b))))
         (testing "flushed"
           (let [rs (await (set/rseq stored {:sync? false}))
                 b  (await (set/equiv-sequential? rs (range 19 9 -1) {:sync? false}))]
             (is (true? b) "rseq works on set that just flushed")))
         (testing "restored"
           (let [rs (await (set/rseq (set/restore address storage) {:sync? false}))
                 b  (await (set/equiv-sequential? rs (range 19 9 -1) {:sync? false}))]
             (is (true? b) "rseq works on unrealized lazy state"))))))
    (testing "medium rseq"
      (let [storage (async-storage)
            stored  (into (set/sorted-set) (range 48 512))
            address (await (set/store stored storage {:sync? false}))]
        (and
         (testing "sync control"
           (let [s (into (set/sorted-set) (range 48 512))]
             (is (= (range 511 47 -1) (set/rseq s)) "control")))
         (testing "async control"
           (let [s (into (set/sorted-set) (range 48 512))
                 rs (await (set/rseq s {:sync? false}))
                 b  (await (set/equiv-sequential? rs (range 511 47 -1) {:sync? false}))]
             (is (true? b))))
         (testing "flushed"
           (let [rs (await (set/rseq stored {:sync? false}))
                 b  (await (set/equiv-sequential? rs (range 511 47 -1) {:sync? false}))]
             (is (true? b) "rseq works on set that just flushed")))
         (testing "restored"
           (let [rs (await (set/rseq (set/restore address storage) {:sync? false}))
                 b  (await (set/equiv-sequential? rs (range 511 47 -1) {:sync? false}))]
             (is (true? b) "rseq works on unrealized lazy state"))))))
    (testing "rslice"
      (let [storage (async-storage)
            stored  (into (set/sorted-set) (range 0 40))
            address (await (set/store stored storage {:sync? false}))]
        (and
         (testing "sync-control"
           (is (= (range 19 9 -1)
                  (set/rslice (into (set/sorted-set) (range 0 40)) 19 10))))
         (testing "async-control"
           (let [s  (into (set/sorted-set) (range 0 40))
                 rs (await (set/rslice s 19 10 compare {:sync? false}))
                 b  (await (set/equiv-sequential? rs (range 19 9 -1) {:sync? false}))]
             (is (true? b))))
         (testing "flushed"
           (let [rs (await (set/rslice stored 19 10 compare {:sync? false}))
                 b  (await (set/equiv-sequential? rs (range 19 9 -1) {:sync? false}))]
             (is (true? b) "rslice works on set that just flushed")))
         (testing "restored"
           (let [rs (await (set/rslice (set/restore address storage) 19 10 compare {:sync? false}))
                 b  (await (set/equiv-sequential? rs (range 19 9 -1) {:sync? false}))]
             (is (true? b) "rslice works on unrealized lazy state"))))))
    (testing "reversing rslice is original sort"
      (let [storage (async-storage)
            stored  (into (set/sorted-set) (shuffle (range 0 5001)))
            address (await (set/store stored storage {:sync? false}))
            asc     (range 0 5001)]
        (and
         (testing "sync control"
           (is (= asc (reverse (set/rslice (into (set/sorted-set) (shuffle (range 0 5001))) 5000 nil)))))
         (testing "async control"
           (let [s (into (set/sorted-set) (shuffle (range 0 5001)))
                 rs (await (set/rslice s 5000 nil compare {:sync? false}))
                 b  (await (set/equiv-sequential? rs (reverse asc) {:sync? false}))]
             (is (true? b))))
         (testing "flushed (async)"
           (let [rs (await (set/rslice stored 5000 nil compare {:sync? false}))
                 b  (await (set/equiv-sequential? rs (reverse asc) {:sync? false}))]
             (is (true? b) "rseq reversibility on flushed set")))
         (testing "restored (async)"
           (let [rs (await (set/rslice (set/restore address storage) 5000 nil compare {:sync? false}))
                 b  (await (set/equiv-sequential? rs (reverse asc) {:sync? false}))]
             (is (true? b) "rseq reversibility on restored set")))))))))

(deftest test-small-async-restoration
  (test/async done
    (run (do-test-small-async-restoration)
      (fn [ok] (done))
      (fn [err]
        (js/console.warn "test-small-async-restoration failed")
        (is (nil? err))
        (js/console.error err)
        (done)))))