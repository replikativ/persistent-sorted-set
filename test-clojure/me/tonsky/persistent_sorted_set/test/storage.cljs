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
   [me.tonsky.persistent-sorted-set.btset :refer [BTSet AsyncSeq]]
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

(defn do-equivalence-tests []
  (async
    (and
     (testing "equiv? on naive sets"
       (and
        (testing "empty set"
          (and
           (is (true? (set/equiv? (set/sorted-set) #{} {:sync? true})))
           (is (true? (await (set/equiv? (set/sorted-set) #{} {:sync? false}))))
           (is (true? (set/equiv? (set/sorted-set) (set/sorted-set) {:sync? true})))
           (is (true? (await (set/equiv? (set/sorted-set) (set/sorted-set) {:sync? false}))))))
        (testing "32^2"
          (is (true? (set/equiv? (into (set/sorted-set) (range 1024)) (into #{} (range 1024)) {:sync? true})))
          (is (true? (await (set/equiv? (into (set/sorted-set) (range 1024)) (into #{} (range 1024)) {:sync? false}))))
          (is (true? (set/equiv? (into (set/sorted-set) (range 1024))
                                 (into (set/sorted-set) (range 1024)) {:sync? true})))
          (is (true? (await (set/equiv? (into (set/sorted-set) (range 1024))
                                        (into (set/sorted-set) (range 1024)) {:sync? false})))))
        (testing "32^2 mutation sanity check"
          (is (false? (await (set/equiv? (disj (into (set/sorted-set) (range 1024)) 512)
                                         (into (set/sorted-set) (range 1024)) {:sync? false}))))
          (is (true? (await (set/equiv? (disj (into (set/sorted-set) (range 1024)) 512)
                                        (disj (into (set/sorted-set) (range 1024)) 512) {:sync? false})))))))
     (testing "equiv? on restored sets"
       (and
        (testing "restored empty set"
          (let [async-storage (async-storage)
                sync-storage  (storage)
                sync-set      (set/sorted-set* {:storage sync-storage})
                async-set     (set/sorted-set* {:storage async-storage})
                sync-address  (set/store sync-set)
                async-address (await (set/store async-set {:sync? false}))]
           (assert (uuid? sync-address))
           (assert (uuid? async-address))
           (and
            (is (true? (set/equiv? (set/restore sync-address sync-storage) #{} {:sync? true})))
            (is (true? (set/equiv? (set/restore sync-address sync-storage) (set/sorted-set) {:sync? true})))
            (is (true? (await (set/equiv? (set/restore async-address async-storage) #{} {:sync? false}))))
            (is (true? (await (set/equiv? (set/restore async-address async-storage) (sorted-set) {:sync? false})))))))
        (testing "restored 32^2"
          (let [async-storage (async-storage)
                sync-storage  (storage)
                sync-set      (into (set/sorted-set* {:storage sync-storage}) (range 1024))
                async-set     (into (set/sorted-set* {:storage async-storage}) (range 1024))
                sync-address  (set/store sync-set)
                async-address (await (set/store async-set {:sync? false}))]
            (assert (uuid? sync-address))
            (assert (uuid? async-address))
            (and
             (is (true? (set/equiv? (set/restore sync-address sync-storage)
                                    (into #{} (range 1024)) {:sync? true})))
             (is (true? (set/equiv? (set/restore sync-address sync-storage)
                                    (into (set/sorted-set) (range 1024)) {:sync? true})))
             (is (true? (await (set/equiv? (set/restore async-address async-storage)
                                           (into #{} (range 1024)) {:sync? false}))))
             (is (true? (await (set/equiv? (set/restore async-address async-storage)
                                           (into (set/sorted-set) (range 1024)) {:sync? false})))))))
        (testing "mutations on restored 32^2"
          (let [async-storage (async-storage)
                async-set     (into (set/sorted-set* {:storage async-storage}) (range 1024))
                async-address (await (set/store async-set {:sync? false}))]
            (assert (uuid? async-address))
            (and
             (is (false? (await (set/equiv? (await (set/disj (set/restore async-address async-storage) 512 {:sync? false}))
                                            (into (set/sorted-set) (range 1024))
                                            {:sync? false}))))
             (is (true?  (await (set/equiv? (await (set/disj (set/restore async-address async-storage) 512 {:sync? false}))
                                            (disj (into (set/sorted-set) (range 1024)) 512)
                                            {:sync? false})))))))))
     (testing
       (and
        (testing-group "equiv-sequential? on naive sets"
         (and
          (is (true? (set/equiv-sequential? (set/sorted-set) ())))
          (is (true? (await (set/equiv-sequential? (set/sorted-set) () {:sync? false}))))
          (is (true? (set/equiv-sequential? (into (set/sorted-set) (range 0 32))
                                            (range 0 32))))
          (is (true? (await (set/equiv-sequential? (into (set/sorted-set) (range 0 32))
                                                   (range 0 32)
                                                   {:sync? false}))))
          (is (false? (set/equiv-sequential? (into (set/sorted-set) (range 0 32))
                                             (shuffle (range 0 32)))))
          (is (false? (await (set/equiv-sequential? (into (set/sorted-set) (range 0 32))
                                                    (shuffle (range 0 32))
                                                    {:sync? false}))))
          (is (true?  (set/equiv-sequential? (into (set/sorted-set) (range 0 32))
                                             (seq (into (sorted-set) (range 0 32))))))
          (is (true?  (await (set/equiv-sequential? (into (set/sorted-set) (range 0 32))
                                                    (seq (into (sorted-set) (range 0 32)))
                                                    {:sync? false}))))
          (is (false? (set/equiv-sequential? (into (set/sorted-set) (range 0 512))
                                             (into #{} (shuffle (range 0 512))))))
          (is (false? (await (set/equiv-sequential? (into (set/sorted-set) (range 0 512))
                                                    (into #{} (shuffle (range 0 512)))
                                                    {:sync? false}))))))
        (testing-group "equiv-sequential? on restored sets"
          (and
           (testing "empty seq"
             (let [async-storage (async-storage)
                   sync-storage  (storage)
                   sync-set      (set/sorted-set* {:storage sync-storage})
                   async-set     (set/sorted-set* {:storage async-storage})
                   sync-address  (set/store sync-set)
                   async-address (await (set/store async-set {:sync? false}))]
               (and
                (is (true? (set/equiv-sequential? (set/restore sync-address sync-storage) ())))
                (is (true? (await (set/equiv-sequential? (set/restore async-address async-storage) () {:sync? false})))))))
           (testing "(range 32)"
             (let [async-storage (async-storage)
                   sync-storage  (storage)
                   sync-set      (into (set/sorted-set* {:storage sync-storage}) (range 32))
                   async-set     (into (set/sorted-set* {:storage async-storage}) (range 32))
                   sync-address  (set/store sync-set)
                   async-address (await (set/store async-set {:sync? false}))]
               (and
                (is (true? (set/equiv-sequential? (set/restore sync-address sync-storage) (range 32))))
                (is (true? (await (set/equiv-sequential? (set/restore async-address async-storage) (range 32) {:sync? false})))))))
           ;; TODO rseq, slicing
           (testing "(range 512)"
             (let [async-storage (async-storage)
                   sync-storage  (storage)
                   sync-set      (into (set/sorted-set* {:storage sync-storage}) (range 512))
                   async-set     (into (set/sorted-set* {:storage async-storage}) (range 512))
                   sync-address  (set/store sync-set)
                   async-address (await (set/store async-set {:sync? false}))]
               (and
                (is (true? (set/equiv-sequential? (set/restore sync-address sync-storage) (range 512))))
                (is (true? (await (set/equiv-sequential? (set/restore async-address async-storage) (range 512) {:sync? false}))))))))))))))


(deftest equivalence-tests
  (test/async done
    (run (do-equivalence-tests)
      (fn [ok] (done))
      (fn [err]
        (js/console.warn "equivalence-tests failed")
        (is (nil? err))
        (js/console.error err)
        (done)))))

(defn do-test-small-async-restoration []
  (async
   #_(and
    (testing-group "aseq"
      (let [storage (storage)
            stored (into (set/sorted-set) (range 10 20))
            address (set/store stored storage)]
        (and
         (testing-group "sync-control"
           (is (= (range 10 20) (seq (into (set/sorted-set) (range 10 20)))) "sync control"))
         (testing-group "async-control"
           (is (instance? BTSet (await (set/async-into (set/sorted-set) (range 10 20)))))
          )
         ; (testing-group "flushed"
         ;   (is (= (range 10 20) (seq stored)) "seq works on set that just flushed"))
         ; (testing-group "restored"
         ;   (is (= (range 10 20) (seq (set/restore address storage))) "seq works on unrealized lazy state"))
         ))))))

(deftest test-small-async-restoration
  (test/async done
    (run (do-test-small-async-restoration)
      (fn [ok] (done))
      (fn [err]
        (js/console.warn "test-small-async-restoration failed")
        (is (nil? err))
        (js/console.error err)
        (done)))))

; (comment
;  (testing "Restore from storage"
;    (let [restored (set/restore store-info storage {:sync? true})]
;      (and
;       (is (= n (count restored)))
;       (is (= 0 (first restored)))
;       (is (= 9999 (last restored)))
;       (testing "Check a sample of elements using contains?"
;         (loop [indices (range 0 10000 1000)]
;           (let [i (first indices)]
;             (if (nil? i)
;               true
;               (if-not (is (contains? restored i) (str "Missing element: " i))
;                 false
;                 (recur (rest indices)))))))
;       (testing "Verify slices on restored set"
;         (and
;          (is (= (range 100) (vec (set/slice restored 0 99))))
;          (is (= (range 5000 5100) (vec (set/slice restored 5000 5099))))
;          (is (= (range 9900 10000) (vec (set/slice restored 9900 9999))))))
;       (testing "conj on restored set"
;         (let [restored-with-new (set/conj restored 10000)]
;           (and
;            (is (= (inc n) (count restored-with-new)))
;            (is (contains? restored-with-new 10000)))))
;       (testing "disj on restored set"
;         (let [restored-without (set/disj restored 5000)]
;           (and
;            (is (= (dec n) (count restored-without)))
;            (is (not (contains? restored-without 5000))))))
;       (testing "iteration over restored set"
;         (and
;          (is (= (range 100) (take 100 restored)))
;          (is (= (range 9900 10000) (take 100 (drop 9900 restored)))))))))
;  )

