(ns me.tonsky.persistent-sorted-set.test.storage.equiv
  (:require [cljs.test :as test :refer-macros [is are deftest testing]]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.test.storage.util
             :refer [storage async-storage]]))

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

     (testing "equiv-sequential"
       (and
        (testing "equiv-sequential? on naive sets"
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
        (testing "equiv-sequential? on restored sets"
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
    ((do-equivalence-tests)
     (fn [ok] (done))
     (fn [err]
       (js/console.warn "equivalence-tests failed")
       (is (nil? err))
       (js/console.error err)
       (done)))))