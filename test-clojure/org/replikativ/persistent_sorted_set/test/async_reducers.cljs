(ns org.replikativ.persistent-sorted-set.test.async-reducers
  (:require [cljs.test :as test :refer-macros [is are deftest testing]]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [is.simm.partial-cps.sequence :as aseq]
            [org.replikativ.persistent-sorted-set :as set]))

(deftype AsyncRange [^number current ^number end ^number step]
  aseq/PAsyncSeq
  (anext [_]
    (async
     [(when (< current end)
        current)
      (when (< (+ current step) end)
        (AsyncRange. (+ current step) end step))])))

(defn async-range
  ([end] (AsyncRange. 0 end 1))
  ([start end] (AsyncRange. start end 1))
  ([start end step] (AsyncRange. start end step)))

(def $conj
  (fn
    ([^BTSet acc]
     (async acc))
    ([^BTSet acc item]
     (set/conj acc item (.-comparator acc) {:sync? false}))))

(defn do-test []
  (async
   (and
    (testing "async-reduce"
      (and
       (testing "async-reduce($conj, async-set, sync-seq)"
         (let [aset (set/reduce $conj (set/sorted-set) (range 0 128) {:sync? false})]
           (is (true? (await (set/equiv-sequential? (await aset)
                                                    (range 0 128)
                                                    {:sync? false}))))))
       (testing "async-reduce($conj, async-set, async-seq)"
         (let [aset (set/reduce $conj (set/sorted-set) (async-range 0 128) {:sync? false})]
           (is (true? (await (set/equiv-sequential? (await aset)
                                                    (range 0 128)
                                                    {:sync? false}))))))))
    (testing "async-into"
      (and
       (testing "async-into(async-set, sync-seq)"
         (let [aset (set/into (set/sorted-set) (range 0 128) {:sync? false})]
           (is (true? (await (set/equiv-sequential? (await aset)
                                                    (range 0 128)
                                                    {:sync? false}))))))
       (testing "async-into(async-set, xform, sync-seq)"
         (let [aset (await (set/into (set/sorted-set) (map inc) (range 0 128) {:sync? false}))]
           (is (true? (await (set/equiv-sequential? aset
                                                    (range 1 129)
                                                    {:sync? false}))))))
       (testing "async-into(async-set, async-seq)"
         (let [aset (await (set/into (set/sorted-set) (async-range 0 128) {:sync? false}))]
           (is (true? (await (set/equiv-sequential? aset (range 0 128) {:sync? false}))))))
       (testing "async-into(async-set, xform async-seq)"
         (let [aset (await (set/into (set/sorted-set) (map inc) (async-range 0 128) {:sync? false}))]
           (is (true? (await (set/equiv-sequential? aset
                                                    (range 1 129)
                                                    {:sync? false}))))))))
    (testing "async-transduce"
      (and
       (testing "async-transduce(filter+map, async-set, sync-seq)"
         (let [xf       (comp (map #(* % %)) (filter even?))
               aset     (await (set/transduce xf $conj (set/sorted-set) (range 0 16) {:sync? false}))
               expected (cljs.core/transduce xf conj (sorted-set) (range 0 16))]
           (and
            (is (= expected (into (sorted-set) [0 4 16 36 64 100 144 196])))
            (is (= (count expected) (await (set/count aset {:sync? false}))))
            (is (true? (await (set/equiv? aset expected {:sync? false}))))
            (is (true? (await (set/equiv-sequential? aset expected {:sync? false})))))))
       (testing "async-transduce(filter+map, async-set, async-seq)"
         (let [xf       (comp (filter even?) (map #(* % %)))
               aset     (await (set/transduce xf $conj (set/sorted-set) (async-range 0 16) {:sync? false}))
               expected (cljs.core/transduce xf conj (sorted-set) (range 0 16))]
           (and
            (is (= expected (into (sorted-set) [0 4 16 36 64 100 144 196])))
            (is (= (count expected) (await (set/count aset {:sync? false}))))
            (is (true? (await (set/equiv? aset expected {:sync? false}))))
            (is (true? (await (set/equiv-sequential? aset expected {:sync? false}))))))))))))

(deftest reducers-test
  (test/async done
              ((do-test)
               (fn [ok] (done))
               (fn [err]
                 (js/console.warn "reducers-test failed")
                 (is (nil? err))
                 (js/console.error err)
                 (done)))))
