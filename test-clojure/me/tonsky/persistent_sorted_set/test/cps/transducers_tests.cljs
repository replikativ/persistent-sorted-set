(ns me.tonsky.persistent-sorted-set.test.cps.transducers-tests
  (:require [cljs.test :as test :refer [deftest is testing]]
            [is.simm.lean-cps.async :refer [await] :refer-macros [async]]
            [is.simm.lean-cps.runtime :refer [run]]
            [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.async-transducers :as at]
            [me.tonsky.persistent-sorted-set.test.cps.utils :as utils]))

(defn async-transduce
  "Reduce an AsyncSeq with a transducer. Returns CPS fn yielding the reduced value."
  ([xform rf init async-seq]
   (async
     (let [rf (xform rf)]
       (loop [acc init, s async-seq]
         (if s
           (let [v (await (set/afirst s))]
             (if (some? v)
               (let [acc' (rf acc v)]
                 (if (reduced? acc')
                   @acc'
                   (recur acc' (await (set/arest s)))))
               (rf acc)))
           (rf acc)))))))

(defn async-into
  "Into via transducer over AsyncSeq."
  ([to xform async-seq]
   (async (await (async-transduce xform conj to async-seq)))))

(defn do-tests []
  (async
   (let [async-set (await (utils/async-build-set 10))]
     (and
      (testing "1. identity"
        (let [aseq   (await (set/async-slice async-set nil nil))
              result (await (async-transduce identity conj [] aseq))]
          (is (= result (vec (range 10))))))
      (testing "2. filter even?"
        (let [aseq   (await (set/async-slice async-set nil nil))
              result (await (async-transduce (filter even?) conj [] aseq))]
          (is (= result [0 2 4 6 8]))))
      (testing "3. map *2"
        (let [aseq   (await (set/async-slice async-set nil nil))
              result (await (async-transduce (map #(* % 2)) conj [] aseq))]
          (is (= result [0 2 4 6 8 10 12 14 16 18]))))
      (testing "4. comp filter odd? -> map *3 -> take 3"
        (let [aseq   (await (set/async-slice async-set nil nil))
              xform  (comp (filter odd?) (map #(* % 3)) (take 3))
              result (await (async-transduce xform conj [] aseq))]
          (is (= result [3 9 15]))))
      (testing "5. take 5 (early termination)"
        (let [aseq   (await (set/async-slice async-set nil nil))
              result (await (async-transduce (take 5) conj [] aseq))]
          (is (= result [0 1 2 3 4]))))
      (testing "6. async-into helper (drop 2, take 3)"
        (let [aseq   (await (set/async-slice async-set nil nil))
              result (await (async-into [] (comp (drop 2) (take 3)) aseq))]
          (is (= result [2 3 4]))))
      (testing "7. partition-all 3"
        (let [aseq   (await (set/async-slice async-set nil nil))
              result (await (async-transduce (partition-all 3) conj [] aseq))]
          (is (= result [[0 1 2] [3 4 5] [6 7 8] [9]]))))
      (testing "8. transduce with custom rf and reduced early-exit (>5 then take 1)"
        (let [aseq  (await (set/async-slice async-set nil nil))
              xf    (comp (filter #(> % 5)) (take 1))
              rf    (fn
                      ([acc] acc)
                      ([acc v] (reduced v)))
              found (await (async-transduce xf rf nil aseq))]
          (is (= found 6))))))))

(deftest transducer-sequence-test
  (test/async done
    (run (do-tests)
      (fn [_] (done))
      (fn [err]
        (js/console.warn "transducer-sequence-test failure")
        (is (nil? err))
        (js/console.log (pr-str (Throwable->map err)))
        (done)))))
