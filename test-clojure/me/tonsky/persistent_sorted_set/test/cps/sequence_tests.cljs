(ns me.tonsky.persistent-sorted-set.test.cps.sequence-tests
  (:require [cljs.test :as test :refer [deftest is testing]]
            [await-cps :refer [await run-async] :refer-macros [async] :rename {run-async run}]
            ; [is.simm.lean-cps.async :refer [await run] :refer-macros [async]]
            [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.async-transducers :as at]
            [me.tonsky.persistent-sorted-set.test.cps.utils :as utils]))

(defn consume-async-seq
  "Helper to consume an entire AsyncSeq into a vector"
  [async-seq]
  (async
   (loop [s async-seq
          result []
          count 0]
     (if (and s (< count 20))  ; Safety limit
       (let [v (await (set/afirst s))]
         (if (some? v)  ; Use some? to handle false/nil correctly
           (let [next-s (await (set/arest s))]
             (recur next-s (conj result v) (inc count)))
           result))
       result))))

(defn do-sequence-test []
  (async
   (let [async-set (await (utils/async-build-set 10))]
     (and
      (testing "1. async-transduce with identity"
        (let [async-seq (await (set/async-slice async-set nil nil))
              result (await (at/async-transduce identity conj [] async-seq))]
          (is (= result (vec (range 10))))))
      (testing "2. async-sequence with map"
        (let [async-seq (await (set/async-slice async-set nil nil))
              mapped-seq (at/async-sequence (map #(* % 2)) async-seq)
              result (await (consume-async-seq mapped-seq))]
          (is (= result [0 2 4 6 8 10 12 14 16 18]))))
      (testing "3. async-sequence with filter"
        (let [async-seq (await (set/async-slice async-set nil nil))
              filtered-seq (at/async-sequence (filter even?) async-seq)
              result (await (consume-async-seq filtered-seq))]
          (is (= result [0 2 4 6 8]))))
      (testing "4. Testing async-sequence with take"
        (let [async-seq (await (set/async-slice async-set nil nil))
              taken-seq (at/async-sequence (take 5) async-seq)
              result (await (consume-async-seq taken-seq))]
          (is (= result [0 1 2 3 4]))))
      (testing "5. Testing async-sequence with drop"
        (let [async-seq (await (set/async-slice async-set nil nil))
              dropped-seq (at/async-sequence (drop 7) async-seq)
              result (await (consume-async-seq dropped-seq))]
          (is (= result [7 8 9]))))
      (testing "6. Testing async-sequence with partition"
        (let [async-seq (await (set/async-slice async-set nil nil))
              partitioned-seq (at/async-sequence (partition-all 3) async-seq)
              result (await (consume-async-seq partitioned-seq))]
          (is (= result [[0 1 2] [3 4 5] [6 7 8] [9]]))))
      (testing "7. Testing async-sequence with composed transducers"
        (let [async-seq (await (set/async-slice async-set nil nil))
              xform (comp (filter odd?) (map #(* % 2)) (take 3))
              transformed-seq (at/async-sequence xform async-seq)
              result (await (consume-async-seq transformed-seq))]
          (is (= result [2 6 10]))))
      (testing "8. Testing async-sequence with mapcat (inflating transducer)"
        (let [async-seq (await (set/async-slice async-set 0 2))  ; Get 0, 1, 2
              mapcatted-seq (at/async-sequence (mapcat #(vector % (* % 10))) async-seq)
              result (await (consume-async-seq mapcatted-seq))]
          (and
           (is (= result [0 0 1 10 2 20]))
           (let [async-seq2 (await (set/async-slice async-set 0 1))  ; Get 0, 1
                 expanded-seq (at/async-sequence (mapcat #(range % (+ % 3))) async-seq2)
                 result2 (await (consume-async-seq expanded-seq))]
             (is (= result2 [0 1 2 1 2 3]))))))
     (testing "9. Testing async-into..."
       (let [async-seq (await (set/async-slice async-set nil nil))
             result (await (at/async-into [] (comp (filter odd?) (map #(* % 3))) async-seq))]
         (is (= result [3 9 15 21 27]))))
     (testing "10. Testing async-transduce with early termination..."
       (let [async-seq (await (set/async-slice async-set nil nil))
             ; Find first element > 5
             xf (comp (filter #(> % 5)) (take 1))
             rf (fn
                  ([result] result)  ; Completion arity
                  ([_ v] (reduced v)))  ; Step arity
             result (await (at/async-transduce xf rf nil async-seq))]
         (is (= result 6))))))))

(deftest sequence-test
  (test/async done
    (run (do-sequence-test)
      (fn [ok] (done))
      (fn [err]
        (js/console.warn "sequence-test failure")
        (is (nil? err))
        (js/console.log (pr-str (Throwable->map err)))
        (done)))))
