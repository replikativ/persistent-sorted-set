(ns org.replikativ.persistent-sorted-set.test.async-exhaustion
  "Pins the PAsyncSeq termination contract: `anext` on an exhausted seq
   resolves nil — never a truthy `[nil nil]`. AsyncSeq (forward) honors this
   via a guard around its tuple; AsyncReverseSeq's `anext` was a literal
   two-slot vector of `when` forms, so an exhausted reverse seq yielded
   `[nil nil]` — a spurious nil element for any consumer written against
   the AsyncSeq contract (aseq/transduce's `if-let`, chunk walkers)."
  (:require [cljs.test :as test :refer-macros [is deftest testing]]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [is.simm.partial-cps.sequence :as aseq]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.btset :as btset]))

(defn- drain
  "Drain an async seq via anext into [elements…]. Rejects (via the async
   error path) if it exceeds `cap` steps — guards against [nil nil] loops."
  [aseq0 cap]
  (async
   (loop [s aseq0 acc [] n 0]
     (when (> n cap)
       (throw (ex-info "drain runaway — termination contract violated" {:acc acc})))
     (if (nil? s)
       acc
       (let [tuple (await (aseq/anext s))]
         (if (some? tuple)
           (let [[v rest] tuple]
             (recur rest (conj acc v) (inc n)))
           acc))))))

(defn- do-test []
  (async
   (let [s (reduce conj (set/sorted-set) (range 0 100))]
     (and
      (testing "forward AsyncSeq drains cleanly (contract control)"
        (let [aslice (await (set/slice s 10 19 (.-comparator s) {:sync? false}))
              elems (await (drain aslice 200))]
          (is (= (vec (range 10 20)) elems))))
      (testing "AsyncReverseSeq drains to descending elements, no trailing nil"
        (let [arslice (await (set/rslice s 19 10 (.-comparator s) {:sync? false}))
              elems (await (drain arslice 200))]
          (is (= (vec (range 19 9 -1)) elems))))
      (testing "anext on an exhausted AsyncSeq resolves nil (control)"
        (let [exhausted (btset/->AsyncSeq s (js* "0n") (js* "0n") nil nil)]
          (is (nil? (await (aseq/anext exhausted))))))
      (testing "anext on an exhausted AsyncReverseSeq resolves nil (the bug)"
        (let [exhausted (btset/->AsyncReverseSeq s (js* "0n") (js* "0n") nil nil)
              tuple (await (aseq/anext exhausted))]
          (is (nil? tuple)
              (str "exhausted reverse anext must resolve nil, got: " (pr-str tuple)))))))))

(deftest async-exhaustion-test
  (test/async done
              ((do-test)
               (fn [_] (done))
               (fn [err]
                 (is (nil? err) "async-exhaustion-test rejected")
                 (js/console.error err)
                 (done)))))
