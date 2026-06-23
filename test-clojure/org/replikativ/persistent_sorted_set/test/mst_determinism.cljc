(ns org.replikativ.persistent-sorted-set.test.mst-determinism
  "The cross-platform determinism linchpin: `boundary/key-level` MUST return the identical
   integer on the JVM and in ClojureScript for every key, or a JVM writer and a cljs reader
   diverge on tree structure. The expected vectors below were computed on the JVM; this test
   runs on BOTH platforms, so a cljs hash/leading-zeros divergence fails it. See
   .internal/SPLIT_SEAM_DESIGN.md."
  (:require [clojure.test :refer [deftest is]]
            [org.replikativ.persistent-sorted-set.boundary :as b]))

(def battery
  (vec (concat (range 0 40)
               ["a" "hello" "" "zzz" "datom" "the quick brown fox"]
               [:kw :ns/kw 'sym :a/b]
               [[1 2] [1 2 3] [:a 1 "x"] [0] [99 99]]
               [1.5 -1 -42 true 9999999])))

;; Known answers computed on the JVM (hasch.fast + Integer/numberOfLeadingZeros).
(def expected-lzpl1
  [0 3 0 1 2 0 0 0 0 0 0 0 0 0 3 1 0 0 1 0 1 0 2 0 1 0 0 0 2 1 0 3 1 0 3 1 1 2 0 0
   1 0 0 4 0 1 1 1 2 0 1 0 1 1 0 1 3 0 1 0])

(def expected-lzpl2
  [0 1 0 0 1 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 1 0 0 1 0 0 1 0 0 1 0 0
   0 0 0 2 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0])

(deftest key-level-cross-platform
  (is (= expected-lzpl1 (mapv #(b/key-level % 1) battery))
      "key-level (lzpl 1) matches the JVM-computed reference on this platform")
  (is (= expected-lzpl2 (mapv #(b/key-level % 2) battery))
      "key-level (lzpl 2) matches the JVM-computed reference on this platform"))
