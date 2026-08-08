(ns org.replikativ.persistent-sorted-set.test.measure-known
  "Every node must carry a known measure, however the set was built.

   `diagnostics/validate-measures-known` is the library's own shipped invariant: with a
   measure configured, no node may be left with a nil measure. A nil is not merely a missing
   cache — nothing on the write path recomputes it, so once a node loses its measure it stays
   lost, and `:measure` is serialized, so the nil goes to disk.

   The invariant was only ever checked on the JVM. Measured on ClojureScript before the fix,
   nodes carrying a cached measure:

                    conj-built                      bulk-built
       JVM    64/64, 330/330, 13/13, 66/66     40/40, 201/201, 9/9, 45/45   validator :pass
       cljs    0/64,   0/330,  0/13,  0/66     40/40, 201/201, 9/9, 45/45   :conj FAIL

   So the shipped validator failed on ClojureScript for every incrementally built set, and
   cljs was internally inconsistent as well: its bulk builders compute measures eagerly, so
   bulk and incremental builds of the SAME set produced different blobs on the same runtime.

   This runs the validator itself rather than a reimplementation, on both runtimes and both
   build paths, so the two can never drift apart again without something failing."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.diagnostics :as diag]
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.numeric-stats :as numeric-stats]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set NumericStatsOps])))

(def ^:private cmp compare)

;; The shipped measure the rest of the suite uses.
(def ^:private stats-ops
  #?(:clj  (NumericStatsOps/instance)
     :cljs numeric-stats/numeric-stats-ops))

(defn- opts [bf] {:comparator cmp :branching-factor bf :measure stats-ops})

(defn- conj-built [bf n]
  (reduce #(set/conj %1 %2 cmp) (set/sorted-set* (opts bf)) (range n)))

(defn- bulk-built [bf n]
  (set/from-sequential cmp (vec (range n)) (opts bf)))

(deftest every-node-has-a-measure-however-it-was-built
  (testing "the shipped invariant, on both build paths — a nil measure is permanent, since
            nothing on the write path recomputes it, and it is serialized"
    (doseq [bf [8 16] n [40 400]]
      (let [lbl (str "bf=" bf " n=" n)]
        (is (true? (diag/validate-measures-known (conj-built bf n)))
            (str lbl " conj-built"))
        (is (true? (diag/validate-measures-known (bulk-built bf n)))
            (str lbl " bulk-built"))))))

(deftest the-two-build-paths-agree
  (testing "bulk and incremental builds of the SAME elements must both satisfy the invariant —
            cljs computed measures eagerly in its bulk builders and not at all incrementally,
            so the two paths disagreed on the same runtime"
    (doseq [bf [8 16] n [40 400]]
      (let [c (conj-built bf n)
            b (bulk-built bf n)]
        (is (= (vec (seq c)) (vec (seq b))) (str "bf=" bf " n=" n ": same contents"))
        (is (true? (diag/validate-measures-known c)) (str "bf=" bf " n=" n ": conj-built"))
        (is (true? (diag/validate-measures-known b)) (str "bf=" bf " n=" n ": bulk-built"))))))
