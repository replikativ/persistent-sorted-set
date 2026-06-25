(ns org.replikativ.persistent-sorted-set.test.mst-iteration
  "Regression: an MST content-defined boundary splits a node only at *boundary
   keys* (hashes with >= lzpl leading zeros), NOT by size — so a node can hold far
   MORE than `branching-factor` keys when no boundary key falls in its range. The
   cljs iterator packs one per-level index into a fixed-width field of a BigInt
   path; that width MUST exceed any node's actual size or `seq`/`rseq`/`slice`
   silently truncate at the field's max.

   The bug: the field was `ceil(log2 branching-factor)` bits (6 for bf 64), so the
   last index of an oversized leaf was masked (`199 & 63 = 7`) and iteration
   stopped early — a 65-key MST leaf iterated to ONE element. The field is now a
   fixed 32 bits/level (like the JVM's int[]-per-level path). This guards every
   read direction across sizes that empirically produce oversized nodes."
  (:require [cljs.test :as test :refer-macros [is deftest]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.boundary :as bnd]))

(deftest mst-oversized-node-full-iteration
  (let [mst {:comparator compare :branching-factor 64 :boundary (bnd/mst-boundary 6)}]
    ;; sizes span below bf (controls) and well above it (oversized-node triggers:
    ;; 65/100/130/200 build a single >bf leaf; 300/1000 build multi-node MST trees).
    (doseq [n [1 8 63 64 65 100 130 200 300 1000]]
      (let [elems (range n)
            s     (into (set/sorted-set* mst) elems)]
        (is (= n (count s))                  (str "count, n=" n))
        (is (= (seq elems) (seq s))          (str "seq, n=" n))
        (is (= (seq (reverse elems)) (rseq s)) (str "rseq, n=" n))
        (when (>= n 64)
          (is (= (range 10 30) (set/slice s 10 29)) (str "slice [10,29], n=" n))
          (is (= (range (- n 5) n) (set/slice s (- n 5) (dec n)))
              (str "slice tail, n=" n)))))))
