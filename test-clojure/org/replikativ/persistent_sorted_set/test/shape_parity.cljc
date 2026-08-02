(ns org.replikativ.persistent-sorted-set.test.shape-parity
  "The JVM and ClojureScript builders must produce the SAME TREE, not merely the
   same elements.

   This file exists because for a long time they did not. `split` on the JVM
   takes `avg` when at least `2*avg` elements remain; `arr-partition-approx` on
   ClojureScript took `chunk-len` when at least `chunk-len + min-len` remained,
   and tested `<= max-len` first rather than second. Since
   `2*avg = min+max > avg+min`, the two disagreed for every remainder in
   `[avg+min, 2*avg)` — with the default branching factor of 512:

       n     JVM              ClojureScript (before)
       640   [320 320]        [384 256]
       700   [350 350]        [384 316]
       767   [383 384]        [384 383]

   Contents equality could never have caught it: those trees are `=`, count the
   same, and iterate identically. What differs is how they are STORED, and under
   content-addressed storage the node bytes are the address — so the same
   datoms, loaded on Node and on the JVM, produced different node addresses and
   a different merkle root, and neither database could share a single node with
   the other.

   The expectations below are written as literals rather than computed from the
   partition rule. A test that derives its expectation from the implementation
   agrees with whatever the implementation does, including with a regression;
   these numbers were measured on both runtimes and are the contract."
  (:require
   [clojure.test :refer [deftest is testing]]
   [org.replikativ.persistent-sorted-set :as set]
   [org.replikativ.persistent-sorted-set.diagnostics :as diag]))

(defn- leaf-widths
  "Element counts of the LEAF level for a set built from `(range n)`."
  [n]
  (first (diag/fanout-profile
          (set/from-sequential compare (vec (range n)) {:branching-factor 512}))))

(def ^:private expected
  "Measured on both runtimes at branching-factor 512. The first three are the
   window where the two implementations used to disagree; the rest are the
   sizes that always agreed, kept so a 'fix' that moves them shows up here."
  {512  [512]
   513  [256 257]
   640  [320 320]
   700  [350 350]
   767  [383 384]
   768  [384 384]
   900  [384 258 258]
   1500 [384 384 366 366]})

(deftest leaf-cuts-match-the-jvm-partition-rule
  (testing "identical cuts on both runtimes — the property that makes a node
            address mean the same thing wherever the tree was built"
    (doseq [[n widths] (sort expected)]
      (is (= widths (leaf-widths n))
          (str "n=" n)))))

(deftest the-disagreement-window-is-covered
  (testing "`[avg+min, 2*avg)` = [640, 768) at the default branching factor is
            exactly where the two rules diverged, so walk it rather than
            trusting the three sampled points.

            Every cut must be at most `max-len`, at least `min-len` unless it is
            the only node, and the widths must sum to n — properties the old
            ClojureScript rule DID satisfy, which is why only a direct
            comparison against the JVM's numbers caught the difference."
    (doseq [n (range 640 768 8)]
      (let [widths (leaf-widths n)]
        (is (= n (reduce + 0 widths)) (str "n=" n ": every element is present"))
        (is (every? #(<= % 512) widths) (str "n=" n ": no node exceeds max-len"))
        (is (or (= 1 (count widths)) (every? #(>= % 256) widths))
            (str "n=" n ": no node falls below min-len"))
        ;; the JVM rule halves in this window, so the cuts come out even
        (is (>= 1 (- (apply max widths) (apply min widths)))
            (str "n=" n ": halved, so the two nodes differ by at most one"))))))

(deftest an-odd-branching-factor-does-not-desync-the-runtimes
  (testing "`min-len` is `bf >>> 1` on the JVM and was `(/ bf 2)` on
            ClojureScript — 256 versus 256.5 for an odd branching factor, which
            made every downstream fanout decision differ by half an element on
            one runtime only. Nothing exercised an odd branching factor, so it
            never showed."
    (let [s (set/from-sequential compare (vec (range 2000)) {:branching-factor 33})
          widths (first (diag/fanout-profile s))]
      (is (= 2000 (reduce + 0 widths)))
      (is (every? #(<= % 33) widths) "no node exceeds an odd max-len")
      (is (every? integer? widths) "and the cuts are whole elements"))))
