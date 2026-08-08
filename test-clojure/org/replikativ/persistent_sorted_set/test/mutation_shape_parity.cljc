(ns org.replikativ.persistent-sorted-set.test.mutation-shape-parity
  "The JVM and ClojureScript must build the SAME TREE through `conj` and `disj`,
   not only through the bulk builders.

   `shape_parity.cljc` pinned `from-sequential`. The mutation paths were never
   compared, and they had drifted in three independent ways — each one a case
   where ClojureScript did something a B-tree does not have to do:

   1. `util/rotate` rebalanced at `len <= min`, where the JVM rebalances only on
      UNDERFLOW (`centerLen >= minBranchingFactor()` leaves the node alone). A
      node at exactly min fill is legally filled. Merging it also THRASHES:
      min + min = `bf` exactly, so the next insert splits it straight back, and
      each cycle rewrites O(bf) nodes — new blobs and freed addresses under a
      content-addressed store, not just CPU.

   2. `rotate`'s join and borrow arms asked different questions from the JVM's.
      The JVM joins when the result FITS (`left.len + len <= bf`) and borrows from
      the LARGER sibling; ClojureScript joined when the sibling was small
      (`left.len <= min-len`) and borrowed from the SMALLER one.

   3. `leaf/add` cut at `middle` and then spliced the new key into whichever half
      contained it, so the extra element went left or right depending on the
      insert POSITION. The JVM is position-independent by construction and says so
      at the call site: `// count: position-independent, ignores ins`.

   Measured before the fixes: ascending `conj` agreed (the extra always lands
   last) while descending diverged almost everywhere, including in LEAF COUNT —
   bf 8 n 33 gave JVM `[8 5 5 5 5 5]` against `[5 4 4 4 4 4 4 4]`. Bulk-built then
   deleting every 3rd at bf 8, the JVM shrank in place to `[4 x14, 5, 5]` (~52%
   full) where ClojureScript merged to `[8 8 8 8 6 6 8 8 6]` (~93% full).

   `min-len` was also `(/ bf 2)`, and ClojureScript `/` is FLOAT division, so an
   odd branching factor gave 7.5 where the JVM's `bf >>> 1` and
   `diagnostics/check-sizes`' `(quot bf 2)` both give 7 — a rebalance threshold
   that disagreed with its own validator.

   ## Why literals

   The expectations are the profiles MEASURED on the JVM, written out rather than
   computed from the partition rule. A test that derives its expectation from the
   implementation agrees with whatever the implementation does, including with a
   regression. These numbers are the contract, and because this is a `.cljc` both
   runtimes are held to them.

   Contents equality cannot see any of this: the trees are `=`, count the same,
   and iterate identically. What differs is how they are STORED — and under
   content-addressed storage the node bytes ARE the address, so a different cut is
   a different merkle root and no node sharing between two databases holding the
   same data."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.diagnostics :as diag]))

(defn- profile [set] (diag/fanout-profile set))

(defn- conj-built [bf order n]
  (reduce #(s/conj %1 %2 compare)
          (s/sorted-set* {:comparator compare :branching-factor bf})
          (case order :asc (range n) :desc (reverse (range n)))))

;; [branching-factor order n expected-profile] — measured on the JVM.
;; Descending is the discriminator: ascending agreed even before the fix, so a
;; table covering only it would prove nothing.
(def ^:private conj-cases
  [[8   :asc  9    [[4 5] [9]]]
   [8   :asc  17   [[4 4 4 5] [17]]]
   [8   :asc  20   [[4 4 4 8] [20]]]
   [8   :asc  33   [[4 4 4 4 4 4 4 5] [33]]]
   [8   :asc  40   [[4 4 4 4 4 4 4 4 8] [16 24] [40]]]
   [8   :asc  100  [[4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 8] [16 16 16 16 36] [100]]]
   [8   :desc 9    [[4 5] [9]]]
   [8   :desc 17   [[7 5 5] [17]]]
   [8   :desc 20   [[5 5 5 5] [20]]]
   [8   :desc 33   [[8 5 5 5 5 5] [33]]]
   [8   :desc 40   [[5 5 5 5 5 5 5 5] [40]]]
   [8   :desc 100  [[5 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5] [25 25 25 25] [100]]]
   [16  :asc  9    [[9]]]
   [16  :asc  17   [[8 9] [17]]]
   [16  :asc  20   [[8 12] [20]]]
   [16  :asc  33   [[8 8 8 9] [33]]]
   [16  :asc  40   [[8 8 8 16] [40]]]
   [16  :asc  100  [[8 8 8 8 8 8 8 8 8 8 8 12] [100]]]
   [16  :desc 9    [[9]]]
   [16  :desc 17   [[8 9] [17]]]
   [16  :desc 20   [[11 9] [20]]]
   [16  :desc 33   [[15 9 9] [33]]]
   [16  :desc 40   [[13 9 9 9] [40]]]
   [16  :desc 100  [[10 9 9 9 9 9 9 9 9 9 9] [100]]]])

;; [branching-factor n drop-every expected-profile]
(def ^:private disj-cases
  [[8   40   2 [[6 6 4 4] [20]]]
   [8   40   3 [[8 8 5 5] [26]]]
   [8   100  2 [[6 6 6 6 4 4 6 4 8] [24 26] [50]]]
   [8   100  3 [[8 8 8 8 5 5 6 8 5 5] [42 24] [66]]]
   [16  40   2 [[12 8] [20]]]
   [16  40   3 [[10 8 8] [26]]]
   [16  100  2 [[12 12 12 14] [50]]]
   [16  100  3 [[10 11 11 10 9 15] [66]]]])

;; [branching-factor n expected-profile] — bulk-built, then every 3rd removed
(def ^:private bulk-disj-cases
  [[8   100  [[4 4 4 4 4 4 4 4 4 4 4 4 4 4 5 5] [24 20 22] [66]]]
   [16  100  [[8 8 8 8 8 8 8 10] [66]]]])

(deftest conj-builds-the-same-tree-on-both-runtimes
  (testing "ascending AND descending insert orders"
    (doseq [[bf order n expected] conj-cases]
      (is (= expected (profile (conj-built bf order n)))
          (str "bf=" bf " " order " n=" n)))))

(deftest disj-builds-the-same-tree-on-both-runtimes
  (testing "rebalancing after removal — the divergence that cost the most, since
            it fired on every delete that brought a node to min fill"
    (doseq [[bf n drop-every expected] disj-cases]
      (let [base  (conj-built bf :asc n)
            after (reduce #(s/disj %1 %2 compare) base
                          (filter #(zero? (mod % drop-every)) (range n)))]
        (is (= expected (profile after))
            (str "bf=" bf " n=" n " drop-every=" drop-every))))))

(deftest disj-from-a-bulk-built-tree-agrees
  (testing "the starting shapes already agreed here, so any difference after the
            deletes comes from rebalancing ALONE — this is where the gap was
            widest, 9 leaves against 16"
    (doseq [[bf n expected] bulk-disj-cases]
      (let [base  (s/from-sequential compare (vec (range n)) {:branching-factor bf})
            after (reduce #(s/disj %1 %2 compare) base (range 0 n 3))]
        (is (= expected (profile after)) (str "bf=" bf " n=" n))))))

(deftest a-node-at-minimum-fill-is-left-alone
  (testing "the invariant behind fix (1), stated directly rather than through a
            literal: deleting down to exactly min must NOT merge, because min is a
            legal fill. Merging there yields a node of exactly bf that the next
            insert splits again — the thrashing signature."
    (doseq [bf [8 16 32]]
      (let [minf  (quot bf 2)
            n     (* bf 8)
            base  (s/from-sequential compare (vec (range n)) {:branching-factor bf})
            after (reduce #(s/disj %1 %2 compare) base (range 0 n 3))
            widths (first (profile after))]
        (is (every? #(>= % minf) widths)
            (str "bf=" bf ": every leaf at or above min fill " minf
                 ", got " (pr-str widths)))
        (is (some #(< % bf) widths)
            (str "bf=" bf ": and NOT everything merged up to full, which is what"
                 " rebalancing at min looks like. got " (pr-str widths)))))))
