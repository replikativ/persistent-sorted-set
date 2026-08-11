(ns org.replikativ.persistent-sorted-set.test.tree-shape
  "The tree a set ends up with must depend only on its CONTENTS and its branching
   factor — not on the runtime that built it, not on the operation that built it,
   and not on whether the caller batched through a transient.

   Contents equality cannot see any of this. The trees compared below are `=`,
   count the same, and iterate identically; what differs is how they are STORED.
   Under content-addressed storage the node bytes ARE the address, so a different
   cut is a different merkle root — the same datoms getting a different address on
   Node than on the JVM, or a different one through `into` than through
   `persistent!`, and no node sharing between two databases holding the same data.
   Shape is therefore compared with `fanout-profile`, never with `=`.

   ## Why the expectations are literals

   The tables below are the profiles MEASURED on the JVM, written out rather than
   computed from the partition rule. A test that derives its expectation from the
   implementation agrees with whatever the implementation does, including with a
   regression. These numbers are the contract, and because this is a `.cljc` both
   runtimes are held to them.

   ## 1. Bulk builders (`from-sequential`)

   `split` on the JVM takes `avg` when at least `2*avg` elements remain;
   `arr-partition-approx` on ClojureScript took `chunk-len` when at least
   `chunk-len + min-len` remained, and tested `<= max-len` first rather than
   second. Since `2*avg = min+max > avg+min`, the two disagreed for every
   remainder in `[avg+min, 2*avg)` — with the default branching factor of 512:

       n     JVM              ClojureScript (before)
       640   [320 320]        [384 256]
       700   [350 350]        [384 316]
       767   [383 384]        [384 383]

   ## 2. Mutation paths (`conj` / `disj`)

   The bulk builders were pinned first; the mutation paths were never compared,
   and had drifted in three independent ways — each one a case where ClojureScript
   did something a B-tree does not have to do:

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

   ## 3. Transients (JVM only)

   A transient must not change the tree, only the cost of building it.
   `Leaf.remove`'s borrow arm chose its sibling with

       if (left != null && (left.editable() || right == null || left._len >= right._len))

   and `left.editable()` is true only inside a transient. So the same logical
   deletion produced a different tree depending on whether the caller batched it.
   Measured before the fix — build by `conj`, then delete every k-th element,
   once persistently and once through a transient:

       bf 16  n 40   drop 3   persistent [10 8 8]             transient [8 8 10]
       bf 16  n 100  drop 3   persistent [10 11 11 10 9 15]   transient [10 11 11 10 16 8]
       bf  8  n 100  drop 2   persistent [6 6 6 6 4 4 6 4 8]  transient [6 6 6 6 4 4 6 4 4 4]

   Three of eight shapes differed, and datahike batches through `db-transient`, so
   both paths are live in one consumer. `Branch.remove`'s corresponding arm never
   had the clause, so the leaf and branch rules also disagreed with each other.
   `editable()` is still consulted inside the arm to rebalance in place; that is
   about HOW, not about WHICH sibling, and does not reach the shape.

   These three tests are `#?(:clj)` because they drive `.asTransient` /
   `.persistent` on `PersistentSortedSet` directly; ClojureScript has no true
   transient to compare against yet."
  (:require
   [clojure.test :refer [deftest is testing]]
   [org.replikativ.persistent-sorted-set :as s]
   [org.replikativ.persistent-sorted-set :as set]
   [org.replikativ.persistent-sorted-set.diagnostics :as diag])
  #?(:clj (:import [org.replikativ.persistent_sorted_set PersistentSortedSet])))

#?(:clj (set! *warn-on-reflection* true))

;; ---------------------------------------------------------------------------
;; 1. Bulk builders
;; ---------------------------------------------------------------------------

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

;; ---------------------------------------------------------------------------
;; 2. Mutation paths
;; ---------------------------------------------------------------------------

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

;; ---------------------------------------------------------------------------
;; 3. Transients (JVM only)
;; ---------------------------------------------------------------------------

#?(:clj
   (defn- conj-built-asc [bf n]
     (reduce #(s/conj %1 %2 compare)
             (s/sorted-set* {:comparator compare :branching-factor bf})
             (range n))))

#?(:clj
   (defn- drop-persistently [base ks]
     (reduce #(s/disj %1 %2 compare) base ks)))

#?(:clj
   (defn- drop-transiently [base ks]
     (let [t (reduce (fn [^PersistentSortedSet t k] (.disjoin t k compare))
                     (.asTransient ^PersistentSortedSet base) ks)]
       (.persistent ^PersistentSortedSet t))))

#?(:clj
   (deftest a-transient-remove-builds-the-same-tree-as-a-persistent-one
     (testing "the whole tree, level by level — not just the leaves, and not just
               the contents"
       (doseq [bf [8 16 32] n [40 100 400] drop-every [2 3 5]]
         (let [base (conj-built-asc bf n)
               ks   (filter #(zero? (mod % drop-every)) (range n))
               pers (drop-persistently base ks)
               tran (drop-transiently base ks)
               lbl  (str "bf=" bf " n=" n " drop-every=" drop-every)]
           (is (= (vec (seq pers)) (vec (seq tran)))
               (str lbl ": contents agree (they always did — this is the precondition,
                    not the point)"))
           (is (= (diag/fanout-profile pers) (diag/fanout-profile tran))
               (str lbl ": SHAPE must agree too. persistent "
                    (pr-str (diag/fanout-profile pers)) " vs transient "
                    (pr-str (diag/fanout-profile tran)))))))))

#?(:clj
   (deftest a-transient-add-builds-the-same-tree-as-a-persistent-one
     (testing "the insert side of the same property, so a future change to the split
               rule cannot break one path and leave the other"
       (doseq [bf [8 16 32] n [40 100 400]]
         (let [xs   (range n)
               pers (reduce #(s/conj %1 %2 compare)
                            (s/sorted-set* {:comparator compare :branching-factor bf}) xs)
               tran (let [t (reduce (fn [^PersistentSortedSet t k] (.cons t k))
                                    (.asTransient ^PersistentSortedSet
                                     (s/sorted-set* {:comparator compare
                                                     :branching-factor bf}))
                                    xs)]
                      (.persistent ^PersistentSortedSet t))
               lbl  (str "bf=" bf " n=" n)]
           (is (= (vec (seq pers)) (vec (seq tran))) (str lbl ": contents"))
           (is (= (diag/fanout-profile pers) (diag/fanout-profile tran))
               (str lbl ": SHAPE. persistent " (pr-str (diag/fanout-profile pers))
                    " vs transient " (pr-str (diag/fanout-profile tran)))))))))

#?(:clj
   (deftest interleaved-add-and-remove-agree
     (testing "a mixed batch, where borrow and merge both fire repeatedly and any
               selection difference compounds"
       (doseq [bf [8 16]]
         (let [ops  (for [i (range 600)]
                      [(if (zero? (mod i 3)) :remove :add) (mod (* i 37) 500)])
               pers (reduce (fn [acc [op k]]
                              (if (= op :add) (s/conj acc k compare) (s/disj acc k compare)))
                            (s/sorted-set* {:comparator compare :branching-factor bf}) ops)
               tran (let [t (reduce (fn [^PersistentSortedSet t [op k]]
                                      (if (= op :add) (.cons t k) (.disjoin t k compare)))
                                    (.asTransient ^PersistentSortedSet
                                     (s/sorted-set* {:comparator compare
                                                     :branching-factor bf}))
                                    ops)]
                      (.persistent ^PersistentSortedSet t))]
           (is (= (vec (seq pers)) (vec (seq tran))) (str "bf=" bf ": contents"))
           (is (= (diag/fanout-profile pers) (diag/fanout-profile tran))
               (str "bf=" bf ": SHAPE after an interleaved batch")))))))
