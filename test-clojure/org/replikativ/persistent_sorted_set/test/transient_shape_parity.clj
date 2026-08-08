(ns org.replikativ.persistent-sorted-set.test.transient-shape-parity
  "A transient must not change the TREE, only the cost of building it.

   `Leaf.remove`'s borrow arm chose its sibling with

       if (left != null && (left.editable() || right == null || left._len >= right._len))

   and `left.editable()` is true only inside a transient. So the same logical
   deletion produced a different tree depending on whether the caller batched it.
   Measured before the fix — build by `conj`, then delete every k-th element,
   once persistently and once through a transient:

       bf 16  n 40   drop 3   persistent [10 8 8]             transient [8 8 10]
       bf 16  n 100  drop 3   persistent [10 11 11 10 9 15]   transient [10 11 11 10 16 8]
       bf  8  n 100  drop 2   persistent [6 6 6 6 4 4 6 4 8]  transient [6 6 6 6 4 4 6 4 4 4]

   Three of eight shapes differed. Contents were identical in every case, which is
   why nothing caught it: `=`, `count` and `seq` all agree, and only the STORED
   form differs. Under content-addressed storage the node bytes are the address,
   so this is the same data getting a different merkle root depending on how it
   was written — and datahike batches through `db-transient`, so both paths are
   live in one consumer.

   `Branch.remove`'s corresponding arm never had the clause, so the leaf and
   branch rules also disagreed with each other. `editable()` is still consulted
   inside the arm to rebalance in place; that is about HOW, not about WHICH
   sibling, and does not reach the shape.

   Shape is compared with `fanout-profile` rather than `=`, because `=` cannot see
   the difference this test exists for."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.diagnostics :as diag])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet]))

(set! *warn-on-reflection* true)

(defn- build [bf n]
  (reduce #(s/conj %1 %2 compare)
          (s/sorted-set* {:comparator compare :branching-factor bf})
          (range n)))

(defn- drop-persistently [base ks]
  (reduce #(s/disj %1 %2 compare) base ks))

(defn- drop-transiently [base ks]
  (let [t (reduce (fn [^PersistentSortedSet t k] (.disjoin t k compare))
                  (.asTransient ^PersistentSortedSet base) ks)]
    (.persistent ^PersistentSortedSet t)))

(deftest a-transient-remove-builds-the-same-tree-as-a-persistent-one
  (testing "the whole tree, level by level — not just the leaves, and not just
            the contents"
    (doseq [bf [8 16 32] n [40 100 400] drop-every [2 3 5]]
      (let [base (build bf n)
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
                 (pr-str (diag/fanout-profile tran))))))))

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
                 " vs transient " (pr-str (diag/fanout-profile tran))))))))

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
            (str "bf=" bf ": SHAPE after an interleaved batch"))))))
