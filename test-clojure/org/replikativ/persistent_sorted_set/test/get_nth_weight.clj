(ns org.replikativ.persistent-sorted-set.test.get-nth-weight
  "`get-nth` has two jobs, and conflating them is what broke it.

   ## What it is for

   The motivating consumer is stratum, whose set elements are CHUNKS: one element holds many
   data points. Its measure summarises a subtree and `weight` answers 'how many logical items
   are under here', which is what lets `getNth` descend by accumulated position and return the
   containing element PLUS an offset inside it. That is why `getNth` has an out-parameter at
   all. It is the measured/annotated B-tree — `splitAt` on a size monoid.

   `weight` must therefore be a MONOID HOMOMORPHISM into (long, +):

       weight(identity())    == 0
       weight(merge(a, b))   == weight(a) + weight(b)

   ## What was wrong

   `IMeasure.weight` defaulted to `return 1`, which satisfies NEITHER law — identity must map
   to 0, and a merged measure must sum rather than stay 1. It is correct only for a
   single-element tree. So any measure that did not override it made `getNth` believe the
   whole tree weighed one:

       10 elements, a counting measure without weight()
       (get-nth s 0) => [0 0]   and (get-nth s 1..11) => nil, every one

   A public function on an advertised feature, silently answering nil.

   ## Why the default was tempting, and what actually fixes that

   Because `getNth` USED to demand a measure even when you only wanted the nth ELEMENT by
   position — so a caller with no interest in weights had to invent one, and a default that
   pretended each entry weighed 1 looked like a kindness. It was not: it was wrong for every
   tree with more than one element.

   The real fix is that the unweighted case no longer needs a measure. The tree already
   maintains `subtreeCount` for `count-slice`, so `getNth` navigates that directly and every
   element weighs exactly one, with offset 0. Weighted navigation is now explicitly opt-in and
   the homomorphism is mandatory."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set])
  (:import [org.replikativ.persistent_sorted_set IMeasure]))

;; A PROPER homomorphism: every element weighs 2, so the tree's weight is 2n and each element
;; owns offsets 0 and 1. Modelled on stratum's chunks, shrunk to something assertable.
(def ^:private weighs-two
  (reify IMeasure
    (identity [_] 0)
    (extract [_ _] 2)
    (merge [_ a b] (+ (long a) (long b)))
    (remove [_ current _ _] (- (long current) 2))
    (weight [_ m] (long m))))

;; A counting measure that does NOT implement weight — the shape that used to answer nil.
(def ^:private no-weight
  (reify IMeasure
    (identity [_] 0)
    (extract [_ _] 1)
    (merge [_ a b] (+ (long a) (long b)))
    (remove [_ current _ _] (dec (long current)))))

(defn- build [n bf opts]
  (set/from-sorted-array compare (to-array (range n)) n (clojure.core/merge
                                                         {:branching-factor bf} opts)))

(deftest get-nth-without-a-measure-indexes-by-element-position
  (testing "the common case must not require inventing a measure — the tree already keeps the
            subtree counts this needs"
    (doseq [bf [4 8 64 512]
            n  [1 5 50 500]]
      (let [s (build n bf {})]
        (is (= (mapv vector (range n) (repeat 0))
               (mapv #(set/get-nth s %) (range n)))
            (str "bf=" bf " n=" n ": every index must give [element 0]"))
        (is (nil? (set/get-nth s n))   (str "bf=" bf " n=" n ": one past the end"))
        (is (nil? (set/get-nth s -1))  (str "bf=" bf " n=" n ": negative"))))))

(deftest get-nth-with-a-weighted-measure-returns-the-element-and-an-offset
  (testing "stratum's case: elements are runs, and rank addresses a position INSIDE one"
    (doseq [bf [4 8 64]]
      (let [n 20
            s (build n bf {:measure weighs-two})]
        (is (= (* 2 n) (set/measure s))
            (str "bf=" bf ": precondition — the tree weighs 2 per element"))
        (is (= (vec (mapcat (fn [e] [[e 0] [e 1]]) (range n)))
               (mapv #(set/get-nth s %) (range (* 2 n))))
            (str "bf=" bf ": each element owns two consecutive ranks"))
        (is (nil? (set/get-nth s (* 2 n)))
            (str "bf=" bf ": past the total weight, not past the element count"))))))

(deftest a-measure-without-weight-is-refused-rather-than-answering-nil
  (testing "the defect: the old default returned 1 for every measure, so the tree believed it
            weighed one and get-nth answered nil for every index above 0"
    (let [s (build 10 4 {:measure no-weight})]
      (is (= 10 (set/measure s)) "precondition: the measure itself is fine, it counts")
      (is (thrown? UnsupportedOperationException (set/get-nth s 1))
          "a measure with no weight() must say so, not answer nil")
      (is (thrown? UnsupportedOperationException (set/get-nth s 0))
          "including at rank 0, which the old default happened to get right"))))

(deftest the-two-paths-agree-where-they-overlap
  (testing "a measure whose weight is 1 per element must give the same answers as no measure
            at all — the cross-check that keeps the unweighted path honest"
    (let [unit (reify IMeasure
                 (identity [_] 0)
                 (extract [_ _] 1)
                 (merge [_ a b] (+ (long a) (long b)))
                 (remove [_ current _ _] (dec (long current)))
                 (weight [_ m] (long m)))]
      (doseq [bf [4 64]]
        (let [n 100
              plain    (build n bf {})
              measured (build n bf {:measure unit})]
          (is (= (mapv #(set/get-nth plain %) (range n))
                 (mapv #(set/get-nth measured %) (range n)))
              (str "bf=" bf ": weighted-by-1 and unweighted must agree")))))))
