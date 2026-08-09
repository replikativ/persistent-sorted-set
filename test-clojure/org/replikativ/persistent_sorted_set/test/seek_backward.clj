(ns org.replikativ.persistent-sorted-set.test.seek-backward
  "`seek` must honour its postcondition in BOTH directions.

   The docstring states it unconditionally — \"all Xs where to <= X\" for an ascending seq —
   and nothing warns or errors otherwise. (`README.md` calls seek a forward reposition, but
   that is a description of the intended use, not a guard: the code neither refuses a
   backward seek nor answers it correctly.)

   Both climb loops tested only the direction of travel:

       ascending    while (maxKey(node) < to)          -- only `to` ABOVE the subtree
       descending   while (to < minKey(node))          -- only `to` BELOW the subtree

   so a `to` on the other side never climbed the parent chain, and the descend re-ran on the
   node the iterator happened to be sitting in. Measured on `(apply sorted-set (range 10000))`
   before the fix:

       ascending   seek 5000 then 2500   ->  5008 elements, first 4992
                                             (documented: 7500 elements, first 2500)
       descending  seek 5000 then 7500   ->  5376 elements, first 5375
                                             (documented: 7501 elements, first 7500)

   The ascending answer is wrong in two directions at once: 2500..4991 are silently missing,
   AND 4992..4999 sit BELOW the position already consumed, so a merge-join re-emits them.
   The rewind distance is the leaf boundary, hence shape-dependent — which is exactly why
   every seek test in the suite, all of them forward, passes.

   ## What a backward seek does NOT restore

   A seq carries only its UPPER bound (`_keyTo`). A slice's lower bound is not part of its
   state, so seeking back below it yields elements under that bound — `(seek (slice s 5000
   5010) 2500)` gives [2500..5010]. That is the same rule as forward seek, which likewise
   keeps `_keyTo` and moves only the start. The upper bound IS retained and is pinned below."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]))

(defn- build [n bf]
  (set/from-sorted-array compare (to-array (range n)) n {:branching-factor bf}))

(deftest ascending-seek-goes-backward-correctly
  (testing "seeking below the current position must give every element at or after `to`,
            not the current leaf's first element"
    (doseq [bf [4 8 64 512]]
      (let [s (build 10000 bf)
            b (-> (seq s) (set/seek 5000) (set/seek 2500))]
        (is (= 2500 (first b)) (str "bf=" bf ": first element"))
        (is (= 7500 (count b)) (str "bf=" bf ": element count"))
        (is (= (range 2500 10000) b) (str "bf=" bf ": full contents"))))))

(deftest descending-seek-goes-backward-correctly
  (testing "the mirror: seeking ABOVE the current position on a descending seq"
    (doseq [bf [4 8 64 512]]
      (let [s (build 10000 bf)
            r (-> (set/rslice s 9999 nil) (set/seek 5000) (set/seek 7500))]
        (is (= 7500 (first r)) (str "bf=" bf ": first element"))
        (is (= 7501 (count r)) (str "bf=" bf ": element count"))
        (is (= (reverse (range 0 7501)) r) (str "bf=" bf ": full contents"))))))

(deftest forward-seek-is-unchanged
  (testing "the control — the direction that always worked must still work, including
            repeated forward seeks, which is how a merge-join drives it"
    (doseq [bf [4 8 64 512]]
      (let [s (build 10000 bf)
            f (-> (seq s) (set/seek 5000))]
        (is (= 5000 (first f)) (str "bf=" bf))
        (is (= 5000 (count f)) (str "bf=" bf))
        (is (= (range 7000 10000) (-> (seq s) (set/seek 5000) (set/seek 7000)))
            (str "bf=" bf ": forward then further forward")))
      (let [s (build 10000 bf)]
        (is (= (reverse (range 0 3001)) (-> (set/rslice s 9999 nil) (set/seek 5000) (set/seek 3000)))
            (str "bf=" bf ": descending, further down"))))))

(deftest seek-past-either-end
  (testing "beyond the last element there is nothing; below the first there is everything"
    (doseq [bf [4 8 512]]
      (let [s (build 10000 bf)]
        (is (nil? (-> (seq s) (set/seek 3000) (set/seek 99999)))
            (str "bf=" bf ": ascending past the end"))
        (is (= (range 0 10000) (-> (seq s) (set/seek 3000) (set/seek -5)))
            (str "bf=" bf ": ascending below the start"))
        (is (nil? (-> (set/rslice s 9999 nil) (set/seek 5000) (set/seek -5)))
            (str "bf=" bf ": descending below the start"))
        (is (= (reverse (range 0 10000)) (-> (set/rslice s 9999 nil) (set/seek 5000) (set/seek 99999)))
            (str "bf=" bf ": descending above the end"))))))

(deftest a-slices-upper-bound-survives-a-backward-seek
  (testing "seek moves the START; it must not widen the seq past the bound it carries"
    (doseq [bf [4 8 512]]
      (let [s (build 10000 bf)
            sl (set/slice s 2500 7500)]
        (is (nil? (set/seek sl 9000))
            (str "bf=" bf ": seeking past the upper bound yields nothing"))
        (is (= 7500 (last (set/seek sl 5000)))
            (str "bf=" bf ": a forward seek stays bounded above"))
        (is (= 7500 (last (set/seek (set/seek sl 5000) 3000)))
            (str "bf=" bf ": and so does a backward one"))))))

(deftest seek-agrees-with-a-fresh-slice-over-a-grid
  (testing "the general property, rather than a handful of points: for any pair of seek
            targets, the result must equal what a fresh slice from the LAST target gives.
            A shape-dependent defect hides from single-point tests, so sweep the grid."
    (doseq [bf [4 8 64]]
      (let [s (build 2000 bf)]
        (doseq [a [37 500 999 1500 1963]
                b [0 37 499 500 501 1000 1500 1999]]
          (let [got  (-> (seq s) (set/seek a) (set/seek b))
                want (seq (set/slice s b nil))]
            (is (= want got)
                (str "bf=" bf " seek " a " then " b))))))))
