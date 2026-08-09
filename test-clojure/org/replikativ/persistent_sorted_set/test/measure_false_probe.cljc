(ns org.replikativ.persistent-sorted-set.test.measure-false-probe
  "`false` is a legal measure value, and was being treated as \"no measure\".

   An `IMeasure` is a monoid over whatever carrier the user picks. Nothing says that carrier
   must exclude `false` — `and` over a predicate is one of the most natural measures there is
   (\"are all elements non-zero\", \"is every entry valid\"), and `or` is its twin. The Java
   core is careful about this: it tests `childMeasure == null`. The Clojure and ClojureScript
   layers were not — they tested the measure VALUE for truthiness, so a `false` measure was
   indistinguishable from an absent one.

   Measured before the fix, `and` over `(not= 0 k)` on 0..19 built with `from-sorted-array`
   at bf 4 — element 0 makes the correct answer `false`:

       root-level 2, measure TRUE      ← wrong; the false child was skipped as if absent

   Not merely a missing cache: a WRONG aggregate, silently. And on ClojureScript it was worse
   than wrong, because every incremental maintenance site is guarded the same way
   (`(and measure-ops (.-_measure this))`), so the first `false` would stop maintenance for
   that node's whole spine — the same permanent-loss shape that a nil measure had.

   20 sites in all: 11 in `branch.cljs`, 8 in `leaf.cljs`, 1 in `persistent_sorted_set.clj`'s
   bulk builder. All now test `some?`.

   This probe came from an independent review that was interrupted before reporting; the
   scenario is preserved here rather than lost."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as p]
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.measure :as measure]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set IMeasure])))

;; `and` over "is non-zero" — identity true, and a subtree containing 0 measures FALSE.
(def ^:private bool-and
  #?(:clj
     (reify IMeasure
       (identity [_] true)
       (extract [_ k] (not= 0 k))
       (merge [_ a b] (and a b))
       (remove [_ _current _key recompute] (.get recompute)))
     :cljs
     (reify measure/IMeasure
       (identity-measure [_] true)
       (extract [_ k] (not= 0 k))
       (merge-measure [_ a b] (and a b))
       (remove-measure [_ _current _key recompute] (recompute))
       (weight [_ _] 1))))

(defn- bulk [n bf]
  #?(:clj  (p/from-sorted-array compare (to-array (range n)) n
                                {:branching-factor bf :measure bool-and})
     :cljs (p/from-sequential compare (vec (range n))
                              {:branching-factor bf :measure bool-and})))

(defn- incremental [n bf]
  (reduce #(p/conj %1 %2 compare)
          (p/sorted-set* {:comparator compare :branching-factor bf :measure bool-and})
          (range n)))

(deftest a-false-measure-survives-the-bulk-builder
  (testing "element 0 is present, so `and` over (not= 0 k) must be FALSE. Truthiness testing
            skipped the false child as though it had no measure and returned true."
    (doseq [bf [4 8]]
      (is (false? (p/measure (bulk 20 bf)))
          (str "bf=" bf ": a subtree containing 0 must measure false, not true")))))

(deftest a-false-measure-survives-the-incremental-path
  (testing "and the same on the conj path, where a false measure previously looked like an
            absent one and would have stopped maintenance for the whole spine"
    (doseq [bf [4 8]]
      (is (false? (p/measure (incremental 20 bf)))
          (str "bf=" bf ": incremental build must agree")))))

(deftest a-true-measure-is-still-true
  (testing "the control — without the zero, the same monoid must yield true. A fix that
            confused false with nil in the other direction would pass the tests above."
    (doseq [bf [4 8]]
      (let [all-nonzero #?(:clj  (p/from-sorted-array compare (to-array (range 1 21)) 20
                                                      {:branching-factor bf :measure bool-and})
                           :cljs (p/from-sequential compare (vec (range 1 21))
                                                    {:branching-factor bf :measure bool-and}))]
        (is (true? (p/measure all-nonzero))
            (str "bf=" bf ": no zero present, so the measure must be true"))))))
