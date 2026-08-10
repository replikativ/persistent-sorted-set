(ns org.replikativ.persistent-sorted-set.test.diagnostics-false-positives
  "Ways `validate-full` called a healthy tree corrupt. They were introduced by this cycle's
   own work on the validator, and found by an adversarial review rather than by the suite —
   because a validator that cries wolf breaks nothing that any existing test looks at.

   A false positive is not a lesser bug than a false negative here. `validate-content` is the
   entry point datahike would call to certify an imported index; one that refuses correct data
   is not a weaker check, it is an unusable one.

   ## 1. An inexact measure drifted, and the oracle called it corruption

   `check-measure-agreement` re-folded a leaf's keys from `identity` and compared with `=`.
   But `IMeasure/remove` exists so a measure can be maintained INCREMENTALLY — the shipped
   NumericStats subtracts from `sum`/`sumSq` — and floating-point addition is neither
   associative nor exactly invertible. Cached and re-folded therefore differ in the last bits
   for any float measure, on a perfectly healthy tree.

   Measured over 72 shapes: 55 false positives, e.g. cached sum 1593.6 against expected
   1593.5999999999995. By key type: `long` 0 of 24, `double` 15-17 of 24 — the signature of
   float arithmetic, not of a tree defect.

   The fix is that measure checking is OPT-IN, with the contract stated: enable it only for an
   exact measure. It is kept rather than deleted because it does catch a wrong measure."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.diagnostics :as diag])
  (:import [org.replikativ.persistent_sorted_set IMeasure]))

;; ---------------------------------------------------------------------------
;; 1. inexact measures

;; INEXACT on purpose: `remove` SUBTRACTS instead of recomputing, which is the whole point of
;; the incremental hook and the reason the cached value drifts from a fresh fold.
(def ^:private drifting-sum
  (reify IMeasure
    (identity [_] 0.0)
    (extract [_ k] (double k))
    (merge [_ a b] (+ (double a) (double b)))
    (remove [_ current key _] (- (double current) (double key)))))

(deftest an-inexact-measure-does-not-fail-validation-by-default
  (testing "the healthy tree that used to be called corrupt.

            TRANSIENT `disj!`, and that is the whole point. The first version of this test
            used `clojure.core/disj` and stayed GREEN against the unfixed build — the
            persistent path REBUILDS nodes and recomputes the measure from children, so
            `IMeasure/remove` is never asked to subtract and no drift is produced. Only the
            in-place transient arm takes the incremental path. Swept over 3 key types x 4
            branching factors x 4 sizes x 3 removal counts: every single false positive was
            `:transient`, and not one was `:persistent`. `long` keys never drifted at any
            shape either, which is the second half of the evidence that this is float
            arithmetic and not a tree defect."
    (doseq [tenth? [true false]
            bf     [16 64 512]
            n      [200 500]]
      (let [ks      (if tenth?
                      (mapv #(+ 0.1 (double %)) (range n))
                      (mapv #(/ (double %) 3.0) (range n)))
            removed (take 160 (drop 10 ks))
            s0      (set/from-sorted-array compare (to-array ks) n
                                           {:branching-factor bf :measure drifting-sum})
            s       (persistent! (reduce disj! (transient s0) removed))
            label   (str (if tenth? "x+0.1" "x/3") " bf=" bf " n=" n)]
        ;; The removals must actually have happened, or the incremental remove path never
        ;; ran and this test passes for the wrong reason against any build.
        (is (= (- n (count removed)) (count s))
            (str label ": precondition — the disj!s took effect"))
        ;; The tree really is healthy, asserted independently of the validator, so a
        ;; validator that simply passed everything could not satisfy this test.
        (is (= (count (seq s)) (count s))
            (str label ": precondition — count agrees with the elements"))
        (is (apply < (seq s))
            (str label ": precondition — still sorted"))
        (is (true? (diag/validate-full s))
            (str label ": a float measure's last-bit drift is not corruption"))))))

(deftest the-measure-check-is-reachable-and-still-has-teeth
  (testing "opt-in must mean opt-in, not deleted: an EXACT measure still validates under the
            flag, so the flag is wired to something"
    (let [exact (reify IMeasure
                  (identity [_] 0)
                  (extract [_ k] k)
                  (merge [_ a b] (+ (long a) (long b)))
                  (remove [_ _current _key recompute] (.get recompute)))]
      (doseq [bf [8 64]]
        (let [s (set/from-sorted-array compare (to-array (range 500)) 500
                                       {:branching-factor bf :measure exact})]
          (is (= (reduce + (range 500)) (set/measure s))
              (str "bf=" bf ": precondition — the measure is the sum"))
          (is (true? (diag/validate-full s {:check-measures? true}))
              (str "bf=" bf ": an exact measure passes under the flag")))))))
