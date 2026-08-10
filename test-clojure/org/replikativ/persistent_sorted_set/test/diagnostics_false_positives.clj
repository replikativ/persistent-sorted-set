(ns org.replikativ.persistent-sorted-set.test.diagnostics-false-positives
  "Two ways `validate-full` called a healthy tree corrupt. Both were introduced by this
   cycle's own work on the validator, and both were found by an adversarial review rather
   than by the suite — because a validator that cries wolf breaks nothing that any existing
   test looks at.

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
   exact measure. It is kept rather than deleted because it does catch a wrong measure.

   ## 2. A cold-restored diff-buf tree tripped `:count-known-child-unknown`

   That arm was reinstated on the strength of a search over bulk, conj, transient-conj and
   disj-churn builds that found the state never arose. All four are IN-MEMORY builds. On the
   restore path it is completely ordinary: under diff-buf, `Branch.child` PROJECTS some
   children — `projectBranch` takes the count from the slot, so it is known — while their
   siblings restore plainly from a blob carrying no `:subtree-count`, so theirs is -1. Every
   child resident, only some self-describing, nothing wrong.

   The fix does not simply drop the arm, because the detection it was reinstated for is real:
   a parent whose count has drifted must not be excused by one child sitting at -1. Instead
   `deep-count` RESOLVES the unknown by descending through resident nodes, so the sum can be
   checked anyway. That is strictly stronger than the arm it replaces, and silent on the lazy
   tree that was never corrupt."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.diagnostics :as diag]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set IMeasure Settings]))

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

;; ---------------------------------------------------------------------------
;; 2. cold-restored diff-buf trees

(defn- dbuf-storage [disk bf dbs]
  (ts/->Storage (atom {}) disk (Settings. (int bf) nil nil nil (int dbs))))

(deftest a-cold-restored-diff-buf-tree-validates
  (testing "projected children carry a count, plainly-restored siblings do not, and a parent
            with both is not corrupt for it.

            THREE conditions, all of which the first version of this test got wrong, so it
            passed against the unfixed build:

              * RANDOM keys. Sequential `(range base (+ base 200))` never produced the state
                at any branching factor or budget tried.
              * PARTIAL warmth. The first version called `(vec cold)`, materialising the
                whole tree — which gives every branch a count and makes the arm unreachable.
                The state needs a tree part-way through a walk.
              * LATER ROUNDS. Nothing fires before round 4; the slots have to accumulate
                across generations first.

            Found by sweeping 864 validate calls over bf/budget/size/seed/warmth and
            watching which shapes tripped the reinstated arm — 26 did, every one of them at
            bf 4 / budget 64. The witness:

                {:error :count-known-child-unknown, :level 2,
                 :branch-count 25, :child-counts [8 -1 -1]}

            on a tree whose contents and count were both exactly right."
    (doseq [per  [50 200]
            seed [1 2 3]]
      (let [bf 4, dbs 64
            rng  (java.util.Random. seed)
            disk (atom {})]
        (loop [s (set/sorted-set* {:branching-factor bf :diff-buf-size dbs
                                   :storage (dbuf-storage disk bf dbs)})
               round 0
               live #{}]
          (when (< round 6)
            (let [adds  (repeatedly per #(.nextInt rng 100000))
                  s'    (persistent! (reduce conj! (transient s) adds))
                  live' (into live adds)
                  addr  (set/store s' (dbuf-storage disk bf dbs))
                  cold  (set/restore addr (dbuf-storage disk bf dbs)
                                     {:branching-factor bf :diff-buf-size dbs})
                  label (str "per=" per " seed=" seed " round=" round)]
              ;; PARTIAL walk — a third of the tree. See the note above: a full walk hides it.
              (dorun (take (max 1 (quot (count live') 3)) (seq cold)))
              ;; The tree is right, asserted independently of the validator so that a
              ;; validator which passed everything could not satisfy this test.
              (is (= (sort live') (vec cold)) (str label ": contents"))
              (is (= (count live') (count cold)) (str label ": count"))
              (is (true? (diag/validate-full cold))
                  (str label ": a healthy cold-restored diff-buf tree must validate"))
              (recur cold (inc round) live'))))))))

;; The OTHER half of finding 2 — that resolving an unknown child count must not cost the
;; drift detection the removed arm existed for — is already pinned by
;; `oracle_power.clj`'s `a-resident-child-with-no-count-is-still-a-violation`: it sets a
;; root's count 7 too high AND child 0's count to -1, and demands a throw. That test was
;; written for the arm this change replaces and passes unchanged against `deep-count`,
;; which is the evidence that the replacement is strictly stronger rather than merely
;; quieter. Not duplicated here.
