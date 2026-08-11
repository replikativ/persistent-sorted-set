(ns org.replikativ.persistent-sorted-set.test.oracle
  "`validate-full` is the only whole-tree integrity check there is, so it has to be both
   POWERFUL (it must catch a corrupted tree) and HONEST (it must not call a healthy one
   corrupt). Section 1 pins the power, section 2 pins the honesty -- they are the two ways
   the same oracle fails, and over-correcting either direction breaks the other.

   FIXTURES ARE OPPOSITE HERE AND MUST STAY SO. Section 1's `CountingStorage` PERSISTS
   `:subtree-count`, because against the in-tree storage -- which does not -- every restored
   branch carries -1, the count check skips itself, and the whole section would pass while
   testing nothing. Section 2's `dbuf-storage` builds a FRESH storage instance per call so
   each restore is genuinely cold and only partially warmed, which is the state that
   reproduced the false positive; a count-persisting or memoising storage makes its arm
   unreachable and it too would pass vacuously. Neither storage may be used by the other
   section.

   NOT MERGED IN, deliberately: `oracle_coverage.clj`. `diagnostics/verification-coverage`
   does not exist before ea67a58, so a namespace mentioning it fails to COMPILE against the
   pre-fix build and takes every sibling deftest down as a compile error rather than as the
   failures they exist to demonstrate. Keeping it out is what makes this namespace
   red-checkable, and `oracle_coverage.clj`'s own docstring records the incident.

   That argument does bound this merge: both files here already call the 2-arity
   `validate-full`, introduced by 641a109, so both were already un-compilable against
   anything older than that. The merge therefore puts more tests behind an EXISTING
   boundary rather than introducing a new one -- which is exactly the distinction that
   keeps `oracle_coverage.clj` out."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.edn :as edn]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.diagnostics :as diag]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set Settings IStorage ANode Branch Leaf
            IMeasure PersistentSortedSet]))

(set! *warn-on-reflection* true)

;; ===========================================================================
;; 1. the oracle must CATCH a corrupted tree (and reach a restored one at all)
;; ===========================================================================

;; A storage that PERSISTS :subtree-count. Against one that drops it (the in-tree test
;; Storage) every restored branch carries -1, the count check skips itself, and this whole
;; namespace would pass while testing nothing — the exact trap that hid the defect.
(defrecord CountingStorage [*disk ^Settings settings]
  IStorage
  (store [_ node]
    (let [^ANode node node
          addr (str (java.util.UUID/randomUUID))]
      (swap! *disk assoc addr
             (pr-str {:level         (.level node)
                      :keys          (vec (.keys node))
                      :addresses     (when (instance? Branch node)
                                       (vec (.addresses ^Branch node)))
                      :subtree-count (when (instance? Branch node)
                                       (.subtreeCount ^Branch node))}))
      addr))
  (accessed [_ _] nil)
  (restore [_ address]
    (let [{:keys [level keys addresses subtree-count]} (edn/read-string (@*disk address))]
      (if addresses
        (let [b (Branch. (int level) ^java.util.List keys ^java.util.List addresses settings)]
          (set! (.-_subtreeCount b) (long (or subtree-count -1)))
          b)
        (Leaf. ^java.util.List keys settings)))))

(defn- storage [disk bf] (->CountingStorage disk (Settings. (int bf) nil nil nil (int 0))))

(defn- build [bf n]
  (set/from-sorted-array compare (to-array (range n)) n {:branching-factor bf}))

;; ---------------------------------------------------------------------------
;; 1. no false positive on a restored tree — and it is a REAL check, not a skipped one

(deftest a-healthy-restored-tree-passes
  (testing "counts persisted by the storage must not be compared against absent children"
    (doseq [[bf n] [[8 200] [8 5000] [64 200] [64 5000]]]
      (let [disk (atom {})
            addr (set/store (build bf n) (storage disk bf))
            cold (set/restore-by compare addr (storage disk bf) {:branching-factor bf})]
        (is (true? (diag/validate-full cold))
            (str "bf=" bf " n=" n ": a healthy cold-restored tree must validate"))
        (is (= n (count cold)) (str "bf=" bf " n=" n ": and hold what was stored"))))))

;; NOTE: the assertions that call `diagnostics/verification-coverage` live in
;; `oracle_coverage.clj`, deliberately. That var does not exist before ea67a58, so a namespace
;; referring to it fails to COMPILE against the pre-fix build — which silently takes every
;; other assertion in the same namespace down with it. This file's red check reported "no such
;; var" rather than the failures it was supposed to demonstrate, so the power assertions here
;; were never actually red-checked until they were separated out. Caught by an adversarial
;; review of this namespace.

(deftest a-resident-child-with-no-count-is-still-a-violation
  (testing "the regression an adversarial review found in the false-positive fix.

            Skipping a NON-RESIDENT child is right. Skipping a RESIDENT child whose cached
            count is -1 is not: measured on a warm, fully-resident tree with no storage
            involved, setting one resident child's count to -1 made validate-full return true
            on a tree whose ROOT count was simultaneously wrong by 7. The old code caught
            that. The same review then searched for the state the relaxation was supposed to
            tolerate — a resident branch with count -1 under a known-count parent — across
            bulk, conj, transient-conj and disj-churn builds at bf 8 / n 3000, and found ZERO
            occurrences, so it cost detection and bought nothing."
    (doseq [[bf n] [[8 200] [64 5000]]]
      (let [s ^PersistentSortedSet (build bf n)
            _ (count (seq s))
            ^Branch root (.root s)
            ^Branch c0 (.child root nil (int 0))
            orig-root (.-_subtreeCount root)
            orig-c0 (.-_subtreeCount c0)]
        (is (instance? Branch c0)
            (str "bf=" bf ": precondition — child 0 must itself be a branch"))
        (set! (.-_subtreeCount root) (long (+ orig-root 7)))
        (set! (.-_subtreeCount c0) (long -1))
        (is (thrown? AssertionError (diag/validate-full s))
            (str "bf=" bf " n=" n ": a resident child with an unknown count, under a parent "
                 "that claims one, must not silence the parent's own drift"))
        (set! (.-_subtreeCount root) (long orig-root))
        (set! (.-_subtreeCount c0) (long orig-c0))
        (is (true? (diag/validate-full s))
            (str "bf=" bf " n=" n ": and the tree must validate again once restored"))))))

;; ---------------------------------------------------------------------------
;; 2. the oracle still CATCHES a corrupted count

(deftest a-drifted-count-is-caught
  (testing "poke a branch's cached count and validate-full must refuse the tree. Without
            this, the false-positive fix above could have been an oracle that passes
            everything."
    (doseq [[bf n] [[8 200] [64 5000]]]
      (let [s ^PersistentSortedSet (build bf n)
            _ (count (seq s))
            root (.root s)]
        (is (instance? Branch root) (str "bf=" bf ": precondition — the root must be a branch"))
        (let [^Branch b root
              original (.-_subtreeCount b)]
          (set! (.-_subtreeCount b) (long (+ original 7)))
          (is (thrown? AssertionError (diag/validate-full s))
              (str "bf=" bf " n=" n ": a count off by 7 must be caught"))
          (set! (.-_subtreeCount b) (long original))
          (is (true? (diag/validate-full s))
              (str "bf=" bf " n=" n ": and the tree must validate again once restored")))))))

;; ---------------------------------------------------------------------------
;; 3. measures are checked at all, and a wrong one is caught

(def ^:private sum-measure
  (reify IMeasure
    (identity [_] 0)
    (extract [_ k] k)
    (merge [_ a b] (+ (long a) (long b)))
    (remove [_ _current _key recompute] (.get recompute))))

(defn- measured [bf n]
  (set/from-sorted-array compare (to-array (range n)) n
                         {:branching-factor bf :measure sum-measure}))

;; `sum-measure` above is EXACT — integer addition, and its `remove` recomputes rather than
;; subtracting — which is what makes it legal to check at all. Measure checking is opt-in
;; (`{:check-measures? true}`) precisely because an INEXACT measure drifts in the last bits
;; and an equality oracle would call that corruption; see `validate-full`'s docstring.

(deftest a-correct-measure-passes
  (testing "the control for the two defect tests below"
    (doseq [[bf n] [[8 200] [64 1000]]]
      (let [s (measured bf n)]
        (is (true? (diag/validate-full s {:check-measures? true})) (str "bf=" bf " n=" n))
        (is (= (reduce + (range n)) (set/measure s))
            (str "bf=" bf " n=" n ": precondition — the measure is the sum"))))))
;; The other half of this — proof the measure arm is not merely SKIPPING everything — was a
;; `verification-coverage` assertion that used to live right here, and it made this whole
;; namespace impossible to red-check: that var does not exist before ea67a58, so reverting
;; `diagnostics.cljc` gave "No such var: diag/verification-coverage" and took all five
;; deftests down with a compile error instead of the failures they were meant to show. The
;; NOTE above already said such assertions belong in `oracle_coverage.clj`; this one had been
;; left behind, so the NOTE described an intent rather than the state of the file. Moved to
;; `oracle_coverage.clj`'s `a-measured-tree-actually-verifies-its-measures`.

(deftest a-wrong-measure-is-caught
  (testing "the defect class this arm exists for: a cached measure that is a WRONG NUMBER
            rather than merely absent. check-all-measures-known only ever tested non-nil,
            so nothing in the tree could see this."
    (doseq [[bf n] [[8 200] [64 1000]]]
      (let [s ^PersistentSortedSet (measured bf n)
            _ (set/measure s)
            ^Branch root (.root s)
            original (.-_measure root)]
        (set! (.-_measure root) (long 999999))
        (is (thrown? AssertionError (diag/validate-full s {:check-measures? true}))
            (str "bf=" bf " n=" n ": a branch measure that disagrees with its children "
                 "must be caught"))
        (set! (.-_measure root) original)
        (is (true? (diag/validate-full s {:check-measures? true}))
            (str "bf=" bf " n=" n ": and the tree must validate again once restored"))))))

(deftest a-wrong-leaf-measure-is-caught
  (testing "and at the leaf, where the rebalance-staleness defect actually lands"
    (let [s ^PersistentSortedSet (measured 8 200)
          _ (set/measure s)
          ^Branch root (.root s)
          leaf-parent (loop [n ^ANode root]
                        (let [c (.child ^Branch n nil (int 0))]
                          (if (instance? Leaf c) n (recur c))))
          ^ANode leaf (.child ^Branch leaf-parent nil (int 0))
          original (.-_measure leaf)]
      (is (some? original) "precondition: the leaf carries a measure")
      (set! (.-_measure leaf) (long 424242))
      (is (thrown? AssertionError (diag/validate-full s {:check-measures? true}))
          "a leaf measure that disagrees with its own keys must be caught")
      (set! (.-_measure leaf) original)
      (is (true? (diag/validate-full s {:check-measures? true})) "and validate again once restored"))))

;; ===========================================================================
;; 2. the oracle must not call a HEALTHY tree corrupt
;; ===========================================================================

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
