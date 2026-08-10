(ns org.replikativ.persistent-sorted-set.test.oracle-coverage
  "`validate-full` returning true must be distinguishable from `validate-full` checking nothing.

   It cannot verify what it cannot see: a node whose children are not resident, or whose
   cached count/measure is absent, is SKIPPED rather than failed. That is the only correct
   behaviour — a lazily-restored tree is not corrupt for being lazy — but it means `true` can
   equally mean \"checked everything and it holds\" or \"could check nothing\". The
   subtree-count check spent its whole life in the second state against restored trees without
   anyone noticing.

   `verification-coverage` is the answer to that, and these are its tests.

   ## Why this is a separate namespace

   `verification-coverage` does not exist before the commit that introduced it, so a namespace
   referring to it fails to COMPILE against the pre-fix build — taking every other assertion
   in the same file down with it. When these tests lived in `oracle_power.clj`, that file's
   red check reported \"No such var: diag/verification-coverage\" instead of the failures it
   was meant to demonstrate, so the corrupt-count and corrupt-measure assertions there had
   never actually been red-checked. An adversarial review caught it. Keeping the
   coverage-dependent assertions here keeps that file red-checkable."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.diagnostics :as diag]
            [clojure.edn :as edn])
  (:import [org.replikativ.persistent_sorted_set Settings IStorage ANode Branch Leaf IMeasure]))

(set! *warn-on-reflection* true)

;; Persists :subtree-count, the way datahike's storage does. Against one that drops it every
;; restored branch carries -1, the count check skips itself, and this whole namespace would
;; pass while measuring nothing.
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

(deftest a-cold-tree-reports-its-checks-as-skipped
  (testing "a cold root's children are not resident, so its count is unverifiable, and the
            report must SAY so rather than let a caller read `true` as a clean bill of health"
    (doseq [[bf n] [[8 5000] [64 5000]]]
      (let [disk (atom {})
            addr (set/store (build bf n) (storage disk bf))
            cold (set/restore-by compare addr (storage disk bf) {:branching-factor bf})
            cov  (diag/verification-coverage cold)]
        (is (true? (diag/validate-full cold))
            (str "bf=" bf ": precondition — a healthy cold tree validates"))
        (is (pos? (:counts-skipped cov))
            (str "bf=" bf ": and the coverage report must show the skip: " cov))))))

;; An EXACT measure — integer addition, and a `remove` that recomputes rather than
;; subtracting. That matters: measure checking is opt-in (`{:check-measures? true}`) because
;; an inexact measure legitimately drifts in the last bits and an equality oracle would call
;; that corruption. See `validate-full`'s docstring.
(def ^:private sum-measure
  (reify IMeasure
    (identity [_] 0)
    (extract [_ k] k)
    (merge [_ a b] (+ (long a) (long b)))
    (remove [_ _current _key recompute] (.get recompute))))

(deftest a-measured-tree-actually-verifies-its-measures
  (testing "moved here from `oracle_power.clj`, where it made that whole namespace
            impossible to red-check: `verification-coverage` does not exist before ea67a58,
            so a namespace referring to it fails to COMPILE against the pre-fix build and
            takes every sibling deftest down with it rather than failing on its own terms.

            What it pins: the measure arm must actually LOOK at measures. Without this, the
            opt-in flag could be wired to nothing at all and every measure test would still
            pass by simply never checking."
    (doseq [[bf n] [[8 200] [64 1000]]]
      (let [s   (set/from-sorted-array compare (to-array (range n)) n
                                       {:branching-factor bf :measure sum-measure})
            _   (set/measure s)
            cov (diag/verification-coverage s)]
        (is (pos? (:measures-verified cov))
            (str "bf=" bf " n=" n ": measures must actually be verified, not skipped: " cov))))))

(deftest a-warm-tree-is-actually-verified
  (testing "the other half. Without this, the fix to the false positive could have been an
            oracle that simply never looks at anything and always returns true."
    (doseq [[bf n] [[8 200] [8 5000] [64 5000]]]
      (let [s   (build bf n)
            _   (count (seq s))
            cov (diag/verification-coverage s)]
        (is (pos? (:counts-verified cov))
            (str "bf=" bf " n=" n ": an in-memory tree must be genuinely count-checked: " cov))
        (is (zero? (:counts-skipped cov))
            (str "bf=" bf " n=" n ": and nothing should be skipped: " cov))))))
