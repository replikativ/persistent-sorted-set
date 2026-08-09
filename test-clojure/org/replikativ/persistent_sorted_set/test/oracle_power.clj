(ns org.replikativ.persistent-sorted-set.test.oracle-power
  "The whole-tree oracle must be usable against restored trees, must actually check measures,
   and must be able to say how much it checked.

   Three faults, all in `validate-full`, which is the only whole-tree integrity check there is:

   1. It FALSE-POSITIVED on every lazily-restored tree. `check-subtree-counts` mapped a
      non-resident child to the number 0, so `all-known?` stayed true and a branch's real
      count was compared against a sum of zeros:

          bf  8  n  200   validate true   validate-full :subtree-count-mismatch
                                          {:branch-count 200, :children-sum 0}
          bf  8  n 5000                   {:branch-count 5000, :children-sum 0}
          bf 64  n  200 / 5000            same

      Healthy trees, every one. Nothing in the suite noticed because the in-tree test storage
      does not persist `:subtree-count`, so its restored branches carry -1 and the check
      skipped itself. The consequence is the serious part: the oracle could not be pointed at
      the restore path, which is exactly the path it is most needed for — and that is where
      the durable count-drift bug fixed in 7e53894 was hiding.

   2. It did not check measures AT ALL, despite its docstring saying \"including subtree
      counts, measures\". The only measure code reachable from the public API was
      `check-all-measures-known`, which tests non-nil. A measure could be a WRONG NUMBER —
      the defect class of \"a leaf shrunk by a sibling rebalance kept its pre-shrink
      measure\" — and every validator in the tree would pass it.

   3. It could not distinguish \"checked everything\" from \"could check nothing\". Both
      return `true`. That is what let (1) go unnoticed for so long.

   The risk in fixing (1) is over-correcting into an oracle that never fails. So this
   namespace pins the oracle's POWER, not just its silence: every deftest below either
   corrupts a tree and demands a throw, or asserts a non-zero verified count."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.diagnostics :as diag]
            [org.replikativ.persistent-sorted-set.test.storage :as ts]
            [clojure.edn :as edn])
  (:import [org.replikativ.persistent_sorted_set Settings IStorage ANode Branch Leaf
            IMeasure PersistentSortedSet]))

(set! *warn-on-reflection* true)

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

(deftest a-correct-measure-passes-and-is-actually-examined
  (testing "the control, plus proof the measure arm is not skipping"
    (doseq [[bf n] [[8 200] [64 1000]]]
      (let [s (measured bf n)]
        (is (true? (diag/validate-full s)) (str "bf=" bf " n=" n))
        (is (= (reduce + (range n)) (set/measure s))
            (str "bf=" bf " n=" n ": precondition — the measure is the sum"))
        (is (pos? (:measures-verified (diag/verification-coverage s)))
            (str "bf=" bf " n=" n ": measures must actually be verified, not skipped"))))))

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
        (is (thrown? AssertionError (diag/validate-full s))
            (str "bf=" bf " n=" n ": a branch measure that disagrees with its children "
                 "must be caught"))
        (set! (.-_measure root) original)
        (is (true? (diag/validate-full s))
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
      (is (thrown? AssertionError (diag/validate-full s))
          "a leaf measure that disagrees with its own keys must be caught")
      (set! (.-_measure leaf) original)
      (is (true? (diag/validate-full s)) "and validate again once restored"))))
