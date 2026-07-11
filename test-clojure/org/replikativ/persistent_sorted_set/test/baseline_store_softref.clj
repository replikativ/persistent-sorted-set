(ns org.replikativ.persistent-sorted-set.test.baseline-store-softref
  "History + current contract of the 'dirty child behind a Reference wrapper' state.

   HISTORY: the two-step settle (write _addresses[i], then wrap _children[i]) could be
   observed torn by a concurrent copier, leaving a derived tree with a DIRTY (null
   address) child slot holding a Soft/WeakReference. The baseline (diffBufSize=0)
   store() loop then cast that slot to ANode without the readReference unwrap every
   other consumer uses — under heap pressure real workloads crashed mid-commit with
   'SoftReference cannot be cast to ANode' (found by the datahike soak harness;
   present since at least 0.4.122). The first fix (#17) added the unwrap.

   CURRENT CONTRACT (NodeState): the per-child {addresses, children, buf} state is one
   immutable snapshot published atomically, so the dirty+Reference state is impossible
   by construction within any snapshot — and store() now enforces that as a PERMANENT
   -ea assert (a torn pair can only mean a memory-model regression, and it must be
   loud, not silently unwrapped). The readReference unwrap remains as production
   robustness when assertions are disabled.

   This test injects the forbidden state deterministically (mutating the snapshot's
   children array through a test-only seam) and asserts:
     - with -ea (the test suite always runs with it): store() fails FAST with the
       slot-invariant AssertionError — enforcement, not accommodation;
     - without -ea (belt and braces if ever run that way): store() unwraps and
       succeeds with content intact (#17 behavior)."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as ss])
  (:import [org.replikativ.persistent_sorted_set Branch PersistentSortedSet IStorage]
           [java.lang.ref SoftReference]))

(defn- mem-storage []
  (let [disk (atom {})]
    (reify IStorage
      (store [_ node] (let [a (random-uuid)] (swap! disk assoc a node) a))
      (accessed [_ _])
      (restore [_ a] (@disk a))
      (markFreed [_ _]) (isFreed [_ _] false) (freedInfo [_ _] nil))))

(deftest baseline-store-slot-invariant-enforced
  (testing "an injected dirty+Reference slot is rejected loudly under -ea (and unwrapped without)"
    (let [storage (mem-storage)
          s0 (reduce (fn [s e] (ss/conj s e compare))
                     (ss/sorted-set* {:storage storage :branching-factor 8})
                     (range 100))
          _  (ss/store s0 storage)                       ; all durable
          s1 (ss/conj s0 1000 compare)                   ; dirty path root->leaf
          ^Branch root (.root ^PersistentSortedSet s1)
          state     (.-_state root)
          ^objects addrs    (.-addresses state)
          ^objects children (.-children state)
          dirty-idx (first (for [i (range (.-_len root))
                                 :when (nil? (aget addrs i))]
                             i))]
      (is (some? dirty-idx) "the conj produced a dirty child on the root")
      ;; inject the forbidden state: a dirty slot behind a Reference wrapper (test-only
      ;; in-place mutation of the snapshot array — exactly the torn shape #17 hit live)
      (aset children (int dirty-idx) (SoftReference. (aget children dirty-idx)))
      (if (.desiredAssertionStatus Branch)
        ;; -ea: the permanent slot-invariant assert in store() must fire
        (is (thrown? AssertionError (ss/store s1 storage))
            "store() rejects the torn slot loudly under -ea")
        ;; no -ea: the #17 unwrap keeps production commits alive
        (do (is (some? (ss/store s1 storage)) "baseline store unwraps and succeeds")
            (is (= (concat (range 100) [1000]) (seq s1)) "content intact"))))))
