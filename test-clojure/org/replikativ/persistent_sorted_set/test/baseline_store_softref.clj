(ns org.replikativ.persistent-sorted-set.test.baseline-store-softref
  "Regression: the BASELINE (diffBufSize=0) store() loop cast _children[i] to
   ANode without the readReference unwrap every other consumer uses. A child
   slot can legitimately hold a Soft/WeakReference wrapper (child() caches
   restored children wrapped), and a wrapped child can be dirty (address
   nulled by an in-place path without replacing the wrapper) — under heap
   pressure real workloads crashed mid-commit with
   'SoftReference cannot be cast to ANode' (found by the datahike soak
   harness; present since at least 0.4.122). This test injects the wrapper
   deterministically instead of waiting for the JVM to clear soft refs."
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

(deftest baseline-store-unwraps-reference-wrapped-dirty-child
  (testing "store() succeeds when a dirty child is behind a Reference wrapper"
    (let [storage (mem-storage)
          s0 (reduce (fn [s e] (ss/conj s e compare))
                     (ss/sorted-set* {:storage storage :branching-factor 8})
                     (range 100))
          _  (ss/store s0 storage)                       ; all durable
          s1 (ss/conj s0 1000 compare)                   ; dirty path root->leaf
          ^Branch root (.root ^PersistentSortedSet s1)
          dirty-idx (first (for [i (range (.-_len root))
                                 :when (nil? (aget (.-_addresses root) i))]
                             i))]
      (is (some? dirty-idx) "the conj produced a dirty child on the root")
      ;; inject the wrapper exactly as a cached restored child would carry it
      (aset (.-_children root) dirty-idx
            (SoftReference. (aget (.-_children root) dirty-idx)))
      ;; before the fix: ClassCastException from the baseline store loop
      (is (some? (ss/store s1 storage)) "baseline store unwraps and succeeds")
      (is (= (concat (range 100) [1000]) (seq s1)) "content intact"))))
