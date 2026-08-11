(ns org.replikativ.persistent-sorted-set.test.compact-preserves
  "`compact` says it \"preserves comparator, settings, and metadata\". It did not.

   Settings round-trip through the private `settings->map`, which omitted the
   BOUNDARY — so compacting an MST set returned a plain count B-tree, silently
   changing how the tree is addressed and therefore what cross-peer dedup can
   match. `compact` also never passed the STORAGE on, so a later
   `(store compacted)` threw NullPointerException, and it dropped METADATA,
   losing the `:pss/storage-id` the wire codec resolves on.

   All three verified before the fix. The docstring was the only thing asserting
   otherwise, which is why nothing failed."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet Settings IBoundary CountBoundary]))

(defn- settings-of [^PersistentSortedSet set]
  (let [f (.getDeclaredField PersistentSortedSet "_settings")]
    (.setAccessible f true)
    ^Settings (.get f set)))

(defn- storage-of [^PersistentSortedSet set]
  (let [f (.getDeclaredField PersistentSortedSet "_storage")]
    (.setAccessible f true)
    (.get f set)))

(defn- build [opts n]
  (reduce #(s/conj %1 %2 compare) (s/sorted-set* opts) (range n)))

(deftest compact-keeps-the-storage
  (testing "a compacted set must still be storable — this threw NPE"
    (let [disk (atom {})
          st   (ts/storage disk)
          s0   (build {:branching-factor 16 :comparator compare} 500)
          _    (s/store s0 st)
          c    (s/compact s0)]
      (is (some? (storage-of c)) "storage carried over")
      (is (some? (s/store c)) "and (store compacted) succeeds")
      (is (= (vec (range 500)) (vec (seq c))) "with the same elements"))))

(deftest compact-keeps-the-metadata
  (testing "metadata carries the :pss/storage-id a wire reader resolves on —
            dropping it is not cosmetic"
    (let [s0 (with-meta (build {:comparator compare} 200) {:pss/storage-id "store-7" :other 1})]
      (is (= {:pss/storage-id "store-7" :other 1} (meta (s/compact s0)))))))

(deftest compact-keeps-a-content-defined-boundary
  (testing "an MST set must not silently become a count B-tree. The boundary is
            what decides where nodes are cut and therefore how they address, so
            losing it changes the tree's identity, not just its shape."
    ;; content-DEFINED, but splitting delegated to the count boundary: this test is
    ;; about whether the boundary SURVIVES compaction, not about MST cut placement.
    (let [boundary (reify IBoundary
                     (splitOnInsert [_ run len ins level st]
                       (.splitOnInsert CountBoundary/INSTANCE run len ins level st))
                     (keyLevel [_ k _] (if (zero? (mod (hash k) 8)) 2 0))
                     (contentDefined [_] true))
          s0 (build {:branching-factor 16 :comparator compare :boundary boundary} 400)
          c  (s/compact s0)]
      (is (.contentDefined (.boundary (settings-of s0))) "precondition: the source is MST")
      (is (.contentDefined (.boundary (settings-of c)))
          "and the compacted set still is")
      (is (= (vec (range 400)) (vec (seq c)))))))

(deftest compact-keeps-the-plain-settings-too
  (testing "the fields that always round-tripped keep doing so"
    (let [s0 (build {:branching-factor 64 :diff-buf-size 128 :comparator compare} 300)
          c  (s/compact s0)]
      (is (= 64 (.branchingFactor (settings-of c))))
      (is (= 128 (.diffBufSize (settings-of c)))))))
