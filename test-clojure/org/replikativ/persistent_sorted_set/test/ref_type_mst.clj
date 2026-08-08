(ns org.replikativ.persistent-sorted-set.test.ref-type-mst
  "`:ref-type` must bound the resident set in content-defined (MST) mode too.

   The MST rebuild paths — `Branch.removeContent` and `Branch.mstMergeWith` — copy each
   unchanged sibling forward as `child(storage, i)`, a BARE `ANode`, while keeping its
   still-valid address. Neither ever called `makeReference`. So no `Reference` was created
   anywhere on those paths, and because a bare strong child pins its whole subtree, a
   single one near the root pinned everything under it.

   Measured before the fix, bf 8, n=20000, `:ref-type :soft`, 20 deletes after a store and
   WITHOUT a second store — the long-lived-writer shape, since a checkpoint re-wraps
   everything and hides it:

       MST    cleared    0 references, 1375 of 1375 nodes still resident (100% pinned)
       count  cleared 2565 references,  4033 of 6598 resident (61%)

   After: MST clears 850 and holds 525 of 1375 (38%). Count mode is unchanged in both
   directions, which is the point — the change is scoped to the MST paths.

   Contents were always correct; this is retention, not corruption.

   ## Why the eviction is forced

   Rather than hoping the GC clears a SoftReference under memory pressure, the test clears
   every reachable `Reference` itself. Note that the walk descends only THROUGH references:
   a bare strong child stops it. That is not a limitation of the test, it is the defect —
   an unevictable child makes everything beneath it unevictable too, which is exactly why
   the before-figure is 0 rather than merely low.

   ## Why no second store

   `store`'s settle wraps children per `:ref-type` already, so checkpointing after the
   deletes hides the problem completely (both modes then clear ~100%). The exposure is a
   reader/writer that mutates for a while between checkpoints."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.boundary :as b]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet Branch ANode]
           [java.lang.ref Reference]))

(set! *warn-on-reflection* true)

(defn- resident-nodes [^PersistentSortedSet s]
  (let [n (atom 0)]
    (letfn [(w [x]
              (when (instance? Branch x)
                (swap! n inc)
                (let [^Branch br x
                      ch (.childrenArray br)]
                  (dotimes [i (.len br)]
                    (let [c (when ch (aget ^objects ch i))
                          c (if (instance? Reference c) (.get ^Reference c) c)]
                      (when (some? c)
                        (if (instance? Branch c) (w c) (swap! n inc))))))))]
      (w (.root s)))
    @n))

(defn- clear-references!
  "Clear every Reference reachable from the root, descending through references only —
   a bare strong child legitimately stops the walk, because it pins its subtree."
  [^PersistentSortedSet s]
  (let [n (atom 0)]
    (letfn [(w [^Branch br]
              (let [ch (.childrenArray br)]
                (dotimes [i (.len br)]
                  (let [x (when ch (aget ^objects ch i))]
                    (when (instance? Reference x)
                      (let [c (.get ^Reference x)]
                        (when (instance? Branch c) (w c)))
                      (.clear ^Reference x)
                      (swap! n inc))))))]
      (w (.root s)))
    @n))

(defn- run [n mst?]
  (let [bf   8
        disk (atom {})
        opts (cond-> {:comparator compare :branching-factor bf :ref-type :soft}
               mst? (assoc :boundary (b/mst-boundary 4)))
        st   (ts/storage (atom {}) disk)
        s0   (reduce #(ss/conj %1 %2 compare) (ss/sorted-set* opts) (range n))
        _    (ss/store s0 st)
        vs   (range 0 (quot n 10) 100)
        s1   (reduce #(ss/disj %1 %2 compare) s0 vs)   ; NO second store — see the ns docstring
        before  (resident-nodes s1)
        cleared (clear-references! s1)
        after   (resident-nodes s1)]
    {:resident-before before :cleared cleared :resident-after after
     :expected (remove (set vs) (range n)) :actual (seq s1)}))

(deftest mst-mode-honours-ref-type
  (testing "a content-defined boundary must not pin the tree between checkpoints"
    (let [{:keys [resident-before cleared resident-after expected actual]} (run 20000 true)]
      (is (pos? cleared)
          (str "MST cleared " cleared " references of " resident-before
               " resident nodes. Zero means nothing in the tree is evictable and `:ref-type`"
               " is inert."))
      (is (< resident-after (* 0.75 resident-before))
          (str "MST pinned " resident-after " of " resident-before
               " nodes after eviction — `:ref-type :soft` must bound the resident set."))
      (is (= expected actual) "contents unaffected — this is retention, not corruption"))))

(deftest count-mode-is-unchanged
  (testing "the MST wrapping must not disturb the count path, which already wrapped
            correctly — a fix that moved BOTH numbers would be doing something else"
    (let [{:keys [resident-before cleared resident-after expected actual]} (run 20000 false)]
      (is (pos? cleared) (str "count mode cleared " cleared))
      (is (< resident-after resident-before) "count mode still evicts")
      (is (= expected actual) "contents"))))
