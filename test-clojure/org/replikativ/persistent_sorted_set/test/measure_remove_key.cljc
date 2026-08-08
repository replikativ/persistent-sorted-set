(ns org.replikativ.persistent-sorted-set.test.measure-remove-key
  "A remove must subtract the contribution of the element the leaf ACTUALLY held.

   `IMeasure.remove` is documented to remove the contribution of the key being
   removed. The leaf passed the CALLER's search key instead. Those differ exactly
   when the operation comparator is coarser than the set's — the `[id value]`
   compared-by-id pattern that `PersistentSortedSet.lookup`'s own docstring
   advertises, and datahike's datom removal.

   Measured before the fix, 200 longs `3, 13, 23, …` removed one at a time via a
   decade comparator (so searching with `30` removes the stored `33`):

       bf  8   set ends EMPTY, cached sum 24.0   (should be 0.0)
       bf 16   set ends EMPTY, cached sum 48.0

   This is not a cache that merely goes stale in memory. `node->map` serializes
   `:measure`, so the wrong aggregate is written to storage, is read back as
   authoritative, and changes the node's content address — two structurally
   identical nodes get different addresses, defeating dedup. `forceComputeMeasure`
   does not save it either: it trusts a non-null child measure rather than
   recomputing.

   ## Why this is a `.cljc`

   On the JVM the persistent path recomputes from the new leaf's keys and is
   correct, so only a TRANSIENT remove was wrong. That half is reproduced and
   pinned below.

   ClojureScript passed the same wrong argument and has no recompute fallback,
   and it was corrected there too — but NOT DEMONSTRATED. Every construction
   tried (`conj`-built and `from-sequential` with `:measure`, at several fanouts
   and removal counts) left the root's `_measure` nil after the removes, so
   `remove-measure` never ran on a populated measure and no assertion could
   observe the difference. The cljs case below therefore guards the invariant
   rather than reproducing the defect, and says so where it would otherwise look
   like coverage. If someone later finds a cljs construction that does carry a
   leaf measure through a remove, this is the test to strengthen.

   Same root cause as the `replace` deposit defect and the same family as the
   `replace` precondition: the search key is not the stored element."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.measure :as measure]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set
                    PersistentSortedSet ANode NumericStats NumericStatsOps])))

;; Elements are longs `3, 13, 23, …`. The REMOVE comparator compares only the
;; decade, so `(disj s 30 by-decade)` finds and removes the stored `33` — the
;; search key and the stored element are comparator-equal and different.
(defn- by-decade [a b] (compare (quot (long a) 10) (quot (long b) 10)))

#?(:clj
   (do
     (def ^:private ops (NumericStatsOps/instance))

     (defn- cached-sum [^PersistentSortedSet v]
       (when-let [m (.-_measure ^ANode (.root v))] (.-sum ^NumericStats m)))

     (defn- build [n bf]
       (reduce #(s/conj %1 %2 compare)
               (s/sorted-set* {:comparator compare :branching-factor bf :measure ops})
               (map #(+ 3 (* 10 %)) (range n))))

     (deftest a-transient-remove-subtracts-the-element-it-removed
       (testing "the JVM half. Its PERSISTENT path recomputes the measure from the
                 new leaf's keys and was always right; the transient path
                 subtracted the caller's search key. Removing every element must
                 leave an empty set with a zero measure — the cached sum was 24.0
                 at bf 8 and 48.0 at bf 16."
         (doseq [[n bf] [[200 8] [200 16] [500 32]]]
           (let [base (build n bf)
                 v (let [t (reduce (fn [^PersistentSortedSet t i]
                                     (.disjoin t (long (* 10 i)) by-decade))
                                   (.asTransient ^PersistentSortedSet base) (range n))]
                     (.persistent ^PersistentSortedSet t))
                 remaining (vec (seq v))]
             (is (zero? (count v)) (str "n=" n " bf=" bf ": every element removed"))
             (is (= (double (reduce + 0.0 remaining)) (or (cached-sum v) 0.0))
                 (str "n=" n " bf=" bf ": cached measure must equal the real sum"))))))

     (deftest a-partial-transient-remove-keeps-the-measure-exact
       (testing "the same, stopping short of empty, so the assertion is about an
                 aggregate over surviving elements rather than about zero"
         (doseq [[n bf drop-n] [[200 8 50] [200 16 137] [500 32 300]]]
           (let [base (build n bf)
                 v (let [t (reduce (fn [^PersistentSortedSet t i]
                                     (.disjoin t (long (* 10 i)) by-decade))
                                   (.asTransient ^PersistentSortedSet base) (range drop-n))]
                     (.persistent ^PersistentSortedSet t))
                 remaining (vec (seq v))]
             (is (= (- n drop-n) (count v)))
             (is (= (double (reduce + 0.0 remaining)) (or (cached-sum v) 0.0))
                 (str "n=" n " bf=" bf " dropped=" drop-n))))))))

;; ---------------------------------------------------------------------------
;; ClojureScript has no NumericStatsOps and no transients; its single remove path
;; is the persistent one, which is exactly the path that was wrong there. The
;; measure is checked through the same public surface using a plain sum monoid.

#?(:cljs
   (do
     ;; A plain invertible sum monoid. `remove-measure` subtracts the element it is
     ;; handed — which is the whole point: hand it the wrong one and the aggregate
     ;; silently drifts from the contents.
     (defrecord SumOps []
       measure/IMeasure
       (identity-measure [_] 0)
       (extract [_ k] k)
       (merge-measure [_ a b] (+ a b))
       (remove-measure [_ current k _recompute] (- current k)))

     (def ^:private sum-ops (->SumOps))

     (defn- cljs-cached [v]
       (some-> (.-root v) (.-_measure)))

     (deftest a-remove-subtracts-the-element-it-removed
       (testing "ClojureScript passed the same wrong argument at BOTH sites that
                 subtract a key — `leaf.cljs $remove` and the branch-level
                 `remove-measure` — and both were corrected. Built with
                 `from-sequential`, which populates the leaf measures eagerly; a
                 `conj`-built cljs set leaves `_measure` nil and this would test
                 nothing.

                 Measure equality IS asserted now. When this was written the cljs
                 root reported 187350 against a survivor sum of 187200 (n=200 bf=8
                 drop=50) — a gap of exactly 150 = 50 x 3, the difference between
                 the search keys and the stored elements — so the assertion was
                 left out and the gap recorded as unattributed. Re-measured
                 2026-08 on both runtimes at (200,8,50), (200,16,137), (200,8,200)
                 and (60,4,20): the cached measure equals the real sum
                 everywhere. Whichever later fix closed it, the gap is gone, and
                 the equality below is live rather than commented about.

                 It is guarded by `(some? (cljs-cached v))` because a cljs set
                 does not always carry a root measure, and by the
                 `(pos? @with-measure)` precondition at the end so the guard
                 cannot make the whole case vacuous."
         (let [with-measure (atom 0)]
           (doseq [[n bf drop-n] [[200 8 50] [200 16 137] [200 8 200]]]
             (let [xs (mapv #(+ 3 (* 10 %)) (range n))
                   base (s/from-sequential compare xs
                                           {:branching-factor bf :measure sum-ops})
                   v (reduce (fn [acc i] (s/disj acc (* 10 i) by-decade)) base (range drop-n))
                   remaining (vec (s/seq v))]
               (is (= (- n drop-n) (count v))
                   (str "n=" n " bf=" bf ": the coarse comparator removed one each time"))
               (is (= (vec (drop drop-n xs)) remaining)
                   (str "n=" n " bf=" bf ": the survivors are the expected ones"))
               (when (some? (cljs-cached v))
                 (swap! with-measure inc)
                 (is (= (reduce + 0 remaining) (cljs-cached v))
                     (str "n=" n " bf=" bf ": cached measure must equal the real sum")))))
           (is (pos? @with-measure)
               "precondition: at least one shape carried a live root measure"))))))
