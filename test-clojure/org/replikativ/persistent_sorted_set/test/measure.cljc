(ns org.replikativ.persistent-sorted-set.test.measure
  "Everything about `:measure` that is not the diagnostics oracle: whether every node HAS
   one, whether it is the RIGHT one, and whether `remove` subtracts the element it actually
   removed.

   No storage anywhere in this namespace -- the fixture axis is the measure implementation
   and the BUILD PATH, not durability.

   THE BUILD PATH IS THE FIXTURE, and it is why the two builders below are named apart.
   Section 2 builds with `from-sequential` (eager: every node gets a measure as it is built)
   and section 4 builds with `conj` (incremental). Both files originally called their builder
   `build`, same arity, opposite strategy -- so a plain concatenation would have silently
   handed one section the other's builder, and section 2's own deftest is named
   `leaves-carry-a-measure-after-an-eager-build`. Worse, only section 2's existed on
   ClojureScript, so the JVM and cljs halves would have diverged under one name. They are now
   `bulk-built-decades` and `conj-built-decades`.

   `by-decade` was byte-identical in both and appears once. Section 4's ClojureScript record
   is renamed `InvertibleSumOps`: two `defrecord`s of one name in a single cljs namespace is
   a redefinition, and this build carries zero warnings."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set :as p]
            [org.replikativ.persistent-sorted-set.diagnostics :as diag]
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.measure :as measure])
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.node :as node])
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.numeric-stats :as numeric-stats]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set
                    PersistentSortedSet ANode Branch
                    IMeasure NumericStats NumericStatsOps])))

;; ===========================================================================
;; 1. every node must KNOW its measure, however the set was built
;; ===========================================================================

(def ^:private cmp compare)

;; The shipped measure the rest of the suite uses.
(def ^:private stats-ops
  #?(:clj  (NumericStatsOps/instance)
     :cljs numeric-stats/numeric-stats-ops))

(defn- opts [bf] {:comparator cmp :branching-factor bf :measure stats-ops})

(defn- conj-built [bf n]
  (reduce #(set/conj %1 %2 cmp) (set/sorted-set* (opts bf)) (range n)))

(defn- bulk-built [bf n]
  (set/from-sequential cmp (vec (range n)) (opts bf)))

(deftest every-node-has-a-measure-however-it-was-built
  (testing "the shipped invariant, on both build paths — a nil measure is permanent, since
            nothing on the write path recomputes it, and it is serialized"
    (doseq [bf [8 16] n [40 400]]
      (let [lbl (str "bf=" bf " n=" n)]
        (is (true? (diag/validate-measures-known (conj-built bf n)))
            (str lbl " conj-built"))
        (is (true? (diag/validate-measures-known (bulk-built bf n)))
            (str lbl " bulk-built"))))))

(deftest the-two-build-paths-agree
  (testing "bulk and incremental builds of the SAME elements must both satisfy the invariant —
            cljs computed measures eagerly in its bulk builders and not at all incrementally,
            so the two paths disagreed on the same runtime"
    (doseq [bf [8 16] n [40 400]]
      (let [c (conj-built bf n)
            b (bulk-built bf n)]
        (is (= (vec (seq c)) (vec (seq b))) (str "bf=" bf " n=" n ": same contents"))
        (is (true? (diag/validate-measures-known c)) (str "bf=" bf " n=" n ": conj-built"))
        (is (true? (diag/validate-measures-known b)) (str "bf=" bf " n=" n ": bulk-built"))))))

;; ===========================================================================
;; 2. an eager (bulk) build populates leaves
;; ===========================================================================

(defn- by-decade [a b] (compare (quot (long a) 10) (quot (long b) 10)))

#?(:clj  (def ^:private ops (NumericStatsOps/instance))
   :cljs (do (defrecord SumOps []
               measure/IMeasure
               (identity-measure [_] 0)
               (extract [_ k] k)
               (merge-measure [_ a b] (+ a b))
               (remove-measure [_ current k _recompute] (- current k)))
             (def ^:private ops (->SumOps))))

(defn- census
  "{level {:with n :without n}} over every resident node of the tree."
  [set]
  (let [acc (atom {})
        note (fn [lvl has?] (swap! acc update-in [lvl (if has? :with :without)] (fnil inc 0)))]
    #?(:clj
       (letfn [(walk [^ANode n]
                 (let [lvl (.level n)]
                   (note lvl (some? (.-_measure n)))
                   (when (instance? Branch n)
                     (dotimes [i (.len ^Branch n)]
                       (walk (.child ^Branch n nil i))))))]
         (walk (.root ^PersistentSortedSet set)))
       :cljs
       ;; a cljs Leaf has no `level` FIELD — only Branch does — so read it through
       ;; the node protocol. Reading `.-level` directly reported every leaf as
       ;; level `undefined` and made this census silently find no leaves at all.
       (letfn [(walk [n]
                 (let [lvl (node/level n)]
                   (note lvl (some? (.-_measure n)))
                   (when (pos? lvl)
                     (let [kids (.-children n)]
                       (dotimes [i (alength (.-keys n))]
                         (when-let [c (and kids (aget kids i))] (walk c)))))))]
         (walk (.-root set))))
    @acc))

(defn- bulk-built-decades [n bf]
  (s/from-sequential compare (vec (map #(+ 3 (* 10 %)) (range n)))
                     {:branching-factor bf :measure ops}))

(deftest leaves-carry-a-measure-after-an-eager-build
  (testing "`from-sequential` with `:measure` must leave every LEAF holding one.
            This is the precondition for a branch being able to fold its children
            at all, and it is where the two runtimes were suspected to differ."
    (doseq [[n bf] [[200 8] [200 16]]]
      (let [c (census (bulk-built-decades n bf))
            leaf (get c 0)]
        (is (some? leaf) (str "n=" n " bf=" bf ": the walk found leaves"))
        (is (zero? (get leaf :without 0))
            (str "n=" n " bf=" bf ": leaves WITHOUT a measure after an eager build: "
                 (pr-str c)))))))

(deftest a-remove-does-not-strip-the-measures-below-it
  (testing "after removals the surviving leaves must still hold measures —
            otherwise every branch above them can only postpone, and the
            aggregate silently disappears from the whole tree"
    (doseq [[n bf drop-n] [[200 8 50] [200 16 137]]]
      (let [base (bulk-built-decades n bf)
            v (reduce (fn [acc i] (s/disj acc (* 10 i) by-decade)) base (range drop-n))
            c (census v)]
        (is (zero? (get-in c [0 :without] 0))
            (str "n=" n " bf=" bf " dropped=" drop-n
                 ": leaves lost their measure: " (pr-str c)))))))

;; ===========================================================================
;; 3. `false` is a measure value, not an absent one
;; ===========================================================================

;; `and` over "is non-zero" — identity true, and a subtree containing 0 measures FALSE.
(def ^:private bool-and
  #?(:clj
     (reify IMeasure
       (identity [_] true)
       (extract [_ k] (not= 0 k))
       (merge [_ a b] (and a b))
       (remove [_ _current _key recompute] (.get recompute)))
     :cljs
     (reify measure/IMeasure
       (identity-measure [_] true)
       (extract [_ k] (not= 0 k))
       (merge-measure [_ a b] (and a b))
       (remove-measure [_ _current _key recompute] (recompute))
       (weight [_ _] 1))))

(defn- bulk [n bf]
  #?(:clj  (p/from-sorted-array compare (to-array (range n)) n
                                {:branching-factor bf :measure bool-and})
     :cljs (p/from-sequential compare (vec (range n))
                              {:branching-factor bf :measure bool-and})))

(defn- incremental [n bf]
  (reduce #(p/conj %1 %2 compare)
          (p/sorted-set* {:comparator compare :branching-factor bf :measure bool-and})
          (range n)))

(deftest a-false-measure-survives-the-bulk-builder
  (testing "element 0 is present, so `and` over (not= 0 k) must be FALSE. Truthiness testing
            skipped the false child as though it had no measure and returned true."
    (doseq [bf [4 8]]
      (is (false? (p/measure (bulk 20 bf)))
          (str "bf=" bf ": a subtree containing 0 must measure false, not true")))))

(deftest a-false-measure-survives-the-incremental-path
  (testing "and the same on the conj path, where a false measure previously looked like an
            absent one and would have stopped maintenance for the whole spine"
    (doseq [bf [4 8]]
      (is (false? (p/measure (incremental 20 bf)))
          (str "bf=" bf ": incremental build must agree")))))

(deftest a-true-measure-is-still-true
  (testing "the control — without the zero, the same monoid must yield true. A fix that
            confused false with nil in the other direction would pass the tests above."
    (doseq [bf [4 8]]
      (let [all-nonzero #?(:clj  (p/from-sorted-array compare (to-array (range 1 21)) 20
                                                      {:branching-factor bf :measure bool-and})
                           :cljs (p/from-sequential compare (vec (range 1 21))
                                                    {:branching-factor bf :measure bool-and}))]
        (is (true? (p/measure all-nonzero))
            (str "bf=" bf ": no zero present, so the measure must be true"))))))

;; ===========================================================================
;; 4. remove must subtract the element it removed, not the search key
;; ===========================================================================

;; Elements are longs `3, 13, 23, …`. The REMOVE comparator compares only the
;; decade, so `(disj s 30 by-decade)` finds and removes the stored `33` — the
;; search key and the stored element are comparator-equal and different.
#?(:clj
   (do
     (def ^:private ops (NumericStatsOps/instance))

     (defn- cached-sum [^PersistentSortedSet v]
       (when-let [m (.-_measure ^ANode (.root v))] (.-sum ^NumericStats m)))

     (defn- conj-built-decades [n bf]
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
           (let [base (conj-built-decades n bf)
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
           (let [base (conj-built-decades n bf)
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
     (defrecord InvertibleSumOps []
       measure/IMeasure
       (identity-measure [_] 0)
       (extract [_ k] k)
       (merge-measure [_ a b] (+ a b))
       (remove-measure [_ current k _recompute] (- current k)))

     (def ^:private sum-ops (->InvertibleSumOps))

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
