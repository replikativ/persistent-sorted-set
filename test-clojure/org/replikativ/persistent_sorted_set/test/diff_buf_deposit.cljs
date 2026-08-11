(ns org.replikativ.persistent-sorted-set.test.diff-buf-deposit
  "ClojureScript, diff-buf: three ways a deposit can record the wrong thing.

   All three are one root cause -- THE CALLER'S SEARCH KEY IS NOT THE ELEMENT THE TREE HOLDS,
   once the operation comparator is coarser than the set's -- reached by three different
   operations:

     * REPLACE, repeated on one key: each deposit must supersede the last, not accumulate.
     * A FALSE element: `false` is a legal value, and a deposit that stored the probe instead
       of the element made it indistinguishable from absence.
     * TWO VERSIONS over one cache: projecting v1 must not clobber the node v2 reads.

   TWO RENAMES WERE FORCED BY THE MERGE, and they are the reason this file is not a plain
   concatenation. Both source files defined `scenario` -- with incompatible argument maps and
   incompatible return maps -- and both defined a deftest called `diff-buf-off-is-the-control`.
   A duplicate deftest name does not error: the later one silently REPLACES the earlier, so a
   naive merge would have dropped a test and taken the cljs count from 243 to 242. They are
   now `replace-cycle-scenario` / `cross-version-scenario` and
   `diff-buf-off-is-the-control-for-the-replace-deposit` /
   `diff-buf-off-is-the-control-for-projection`.

   The three fixtures stay separate for a further reason: the false-element section RESETS
   the storage cache to force a cold read, while the cross-version section REQUIRES the cache
   to persist across versions -- that shared cache is the whole point of its scenario. One
   shared storage helper here would make the second silently untestable."
  (:require [cljs.test :refer-macros [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.test.storage.util :as u]))

;; ===========================================================================
;; 1. repeated replace on one key -- each deposit supersedes the last
;; ===========================================================================

;; Elements are [k v]; the SET orders by both, the REPLACE comparator by k
;; alone — so `(replace [k 0] [k v'])` is a content-only upsert of k.
(defn- full-cmp [a b]
  (let [c (compare (nth a 0) (nth b 0))]
    (if-not (zero? c) c (compare (nth a 1) (nth b 1)))))

(defn- by-k [a b] (compare (nth a 0) (nth b 0)))

(defn- entries-for [set k]
  (vec (filter #(= k (nth % 0)) (s/seq set))))

(defn- replace-cycle-scenario
  "Build, store, then `n-replaces` upserts of ONE key in a single derive, store,
   restore. Returns what key 5 reads back after the reload, plus the counts."
  [{:keys [diff-buf n-replaces]}]
  (let [opts {:branching-factor 4 :diff-buf-size diff-buf :comparator full-cmp}
        disk (atom {})
        mem  (atom {})
        st   (u/storage mem disk opts)
        base (reduce #(s/conj %1 [%2 0] full-cmp) (s/sorted-set* opts) (range 16))
        _    (s/store base st)                 ; every leaf gets a durable anchor
        v1   (reduce (fn [acc i] (s/replace acc [5 0] [5 (inc i)] by-k))
                     base (range n-replaces))
        a1   (s/store v1 st)
        cold (s/restore a1 st opts)]
    {:in-memory (entries-for v1 5)
     :restored  (entries-for cold 5)
     :count     (count cold)
     :seq-count (count (vec (s/seq cold)))}))

(deftest a-buffered-replace-supersedes-the-previous-one
  (testing "two upserts of one key in a single derive. Measured before the fix:
            restored as [[5 1] [5 2]], count 16 against a seq of 17 — while the
            in-memory set read [[5 2]] throughout."
    (let [r (replace-cycle-scenario {:diff-buf 128 :n-replaces 2})]
      (is (= [[5 2]] (:in-memory r)) "the in-memory set was never wrong")
      (is (= [[5 2]] (:restored r)) "and the reload must agree with it")
      (is (= 16 (:count r)))
      (is (= 16 (:seq-count r)) "count and seq must describe the same tree"))))

(deftest more-replaces-of-one-key-still-collapse-to-the-last
  (testing "the diff is latest-wins per key however many times it is touched"
    (let [r (replace-cycle-scenario {:diff-buf 128 :n-replaces 5})]
      (is (= [[5 5]] (:restored r)))
      (is (= 16 (:count r)))
      (is (= 16 (:seq-count r))))))

(deftest diff-buf-off-is-the-control-for-the-replace-deposit
  (testing "with no buffering there is no diff to mis-key — if this fails the
            fault is not in the deposit"
    (doseq [n [2 5]]
      (let [r (replace-cycle-scenario {:diff-buf 0 :n-replaces n})]
        (is (= [[5 n]] (:restored r)) (str n " replaces"))
        (is (= 16 (:count r)))
        (is (= 16 (:seq-count r)))))))

(deftest replace-requires-no-equal-sibling-under-the-operation-comparator
  (testing "PRECONDITION, now asserted rather than silently violated.

            `replace` writes the new element into the OLD element's slot. That is
            sound only while the replacement sorts into the same position under
            the SET's comparator. With an operation comparator coarser than the
            set's, a leaf may hold several elements equal under it, `binarySearch`
            picks an ARBITRARY one, and replacing it can move it past a sibling —
            leaving the sorted set UNSORTED.

            Measured before the assertion existed, on BOTH runtimes: a set
            containing [5 0] and [5 7], `(replace s [5 0] [5 9] by-k)`, yielded
            [[5 9] [5 7]] — out of order. What followed then DIVERGED: on the JVM
            `contains? s [5 7]` returned FALSE for an element still in the set (its
            binary search walks an unsorted array), while ClojureScript still found
            it. Silent, runtime-dependent corruption.

            It is an ASSERTION, not a runtime check: elided from release builds
            (`:elide-asserts`, and `-ea` on the JVM), so it costs users nothing in
            production and catches the misuse in their tests. Repositioning instead
            would make `replace` a disj+conj with a different cost.

            datahike is not exposed either way: it only replaces on CARDINALITY-ONE
            attributes (`upsert? (not (multival? db a))`), where no second datom
            shares [e a], and `:avet` uses disj+conj instead of replace because v is
            part of that index's leading key."
    (let [opts {:branching-factor 4 :diff-buf-size 128 :comparator full-cmp}
          st   (u/storage (atom {}) (atom {}) opts)
          base (as-> (s/sorted-set* opts) $
                 (reduce #(s/conj %1 [%2 0] full-cmp) $ (range 40))
                 (s/conj $ [5 7] full-cmp))]
      (s/store base st)
      (is (= 41 (count (vec (s/seq base))))
          "precondition: both elements for key 5 are present, in one leaf")
      (is (thrown-with-msg? js/Error #"may be UNSORTED"
                            (s/replace base [5 0] [5 9] by-k))
          "the ambiguous replace is refused by name instead of corrupting the set")))

  (testing "and the supported case — one element per operation-comparator key —
            is untouched by the assertion"
    (let [opts {:branching-factor 4 :diff-buf-size 128 :comparator full-cmp}
          st   (u/storage (atom {}) (atom {}) opts)
          base (reduce #(s/conj %1 [%2 0] full-cmp) (s/sorted-set* opts) (range 40))]
      (s/store base st)
      (let [v1 (s/replace base [5 0] [5 9] by-k)]
        (is (= [[5 9]] (entries-for v1 5)))
        (is (= 40 (count (vec (s/seq v1)))))))))

;; ===========================================================================
;; 2. `false` is an element, not an absence
;; ===========================================================================

;; Two comparators over the same elements. `stable-cmp` is the SET's: it separates `false`,
;; `:probe` and `:replacement`. `op-cmp` is the one passed to the operation and is COARSER —
;; it ranks all three equal, which is the documented "[id value] compared by id" pattern and
;; the only way to reach an element by a probe that is not the element itself.
(defn- rank [x]
  (cond (false? x) 0.25 (= x :probe) 0.5 (= x :replacement) 0.75 :else x))
(defn- stable-cmp [a b] (compare (rank a) (rank b)))
(defn- op-rank [x]
  (if (or (false? x) (= x :probe) (= x :replacement)) 0.25 x))
(defn- op-cmp [a b] (compare (op-rank a) (op-rank b)))

(defn- cold-roundtrip
  "Build a set containing `false`, apply `f`, store, drop the node cache, restore. Returns
   the in-memory and the cold-restored element vectors."
  [budget f]
  (let [opts    {:branching-factor 4 :diff-buf-size budget :comparator stable-cmp}
        memory  (atom {})
        disk    (atom {})
        storage (u/storage memory disk opts)
        base    (reduce #(s/conj %1 %2 stable-cmp)
                        (s/sorted-set* opts)
                        (concat (range 5) [false]))
        _       (s/store base storage)
        changed (f base)
        address (s/store changed storage)
        _       (reset! memory {})                 ; force a genuinely COLD read
        cold    (s/restore address storage opts)]
    {:memory (vec (s/seq changed)) :cold (vec (s/seq cold))}))

(deftest removing-a-false-element-through-an-equivalent-probe-survives-a-cold-restore
  (testing "the diff must record the element that was actually removed, not the probe"
    (doseq [budget [128 0]]
      (let [{:keys [memory cold]} (cold-roundtrip budget #(s/disj % :probe op-cmp))]
        (is (= memory cold)
            (str "budget=" budget ": the cold restore must agree with memory, got "
                 (pr-str memory) " vs " (pr-str cold)))
        (is (not (some false? cold))
            (str "budget=" budget ": `false` was removed and must not come back: "
                 (pr-str cold)))))))

(deftest replacing-a-false-element-does-not-leave-both-behind
  (testing "the same box, on the replace path — a wrong key here duplicates rather than
            resurrects, because the replacement is also inserted"
    (doseq [budget [128 0]]
      (let [{:keys [memory cold]}
            (cold-roundtrip budget #(s/replace % :probe :replacement op-cmp))]
        (is (= memory cold)
            (str "budget=" budget ": " (pr-str memory) " vs " (pr-str cold)))
        (is (not (some false? cold))
            (str "budget=" budget ": `false` was replaced and must be gone: "
                 (pr-str cold)))
        (is (some #(= :replacement %) cold)
            (str "budget=" budget ": and the replacement must be present"))))))

(deftest a-false-element-round-trips-untouched
  (testing "the control: without any operation, `false` must survive store and restore, or
            the tests above could pass for the wrong reason"
    (let [{:keys [memory cold]} (cold-roundtrip 128 identity)]
      (is (= memory cold))
      (is (some false? cold) "precondition: the set really does contain `false`"))))

;; ===========================================================================
;; 3. two versions over one cache -- projection must not clobber
;; ===========================================================================

(def ^:private n 4000)
(def ^:private scatter1 (vec (range 1 n 37)))
(def ^:private scatter2 (vec (range 2 n 53)))

(defn- cross-version-scenario
  "base → v1 → v2, each derived from a RESTORED set, all through ONE storage so
   the node cache is shared. Returns what each version reads back, and what it
   should read, with `read-order` deciding which is materialized first."
  [{:keys [diff-buf read-order]}]
  (let [opts {:branching-factor 8 :diff-buf-size diff-buf :comparator compare}
        disk (atom {})
        mem  (atom {})
        st   (u/storage mem disk opts)
        base (reduce #(s/conj %1 %2 compare) (s/sorted-set* opts) (range 0 n 4))
        a0   (s/store base st)
        v1   (reduce #(s/conj %1 %2 compare) (s/restore a0 st opts) scatter1)
        a1   (s/store v1 st)
        v2   (reduce #(s/conj %1 %2 compare) (s/restore a1 st opts) scatter2)
        a2   (s/store v2 st)
        exp1 (vec (sort (distinct (concat (range 0 n 4) scatter1))))
        exp2 (vec (sort (distinct (concat (range 0 n 4) scatter1 scatter2))))
        r1   (s/restore a1 st opts)
        r2   (s/restore a2 st opts)
        [g1 g2] (if (= :v1-first read-order)
                  (let [x (vec (s/seq r1))] [x (vec (s/seq r2))])
                  (let [y (vec (s/seq r2))] [(vec (s/seq r1)) y]))]
    {:v1-missing (count (remove (set g1) exp1))
     :v1-extra   (count (remove (set exp1) g1))
     :v2-missing (count (remove (set g2) exp2))
     :v2-extra   (count (remove (set exp2) g2))}))

(deftest projection-does-not-clobber-the-version-it-projects-from
  (testing "two versions restored through ONE cache, in both read orders"
    (doseq [order [:v1-first :v2-first]]
      (let [r (cross-version-scenario {:diff-buf 512 :read-order order})]
        (is (= {:v1-missing 0 :v1-extra 0 :v2-missing 0 :v2-extra 0} r)
            (str "read-order " order ": " (pr-str r)))))))

(deftest diff-buf-off-is-the-control-for-projection
  (testing "with no buffering there is no projection, so nothing can leak — if
            this fails the fault is not in projection"
    (doseq [order [:v1-first :v2-first]]
      (let [r (cross-version-scenario {:diff-buf 0 :read-order order})]
        (is (= {:v1-missing 0 :v1-extra 0 :v2-missing 0 :v2-extra 0} r)
            (str "read-order " order ": " (pr-str r)))))))
