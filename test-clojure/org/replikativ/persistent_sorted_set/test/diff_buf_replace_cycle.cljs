(ns org.replikativ.persistent-sorted-set.test.diff-buf-replace-cycle
  "A buffered `replace` must record the element it actually removed.

   The ClojureScript half of the JVM's `test.diff-buf-restore-cycle` defect A.
   `$replace` deposited `Absent(old-key)` using the CALLER's search key. When
   the operation comparator is COARSER than the set's — datahike's
   value-changing datom upsert searches `[e a _ _]` and replaces the whole datom
   — that is not the element the leaf holds once an earlier buffered replace has
   already changed it. The Absent then cancelled nothing and the new Present was
   added ALONGSIDE the old one.

   It is invisible until a reload: the transient leaf is mutated in place, so
   the in-memory set reads correctly and only the buffered DIFF is wrong. The
   duplicate appears when that diff is projected onto the durable leaf.

   Both implementations had it, independently — the port mirrored the JVM
   faithfully, including the bug. That is the third time a diff-buf defect has
   been present on both sides (see `diff_buf_cross_version.cljs` for the one
   that was JVM-only), and the reason this file exists rather than trusting the
   JVM test to cover the behaviour."
  (:require [cljs.test :refer-macros [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.test.storage.util :as u]))

;; Elements are [k v]; the SET orders by both, the REPLACE comparator by k
;; alone — so `(replace [k 0] [k v'])` is a content-only upsert of k.
(defn- full-cmp [a b]
  (let [c (compare (nth a 0) (nth b 0))]
    (if-not (zero? c) c (compare (nth a 1) (nth b 1)))))

(defn- by-k [a b] (compare (nth a 0) (nth b 0)))

(defn- entries-for [set k]
  (vec (filter #(= k (nth % 0)) (s/seq set))))

(defn- scenario
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
    (let [r (scenario {:diff-buf 128 :n-replaces 2})]
      (is (= [[5 2]] (:in-memory r)) "the in-memory set was never wrong")
      (is (= [[5 2]] (:restored r)) "and the reload must agree with it")
      (is (= 16 (:count r)))
      (is (= 16 (:seq-count r)) "count and seq must describe the same tree"))))

(deftest more-replaces-of-one-key-still-collapse-to-the-last
  (testing "the diff is latest-wins per key however many times it is touched"
    (let [r (scenario {:diff-buf 128 :n-replaces 5})]
      (is (= [[5 5]] (:restored r)))
      (is (= 16 (:count r)))
      (is (= 16 (:seq-count r))))))

(deftest diff-buf-off-is-the-control
  (testing "with no buffering there is no diff to mis-key — if this fails the
            fault is not in the deposit"
    (doseq [n [2 5]]
      (let [r (scenario {:diff-buf 0 :n-replaces n})]
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
