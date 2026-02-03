(ns me.tonsky.persistent-sorted-set.test.generative
  "Cross-platform generative tests for persistent sorted set.
   Tests behavioral properties that work on both JVM and JS."
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.test.structural-invariants :as inv]))

;; =============================================================================
;; Helpers
;; =============================================================================

(defn irange
  "Inclusive range helper - returns range including both endpoints."
  [from to]
  (if (<= from to)
    (range from (inc to))
    (range from (dec to) -1)))

(defn expected-slice
  "Reference implementation: what slice should return."
  [sorted-elements from to]
  (let [from-val (or from ##-Inf)
        to-val (or to ##Inf)]
    (seq (filter #(and (>= (compare % from-val) 0)
                       (<= (compare % to-val) 0))
                 sorted-elements))))

(defn expected-rslice
  "Reference implementation: what rslice should return."
  [sorted-elements from to]
  (let [from-val (or from ##Inf)
        to-val (or to ##-Inf)]
    (seq (reverse (filter #(and (<= (compare % from-val) 0)
                                (>= (compare % to-val) 0))
                          sorted-elements)))))

(defn check-structure
  "Check key structural invariants: sorted, count, balanced, branch-keys.
   Returns true if all pass, false otherwise."
  [tree elements]
  (and (:pass? (inv/check-sorted-order tree))
       (:pass? (inv/check-count-consistency tree (count (distinct elements))))
       (:pass? (inv/check-balanced tree))
       (:pass? (inv/check-branch-keys tree))))

;; =============================================================================
;; Generators
;; =============================================================================

(def gen-small-int
  "Generator for small integers suitable for set elements."
  (gen/choose -1000 1000))

(def gen-element-count
  "Generator for number of elements in a set."
  (gen/choose 0 500))

(def gen-large-element-count
  "Generator for larger sets."
  (gen/choose 100 2000))

;; =============================================================================
;; Property: Basic set operations
;; =============================================================================

(deftest test-gen-conj-maintains-sorted-order
  (testing "Property: conj maintains sorted order"
    (let [result
          (tc/quick-check 100
                          (prop/for-all [elements (gen/vector gen-small-int 0 200)]
                                        (let [s (reduce conj (set/sorted-set) elements)
                                              as-vec (vec s)]
                                          (= as-vec (sort (distinct elements))))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

(deftest test-gen-count-matches
  (testing "Property: count matches number of unique elements"
    (let [result
          (tc/quick-check 100
                          (prop/for-all [elements (gen/vector gen-small-int 0 200)]
                                        (let [s (into (set/sorted-set) elements)]
                                          (= (count s) (count (distinct elements))))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

(deftest test-gen-contains-all-elements
  (testing "Property: all added elements are findable"
    (let [result
          (tc/quick-check 100
                          (prop/for-all [elements (gen/vector gen-small-int 1 200)]
                                        (let [s (into (set/sorted-set) elements)]
                                          (every? #(contains? s %) elements))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

;; =============================================================================
;; Property: slice operations
;; =============================================================================

(deftest test-gen-slice-returns-correct-elements
  (testing "Property: slice returns exactly the elements in range"
    (let [result
          (tc/quick-check 100
                          (prop/for-all [elements (gen/vector gen-small-int 1 200)
                                         from gen-small-int
                                         to gen-small-int]
                                        (let [s (into (set/sorted-set) elements)
                                              sorted (sort (distinct elements))
                                              actual (set/slice s from to)
                                              expected (expected-slice sorted from to)]
                                          (= (seq actual) expected))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

(deftest test-gen-slice-with-nil-bounds
  (testing "Property: slice with nil bounds returns correct elements"
    (let [result
          (tc/quick-check 100
                          (prop/for-all [elements (gen/vector gen-small-int 1 200)
                                         bound gen-small-int
                                         which-nil (gen/elements [:from :to :both])]
                                        (let [s (into (set/sorted-set) elements)
                                              sorted (sort (distinct elements))
                                              [from to] (case which-nil
                                                          :from [nil bound]
                                                          :to [bound nil]
                                                          :both [nil nil])
                                              actual (set/slice s from to)
                                              expected (expected-slice sorted from to)]
                                          (= (seq actual) expected))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

;; =============================================================================
;; Property: rslice operations
;; =============================================================================

(deftest test-gen-rslice-returns-correct-elements
  (testing "Property: rslice returns elements in reverse order"
    (let [result
          (tc/quick-check 100
                          (prop/for-all [elements (gen/vector gen-small-int 1 200)
                                         from gen-small-int
                                         to gen-small-int]
                                        (let [s (into (set/sorted-set) elements)
                                              sorted (sort (distinct elements))
                                              actual (set/rslice s from to)
                                              expected (expected-rslice sorted from to)]
                                          (= (seq actual) expected))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

(deftest test-gen-rslice-with-nil-bounds
  (testing "Property: rslice with nil bounds returns correct elements"
    (let [result
          (tc/quick-check 100
                          (prop/for-all [elements (gen/vector gen-small-int 1 200)
                                         bound gen-small-int
                                         which-nil (gen/elements [:from :to :both])]
                                        (let [s (into (set/sorted-set) elements)
                                              sorted (sort (distinct elements))
                                              [from to] (case which-nil
                                                          :from [nil bound]
                                                          :to [bound nil]
                                                          :both [nil nil])
                                              actual (set/rslice s from to)
                                              expected (expected-rslice sorted from to)]
                                          (= (seq actual) expected))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

;; =============================================================================
;; Property: slice/rslice relationship
;; =============================================================================

(deftest test-gen-rslice-is-reverse-of-slice
  (testing "Property: (reverse (rslice s a b)) = (slice s b a)"
    (let [result
          (tc/quick-check 100
                          (prop/for-all [elements (gen/vector gen-small-int 1 200)
                                         from gen-small-int
                                         to gen-small-int]
                                        (let [s (into (set/sorted-set) elements)
                                              rslice-reversed (some-> (set/rslice s from to) reverse seq)
                                              slice-result (seq (set/slice s to from))]
                                          (= rslice-reversed slice-result))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

(deftest test-gen-slice-rseq-equals-rslice
  (testing "Property: (rseq (slice s a b)) = (rslice s b a)"
    (let [result
          (tc/quick-check 100
                          (prop/for-all [elements (gen/vector gen-small-int 1 200)
                                         from gen-small-int
                                         to gen-small-int]
                                        (let [s (into (set/sorted-set) elements)
                                              slice-rseq (some-> (set/slice s from to) rseq seq)
                                              rslice-result (seq (set/rslice s to from))]
                                          (= slice-rseq rslice-result))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

;; =============================================================================
;; Property: larger scale tests
;; =============================================================================

(deftest test-gen-large-set-slice
  (testing "Property: slice works correctly on larger sets"
    (let [result
          (tc/quick-check 50
                          (prop/for-all [n gen-large-element-count
                                         from-pct (gen/double* {:min 0.0 :max 1.0})
                                         to-pct (gen/double* {:min 0.0 :max 1.0})]
                                        (let [elements (range n)
                                              s (into (set/sorted-set) (shuffle elements))
                                              from (int (* from-pct n))
                                              to (int (* to-pct n))
                                              actual (set/slice s from to)
                                              expected (expected-slice elements from to)]
                                          (= (seq actual) expected))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

(deftest test-gen-large-set-rslice
  (testing "Property: rslice works correctly on larger sets"
    (let [result
          (tc/quick-check 50
                          (prop/for-all [n gen-large-element-count
                                         from-pct (gen/double* {:min 0.0 :max 1.0})
                                         to-pct (gen/double* {:min 0.0 :max 1.0})]
                                        (let [elements (range n)
                                              s (into (set/sorted-set) (shuffle elements))
                                              from (int (* from-pct n))
                                              to (int (* to-pct n))
                                              actual (set/rslice s from to)
                                              expected (expected-rslice elements from to)]
                                          (= (seq actual) expected))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

;; =============================================================================
;; Property: disj operations
;; =============================================================================

(deftest test-gen-disj-removes-elements
  (testing "Property: disj removes elements correctly"
    (let [result
          (tc/quick-check 100
                          (prop/for-all [elements (gen/vector gen-small-int 1 200)
                                         to-remove (gen/vector gen-small-int 0 50)]
                                        (let [s (into (set/sorted-set) elements)
                                              result (reduce disj s to-remove)
                                              expected (sort (remove (set to-remove) (distinct elements)))]
                                          (= (vec result) expected))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

(deftest test-gen-disj-then-slice
  (testing "Property: slice works correctly after disj"
    (let [result
          (tc/quick-check 50
                          (prop/for-all [elements (gen/vector gen-small-int 10 200)
                                         to-remove (gen/vector gen-small-int 0 30)
                                         from gen-small-int
                                         to gen-small-int]
                                        (let [s (reduce disj (into (set/sorted-set) elements) to-remove)
                                              remaining (sort (remove (set to-remove) (distinct elements)))
                                              actual (set/slice s from to)
                                              expected (expected-slice remaining from to)]
                                          (= (seq actual) expected))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

;; =============================================================================
;; Property: seek operations
;; =============================================================================

(deftest test-gen-seek-returns-correct-position
  (testing "Property: seek returns elements >= target"
    (let [result
          (tc/quick-check 100
                          (prop/for-all [elements (gen/vector gen-small-int 1 200)
                                         target gen-small-int]
                                        (let [s (into (set/sorted-set) elements)
                                              sought (set/seek (seq s) target)
                                              sorted (sort (distinct elements))
                                              expected (seq (drop-while #(< % target) sorted))]
                                          (= (seq sought) expected))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

(deftest test-gen-seek-on-slice
  (testing "Property: seek works correctly on slices"
    (let [result
          (tc/quick-check 50
                          (prop/for-all [elements (gen/vector gen-small-int 10 200)
                                         slice-from gen-small-int
                                         slice-to gen-small-int
                                         seek-target gen-small-int]
                                        (let [s (into (set/sorted-set) elements)
                                              sliced (set/slice s slice-from slice-to)
                                              sought (when sliced (set/seek sliced seek-target))
                    ;; Expected: elements in slice range, then >= seek-target
                                              slice-expected (expected-slice (sort (distinct elements)) slice-from slice-to)
                                              expected (when slice-expected
                                                         (seq (drop-while #(< % seek-target) slice-expected)))]
                                          (= (seq sought) expected))))]
      (is (:pass? result) (pr-str (:shrunk result))))))
