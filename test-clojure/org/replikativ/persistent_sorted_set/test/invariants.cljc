(ns org.replikativ.persistent-sorted-set.test.invariants
  "B-tree structural invariant tests.

   Uses the diagnostics library namespace for validation functions.
   The `validate-tree` function is public and intended to be called from
   other test namespaces (e.g. generative.cljc) to ensure invariants hold
   throughout all property-based tests."
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop #?@(:cljs [:include-macros true])]
   [clojure.test.check.clojure-test :refer [defspec] #?@(:cljs [:include-macros true])]
   [org.replikativ.persistent-sorted-set :as set]
   [org.replikativ.persistent-sorted-set.diagnostics :as diag]))

;; =============================================================================
;; Public wrapper — used by other test namespaces
;; =============================================================================

(defn validate-tree
  "Validate all B-tree structural invariants via the diagnostics library.
   Returns true if valid. Throws with details if any invariant is violated."
  [set]
  (diag/validate-full set))

;; =============================================================================
;; Generators
;; =============================================================================

(def gen-int (gen/choose -1000 1000))

(def gen-operation
  (gen/tuple (gen/elements [:add :remove]) gen-int))

(def gen-operations
  (gen/vector gen-operation 0 300))

;; =============================================================================
;; Property: structural invariants hold after arbitrary operations
;; =============================================================================

(defspec structural-invariants-after-ops 200
  (prop/for-all [elements (gen/vector gen-int 0 300)
                 ops gen-operations]
                (let [pss (reduce conj (set/sorted-set* {:branching-factor 4}) elements)
                      final (reduce (fn [s [op v]]
                                      (case op :add (conj s v) :remove (disj s v)))
                                    pss ops)]
                  (validate-tree final))))

(defspec structural-invariants-after-transient 200
  (prop/for-all [elements (gen/vector gen-int 0 300)
                 ops gen-operations]
                (let [pss (reduce conj (set/sorted-set* {:branching-factor 4}) elements)
                      final (persistent!
                             (reduce (fn [t [op v]]
                                       (case op :add (conj! t v) :remove (disj! t v)))
                                     (transient pss) ops))]
                  (validate-tree final))))

(defspec structural-invariants-with-replace 200
  (prop/for-all [elements (gen/vector gen-int 10 300)]
                (let [pss (reduce conj (set/sorted-set* {:branching-factor 4}) elements)
                      elems (vec pss)
                      final (reduce (fn [s k] (set/replace s k k)) pss elems)]
                  (validate-tree final))))

(defspec structural-invariants-heavy-rebalancing 200
  (prop/for-all [elements (gen/vector gen-int 50 500)]
                (let [pss (reduce conj (set/sorted-set* {:branching-factor 4}) elements)
                      to-remove (take (* 3 (quot (count elements) 4)) (shuffle elements))
                      final (reduce disj pss to-remove)]
                  (validate-tree final))))

(defspec structural-invariants-default-bf 100
  (prop/for-all [elements (gen/vector gen-int 0 2000)
                 ops (gen/vector gen-operation 0 500)]
                (let [pss (into (set/sorted-set) elements)
                      final (reduce (fn [s [op v]]
                                      (case op :add (conj s v) :remove (disj s v)))
                                    pss ops)]
                  (validate-tree final))))

;; =============================================================================
;; Property: invariants hold after EVERY SINGLE operation (not just at end)
;; =============================================================================

(defspec invariants-after-every-op 100
  (prop/for-all [elements (gen/vector gen-int 0 100)
                 ops (gen/vector gen-operation 0 100)]
                (let [initial (reduce conj (set/sorted-set* {:branching-factor 4}) elements)]
                  (reduce (fn [s [op v]]
                            (let [s' (case op :add (conj s v) :remove (disj s v))]
                              (validate-tree s')
                              s'))
                          initial ops)
                  true)))

;; =============================================================================
;; Property: subtree counts correct when known
;; =============================================================================

(defspec subtree-counts-correct-after-ops 200
  (prop/for-all [elements (gen/vector gen-int 0 300)
                 ops gen-operations]
                (let [pss (reduce conj (set/sorted-set* {:branching-factor 4}) elements)
                      clj-set (into (sorted-set) elements)
                      [final-pss final-clj]
                      (reduce (fn [[s c] [op v]]
                                (case op
                                  :add [(conj s v) (conj c v)]
                                  :remove [(disj s v) (disj c v)]))
                              [pss clj-set] ops)]
                  ;; count must match actual count
                  (= (count final-pss) (count final-clj)))))

;; =============================================================================
;; Non-deterioration: fresh trees never have unknown subtree counts
;; =============================================================================

(defspec subtree-counts-always-known-for-fresh-trees 200
  (prop/for-all [elements (gen/vector gen-int 0 300)
                 ops gen-operations]
                (let [pss (reduce conj (set/sorted-set* {:branching-factor 4}) elements)
                      final (reduce (fn [s [op v]]
                                      (case op :add (conj s v) :remove (disj s v)))
                                    pss ops)]
                  (diag/validate-counts-known final))))

(defspec subtree-counts-always-known-after-transient 200
  (prop/for-all [elements (gen/vector gen-int 0 300)
                 ops gen-operations]
                (let [pss (reduce conj (set/sorted-set* {:branching-factor 4}) elements)
                      final (persistent!
                             (reduce (fn [t [op v]]
                                       (case op :add (conj! t v) :remove (disj! t v)))
                                     (transient pss) ops))]
                  (diag/validate-counts-known final))))

(defspec fresh-tree-count-equals-actual-count 200
  (prop/for-all [elements (gen/vector gen-int 0 300)
                 ops gen-operations]
                (let [pss (reduce conj (set/sorted-set* {:branching-factor 4}) elements)
                      clj-set (into (sorted-set) elements)
                      [final-pss final-clj]
                      (reduce (fn [[s c] [op v]]
                                (case op
                                  :add [(conj s v) (conj c v)]
                                  :remove [(disj s v) (disj c v)]))
                              [pss clj-set] ops)]
                  ;; For fresh trees, count should be O(1) and always correct
                  (and (diag/validate-counts-known final-pss)
                       (= (count final-pss) (count final-clj))))))

;; =============================================================================
;; Equational properties
;; =============================================================================

(defspec first-equals-min 200
  (prop/for-all [elements (gen/not-empty (gen/vector gen-int 1 500))]
                (let [pss (into (set/sorted-set) elements)]
                  (= (first pss) (apply min elements)))))

(defspec last-equals-max 200
  (prop/for-all [elements (gen/not-empty (gen/vector gen-int 1 500))]
                (let [pss (into (set/sorted-set) elements)]
                  (= (first (rseq pss)) (apply max elements)))))

(defspec conj-is-idempotent 200
  (prop/for-all [elements (gen/vector gen-int 0 200)
                 x gen-int]
                (let [pss (into (set/sorted-set) elements)]
                  (= (vec (conj pss x))
                     (vec (conj (conj pss x) x))))))

(defspec disj-nonmember-is-noop 200
  (prop/for-all [elements (gen/vector gen-int 0 200)
                 x gen-int]
                (let [pss (into (set/sorted-set) elements)]
                  (if (contains? pss x)
                    true
                    (= (vec pss) (vec (disj pss x)))))))

(defspec count-after-conj 200
  (prop/for-all [elements (gen/vector gen-int 0 200)
                 x gen-int]
                (let [pss (into (set/sorted-set) elements)
                      was-member (contains? pss x)]
                  (= (count (conj pss x))
                     (+ (count pss) (if was-member 0 1))))))

(defspec count-after-disj 200
  (prop/for-all [elements (gen/vector gen-int 0 200)
                 x gen-int]
                (let [pss (into (set/sorted-set) elements)
                      was-member (contains? pss x)]
                  (= (count (disj pss x))
                     (- (count pss) (if was-member 1 0))))))

(defspec lookup-returns-stored-element 200
  (prop/for-all [elements (gen/not-empty (gen/vector gen-int 1 500))]
                (let [pss (into (set/sorted-set) elements)
                      elems (vec pss)]
                  (every? (fn [k]
                            (= (set/lookup pss k) k))
                          (take 20 elems)))))

(defspec contains-consistent-with-seq 200
  (prop/for-all [elements (gen/vector gen-int 0 300)
                 test-vals (gen/vector gen-int 0 50)]
                (let [pss (into (set/sorted-set) elements)
                      elems-set (set (vec pss))]
                  (every? (fn [v]
                            (= (contains? pss v)
                               (contains? elems-set v)))
                          test-vals))))

;; =============================================================================
;; Structural sharing: derived sets don't corrupt each other
;; =============================================================================

(defspec structural-sharing-preserves-invariants 200
  (prop/for-all [elements (gen/vector gen-int 0 200)
                 ops1 (gen/vector gen-operation 0 50)
                 ops2 (gen/vector gen-operation 0 50)]
                (let [base (reduce conj (set/sorted-set* {:branching-factor 4}) elements)
                      s1 (reduce (fn [s [op v]]
                                   (case op :add (conj s v) :remove (disj s v)))
                                 base ops1)
                      s2 (reduce (fn [s [op v]]
                                   (case op :add (conj s v) :remove (disj s v)))
                                 base ops2)]
                  (and (validate-tree base)
                       (validate-tree s1)
                       (validate-tree s2)))))

;; =============================================================================
;; Basic deterministic tests
;; =============================================================================

(deftest basic-invariant-checks
  (testing "Empty set"
    (is (validate-tree (set/sorted-set))))
  (testing "Single element"
    (is (validate-tree (conj (set/sorted-set* {:branching-factor 4}) 42))))
  (testing "Many elements with small BF"
    (let [pss (reduce conj (set/sorted-set* {:branching-factor 4}) (range 100))]
      (is (validate-tree pss))))
  (testing "After removes"
    (let [pss (reduce conj (set/sorted-set* {:branching-factor 4}) (range 100))
          result (reduce disj pss (range 0 100 2))]
      (is (validate-tree result))))
  (testing "After transient batch"
    (let [pss (persistent!
               (reduce conj! (transient (set/sorted-set* {:branching-factor 4}))
                       (range 200)))]
      (is (validate-tree pss)))))

;; =============================================================================
;; Diagnostics API tests
;; =============================================================================

(deftest diagnostics-validate-test
  (testing "validate on valid set"
    (is (diag/validate (into (set/sorted-set) (range 100)))))
  (testing "validate-full on valid set"
    (is (diag/validate-full (into (set/sorted-set) (range 100)))))
  (testing "validate-counts-known on fresh set"
    (is (diag/validate-counts-known
         (reduce conj (set/sorted-set* {:branching-factor 4}) (range 100)))))
  (testing "validate-content with passing check"
    (is (diag/validate-content (into (set/sorted-set) (range 10))
                               (fn [_keys] nil))))
  (testing "tree-stats returns expected structure"
    (let [stats (diag/tree-stats (reduce conj (set/sorted-set* {:branching-factor 4}) (range 100)))]
      (is (= 100 (:element-count stats)))
      (is (pos? (:depth stats)))
      (is (:counts-known? stats)))))
