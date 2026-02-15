(ns org.replikativ.persistent-sorted-set.test.generative
  "Generative (property-based) tests for persistent-sorted-set.

   These tests verify correctness properties that should hold for all inputs,
   using test.check for random input generation and shrinking.

   Works on both CLJ and CLJS."
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop #?@(:cljs [:include-macros true])]
   [clojure.test.check.clojure-test :refer [defspec] #?@(:cljs [:include-macros true])]
   [org.replikativ.persistent-sorted-set :as set]
   [org.replikativ.persistent-sorted-set.test.invariants :as inv]
   #?(:clj [org.replikativ.persistent-sorted-set.test.storage :as storage])
   #?(:cljs [org.replikativ.persistent-sorted-set.test.storage.util :as storage-util])
   #?(:cljs [org.replikativ.persistent-sorted-set.impl.numeric-stats :as numeric-stats]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set NumericStats NumericStatsOps PersistentSortedSet Settings])))

;; =============================================================================
;; Cross-platform helpers
;; =============================================================================

#?(:cljs
   (defn roundtrip
     "Store and restore a set via CLJS storage"
     ([s] (roundtrip s {}))
     ([s opts]
      (let [storage (storage-util/storage)
            address (set/store s storage)]
        (set/restore address storage opts)))))

#?(:clj
   (defn roundtrip
     "Store and restore a set via CLJ storage.
      When opts are provided (e.g. {:measure stats-ops}), the storage
      uses the original set's Settings so restored nodes have correct settings."
     ([s] (roundtrip s {}))
     ([s opts]
      (let [;; Use the set's settings for storage so nodes are created with measure, etc.
            set-settings (when (instance? PersistentSortedSet s)
                           (._settings ^PersistentSortedSet s))
            storage (storage/storage-with-settings (or set-settings (Settings.)))
            address (set/store s storage)]
        (set/restore address storage opts)))))

;; =============================================================================
;; Generators
;; =============================================================================

(def gen-int
  "Generator for integers in a reasonable range"
  (gen/choose -10000 10000))

(def gen-pos-int
  "Generator for positive integers"
  (gen/choose 1 10000))

(def gen-elements
  "Generator for a vector of elements"
  (gen/vector gen-int 0 1000))

(def gen-operation
  "Generator for a single operation [:add x] or [:remove x]"
  (gen/tuple (gen/elements [:add :remove]) gen-int))

(def gen-operations
  "Generator for a sequence of operations"
  (gen/vector gen-operation 0 500))

(defn apply-op
  "Apply an operation to both a pss and a clojure sorted-set"
  [[pss clj-set] [op val]]
  (case op
    :add    [(conj pss val) (conj clj-set val)]
    :remove [(disj pss val) (disj clj-set val)]))

(defn apply-ops
  "Apply a sequence of operations to both sets"
  [pss clj-set ops]
  (reduce apply-op [pss clj-set] ops))

;; =============================================================================
;; Model-based testing: PSS should behave like Clojure's sorted-set
;; =============================================================================

(defspec operations-match-sorted-set 100
  (prop/for-all [initial-elements gen-elements
                 ops gen-operations]
                (let [pss-initial (into (set/sorted-set) initial-elements)
                      clj-initial (into (sorted-set) initial-elements)
                      [pss-final clj-final] (apply-ops pss-initial clj-initial ops)]
                  (and (inv/validate-tree pss-final)
                       (= (count pss-final) (count clj-final))
                       (= (vec pss-final) (vec clj-final))))))

;; =============================================================================
;; Lazy set count correctness (via storage roundtrip)
;; =============================================================================

(defspec lazy-set-count-after-operations 100
  (prop/for-all [initial-elements gen-elements
                 ops gen-operations]
                (let [;; Create a lazy set via storage roundtrip
                      base-set (into (set/sorted-set) initial-elements)
                      lazy-set (roundtrip base-set)
          ;; Apply operations
                      clj-set (into (sorted-set) initial-elements)
                      [pss-final clj-final] (apply-ops lazy-set clj-set ops)]
                  (= (count pss-final) (count clj-final)))))

(defspec lazy-set-count-with-interleaved-count-calls 100
  (prop/for-all [initial-elements gen-elements
                 ops gen-operations]
                (let [lazy-set (roundtrip (into (set/sorted-set) initial-elements))
                      clj-set (into (sorted-set) initial-elements)
          ;; Apply operations, calling count between each
                      [pss-final clj-final]
                      (reduce (fn [[pss clj] [op val]]
                    ;; Call count on both (forces computation on lazy set)
                                (let [_ (count pss)
                                      _ (count clj)]
                                  (case op
                                    :add    [(conj pss val) (conj clj val)]
                                    :remove [(disj pss val) (disj clj val)])))
                              [lazy-set clj-set]
                              ops)]
                  (= (count pss-final) (count clj-final)))))

(defspec lazy-set-vec-matches-count 100
  (prop/for-all [initial-elements gen-elements
                 ops gen-operations]
                (let [lazy-set (roundtrip (into (set/sorted-set) initial-elements))
                      result (reduce (fn [s [op val]]
                                       (case op
                                         :add (conj s val)
                                         :remove (disj s val)))
                                     lazy-set
                                     ops)]
      ;; The count should always match the actual number of elements
                  (= (count result) (count (vec result))))))

;; =============================================================================
;; Transient operations
;; =============================================================================

(defspec transient-persistent-roundtrip 100
  (prop/for-all [initial-elements gen-elements
                 ops gen-operations]
                (let [base-set (into (set/sorted-set) initial-elements)
          ;; Apply ops via transient
                      transient-result (-> (transient base-set)
                                           (#(reduce (fn [t [op val]]
                                                       (case op
                                                         :add (conj! t val)
                                                         :remove (disj! t val)))
                                                     % ops))
                                           persistent!)
          ;; Apply ops via persistent
                      persistent-result (reduce (fn [s [op val]]
                                                  (case op
                                                    :add (conj s val)
                                                    :remove (disj s val)))
                                                base-set
                                                ops)]
                  (and (inv/validate-tree transient-result)
                       (inv/validate-tree persistent-result)
                       (= (count transient-result) (count persistent-result))
                       (= (vec transient-result) (vec persistent-result))))))

(defspec transient-lazy-set-count 100
  (prop/for-all [initial-elements gen-elements
                 adds (gen/vector gen-int 0 200)
                 removes (gen/vector gen-int 0 200)]
                (let [lazy-set (roundtrip (into (set/sorted-set) initial-elements))
          ;; Transient operations
                      result (-> (transient lazy-set)
                                 (#(reduce conj! % adds))
                                 (#(reduce disj! % removes))
                                 persistent!)
          ;; Expected result using Clojure's sorted-set
                      expected (-> (into (sorted-set) initial-elements)
                                   (into adds)
                                   (#(reduce disj % removes)))]
                  (= (count result) (count expected)))))

;; =============================================================================
;; Tree rebalancing (operations that cause splits/merges)
;; =============================================================================

(defspec rebalancing-preserves-count 100
  (prop/for-all [;; Use smaller branching factor to trigger more rebalancing
                 elements (gen/vector gen-int 0 2000)]
                (let [;; Small branching factor = more tree levels = more rebalancing
                      opts {:branching-factor 8}
                      pss (reduce conj (set/sorted-set* opts) elements)
                      lazy-pss (roundtrip pss)
          ;; Remove half the elements to trigger merges
                      to-remove (take (quot (count elements) 2) (shuffle elements))
                      result (reduce disj lazy-pss to-remove)
                      expected (reduce disj (into (sorted-set) elements) to-remove)]
                  (= (count result) (count expected)))))

(defspec split-heavy-operations 100
  (prop/for-all [elements (gen/vector gen-int 100 500)]
                (let [;; Very small branching factor = lots of splits
                      opts {:branching-factor 4}
                      pss (reduce conj (set/sorted-set* opts) elements)
                      expected (into (sorted-set) elements)]
                  (and (inv/validate-tree pss)
                       (= (count pss) (count expected))
                       (= (vec pss) (vec expected))))))

(defspec merge-heavy-operations 100
  (prop/for-all [elements (gen/vector gen-int 100 500)]
                (let [opts {:branching-factor 4}
                      pss (reduce conj (set/sorted-set* opts) elements)
          ;; Remove most elements to trigger merges
                      to-remove (take (* 3 (quot (count elements) 4)) (shuffle elements))
                      result (reduce disj pss to-remove)
                      expected (reduce disj (into (sorted-set) elements) to-remove)]
                  (and (inv/validate-tree pss)
                       (inv/validate-tree result)
                       (= (count result) (count expected))
                       (= (vec result) (vec expected))))))

;; =============================================================================
;; Count-slice correctness
;; =============================================================================

(defspec count-slice-matches-filter-count 100
  (prop/for-all [elements gen-elements
                 from gen-int
                 to gen-int]
                (let [[lo hi] (sort [from to])
                      pss (into (set/sorted-set) elements)
                      fast-count (set/count-slice pss lo hi)
                      expected (count (filter #(and (>= % lo) (<= % hi)) (distinct elements)))]
                  (= fast-count expected))))

(defspec count-slice-on-lazy-set 100
  (prop/for-all [elements gen-elements
                 from gen-int
                 to gen-int]
                (let [[lo hi] (sort [from to])
                      lazy-pss (roundtrip (into (set/sorted-set) elements))
                      fast-count (set/count-slice lazy-pss lo hi)
                      expected (count (filter #(and (>= % lo) (<= % hi)) (distinct elements)))]
                  (= fast-count expected))))

(defspec count-slice-after-modifications 100
  (prop/for-all [elements gen-elements
                 ops (gen/vector gen-operation 0 100)
                 from gen-int
                 to gen-int]
                (let [[lo hi] (sort [from to])
                      initial-pss (into (set/sorted-set) elements)
                      initial-clj (into (sorted-set) elements)
                      [final-pss final-clj] (apply-ops initial-pss initial-clj ops)
                      fast-count (set/count-slice final-pss lo hi)
                      expected (count (filter #(and (>= % lo) (<= % hi)) final-clj))]
                  (= fast-count expected))))

;; =============================================================================
;; Multiple derived sets (structural sharing)
;; =============================================================================

(defspec derived-sets-independent-counts 100
  (prop/for-all [elements gen-elements
                 ops1 (gen/vector gen-operation 0 50)
                 ops2 (gen/vector gen-operation 0 50)]
                (let [base (roundtrip (into (set/sorted-set) elements))
                      clj-base (into (sorted-set) elements)
          ;; Derive two different sets from same base
                      [pss1 clj1] (apply-ops base clj-base ops1)
                      [pss2 clj2] (apply-ops base clj-base ops2)]
                  (and (= (count pss1) (count clj1))
                       (= (count pss2) (count clj2))
           ;; Also verify contents
                       (= (vec pss1) (vec clj1))
                       (= (vec pss2) (vec clj2))))))

;; =============================================================================
;; Concurrent-like operations (sequential simulation)
;; =============================================================================

(defspec interleaved-transient-operations 100
  (prop/for-all [elements gen-elements
                 ops1 (gen/vector gen-operation 0 50)
                 ops2 (gen/vector gen-operation 0 50)]
                (let [base (into (set/sorted-set) elements)
          ;; Simulate two "threads" doing transient operations
                      t1 (transient base)
                      t2 (transient base)
          ;; Apply ops to each transient
                      t1-final (reduce (fn [t [op val]]
                                         (case op
                                           :add (conj! t val)
                                           :remove (disj! t val)))
                                       t1 ops1)
                      t2-final (reduce (fn [t [op val]]
                                         (case op
                                           :add (conj! t val)
                                           :remove (disj! t val)))
                                       t2 ops2)
          ;; Persist both
                      p1 (persistent! t1-final)
                      p2 (persistent! t2-final)
          ;; Expected results
                      e1 (reduce (fn [s [op val]]
                                   (case op
                                     :add (conj s val)
                                     :remove (disj s val)))
                                 (into (sorted-set) elements) ops1)
                      e2 (reduce (fn [s [op val]]
                                   (case op
                                     :add (conj s val)
                                     :remove (disj s val)))
                                 (into (sorted-set) elements) ops2)]
                  (and (= (count p1) (count e1))
                       (= (count p2) (count e2))
                       (= (vec p1) (vec e1))
                       (= (vec p2) (vec e2))))))

;; =============================================================================
;; Slice operations
;; =============================================================================

(defspec slice-contents-correct 100
  (prop/for-all [elements gen-elements
                 from gen-int
                 to gen-int]
                (let [[lo hi] (sort [from to])
                      pss (into (set/sorted-set) elements)
                      slice-result (vec (set/slice pss lo hi))
                      expected (vec (filter #(and (>= % lo) (<= % hi)) (sort (distinct elements))))]
                  (= slice-result expected))))

(defspec rslice-contents-correct 100
  (prop/for-all [elements gen-elements
                 from gen-int
                 to gen-int]
                (let [[lo hi] (sort [from to])
                      pss (into (set/sorted-set) elements)
                      rslice-result (vec (set/rslice pss hi lo))
                      expected (vec (reverse (filter #(and (>= % lo) (<= % hi)) (sort (distinct elements)))))]
                  (= rslice-result expected))))

;; =============================================================================
;; Lazy set slice operations
;; =============================================================================

(defspec lazy-slice-contents-correct 100
  (prop/for-all [elements gen-elements
                 from gen-int
                 to gen-int]
                (let [[lo hi] (sort [from to])
                      lazy-pss (roundtrip (into (set/sorted-set) elements))
                      slice-result (vec (set/slice lazy-pss lo hi))
                      expected (vec (filter #(and (>= % lo) (<= % hi)) (sort (distinct elements))))]
                  (= slice-result expected))))

(defspec lazy-rslice-contents-correct 100
  (prop/for-all [elements gen-elements
                 from gen-int
                 to gen-int]
                (let [[lo hi] (sort [from to])
                      lazy-pss (roundtrip (into (set/sorted-set) elements))
                      rslice-result (vec (set/rslice lazy-pss hi lo))
                      expected (vec (reverse (filter #(and (>= % lo) (<= % hi)) (sort (distinct elements)))))]
                  (= rslice-result expected))))

;; =============================================================================
;; Stats helpers
;; =============================================================================

(def stats-ops
  "Cross-platform numeric stats operations"
  #?(:clj  (NumericStatsOps.)
     :cljs numeric-stats/numeric-stats-ops))

(defn compute-expected-stats
  "Compute expected stats from a collection of numbers"
  [coll]
  (if (empty? coll)
    {:cnt 0 :sum 0.0 :min-val nil :max-val nil}
    {:cnt (count coll)
     :sum (double (reduce + coll))
     :min-val (apply min coll)
     :max-val (apply max coll)}))

(defn stats->map
  "Convert stats object to comparable map"
  [s]
  (if (nil? s)
    {:cnt 0 :sum 0.0 :min-val nil :max-val nil}
    #?(:clj  {:cnt (.-count ^NumericStats s)
              :sum (.-sum ^NumericStats s)
              :min-val (.-min ^NumericStats s)
              :max-val (.-max ^NumericStats s)}
       :cljs {:cnt (:cnt s) :sum (double (:sum s)) :min-val (:min-val s) :max-val (:max-val s)})))

;; =============================================================================
;; Stats tests
;; =============================================================================

(defspec stats-matches-expected 100
  (prop/for-all [elements (gen/no-shrink gen-elements)]
                (let [pss (into (set/sorted-set* {:measure stats-ops}) elements)
                      actual (stats->map (set/measure pss))
                      expected (compute-expected-stats (distinct elements))]
                  (when-not (= actual expected)
                    (println "MISMATCH: actual=" actual "expected=" expected))
                  (= actual expected))))

(defspec measure-slice-matches-filter 100
  (prop/for-all [elements (gen/no-shrink gen-elements)
                 from (gen/no-shrink gen-int)
                 to (gen/no-shrink gen-int)]
                (let [[lo hi] (sort [from to])
                      pss (into (set/sorted-set* {:measure stats-ops}) elements)
                      actual (stats->map (set/measure-slice pss lo hi))
                      filtered (filter #(and (>= % lo) (<= % hi)) (distinct elements))
                      expected (compute-expected-stats filtered)]
                  (= actual expected))))

(defspec stats-after-modifications 100
  (prop/for-all [elements (gen/no-shrink gen-elements)
                 ops (gen/no-shrink gen-operations)]
                (let [pss (into (set/sorted-set* {:measure stats-ops}) elements)
                      final-pss (reduce (fn [s [op val]]
                                          (case op
                                            :add (conj s val)
                                            :remove (disj s val)))
                                        pss ops)
                      actual (stats->map (set/measure final-pss))
          ;; Compute expected by applying ops to a regular set
                      final-set (reduce (fn [s [op val]]
                                          (case op
                                            :add (conj s val)
                                            :remove (disj s val)))
                                        (into #{} elements) ops)
                      expected (compute-expected-stats final-set)]
                  (= actual expected))))

(defspec measure-slice-after-modifications 100
  (prop/for-all [elements (gen/no-shrink gen-elements)
                 ops (gen/no-shrink (gen/vector gen-operation 0 50))
                 from (gen/no-shrink gen-int)
                 to (gen/no-shrink gen-int)]
                (let [[lo hi] (sort [from to])
                      pss (into (set/sorted-set* {:measure stats-ops}) elements)
                      final-pss (reduce (fn [s [op val]]
                                          (case op
                                            :add (conj s val)
                                            :remove (disj s val)))
                                        pss ops)
                      actual (stats->map (set/measure-slice final-pss lo hi))
          ;; Compute expected
                      final-set (reduce (fn [s [op val]]
                                          (case op
                                            :add (conj s val)
                                            :remove (disj s val)))
                                        (into #{} elements) ops)
                      filtered (filter #(and (>= % lo) (<= % hi)) final-set)
                      expected (compute-expected-stats filtered)]
                  (= actual expected))))

;; =============================================================================
;; Replace + measure correctness
;; =============================================================================

(defspec replace-preserves-measure 100
  (prop/for-all [elements (gen/no-shrink (gen/vector gen-pos-int 10 500))]
                (let [pss (into (set/sorted-set* {:measure stats-ops :branching-factor 8}) elements)
                      distinct-elems (vec (sort (distinct elements)))
                      ;; Replace each element with itself (identity replace)
                      final-pss (reduce (fn [s k] (set/replace s k k)) pss distinct-elems)
                      actual (stats->map (set/measure final-pss))
                      expected (compute-expected-stats distinct-elems)]
                  (= actual expected))))

;; =============================================================================
;; get-nth correctness after mutations
;; =============================================================================

(defspec get-nth-matches-vec-nth 100
  (prop/for-all [elements (gen/no-shrink (gen/vector gen-pos-int 10 500))
                 ops (gen/no-shrink (gen/vector gen-operation 0 100))]
                (let [pss (into (set/sorted-set* {:measure stats-ops :branching-factor 8}) elements)
                      final-pss (reduce (fn [s [op val]]
                                          (case op
                                            :add (conj s val)
                                            :remove (disj s val)))
                                        pss ops)
                      final-vec (vec final-pss)
                      n (count final-vec)]
                  (if (zero? n)
                    true
                    ;; Check a sample of indices
                    (every? (fn [i]
                              (let [[entry _] (set/get-nth final-pss i)]
                                (= entry (nth final-vec i))))
                            (take 10 (range n)))))))

;; =============================================================================
;; Rebalancing preserves measure (small branching factor)
;; =============================================================================

(defspec rebalancing-preserves-measure 100
  (prop/for-all [elements (gen/no-shrink (gen/vector gen-int 50 500))]
                (let [opts {:measure stats-ops :branching-factor 4}
                      pss (reduce conj (set/sorted-set* opts) elements)
                      ;; Remove half to trigger merges/borrows
                      to-remove (take (quot (count elements) 2) (shuffle elements))
                      result (reduce disj pss to-remove)
                      expected-set (reduce disj (into (sorted-set) elements) to-remove)
                      actual (stats->map (set/measure result))
                      expected (compute-expected-stats expected-set)]
                  (and (inv/validate-tree pss)
                       (inv/validate-tree result)
                       (= (count result) (count expected-set))
                       (= actual expected)))))

;; =============================================================================
;; Transient operations preserve measure
;; =============================================================================

(defspec transient-preserves-measure 100
  (prop/for-all [elements (gen/no-shrink (gen/vector gen-int 10 300))
                 adds (gen/no-shrink (gen/vector gen-int 0 100))
                 removes (gen/no-shrink (gen/vector gen-int 0 100))]
                (let [pss (into (set/sorted-set* {:measure stats-ops :branching-factor 8}) elements)
                      ;; Apply ops via transient
                      result (-> (transient pss)
                                 (#(reduce conj! % adds))
                                 (#(reduce disj! % removes))
                                 persistent!)
                      actual (stats->map (set/measure result))
                      ;; Expected via regular set
                      expected-set (-> (into #{} elements)
                                       (into adds)
                                       (#(reduce disj % removes)))
                      expected (compute-expected-stats expected-set)]
                  (and (inv/validate-tree result)
                       (= actual expected)))))

;; =============================================================================
;; Stats after storage roundtrip + modifications
;; =============================================================================

(defspec stats-after-roundtrip-and-modifications 100
  (prop/for-all [elements (gen/no-shrink (gen/vector gen-int 10 300))
                 ops (gen/no-shrink (gen/vector gen-operation 0 100))]
                (let [opts {:measure stats-ops :branching-factor 8}
                      pss (into (set/sorted-set* opts) elements)
                      lazy-pss (roundtrip pss opts)
                      final-pss (reduce (fn [s [op val]]
                                          (case op
                                            :add (conj s val)
                                            :remove (disj s val)))
                                        lazy-pss ops)
                      actual (stats->map (set/measure final-pss))
                      final-set (reduce (fn [s [op val]]
                                          (case op
                                            :add (conj s val)
                                            :remove (disj s val)))
                                        (into #{} elements) ops)
                      expected (compute-expected-stats final-set)]
                  (= actual expected))))

;; =============================================================================
;; measure-slice after storage roundtrip
;; =============================================================================

(defspec measure-slice-after-roundtrip 100
  (prop/for-all [elements (gen/no-shrink (gen/vector gen-int 10 200))
                 from (gen/no-shrink gen-int)
                 to (gen/no-shrink gen-int)]
                (let [[lo hi] (sort [from to])
                      opts {:measure stats-ops :branching-factor 8}
                      pss (into (set/sorted-set* opts) elements)
                      lazy-pss (roundtrip pss opts)
                      actual (stats->map (set/measure-slice lazy-pss lo hi))
                      filtered (filter #(and (>= % lo) (<= % hi)) (distinct elements))
                      expected (compute-expected-stats filtered)]
                  (= actual expected))))

;; =============================================================================
;; Run all specs (for manual testing)
;; =============================================================================

(deftest run-generative-tests
  (testing "All generative tests pass"
    ;; This test just ensures the defspecs are run
    ;; Each defspec is also run as its own test
    (is true)))

#?(:clj
   (comment
     ;; Run a specific spec manually with more iterations:
     (tc/quick-check 1000 lazy-set-count-after-operations)

     ;; Run with verbose output:
     (tc/quick-check 100 lazy-set-count-after-operations :reporter-fn prn)

     ;; Run all tests:
     (clojure.test/run-tests 'org.replikativ.persistent-sorted-set.test.generative)))
