(ns me.tonsky.persistent-sorted-set.test.invariants
  "Structural invariant checks for the B-tree implementation.
   Walks the tree to verify shape, ordering, and aggregate consistency
   after arbitrary operations.

   The `validate-tree` function is public and intended to be called from
   other test namespaces (e.g. generative.cljc) to ensure invariants hold
   throughout all property-based tests."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop #?@(:cljs [:include-macros true])]
   [clojure.test.check.clojure-test :refer [defspec] #?@(:cljs [:include-macros true])]
   [me.tonsky.persistent-sorted-set :as set]
   #?(:cljs [me.tonsky.persistent-sorted-set.branch :refer [Branch]])
   #?(:cljs [me.tonsky.persistent-sorted-set.leaf :refer [Leaf]]))
  #?(:clj
     (:import
      [java.util Comparator]
      [me.tonsky.persistent_sorted_set ANode Branch Leaf PersistentSortedSet Settings])))

;; =============================================================================
;; Platform-specific node access
;; =============================================================================

(defn- get-root [set]
  #?(:clj  (.root ^PersistentSortedSet set)
     :cljs (.-root set)))

(defn- get-cmp
  "Returns a callable comparator (IFn) for the set."
  [set]
  #?(:clj  (let [^Comparator c (.comparator ^PersistentSortedSet set)]
             (fn [a b] (.compare c a b)))
     :cljs (.-comparator set)))

(defn- get-bf [set]
  #?(:clj  (.-_branchingFactor (.-_settings ^PersistentSortedSet set))
     :cljs (:branching-factor (.-settings set))))

(defn- branch? [node] (instance? Branch node))
(defn- leaf? [node] (instance? Leaf node))

(defn- nlevel [node]
  #?(:clj  (.level ^ANode node)
     :cljs (if (instance? Branch node) (.-level node) 0)))

(defn- nlen [node]
  #?(:clj  (.-_len ^ANode node)
     :cljs (alength (.-keys node))))

(defn- nkey [node i]
  #?(:clj  (aget ^objects (.-_keys ^ANode node) i)
     :cljs (aget (.-keys node) i)))

(defn- nkeys [node]
  (mapv #(nkey node %) (range (nlen node))))

(defn- child-node
  "Get the i-th child of a branch, dereferencing SoftReference if needed.
   Returns nil if child is not in memory."
  [node i]
  #?(:clj
     (let [^Branch b node
           children (.-_children b)]
       (when children
         (let [ref (aget ^objects children i)]
           (when ref
             (let [settings (.-_settings ^ANode b)]
               (.readReference ^Settings settings ref))))))
     :cljs
     (when-let [children (.-children node)]
       (aget children i))))

(defn- children-seq [node]
  (mapv #(child-node node %) (range (nlen node))))

(defn- subtree-count* [node]
  #?(:clj
     (if (instance? Branch node)
       (.-_subtreeCount ^Branch node)
       (long (.-_len ^Leaf node)))
     :cljs
     (if (instance? Branch node)
       (.-subtree-count node)
       (alength (.-keys node)))))

;; =============================================================================
;; Invariant 1: Balanced tree (all leaves at same depth)
;; =============================================================================

(defn- collect-leaf-depths [node depth]
  (if (leaf? node)
    [depth]
    (into [] (mapcat #(when % (collect-leaf-depths % (inc depth))))
          (children-seq node))))

(defn- check-balance [root]
  (let [depths (set (collect-leaf-depths root 0))]
    (when (> (count depths) 1)
      [{:error :unbalanced-tree :leaf-depths depths}])))

;; =============================================================================
;; Invariant 2: Node sizes within [B/2, B] (except root)
;; =============================================================================

(defn- check-sizes [node bf root?]
  (let [n (nlen node)
        min-bf (quot bf 2)
        errors (cond
                 root? []
                 (< n min-bf) [{:error :node-too-small :level (nlevel node) :len n :min min-bf}]
                 (> n bf) [{:error :node-too-large :level (nlevel node) :len n :max bf}]
                 :else [])]
    (if (branch? node)
      (reduce into errors
              (map #(when % (check-sizes % bf false)) (children-seq node)))
      errors)))

;; =============================================================================
;; Invariant 3: Keys strictly sorted within each node
;; =============================================================================

(defn- check-ordering [node cmp]
  (let [ks (nkeys node)
        n (count ks)
        errors (loop [i 1 errs []]
                 (if (>= i n) errs
                     (let [prev (nth ks (dec i))
                           curr (nth ks i)
                           c (cmp prev curr)]
                       ;; For a set, keys must be strictly increasing
                       (if (>= c 0)
                         (recur (inc i)
                                (conj errs {:error :not-strictly-sorted
                                            :level (nlevel node)
                                            :i (dec i) :prev prev :curr curr}))
                         (recur (inc i) errs)))))]
    (if (branch? node)
      (reduce into errors
              (map #(when % (check-ordering % cmp)) (children-seq node)))
      errors)))

;; =============================================================================
;; Invariant 4: Branch separator keys = max key of each child
;; =============================================================================

(defn- check-separators [node cmp]
  (if (leaf? node) []
      (let [cs (children-seq node)
            n (nlen node)
            errors (loop [i 0 errs []]
                     (if (>= i n) errs
                         (let [child (nth cs i nil)]
                           (if (or (nil? child) (zero? (nlen child)))
                             (recur (inc i) errs)
                             (let [sep (nkey node i)
                                   child-max (nkey child (dec (nlen child)))]
                               (if (not= 0 (cmp sep child-max))
                                 (recur (inc i)
                                        (conj errs {:error :separator-mismatch
                                                    :level (nlevel node) :i i
                                                    :separator sep :child-max child-max}))
                                 (recur (inc i) errs)))))))]
        (reduce into errors
                (map #(when % (check-separators % cmp)) cs)))))

;; =============================================================================
;; Invariant 5: Subtree counts consistent
;; =============================================================================

(defn- check-subtree-counts [node]
  (if (leaf? node)
    (let [sc (subtree-count* node)
          n (nlen node)]
      (if (not= sc n)
        [{:error :leaf-count-mismatch :count sc :actual-keys n}]
        []))
    (let [sc (subtree-count* node)
          cs (children-seq node)
          child-counts (mapv #(if % (subtree-count* %) 0) cs)
          all-known? (every? #(>= % 0) child-counts)]
      (into
       (cond
         ;; Known count, all children known: verify sum
         (and (>= sc 0) all-known?)
         (let [expected (reduce + 0 child-counts)]
           (if (not= sc expected)
             [{:error :subtree-count-mismatch
               :level (nlevel node) :branch-count sc :children-sum expected}]
             []))

         ;; Known count but some child unknown: violation
         (and (>= sc 0) (not all-known?))
         [{:error :count-known-child-unknown
           :level (nlevel node) :branch-count sc :child-counts child-counts}]

         ;; Branch has -1: fine (lazy / post-split)
         :else [])
       (reduce into [] (map #(when % (check-subtree-counts %)) cs))))))

;; =============================================================================
;; Invariant 6: Root shape (branch root should have >= 2 children)
;; =============================================================================

(defn- check-root-shape [root]
  (when (and (branch? root) (< (nlen root) 2) (> (nlen root) 0))
    [{:error :branch-root-with-one-child :len (nlen root)}]))

;; =============================================================================
;; Invariant 7: Level consistency (branches at level L have children at level L-1)
;; =============================================================================

(defn- check-levels [node]
  (if (leaf? node)
    (when (not= 0 (nlevel node))
      [{:error :leaf-not-level-zero :level (nlevel node)}])
    (let [expected-child-level (dec (nlevel node))
          cs (children-seq node)
          errors (loop [i 0 errs []]
                   (if (>= i (count cs)) errs
                       (let [child (nth cs i nil)]
                         (if (nil? child)
                           (recur (inc i) errs)
                           (if (not= (nlevel child) expected-child-level)
                             (recur (inc i)
                                    (conj errs {:error :level-mismatch
                                                :parent-level (nlevel node)
                                                :child-index i
                                                :child-level (nlevel child)
                                                :expected expected-child-level}))
                             (recur (inc i) errs))))))]
      (reduce into errors
              (map #(when % (check-levels %)) cs)))))

;; =============================================================================
;; Invariant 8: Inter-node key ordering (no overlap between siblings)
;; =============================================================================

(defn- check-sibling-ordering [node cmp]
  (if (leaf? node) []
      (let [cs (children-seq node)
            n (count cs)
            errors (loop [i 0 errs []]
                     (if (>= i (dec n)) errs
                         (let [left (nth cs i nil)
                               right (nth cs (inc i) nil)]
                           (if (or (nil? left) (nil? right)
                                   (zero? (nlen left)) (zero? (nlen right)))
                             (recur (inc i) errs)
                             (let [left-max (nkey left (dec (nlen left)))
                                   right-min (nkey right 0)]
                               (if (>= (cmp left-max right-min) 0)
                                 (recur (inc i)
                                        (conj errs {:error :sibling-key-overlap
                                                    :level (nlevel node) :i i
                                                    :left-max left-max :right-min right-min}))
                                 (recur (inc i) errs)))))))]
        (reduce into errors
                (map #(when % (check-sibling-ordering % cmp)) cs)))))

;; =============================================================================
;; Main validation function (PUBLIC - used by other test namespaces)
;; =============================================================================

(defn validate-tree
  "Validate all B-tree structural invariants. Returns true if valid.
   Throws with details if any invariant is violated.

   Checks:
   1. Balanced: all leaves at same depth
   2. Node sizes: non-root nodes have [B/2, B] keys
   3. Key ordering: strictly increasing within each node
   4. Separators: branch keys[i] = max-key(children[i])
   5. Subtree counts: when known, must equal sum of children
   6. Root shape: branch root has >= 2 children
   7. Level consistency: branches at level L have children at L-1
   8. Sibling ordering: no key overlap between adjacent children"
  [set]
  (let [root (get-root set)
        cmp (get-cmp set)
        bf (get-bf set)]
    (if (or (nil? root) (zero? (nlen root)))
      true
      (let [errors (concat
                    (check-balance root)
                    (check-sizes root bf true)
                    (check-ordering root cmp)
                    (check-separators root cmp)
                    (check-subtree-counts root)
                    (check-root-shape root)
                    (check-levels root)
                    (check-sibling-ordering root cmp))]
        (when (seq errors)
          (throw (#?(:clj AssertionError. :cljs js/Error.)
                  (str "B-tree invariant violations (" (count errors) "):\n"
                       (str/join "\n" (map pr-str (take 10 errors)))))))
        true))))

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
                              [pss clj-set] ops)
                      root (get-root final-pss)
                      sc (when root (subtree-count* root))]
                  ;; If subtree count is known, it must match actual count
                  (if (and sc (>= sc 0))
                    (= sc (count final-clj))
                    true))))

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
