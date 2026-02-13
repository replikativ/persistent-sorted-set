(ns org.replikativ.persistent-sorted-set.diagnostics
  "Diagnostic API for B-tree structural integrity checking.

   Similar to PostgreSQL's amcheck or SQLite's PRAGMA integrity_check,
   this namespace provides functions to validate the internal structure
   of persistent sorted sets.

   Use `validate` for quick structural checks, `validate-full` to also
   verify subtree counts and measures, and `validate-content` to run
   user-defined content checks on leaf data (e.g., for Datahike index
   consistency verification)."
  (:require
   [clojure.string :as str]
   [org.replikativ.persistent-sorted-set :as set]
   #?(:cljs [org.replikativ.persistent-sorted-set.branch :refer [Branch]])
   #?(:cljs [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]]))
  #?(:clj
     (:import
      [java.util Comparator]
      [org.replikativ.persistent_sorted_set ANode Branch Leaf PersistentSortedSet Settings])))

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

(defn- get-settings [set]
  #?(:clj  (.-_settings ^PersistentSortedSet set)
     :cljs (.-settings set)))

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

(defn- node-measure [node]
  #?(:clj  (.-_measure ^ANode node)
     :cljs (.-_measure node)))

(defn- leaf-keys-array [node]
  #?(:clj  (.-_keys ^ANode node)
     :cljs (.-keys node)))

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
;; Invariant 9: All subtree counts known (non-deterioration check)
;; =============================================================================

(defn- check-all-counts-known [node]
  (if (leaf? node) []
      (let [sc (subtree-count* node)
            errors (if (< sc 0)
                     [{:error :unknown-subtree-count :level (nlevel node)}]
                     [])]
        (reduce into errors
                (map #(when % (check-all-counts-known %)) (children-seq node))))))

;; =============================================================================
;; Invariant 10: All measures known (non-deterioration check)
;; =============================================================================

(defn- check-all-measures-known [node has-measure?]
  (when has-measure?
    (let [m (node-measure node)
          errors (if (nil? m)
                   [{:error :unknown-measure :level (nlevel node)
                     :node-type (if (branch? node) :branch :leaf)}]
                   [])]
      (if (branch? node)
        (reduce into errors
                (map #(when % (check-all-measures-known % true)) (children-seq node)))
        errors))))

;; =============================================================================
;; Tree statistics
;; =============================================================================

(defn- collect-stats [node depth]
  (if (leaf? node)
    {:depth depth
     :branch-count 0
     :leaf-count 1
     :element-count (nlen node)
     :min-fill (nlen node)
     :max-fill (nlen node)
     :total-fill (nlen node)
     :node-count 1
     :counts-known? true
     :measure-known? (some? (node-measure node))
     :leaf-fills [(nlen node)]
     :branch-fills []}
    (let [cs (children-seq node)
          child-stats (map #(when % (collect-stats % (inc depth))) cs)
          child-stats (remove nil? child-stats)]
      (reduce (fn [acc s]
                {:depth (max (:depth acc) (:depth s))
                 :branch-count (+ (:branch-count acc) (:branch-count s))
                 :leaf-count (+ (:leaf-count acc) (:leaf-count s))
                 :element-count (+ (:element-count acc) (:element-count s))
                 :min-fill (min (:min-fill acc) (:min-fill s))
                 :max-fill (max (:max-fill acc) (:max-fill s))
                 :total-fill (+ (:total-fill acc) (:total-fill s))
                 :node-count (+ (:node-count acc) (:node-count s))
                 :counts-known? (and (:counts-known? acc) (:counts-known? s))
                 :measure-known? (and (:measure-known? acc) (:measure-known? s))
                 :leaf-fills (clojure.core/into (:leaf-fills acc) (:leaf-fills s))
                 :branch-fills (clojure.core/into (:branch-fills acc) (:branch-fills s))})
              {:depth depth
               :branch-count 1
               :leaf-count 0
               :element-count 0
               :min-fill (nlen node)
               :max-fill (nlen node)
               :total-fill (nlen node)
               :node-count 1
               :counts-known? (>= (subtree-count* node) 0)
               :measure-known? (some? (node-measure node))
               :leaf-fills []
               :branch-fills [(nlen node)]}
              child-stats))))

(defn- percentiles
  "Compute percentile distribution from a sorted vector of values, normalized by bf."
  [fills bf]
  (if (empty? fills)
    {:min 0.0 :p25 0.0 :p50 0.0 :p75 0.0 :p90 0.0 :max 0.0}
    (let [sorted (vec (sort fills))
          n (count sorted)
          at (fn [p] (/ (double (nth sorted (min (int (* n p)) (dec n)))) bf))]
      {:min (/ (double (first sorted)) bf)
       :p25 (at 0.25)
       :p50 (at 0.50)
       :p75 (at 0.75)
       :p90 (at 0.90)
       :max (/ (double (peek sorted)) bf)})))

;; =============================================================================
;; Error formatting
;; =============================================================================

(defn- throw-errors [errors context]
  (when (seq errors)
    (throw (#?(:clj AssertionError. :cljs js/Error.)
            (str context " (" (count errors) " violations):\n"
                 (str/join "\n" (map pr-str (take 10 errors))))))))

;; =============================================================================
;; Public API
;; =============================================================================

(defn validate
  "Quick structural integrity check. Returns true if valid, throws with
   structured error data if any invariant is violated.

   Checks: balance, node sizes, key ordering, separator keys, levels,
   sibling ordering, root shape."
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
                    (check-root-shape root)
                    (check-levels root)
                    (check-sibling-ordering root cmp))]
        (throw-errors errors "B-tree structural invariant violations")
        true))))

(defn validate-full
  "Full integrity check including subtree counts and measures.
   Returns true if valid, throws with structured error data."
  [set]
  (validate set)
  (let [root (get-root set)]
    (when (and root (pos? (nlen root)))
      (let [errors (concat
                    (check-subtree-counts root))]
        (throw-errors errors "B-tree count/measure invariant violations")))
    true))

(defn validate-counts-known
  "Verify every branch in the tree has a known subtree count (>= 0).
   For fresh (non-restored) trees, counts should never be -1.
   Returns true if valid, throws with error data."
  [set]
  (let [root (get-root set)]
    (when (and root (pos? (nlen root)))
      (let [errors (check-all-counts-known root)]
        (throw-errors errors "Subtree count deterioration detected")))
    true))

(defn validate-measures-known
  "Verify every node in the tree has a known measure (non-nil).
   Only meaningful when the set was created with a measure function.
   Returns true if valid, throws with error data."
  [set]
  (let [root (get-root set)
        settings (get-settings set)
        has-measure? #?(:clj  (some? (.measure ^Settings settings))
                        :cljs (some? (:measure settings)))]
    (when (and root (pos? (nlen root)) has-measure?)
      (let [errors (check-all-measures-known root true)]
        (throw-errors errors "Measure deterioration detected")))
    true))

(defn validate-content
  "Full integrity check with content verification. Calls (content-fn keys)
   for each leaf's key array, where content-fn should return nil if valid
   or a map describing the error. For Datahike: verify each datom exists
   in the expected table/index.

   Runs validate-full first, then walks all leaves."
  [set content-fn]
  (validate-full set)
  (let [root (get-root set)]
    (when (and root (pos? (nlen root)))
      (letfn [(walk-leaves [node]
                (if (leaf? node)
                  (let [ks (leaf-keys-array node)
                        error (content-fn ks)]
                    (when error
                      [error]))
                  (reduce into []
                          (map #(when % (walk-leaves %)) (children-seq node)))))]
        (let [errors (walk-leaves root)]
          (throw-errors errors "Content validation failed"))))
    true))

;; =============================================================================
;; Root-descend verification
;; =============================================================================

(defn- collect-all-keys
  "Walk tree collecting all leaf keys into a vector."
  [node]
  (if (leaf? node)
    (nkeys node)
    (into [] (mapcat #(when % (collect-all-keys %))) (children-seq node))))

(defn validate-navigation
  "Root-descend verification: re-looks up every element from the root to verify
   the search path is correct. Catches subtle comparator bugs, collation changes
   after restore, and navigation corruption that structural checks miss.

   O(n log n) — diagnostic, not a hot path. Returns true if valid, throws with
   structured error data if any key cannot be found via lookup."
  [set]
  (let [root (get-root set)]
    (when (and root (pos? (nlen root)))
      (let [all-keys (collect-all-keys root)
            errors (reduce
                    (fn [errs key]
                      (let [found (set/lookup set key)]
                        (if (nil? found)
                          (clojure.core/conj errs {:error :key-not-found-via-lookup :key key})
                          errs)))
                    []
                    all-keys)]
        (throw-errors errors "Root-descend navigation verification failed")))
    true))

(defn tree-stats
  "Returns diagnostic statistics about the tree structure:
   {:depth N, :branch-count N, :leaf-count N, :element-count N,
    :min-fill-ratio F, :avg-fill-ratio F, :max-fill-ratio F,
    :fill-histogram {:min F :p25 F :p50 F :p75 F :p90 F :max F},
    :leaf-fill-histogram {...}, :branch-fill-histogram {...},
    :counts-known? bool, :measure-known? bool}"
  [set]
  (let [root (get-root set)
        bf (get-bf set)]
    (if (or (nil? root) (zero? (nlen root)))
      {:depth 0 :branch-count 0 :leaf-count 0 :element-count 0
       :min-fill-ratio 0.0 :avg-fill-ratio 0.0 :max-fill-ratio 0.0
       :fill-histogram {:min 0.0 :p25 0.0 :p50 0.0 :p75 0.0 :p90 0.0 :max 0.0}
       :leaf-fill-histogram {:min 0.0 :p25 0.0 :p50 0.0 :p75 0.0 :p90 0.0 :max 0.0}
       :branch-fill-histogram {:min 0.0 :p25 0.0 :p50 0.0 :p75 0.0 :p90 0.0 :max 0.0}
       :counts-known? true :measure-known? true}
      (let [stats (collect-stats root 0)
            all-fills (clojure.core/into (:leaf-fills stats) (:branch-fills stats))]
        {:depth (:depth stats)
         :branch-count (:branch-count stats)
         :leaf-count (:leaf-count stats)
         :element-count (:element-count stats)
         :min-fill-ratio (/ (double (:min-fill stats)) bf)
         :avg-fill-ratio (/ (double (:total-fill stats)) (* (:node-count stats) bf))
         :max-fill-ratio (/ (double (:max-fill stats)) bf)
         :fill-histogram (percentiles all-fills bf)
         :leaf-fill-histogram (percentiles (:leaf-fills stats) bf)
         :branch-fill-histogram (percentiles (:branch-fills stats) bf)
         :counts-known? (:counts-known? stats)
         :measure-known? (:measure-known? stats)}))))
