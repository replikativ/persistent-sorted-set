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
   #?(:cljs [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]])
   #?(:cljs [org.replikativ.persistent-sorted-set.impl.measure :as measure]))
  #?(:clj
     (:import
      [java.util Comparator]
      [org.replikativ.persistent_sorted_set ANode Branch Leaf IMeasure PersistentSortedSet
       Settings])))

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
           children (.childrenArray b)]  ; one snapshot; read-only
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
    ;; A NON-RESIDENT child (nil) is not evidence of anything: its count lives in its own
    ;; blob, and this node's count came from this node's blob. That is the ordinary state of
    ;; every lazily-restored tree.
    ;;
    ;; It used to be mapped to the number 0, so `all-known?` stayed true and the branch's real
    ;; count was compared against a sum of zeros. Every healthy cold-restored tree failed:
    ;;
    ;;     bf  8  n  200   validate true   validate-full :subtree-count-mismatch
    ;;                                     {:branch-count 200, :children-sum 0}
    ;;     bf  8  n 5000                   {:branch-count 5000, :children-sum 0}
    ;;     bf 64  n  200 / 5000            same
    ;;
    ;; Nothing in the suite hit it because the in-tree test storage does not persist
    ;; `:subtree-count`, so its restored branches carry -1 and the check skipped itself —
    ;; the whole-tree oracle could not be run against the code path that most needs it.
    ;;
    ;; Mapping a missing child to -1 instead is NOT enough: that lands in the
    ;; "known count, some child unknown" arm, which reported a different false positive on
    ;; the same trees. A parent legitimately knows a count whose children are not loaded, and
    ;; a resident Branch legitimately carries -1 ("lazy / post-split"), so neither case is a
    ;; violation — both simply mean the sum CANNOT BE VERIFIED here. Skip, and keep
    ;; descending into whichever children are resident.
    (let [sc (subtree-count* node)
          cs (children-seq node)
          all-resident? (every? some? cs)
          child-counts (mapv #(if % (subtree-count* %) -1) cs)
          all-known? (every? #(>= % 0) child-counts)]
      (into
       (cond
         ;; Known count, every child resident AND self-describing: verify the sum. This is
         ;; the only arm that can prove anything, and it is the one that catches a drift.
         (and (>= sc 0) all-resident? all-known?)
         (let [expected (reduce + 0 child-counts)]
           (if (not= sc expected)
             [{:error :subtree-count-mismatch
               :level (nlevel node) :branch-count sc :children-sum expected}]
             []))

         ;; Every child RESIDENT but one of them has no count, while this node claims one.
         ;; Still a violation, and keeping it matters: an adversarial review showed that
         ;; folding this into the skip above let `validate-full` return true on a warm,
         ;; fully-resident tree whose root count was wrong by 7, merely because one resident
         ;; child had been set to -1. The same review then looked for the state this arm
         ;; supposedly needed to tolerate — a resident branch with count -1 under a
         ;; known-count parent — across bulk, conj, transient-conj and disj-churn builds at
         ;; bf 8 / n 3000, and found ZERO occurrences in all four. So relaxing it bought
         ;; nothing and cost the detection.
         (and (>= sc 0) all-resident? (not all-known?))
         [{:error :count-known-child-unknown
           :level (nlevel node) :branch-count sc :child-counts child-counts}]

         ;; A child is NOT RESIDENT: its count lives in its own blob and cannot be seen from
         ;; here. Unverifiable, not violated — this is the ordinary state of every lazily
         ;; restored tree, and treating it as a violation is what made this check unusable
         ;; against the restore path.
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
;; Invariant 9b: Cached measures AGREE with the subtree they summarize
;; =============================================================================
;;
;; There was no such check anywhere. `validate-full`'s docstring has always said "including
;; subtree counts, measures", but it ran `validate` + `check-subtree-counts` + navigation;
;; the only measure code reachable from the public API was `check-all-measures-known`, which
;; tests non-nil and nothing more. So a measure could be a WRONG NUMBER — the exact defect
;; class of "a leaf shrunk by a sibling rebalance kept its pre-shrink measure" — and every
;; validator in the tree would pass it.
;;
;; Verified bottom-up by induction, the same way the maintenance code computes: a leaf's
;; measure is `identity` merged with each extracted key; a branch's is `identity` merged with
;; its children's CACHED measures. Because every node is checked, a wrong child measure is
;; caught at the child, so merging cached values at the parent loses nothing.
;;
;; Nothing is claimed where nothing can be seen: a nil cached measure means "not computed
;; yet", which is legal everywhere, and a non-resident child means the parent's merge is
;; unverifiable. Both skip rather than fail — the mistake that made the subtree-count check
;; useless on restored trees.

(defn- measure-ops [set]
  (let [settings (get-settings set)]
    #?(:clj  (.measure ^Settings settings)
       :cljs (:measure settings))))

(defn- m-identity [ops] #?(:clj (.identity ^IMeasure ops) :cljs (measure/identity-measure ops)))
(defn- m-extract [ops k] #?(:clj (.extract ^IMeasure ops k) :cljs (measure/extract ops k)))
(defn- m-merge [ops a b] #?(:clj (.merge ^IMeasure ops a b) :cljs (measure/merge-measure ops a b)))

(defn- expected-measure
  "The measure this node should carry, or ::unverifiable when it cannot be derived here."
  [node ops]
  (if (leaf? node)
    (reduce (fn [acc i] (m-merge ops acc (m-extract ops (nkey node i))))
            (m-identity ops)
            (range (nlen node)))
    (let [cs (children-seq node)
          ms (mapv #(when % (node-measure %)) cs)]
      (if (or (some nil? cs) (some nil? ms))
        ::unverifiable
        (reduce (fn [acc m] (m-merge ops acc m)) (m-identity ops) ms)))))

(defn- check-measure-agreement [node ops]
  (let [cached (node-measure node)
        errors (if (nil? cached)
                 []                       ; not computed yet — legal, no claim
                 (let [expected (expected-measure node ops)]
                   (cond
                     (= ::unverifiable expected) []
                     (= cached expected) []
                     :else [{:error (if (branch? node)
                                      :branch-measure-mismatch
                                      :leaf-measure-mismatch)
                             :level (nlevel node)
                             :cached cached
                             :expected expected}])))]
    (if (branch? node)
      (reduce into errors
              (map #(when % (check-measure-agreement % ops)) (children-seq node)))
      errors)))

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
;; Internal helpers for public API
;; =============================================================================

(defn- collect-all-keys
  "Walk tree collecting all leaf keys into a vector."
  [node]
  (if (leaf? node)
    (nkeys node)
    (into [] (mapcat #(when % (collect-all-keys %))) (children-seq node))))

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

(defn fanout-profile
  "The tree's SHAPE, bottom level first: a vector of vectors giving the element
   count of every node at each level, root last.

   Contents equality cannot see a shape difference — two trees holding the same
   elements with different cuts are `=` and iterate identically, and differ only
   in how they are stored. That difference is not cosmetic under
   content-addressed storage, where the node bytes ARE the address: a different
   cut is a different address, a different merkle root, and no node sharing
   between two databases that hold the same data.

   Reads only nodes already in memory (`child-node` returns nil for one that is
   not), so it describes a resident tree and is not a substitute for walking
   storage."
  [set]
  (let [root (get-root set)]
    (if (or (nil? root) (zero? (nlen root)))
      []
      (loop [level [root] acc []]
        (let [widths (mapv subtree-count* level)
              acc (clojure.core/conj acc widths)
              kids (when (every? branch? level)
                     (let [cs (into [] (mapcat children-seq) level)]
                       (when (every? some? cs) cs)))]
          (if (seq kids)
            (recur kids acc)
            (vec (reverse acc))))))))

(defn validate-full
  "Full integrity check including subtree counts, measures, and element-wise
   navigation. Every key in the tree is re-looked up from the root to verify
   search paths are correct. Catches separator key corruption, comparator bugs,
   and any issue where elements are structurally present but unreachable.
   Returns true if valid, throws with structured error data."
  [set]
  (validate set)
  (let [root (get-root set)]
    (when (and root (pos? (nlen root)))
      (let [errors (check-subtree-counts root)]
        (throw-errors errors "B-tree count/measure invariant violations"))
      ;; Measures, which this docstring has always claimed and which nothing checked until
      ;; now. Only runs when the set actually carries measure ops.
      (when-let [ops (measure-ops set)]
        (throw-errors (check-measure-agreement root ops)
                      "B-tree measure invariant violations"))
      ;; Element-wise navigation check: every key must be findable via lookup
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

(defn verification-coverage
  "How much of `validate-full`'s count and measure checking actually ran.

   `validate-full` cannot verify what it cannot see: a node whose children are not resident,
   or whose cached count/measure is absent, is SKIPPED rather than failed. That is the only
   correct behaviour — a lazily-restored tree is not corrupt for being lazy — but it means a
   `true` can equally mean \"checked everything and it holds\" or \"could check nothing\".
   The subtree-count check spent its whole life in the second state against restored trees
   without anyone noticing, which is what this exists to make visible.

   Returns `{:branches n :counts-verified n :counts-skipped n
             :measures-verified n :measures-skipped n}`. A test that cares should assert
   `counts-verified` is non-zero, not merely that `validate-full` returned true."
  [set]
  (let [root (get-root set)
        ops  (measure-ops set)]
    (if-not (and root (pos? (nlen root)))
      {:branches 0 :counts-verified 0 :counts-skipped 0
       :measures-verified 0 :measures-skipped 0}
      (loop [[node & more] [root]
             acc {:branches 0 :counts-verified 0 :counts-skipped 0
                  :measures-verified 0 :measures-skipped 0}]
        (if (nil? node)
          acc
          (let [branch? (branch? node)
                cs      (when branch? (children-seq node))
                count-ok? (and branch?
                               (>= (subtree-count* node) 0)
                               (every? some? cs)
                               (every? #(>= (subtree-count* %) 0) cs))
                measure-ok? (and ops
                                 (some? (node-measure node))
                                 (not= ::unverifiable (expected-measure node ops)))
                acc' (cond-> acc
                       branch? (update :branches inc)
                       (and branch? count-ok?) (update :counts-verified inc)
                       (and branch? (not count-ok?)) (update :counts-skipped inc)
                       (and ops measure-ok?) (update :measures-verified inc)
                       (and ops (not measure-ok?)) (update :measures-skipped inc))]
            (recur (concat (filter some? cs) more) acc')))))))

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
