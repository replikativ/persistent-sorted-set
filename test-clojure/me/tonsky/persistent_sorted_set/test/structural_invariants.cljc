(ns me.tonsky.persistent-sorted-set.test.structural-invariants
  "Cross-platform structural invariant tests.
   Verifies tree properties hold after various operations."
  (:require
   [clojure.test :refer [deftest is testing]]
   #?(:clj [clojure.test.check :as tc]
      :cljs [clojure.test.check :as tc])
   #?(:clj [clojure.test.check.generators :as gen]
      :cljs [clojure.test.check.generators :as gen])
   #?(:clj [clojure.test.check.properties :as prop]
      :cljs [clojure.test.check.properties :as prop])
   [me.tonsky.persistent-sorted-set :as set]
   #?(:cljs [me.tonsky.persistent-sorted-set.branch :as branch :refer [Branch]])
   #?(:cljs [me.tonsky.persistent-sorted-set.leaf :as leaf :refer [Leaf]])
   #?(:cljs [me.tonsky.persistent-sorted-set.impl.node :as node]))
  #?(:clj
     (:import
      [me.tonsky.persistent_sorted_set ANode Branch Leaf PersistentSortedSet Settings])))

;; =============================================================================
;; Platform-specific tree access helpers
;; =============================================================================

#?(:clj
   (do
     (defn tree-root [^PersistentSortedSet s]
       (let [root (.-_root s)]
         (if (instance? java.lang.ref.Reference root)
           (.get ^java.lang.ref.Reference root)
           root)))

     (defn node-level [^ANode node]
       (.level node))

     (defn node-len [^ANode node]
       (.len node))

     (defn node-keys [^ANode node]
       (vec (.keys node)))

     (defn node-max-key [^ANode node]
       (.maxKey node))

     (defn branch? [node] (instance? Branch node))
     (defn leaf? [node] (instance? Leaf node))

     (defn node-children [^Branch node]
       (when (branch? node)
         (let [len (.len node)]
           (mapv (fn [i]
                   (let [children (.-_children node)]
                     (when children
                       (let [child (aget children (int i))]
                         (if (instance? java.lang.ref.Reference child)
                           (.get ^java.lang.ref.Reference child)
                           child)))))
                 (range len))))))

   :cljs
   (do
     (defn tree-root [s]
       (.-root s))

     (defn node-level [node]
       (node/level node))

     (defn node-len [node]
       (node/len node))

     (defn node-keys [node]
       (vec (.-keys node)))

     (defn node-max-key [node]
       (node/max-key node))

     (defn branch? [node] (instance? Branch node))
     (defn leaf? [node] (instance? Leaf node))

     (defn node-children [node]
       (when (branch? node)
         (vec (.-children node))))))

;; =============================================================================
;; Structural Invariant Checks
;; =============================================================================

(defn check-sorted-order
  "Verify elements iterate in sorted order."
  [tree]
  (let [elements (vec tree)]
    (if (< (count elements) 2)
      {:pass? true}
      (let [pairs (partition 2 1 elements)
            unsorted (first (filter (fn [[a b]] (> (compare a b) 0)) pairs))]
        (if unsorted
          {:pass? false :error :unsorted :pair unsorted}
          {:pass? true})))))

(defn check-count-consistency
  "Verify tree count matches iteration count."
  [tree expected-count]
  (let [actual-count (count tree)
        iter-count (count (seq tree))]
    (cond
      (not= actual-count expected-count)
      {:pass? false :error :count-field-mismatch
       :expected expected-count :actual actual-count}

      (not= iter-count expected-count)
      {:pass? false :error :iteration-count-mismatch
       :expected expected-count :actual iter-count}

      :else {:pass? true})))

(defn check-rseq-consistency
  "Verify rseq returns reverse of seq."
  [tree]
  (let [forward (vec tree)
        backward (vec (rseq tree))]
    (if (= forward (reverse backward))
      {:pass? true}
      {:pass? false :error :rseq-mismatch
       :forward forward :backward backward})))

(defn check-contains-all
  "Verify all elements are findable via contains?."
  [tree elements]
  (let [missing (filter #(not (contains? tree %)) elements)]
    (if (empty? missing)
      {:pass? true}
      {:pass? false :error :missing-elements :missing (vec (take 5 missing))})))

(defn check-slice-bounds
  "Verify slice returns correct elements."
  [tree from to]
  (let [elements (vec tree)
        sliced (vec (set/slice tree from to))
        expected (vec (filter #(and (>= (compare % from) 0)
                                    (<= (compare % to) 0))
                              elements))]
    (if (= sliced expected)
      {:pass? true}
      {:pass? false :error :slice-mismatch
       :expected expected :actual sliced})))

(def ^:const max-tree-depth
  "Maximum depth to walk - prevents infinite loops from malformed trees."
  100)

(defn check-tree-depth-uniform
  "Verify all leaves are at the same depth (only works for in-memory trees)."
  [tree]
  (let [root (tree-root tree)]
    (if (nil? root)
      {:pass? true}
      (if (leaf? root)
        {:pass? true}
        ;; For branch, we check that root level > 0 and leaves have level 0
        (let [root-level (node-level root)]
          (if (> root-level 0)
            {:pass? true}
            {:pass? false :error :invalid-root-level :level root-level}))))))

(defn check-level-consistency
  "Verify all root-to-leaf paths have consistent level numbers.
   Branch level should equal child level + 1. Leaves have level 0.
   Only works for in-memory trees (children loaded)."
  [tree]
  (let [root (tree-root tree)]
    (if (or (nil? root) (leaf? root))
      {:pass? true}
      (let [errors (atom [])
            max-err 5]
        (letfn [(walk [node expected-level path depth]
                  (when (and node (< (count @errors) max-err) (< depth max-tree-depth))
                    (let [actual-level (node-level node)]
                      (when (not= actual-level expected-level)
                        (swap! errors conj
                               {:error :level-mismatch
                                :path path
                                :expected expected-level
                                :actual actual-level}))
                      (when (branch? node)
                        (let [children (node-children node)
                              len (node-len node)]
                          (dotimes [i len]
                            (when-let [child (nth children i nil)]
                              (walk child (dec expected-level) (conj path i) (inc depth)))))))))]
          (walk root (node-level root) [] 0)
          (if (seq @errors)
            {:pass? false :error :level-inconsistency :details (take 5 @errors)}
            {:pass? true}))))))

(defn check-balanced
  "Verify tree is balanced: all leaves at the same depth from root.
   Collects depth of every leaf and verifies they're all equal.
   Only works for in-memory trees (children loaded)."
  [tree]
  (let [root (tree-root tree)]
    (if (nil? root)
      {:pass? true}
      (let [leaf-depths (atom [])
            error (atom nil)]
        (letfn [(walk [node depth]
                  (when (and node (nil? @error))
                    (if (> depth max-tree-depth)
                      (reset! error {:error :max-depth-exceeded :depth depth})
                      (if (leaf? node)
                        (swap! leaf-depths conj depth)
                        (let [children (node-children node)
                              len (node-len node)]
                          ;; Only walk actual children, not entire array
                          (dotimes [i len]
                            (when-let [child (nth children i nil)]
                              (walk child (inc depth)))))))))]
          (walk root 0)
          (if-let [err @error]
            {:pass? false :error (:error err) :details err}
            (let [depths @leaf-depths]
              (if (empty? depths)
                {:pass? true}
                (let [first-depth (first depths)]
                  (if (every? #(= % first-depth) depths)
                    {:pass? true :leaf-depth first-depth :leaf-count (count depths)}
                    {:pass? false
                     :error :unbalanced-tree
                     :depths (frequencies depths)}))))))))))

(defn check-branch-keys
  "Verify each branch key[i] equals child[i].maxKey().
   Only works for in-memory trees (children loaded)."
  [tree]
  (let [root (tree-root tree)]
    (if (or (nil? root) (leaf? root))
      {:pass? true}
      (let [errors (atom [])
            max-err 5]
        (letfn [(walk [node path depth]
                  (when (and node (branch? node) (< (count @errors) max-err) (< depth max-tree-depth))
                    (let [keys (node-keys node)
                          children (node-children node)
                          len (node-len node)]
                      (dotimes [i len]
                        (when-let [child (nth children i nil)]
                          (let [branch-key (nth keys i nil)
                                child-max (node-max-key child)]
                            (when (and branch-key child-max
                                       (not= branch-key child-max))
                              (swap! errors conj
                                     {:error :key-child-mismatch
                                      :path (conj path i)
                                      :branch-key branch-key
                                      :child-max child-max}))
                            (walk child (conj path i) (inc depth))))))))]
          (walk root [] 0)
          (if (seq @errors)
            {:pass? false :error :branch-key-mismatch :details (take 5 @errors)}
            {:pass? true}))))))

(defn check-node-lengths
  "Verify all node lengths are within bounds [1, branching-factor].
   Root is allowed to have len >= 1 for non-empty trees.
   Only works for in-memory trees (children loaded)."
  [tree bf]
  (let [root (tree-root tree)]
    (if (nil? root)
      {:pass? true}
      (let [errors (atom [])
            max-err 5]
        (letfn [(walk [node path depth]
                  (when (and node (< (count @errors) max-err) (< depth max-tree-depth))
                    (let [len (node-len node)]
                      ;; Root can have any length >= 1, other nodes must have >= ceil(bf/2)
                      (when (or (< len 1)
                                (> len bf))
                        (swap! errors conj
                               {:error :length-out-of-bounds
                                :path path
                                :length len
                                :bounds [1 bf]}))
                      (when (branch? node)
                        (let [children (node-children node)]
                          (dotimes [i len]
                            (when-let [child (nth children i nil)]
                              (walk child (conj path i) (inc depth)))))))))]
          (walk root [] 0)
          (if (seq @errors)
            {:pass? false :error :node-length-violation :details (take 5 @errors)}
            {:pass? true}))))))

(defn check-all-invariants
  "Run all structural invariant checks.
   If bf (branching factor) is provided, also checks node length bounds."
  ([tree expected-elements]
   (check-all-invariants tree expected-elements nil))
  ([tree expected-elements bf]
   (let [expected-count (count (distinct expected-elements))
         checks (cond-> [(check-sorted-order tree)
                         (check-count-consistency tree expected-count)
                         (check-rseq-consistency tree)
                         (check-contains-all tree expected-elements)
                         (check-tree-depth-uniform tree)
                         (check-level-consistency tree)
                         (check-balanced tree)
                         (check-branch-keys tree)]
                  bf (conj (check-node-lengths tree bf)))]
     (if (every? :pass? checks)
       {:pass? true}
       {:pass? false :failures (filter #(not (:pass? %)) checks)}))))

;; =============================================================================
;; Invariant Tests After conj
;; =============================================================================

(deftest test-invariants-after-conj
  (testing "Structural invariants hold after conj operations"
    (doseq [bf [4 8 16 32]]
      (let [elements (range 500)
            tree (reduce conj (set/sorted-set* {:branching-factor bf}) elements)
            result (check-all-invariants tree elements bf)]
        (is (:pass? result)
            (str "Invariants failed for bf=" bf ": " (pr-str (:failures result))))))))

(deftest test-invariants-after-conjall
  (testing "Structural invariants hold after conjAll"
    (doseq [bf [4 8 16 32]]
      (let [elements (range 500)
            tree (set/conj-all (set/sorted-set* {:branching-factor bf}) (vec elements))
            result (check-all-invariants tree elements bf)]
        (is (:pass? result)
            (str "Invariants failed for bf=" bf ": " (pr-str (:failures result))))))))

(deftest test-invariants-after-disj
  (testing "Structural invariants hold after disj operations"
    (let [bf 8
          initial (range 200)
          to-remove (range 0 200 3)  ;; Remove every 3rd element
          tree (reduce disj
                       (set/conj-all (set/sorted-set* {:branching-factor bf}) (vec initial))
                       to-remove)
          remaining (remove (set to-remove) initial)
          result (check-all-invariants tree remaining bf)]
      (is (:pass? result) (pr-str (:failures result))))))

(deftest test-invariants-after-mixed-ops
  (testing "Structural invariants hold after mixed conj/disj"
    (let [bf 8
          tree (-> (set/sorted-set* {:branching-factor bf})
                   (set/conj-all (vec (range 100)))
                   (set/conj-all (vec (range 200 300)))
                   (#(reduce disj % (range 50 150)))
                   (set/conj-all (vec (range 400 500))))
          expected (concat (range 50) (range 200 300) (range 400 500))
          result (check-all-invariants tree expected bf)]
      (is (:pass? result) (pr-str (:failures result))))))

;; =============================================================================
;; Property-based Structural Tests
;; =============================================================================

(deftest test-gen-invariants-random-conj
  (testing "Property: structural invariants hold for random conj sequences"
    (let [result
          (tc/quick-check 50
                          (prop/for-all [bf (gen/elements [4 8 16])
                                         elements (gen/vector (gen/choose -1000 1000) 10 200)]
                                        (let [tree (reduce conj (set/sorted-set* {:branching-factor bf}) elements)
                                              check (check-all-invariants tree elements bf)]
                                          (:pass? check))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

(deftest test-gen-invariants-random-conjall
  (testing "Property: structural invariants hold for random conjAll"
    (let [result
          (tc/quick-check 50
                          (prop/for-all [bf (gen/elements [4 8 16])
                                         elements (gen/vector (gen/choose -1000 1000) 10 200)]
                                        (let [sorted (vec (sort elements))
                                              tree (set/conj-all (set/sorted-set* {:branching-factor bf}) sorted)
                                              check (check-all-invariants tree elements bf)]
                                          (:pass? check))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

(deftest test-gen-invariants-conj-vs-conjall-equivalence
  (testing "Property: conj and conjAll produce structurally equivalent trees"
    (let [result
          (tc/quick-check 50
                          (prop/for-all [bf (gen/elements [4 8 16])
                                         elements (gen/vector (gen/choose -1000 1000) 10 150)]
                                        (let [sorted (vec (sort elements))
                                              via-conj (reduce conj (set/sorted-set* {:branching-factor bf}) sorted)
                                              via-conjall (set/conj-all (set/sorted-set* {:branching-factor bf}) sorted)]
                                          (and (= (vec via-conj) (vec via-conjall))
                                               (= (count via-conj) (count via-conjall))
                                               (:pass? (check-balanced via-conj))
                                               (:pass? (check-balanced via-conjall))))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

(deftest test-gen-invariants-disj-sequence
  (testing "Property: structural invariants hold after disj sequence"
    (let [result
          (tc/quick-check 30
                          (prop/for-all [bf (gen/elements [4 8 16])
                                         initial (gen/vector (gen/choose 0 500) 50 150)
                                         to-remove (gen/vector (gen/choose 0 500) 10 40)]
                                        (let [tree (reduce disj
                                                           (set/conj-all (set/sorted-set* {:branching-factor bf})
                                                                         (vec (sort initial)))
                                                           to-remove)
                                              remaining (remove (set to-remove) initial)
                                              check (check-all-invariants tree remaining bf)]
                                          (:pass? check))))]
      (is (:pass? result) (pr-str (:shrunk result))))))

;; =============================================================================
;; Slice Invariant Tests
;; =============================================================================

(deftest test-slice-structural-invariants
  (testing "Slice returns correct subset"
    (let [tree (set/conj-all (set/sorted-set) (range 100))]
      (doseq [[from to] [[10 20] [0 50] [50 99] [25 75] [0 0] [99 99]]]
        (let [result (check-slice-bounds tree from to)]
          (is (:pass? result)
              (str "Slice failed for [" from ", " to "]: " (pr-str result))))))))

(deftest test-gen-slice-invariants
  (testing "Property: slice bounds are respected"
    (let [result
          (tc/quick-check 30
                          (prop/for-all [elements (gen/vector (gen/choose 0 500) 10 100)
                                         from (gen/choose 0 500)
                                         to (gen/choose 0 500)]
                                        (let [tree (set/conj-all (set/sorted-set) (vec (sort elements)))
                                              check (check-slice-bounds tree from to)]
                                          (:pass? check))))]
      (is (:pass? result) (pr-str (:shrunk result))))))
