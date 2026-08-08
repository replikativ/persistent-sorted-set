(ns org.replikativ.persistent-sorted-set.test.leaf-processor
  (:require
   [clojure.test :as t :refer [is deftest testing]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :refer [defspec]]
   [org.replikativ.persistent-sorted-set :as set]
   [org.replikativ.persistent-sorted-set.diagnostics :as diag]
   [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import
   [java.util Comparator List ArrayList Collections]
   [org.replikativ.persistent_sorted_set ILeafProcessor Settings PersistentSortedSet Branch RefType]))

(set! *warn-on-reflection* true)

;; =============================================================================
;; Helpers to avoid reflection on cons/disjoin (overloaded methods + primitive args)
;; =============================================================================

(defn pss-conj ^PersistentSortedSet [^PersistentSortedSet s v]
  (.cons s v))

(defn pss-disj ^PersistentSortedSet [^PersistentSortedSet s v]
  (.disjoin s v))

;; =============================================================================
;; Processor implementations
;; =============================================================================

(defn identity-processor
  "Always processes but returns entries unchanged."
  []
  (reify ILeafProcessor
    (shouldProcess [_ _leafSize _settings] true)
    (processLeaf [_ entries _storage _settings] entries)))

(defn compacting-processor
  "Drops every other entry when leaf has more than threshold entries.
   Simulates chunk merging that reduces entry count."
  [^long threshold]
  (reify ILeafProcessor
    (shouldProcess [_ leafSize _settings] (> (long leafSize) threshold))
    (processLeaf [_ entries _storage _settings]
      (let [^List entries entries
            result (ArrayList.)]
        (dotimes [i (.size entries)]
          (when (even? i)
            (.add result (.get entries i))))
        result))))

(defn expanding-processor
  "When leaf reaches target-size entries, inserts up to extra-per-gap entries
   in each gap between consecutive entries. Only inserts where gap > 1.
   Self-limiting: as gaps fill to size 1, no more expansion occurs.
   Test data MUST use spaced values (e.g., multiples of 10 or 100).
   Simulates chunk splitting that increases entry count."
  [^long target-size ^long extra-per-gap]
  (reify ILeafProcessor
    (shouldProcess [_ leafSize _settings] (>= (long leafSize) target-size))
    (processLeaf [_ entries _storage _settings]
      (let [^List entries entries
            n (.size entries)
            result (ArrayList. ^java.util.Collection entries)]
        (dotimes [i (dec n)]
          (let [lo (long (.get entries i))
                hi (long (.get entries (inc i)))
                gap (- hi lo)]
            (when (> gap 1)
              (let [cnt (min (long extra-per-gap) (dec gap))]
                (dotimes [j cnt]
                  (.add result (+ lo 1 (long j))))))))
        (Collections/sort result)
        result))))

(defn mixed-processor
  "Merges adjacent entries within merge-gap (keeps first), then inserts
   up to extra-per-gap entries in each remaining gap > min-expand-gap.
   Both operations stay within the leaf's key range. Self-limiting.
   Simulates stratum's ChunkCompactionProcessor pattern."
  [^long merge-gap ^long min-expand-gap ^long extra-per-gap]
  (reify ILeafProcessor
    (shouldProcess [_ leafSize _settings] (> (long leafSize) 1))
    (processLeaf [_ entries _storage _settings]
      (let [^List entries entries
            ;; Phase 1: compact adjacent entries within merge-gap
            compacted (ArrayList.)
            _ (dotimes [i (.size entries)]
                (let [v (long (.get entries i))]
                  (if (and (pos? (.size compacted))
                           (<= (Math/abs (- v (long (.get compacted (dec (.size compacted))))))
                               merge-gap))
                    nil ;; skip — merge with previous
                    (.add compacted v))))
            ;; Phase 2: expand by inserting entries in large gaps
            result (ArrayList.)]
        (dotimes [i (.size compacted)]
          (let [v (long (.get compacted i))]
            (.add result v)
            (when (< i (dec (.size compacted)))
              (let [next-v (long (.get compacted (inc i)))
                    gap (- next-v v)]
                (when (> gap min-expand-gap)
                  (let [cnt (min (long extra-per-gap) (dec gap))]
                    (dotimes [j cnt]
                      (.add result (+ v 1 (long j))))))))))
        (Collections/sort result)
        result))))

(defn conditional-processor
  "Only processes when leaf size is in the range [lo, hi].
   Tests shouldProcess returning false for leaves outside the range."
  [^long lo ^long hi]
  (let [^ILeafProcessor inner (identity-processor)]
    (reify ILeafProcessor
      (shouldProcess [_ leafSize _settings]
        (let [s (long leafSize)]
          (and (>= s lo) (<= s hi))))
      (processLeaf [_ entries _storage _settings]
        (.processLeaf inner entries _storage _settings)))))

;; =============================================================================
;; Helpers
;; =============================================================================

(defn make-set
  "Create a PersistentSortedSet with the given ILeafProcessor."
  ([^ILeafProcessor processor items]
   (make-set processor 8 items))
  ([^ILeafProcessor processor branching-factor items]
   (let [settings (Settings. (int branching-factor)
                             nil nil processor)
         ^Comparator cmp (comparator <)
         empty (PersistentSortedSet. nil cmp nil settings)]
     (reduce pss-conj empty items))))

(defn set-seq [^PersistentSortedSet s]
  (when (pos? (.count s))
    (into [] (iterator-seq (.iterator s)))))

(defn validate-tree [s]
  (diag/validate-full s))

;; Spaced data generators for expanding processor tests
(defn spaced-range
  "Returns a seq of values spaced apart: (spacing, 2*spacing, ..., n*spacing)"
  [n spacing]
  (mapv #(* (long %) (long spacing)) (range 1 (inc n))))

;; =============================================================================
;; Basic processor tests
;; =============================================================================

(deftest test-identity-processor
  (testing "Identity processor doesn't change tree behavior"
    (let [items (range 1 101)
          s (make-set (identity-processor) items)]
      (is (= (count items) (.count ^PersistentSortedSet s)))
      (is (= (vec items) (set-seq s)))
      (is (validate-tree s)))))

(deftest test-identity-processor-with-removal
  (testing "Identity processor works with disj"
    (let [items (range 1 51)
          s (make-set (identity-processor) items)
          s2 (reduce pss-disj s (range 10 30))]
      (is (= 30 (.count ^PersistentSortedSet s2)))
      (is (= (vec (concat (range 1 10) (range 30 51)))
             (set-seq s2)))
      (is (validate-tree s2)))))

(deftest test-identity-processor-transient
  (testing "Identity processor with transient operations"
    (let [items (range 1 101)
          s (make-set (identity-processor) items)
          ;; transient add
          s2 (persistent!
              (reduce (fn [t item]
                        (conj! t item))
                      (transient s)
                      (range 101 201)))]
      (is (= 200 (count s2)))
      (is (= (vec (range 1 201)) (set-seq s2)))
      (is (validate-tree s2)))))

(deftest test-no-processor-baseline
  (testing "Null processor has zero overhead (no change to behavior)"
    (let [settings (Settings. (int 8) nil nil nil)
          ^Comparator cmp (comparator <)
          s (reduce pss-conj
                    (PersistentSortedSet. nil cmp nil settings)
                    (range 1 51))]
      (is (= 50 (.count ^PersistentSortedSet s)))
      (is (= (vec (range 1 51)) (set-seq s)))
      (is (validate-tree s)))))

;; =============================================================================
;; Compacting processor tests
;; =============================================================================

(deftest test-compacting-processor-triggers-merge
  (testing "Compaction that shrinks entries below minBF triggers merge/borrow"
    (let [s (make-set (compacting-processor 3) 8 (range 1 51))
          result (set-seq s)]
      (is (seq result) "Set should have some entries")
      (is (apply < result) "Entries should be sorted")
      (is (validate-tree s) "Tree structure must be valid after compaction")
      ;; Every entry in the set must be findable
      (doseq [v result]
        (is (.contains ^PersistentSortedSet s v)
            (str "Entry " v " should be findable via contains")))
      ;; Remove some entries and verify tree stays valid
      (let [to-remove (take 5 result)
            s2 (reduce pss-disj s to-remove)
            result2 (set-seq s2)]
        (is (validate-tree s2) "Tree valid after removal from compacted set")
        (is (or (nil? result2) (apply < result2)) "Entries should be sorted after removal")
        (doseq [v to-remove]
          (is (not (.contains ^PersistentSortedSet s2 v))
              (str "Removed entry " v " should be gone")))))))

(deftest test-compacting-processor-remove-all
  (testing "Compaction during removal until set is empty"
    (let [s (make-set (compacting-processor 3) 8 (range 1 21))
          entries (set-seq s)
          s2 (reduce (fn [^PersistentSortedSet s item]
                       (if (.contains s item)
                         (pss-disj s item)
                         s))
                     s entries)]
      (is (= 0 (.count ^PersistentSortedSet s2))))))

(deftest test-compacting-processor-heavy
  (testing "Heavy compaction with small BF forces many merge/borrow operations"
    (let [;; BF=4, threshold=1: every leaf with >1 entry gets halved
          s (make-set (compacting-processor 1) 4 (range 1 101))
          result (set-seq s)]
      (is (seq result))
      (is (apply < result))
      (is (validate-tree s))
      ;; Remove entries one by one, validate each step
      (loop [s s
             entries (set-seq s)]
        (when (seq entries)
          (let [v (first entries)
                s' (pss-disj s v)]
            (is (validate-tree s') (str "Valid after removing " v))
            (recur s' (rest entries))))))))

;; =============================================================================
;; Expanding processor tests (use spaced data to ensure valid gaps)
;; =============================================================================

(deftest test-expanding-processor-causes-split
  (testing "Expansion that grows entries beyond BF triggers proper split"
    (let [items (spaced-range 4 100) ;; [100, 200, 300, 400]
          s (make-set (expanding-processor 4 6) 8 items)]
      (is (> (.count ^PersistentSortedSet s) (count items))
          "Set should have more entries than added (processor expanded)")
      (let [result (set-seq s)]
        (is (apply < result) "Entries should be sorted")
        (is (validate-tree s) "Tree structure must be valid after expansion")
        ;; Original entries should be present
        (doseq [i items]
          (is (.contains ^PersistentSortedSet s i)
              (str "Original entry " i " should be present")))))))

(deftest test-expanding-processor-with-removal
  (testing "Expanded set handles removal correctly"
    ;; Use target-size > BF so processor only fires during add (leaf overflow),
    ;; never during removal (leaves have at most BF entries after split).
    ;; Expansion during removal is nonsensical and not supported by the remove path.
    (let [items (spaced-range 10 100) ;; [100, 200, ..., 1000]
          s (make-set (expanding-processor 9 3) 8 items)
          s2 (reduce pss-disj s [(first items) (second items)])
          result (set-seq s2)]
      (is (not (.contains ^PersistentSortedSet s2 (first items))))
      (is (not (.contains ^PersistentSortedSet s2 (second items))))
      (is (.contains ^PersistentSortedSet s2 (nth items 2)))
      (is (apply < result) "Entries should remain sorted")
      (is (validate-tree s2)))))

(deftest test-expanding-processor-large
  (testing "Expanding processor with many additions"
    (let [items (spaced-range 30 100) ;; [100, 200, ..., 3000]
          s (make-set (expanding-processor 3 3) 8 items)]
      (is (> (.count ^PersistentSortedSet s) (count items))
          "Should have expanded beyond input size")
      (let [result (set-seq s)]
        (is (apply < result))
        (is (validate-tree s))
        ;; Original entries should all be present
        (doseq [i items]
          (is (.contains ^PersistentSortedSet s i)))))))

;; =============================================================================
;; Mixed processor tests (compact + expand, like stratum)
;; =============================================================================

(deftest test-mixed-processor
  (testing "Mixed processor that both compacts and expands"
    (let [;; merge-gap=2: merge entries within 2 of each other
          ;; min-expand-gap=50: only expand gaps larger than 50
          ;; extra-per-gap=3: add up to 3 entries per gap
          ;; Use spaced data so there are large gaps to expand into
          items (spaced-range 30 100) ;; [100, 200, ..., 3000]
          s (make-set (mixed-processor 2 50 3) 8 items)]
      (let [result (set-seq s)]
        (is (seq result))
        (is (apply < result))
        (is (validate-tree s))
        ;; Remove some entries
        (let [to-remove (take 10 result)
              s2 (reduce pss-disj s to-remove)]
          (is (validate-tree s2))
          (doseq [v to-remove]
            (is (not (.contains ^PersistentSortedSet s2 v)))))))))

;; =============================================================================
;; Conditional shouldProcess tests
;; =============================================================================

(deftest test-conditional-processor
  (testing "Processor that only fires for certain leaf sizes"
    (let [;; Only process leaves with 4-6 entries (identity, so no change)
          s (make-set (conditional-processor 4 6) 8 (range 1 51))]
      (is (= 50 (.count ^PersistentSortedSet s)))
      (is (= (vec (range 1 51)) (set-seq s)))
      (is (validate-tree s)))))

;; =============================================================================
;; Slice/rslice/seek on processor-built trees
;; =============================================================================

(deftest test-slice-with-processor
  (testing "Slice works correctly on identity-processor tree"
    (let [s (make-set (identity-processor) 8 (range 1 101))]
      (is (= (vec (range 20 41))
             (vec (set/slice s 20 40))))
      (is (= (vec (range 90 101))
             (vec (set/slice s 90 100))))
      (is (= [1]
             (vec (set/slice s 1 1))))))

  (testing "Slice works on compacted tree"
    (let [s (make-set (compacting-processor 3) 8 (range 1 51))
          all (set-seq s)]
      (when (> (count all) 4)
        (let [lo (nth all 1)
              hi (nth all (- (count all) 2))
              sliced (vec (set/slice s lo hi))
              expected (vec (filter #(and (>= (long %) (long lo)) (<= (long %) (long hi))) all))]
          (is (= expected sliced) "Slice on compacted tree should return correct range")))))

  (testing "Slice works on expanded tree"
    (let [items (spaced-range 20 100) ;; [100, 200, ..., 2000]
          s (make-set (expanding-processor 3 3) 8 items)
          all (set-seq s)]
      (when (> (count all) 4)
        (let [lo (nth all 1)
              hi (nth all (- (count all) 2))
              sliced (vec (set/slice s lo hi))
              expected (vec (filter #(and (>= (long %) (long lo)) (<= (long %) (long hi))) all))]
          (is (= expected sliced)))))))

(deftest test-rslice-with-processor
  (testing "Rslice works on processor-built trees"
    (let [s (make-set (identity-processor) 8 (range 1 51))]
      (is (= (vec (reverse (range 10 31)))
             (vec (set/rslice s 30 10))))
      (is (= (vec (reverse (range 1 51)))
             (vec (set/rslice s 50 1)))))))

(deftest test-seek-with-processor
  (testing "Seek works on processor-built trees"
    (let [s (make-set (identity-processor) 8 (range 1 51))]
      (is (= 25 (first (set/seek (seq s) 25))))
      (is (= 26 (first (set/seek (seq s) 25.5)))))))

;; =============================================================================
;; Replace on processor-built trees
;; =============================================================================

(deftest test-replace-on-processor-tree
  (testing "Replace works on tree built with identity processor"
    (let [s (make-set (identity-processor) 8 (range 1 51))
          ;; Replace doesn't invoke processor, but must work on processor-built trees
          s2 (set/replace s 25 25)]
      (is (= 50 (count s2)))
      (is (validate-tree s2))))

  (testing "Replace works on compacted tree"
    (let [s (make-set (compacting-processor 3) 8 (range 1 51))
          entries (set-seq s)]
      (when (seq entries)
        (let [v (first entries)
              s2 (set/replace s v v)]
          (is (validate-tree s2))
          (is (= (set-seq s) (set-seq s2))))))))

;; =============================================================================
;; Stratum-like disj+conj pattern
;; =============================================================================

(deftest test-disj-conj-pattern
  (testing "disj old + conj new (stratum's idx-insert pattern)"
    (let [s (make-set (identity-processor) 8 (range 1 51))]
      ;; Replace entry 25 with 25 (same position, different identity)
      (let [s2 (-> s
                   (disj 25)
                   (conj 25))]
        (is (= 50 (count s2)))
        (is (validate-tree s2)))
      ;; Remove 25, add 2500 (different position)
      (let [s3 (-> s
                   (disj 25)
                   (conj 2500))]
        (is (= 50 (count s3)))
        (is (contains? s3 2500))
        (is (not (contains? s3 25)))
        (is (validate-tree s3))))))

(deftest test-disj-conj-pattern-with-compacting-processor
  (testing "disj+conj pattern on compacted tree"
    (let [s (make-set (compacting-processor 3) 8 (range 1 51))
          entries (set-seq s)]
      (when (>= (count entries) 2)
        (let [old-v (first entries)
              new-v (+ (long (last entries)) 100)
              s2 (-> s
                     (disj old-v)
                     (conj new-v))
              result (set-seq s2)]
          (is (validate-tree s2))
          (is (not (contains? s2 old-v)))
          (when result
            (is (apply < result))))))))

;; =============================================================================
;; Large set structure tests
;; =============================================================================

(deftest test-processor-preserves-tree-structure
  (testing "Large set with identity processor maintains structure"
    (let [items (range 1 1001)
          s (make-set (identity-processor) 32 items)]
      (is (= 1000 (.count ^PersistentSortedSet s)))
      (is (= 1 (long (first (set-seq s)))))
      (is (= 1000 (long (last (set-seq s)))))
      (is (validate-tree s)))))

(deftest test-processor-with-interleaved-add-remove
  (testing "Interleaved add/remove with identity processor"
    (let [s (make-set (identity-processor) 8 (range 1 51))
          ;; Remove some
          s2 (reduce pss-disj s (range 20 40))
          ;; Add back different values
          s3 (reduce pss-conj s2 (range 60 80))]
      (is (= (count (distinct (concat (range 1 20) (range 40 51) (range 60 80))))
             (.count ^PersistentSortedSet s3)))
      (is (validate-tree s3)))))

;; =============================================================================
;; Validate every intermediate step
;; =============================================================================

(deftest test-validate-every-add-with-processor
  (testing "Validate tree after every single add with identity processor"
    (let [^Comparator cmp (comparator <)
          settings (Settings. (int 4) nil nil (identity-processor))
          empty (PersistentSortedSet. nil cmp nil settings)]
      (reduce (fn [s i]
                (let [s' (pss-conj s i)]
                  (is (validate-tree s') (str "Valid after adding " i))
                  s'))
              empty
              (range 1 101)))))

(deftest test-validate-every-remove-with-processor
  (testing "Validate tree after every single remove with identity processor"
    (let [s (make-set (identity-processor) 4 (range 1 51))]
      (reduce (fn [s i]
                (let [s' (pss-disj s i)]
                  (is (validate-tree s') (str "Valid after removing " i))
                  s'))
              s
              (range 1 51)))))

(deftest test-validate-every-add-with-compacting-processor
  (testing "Validate tree after every add with compacting processor"
    (let [^Comparator cmp (comparator <)
          settings (Settings. (int 4) nil nil (compacting-processor 2))
          empty (PersistentSortedSet. nil cmp nil settings)]
      (reduce (fn [s i]
                (let [s' (pss-conj s i)]
                  (is (validate-tree s') (str "Valid after adding " i))
                  s'))
              empty
              (range 1 51)))))

(deftest test-validate-every-add-with-expanding-processor
  (testing "Validate tree after every add with expanding processor"
    (let [^Comparator cmp (comparator <)
          settings (Settings. (int 8) nil nil (expanding-processor 3 2))
          empty (PersistentSortedSet. nil cmp nil settings)]
      ;; Use spaced values so expanding processor has gaps to fill
      (reduce (fn [s i]
                (let [s' (pss-conj s i)]
                  (is (validate-tree s') (str "Valid after adding " i))
                  s'))
              empty
              (spaced-range 30 100)))))

;; =============================================================================
;; Transient + persistent interaction with processor
;; =============================================================================

(deftest test-transient-on-processor-tree
  (testing "Transient add on processor-built tree produces valid tree"
    (let [s (make-set (identity-processor) 8 (range 1 51))
          s2 (persistent!
              (reduce (fn [t item]
                        (conj! t item))
                      (transient s)
                      (range 51 101)))]
      (is (= 100 (count s2)))
      (doseq [i (range 1 101)]
        (is (contains? s2 (long i))))
      (is (validate-tree s2)))))

(deftest test-transient-disj-on-processor-tree
  (testing "Transient removal on processor-built tree"
    (let [s (make-set (identity-processor) 8 (range 1 101))
          s2 (persistent!
              (reduce (fn [t item]
                        (disj! t item))
                      (transient s)
                      (range 1 51)))]
      (is (= 50 (count s2)))
      (is (= (vec (range 51 101)) (set-seq s2)))
      (is (validate-tree s2)))))

;; =============================================================================
;; Edge cases
;; =============================================================================

(deftest test-processor-single-element
  (testing "Processor with single-element set"
    (let [s (make-set (identity-processor) 8 [42])]
      (is (= 1 (.count ^PersistentSortedSet s)))
      (is (= [42] (set-seq s)))
      (is (validate-tree s))
      ;; Remove the only element
      (let [s2 (pss-disj s 42)]
        (is (= 0 (.count ^PersistentSortedSet s2)))
        (is (validate-tree s2))))))

(deftest test-processor-duplicate-add
  (testing "Adding duplicate with processor is a no-op"
    (let [s (make-set (identity-processor) 8 (range 1 11))
          s2 (pss-conj s 5)]
      (is (= 10 (.count ^PersistentSortedSet s2)))
      (is (= (set-seq s) (set-seq s2))))))

(deftest test-processor-remove-nonexistent
  (testing "Removing non-existent element from processor tree is a no-op"
    (let [s (make-set (identity-processor) 8 (range 1 11))
          s2 (pss-disj s 999)]
      (is (= 10 (.count ^PersistentSortedSet s2)))
      (is (= (set-seq s) (set-seq s2))))))

(deftest test-expanding-at-bf-boundary
  (testing "Expanding within BF produces valid single-leaf tree"
    (let [;; BF=8. [100, 200] with extra-per-gap=5 → 2 + 5 = 7 entries (< BF)
          s (make-set (expanding-processor 2 5) 8 [100 200])]
      (is (= 7 (.count ^PersistentSortedSet s)))
      (is (validate-tree s))))

  (testing "Expanding past BF triggers split"
    (let [;; BF=8. [100, 200] with extra-per-gap=8 → 2 + 8 = 10 entries (> BF)
          s (make-set (expanding-processor 2 8) 8 [100 200])]
      (is (= 10 (.count ^PersistentSortedSet s)))
      (is (validate-tree s)))))

;; =============================================================================
;; Stress test
;; =============================================================================

(deftest test-stress-random-ops-with-identity-processor
  (testing "Random interleaved add/remove with identity processor, validate at end"
    (let [rng (java.util.Random. 42)
          ^Comparator cmp (comparator <)
          settings (Settings. (int 4) nil nil (identity-processor))
          empty (PersistentSortedSet. nil cmp nil settings)
          ref (atom (sorted-set))
          pss (atom empty)]
      (dotimes [_ 1000]
        (let [v (long (.nextInt rng 500))]
          (if (.nextBoolean rng)
            (do (swap! ref conj v)
                (swap! pss pss-conj v))
            (do (swap! ref disj v)
                (swap! pss pss-disj v)))))
      (is (= (vec @ref) (set-seq @pss)))
      (is (= (count @ref) (.count ^PersistentSortedSet @pss)))
      (is (validate-tree @pss)))))

(deftest test-stress-random-ops-with-compacting-processor
  (testing "Random add/remove with compacting processor"
    (let [rng (java.util.Random. 123)
          ^Comparator cmp (comparator <)
          settings (Settings. (int 4) nil nil (compacting-processor 2))
          empty (PersistentSortedSet. nil cmp nil settings)
          pss (atom empty)]
      ;; Add many entries
      (dotimes [_ 200]
        (let [v (long (.nextInt rng 1000))]
          (swap! pss pss-conj v)))
      (is (validate-tree @pss) "Valid after additions")
      ;; Remove some
      (let [entries (set-seq @pss)]
        (doseq [v (take 50 entries)]
          (swap! pss pss-disj v)))
      (is (validate-tree @pss) "Valid after removals")
      (let [result (set-seq @pss)]
        (when (seq result)
          (is (apply < result) "Sorted after stress"))))))

;; =============================================================================
;; Property-based tests
;; =============================================================================

(def gen-int (gen/choose 0 2000))

(def gen-operation
  (gen/tuple (gen/elements [:add :remove]) gen-int))

(defspec structural-invariants-with-identity-processor 100
  (prop/for-all [elements (gen/vector gen-int 0 200)
                 ops (gen/vector gen-operation 0 100)]
                (let [^Comparator cmp (comparator <)
                      settings (Settings. (int 4) nil nil (identity-processor))
                      pss (reduce pss-conj
                                  (PersistentSortedSet. nil cmp nil settings)
                                  elements)
                      final (reduce (fn [s [op v]]
                                      (case op
                                        :add (pss-conj s v)
                                        :remove (pss-disj s v)))
                                    pss ops)]
                  (validate-tree final))))

(defspec structural-invariants-with-compacting-processor 100
  (prop/for-all [elements (gen/vector gen-int 0 100)
                 ops (gen/vector gen-operation 0 50)]
                (let [^Comparator cmp (comparator <)
                      settings (Settings. (int 4) nil nil (compacting-processor 2))
                      pss (reduce pss-conj
                                  (PersistentSortedSet. nil cmp nil settings)
                                  elements)
                      final (reduce (fn [s [op v]]
                                      (case op
                                        :add (pss-conj s v)
                                        :remove (if (.contains ^PersistentSortedSet s v)
                                                  (pss-disj s v)
                                                  s)))
                                    pss ops)]
                  (and (validate-tree final)
                       (let [result (set-seq final)]
                         (or (nil? result)
                             (empty? result)
                             (apply < result)))))))

;; Use spaced values for expanding processor property tests
(def gen-spaced-int
  "Generate integers spaced by 10 to ensure gaps for expanding processor"
  (gen/fmap #(* (long %) 10) (gen/choose 0 500)))

(def gen-spaced-operation
  (gen/tuple (gen/elements [:add :remove]) gen-spaced-int))

(defspec structural-invariants-with-expanding-processor 100
  (prop/for-all [elements (gen/vector gen-spaced-int 0 50)]
                (let [^Comparator cmp (comparator <)
                      settings (Settings. (int 8) nil nil (expanding-processor 4 2))
                      final (reduce pss-conj
                                    (PersistentSortedSet. nil cmp nil settings)
                                    elements)]
                  (and (validate-tree final)
                       (let [result (set-seq final)]
                         (or (nil? result)
                             (empty? result)
                             (apply < result)))))))

(defspec identity-processor-matches-reference 200
  (prop/for-all [elements (gen/vector gen-int 0 200)
                 ops (gen/vector gen-operation 0 100)]
                (let [;; Build reference with clojure sorted-set
                      ref-initial (into (sorted-set) elements)
                      ref-final (reduce (fn [s [op v]]
                                          (case op :add (conj s v) :remove (disj s v)))
                                        ref-initial ops)
          ;; Build processor set
                      ^Comparator cmp (comparator <)
                      settings (Settings. (int 4) nil nil (identity-processor))
                      pss (reduce pss-conj
                                  (PersistentSortedSet. nil cmp nil settings)
                                  elements)
                      pss-final (reduce (fn [s [op v]]
                                          (case op
                                            :add (pss-conj s v)
                                            :remove (pss-disj s v)))
                                        pss ops)]
      ;; Identity processor should produce identical results to reference
                  (and (= (vec ref-final) (or (set-seq pss-final) []))
                       (= (count ref-final) (.count ^PersistentSortedSet pss-final))
                       (validate-tree pss-final)))))

(defspec contains-consistent-after-processor-ops 100
  (prop/for-all [elements (gen/vector gen-int 0 100)
                 test-vals (gen/vector gen-int 0 30)]
                (let [^Comparator cmp (comparator <)
                      settings (Settings. (int 4) nil nil (identity-processor))
                      pss (reduce pss-conj
                                  (PersistentSortedSet. nil cmp nil settings)
                                  elements)
                      elems-set (set (or (set-seq pss) []))]
                  (every? (fn [v]
                            (= (.contains ^PersistentSortedSet pss v)
                               (contains? elems-set v)))
                          test-vals))))

(defspec transient-on-processor-tree-valid 100
  (prop/for-all [elements (gen/vector gen-int 0 100)
                 ops (gen/vector gen-operation 0 100)]
                (let [^Comparator cmp (comparator <)
                      settings (Settings. (int 4) nil nil (identity-processor))
                      pss (reduce pss-conj
                                  (PersistentSortedSet. nil cmp nil settings)
                                  elements)
                      final (persistent!
                             (reduce (fn [t [op v]]
                                       (case op
                                         :add (conj! t v)
                                         :remove (disj! t v)))
                                     (transient pss) ops))]
                  (validate-tree final))))

;; =============================================================================
;; Transient + processor correctness (processor fires on transient paths)
;; =============================================================================

(deftest test-transient-add-fires-processor
  (testing "Processor fires during transient add operations"
    (let [call-count (atom 0)
          counting-proc (reify ILeafProcessor
                          (shouldProcess [_ _leafSize _settings] true)
                          (processLeaf [_ entries _storage _settings]
                            (swap! call-count inc)
                            entries))
          ^Comparator cmp (comparator <)
          settings (Settings. (int 8) nil nil counting-proc)
          empty (PersistentSortedSet. nil cmp nil settings)
          s (persistent!
             (reduce (fn [t item] (conj! t item))
                     (transient empty)
                     (range 1 51)))]
      (is (pos? @call-count) "Processor should have been called during transient adds")
      (is (= 50 (count s)))
      (is (= (vec (range 1 51)) (set-seq s)))
      (is (validate-tree s)))))

(deftest test-transient-remove-fires-processor
  (testing "Processor fires during transient remove operations"
    (let [call-count (atom 0)
          counting-proc (reify ILeafProcessor
                          (shouldProcess [_ _leafSize _settings] true)
                          (processLeaf [_ entries _storage _settings]
                            (swap! call-count inc)
                            entries))
          s (make-set (identity-processor) 8 (range 1 51))
          ;; Replace processor with counting version
          ^Comparator cmp (comparator <)
          settings (Settings. (int 8) nil nil counting-proc)
          s-with-counter (reduce pss-conj
                                 (PersistentSortedSet. nil cmp nil settings)
                                 (set-seq s))
          _ (reset! call-count 0) ;; reset after construction
          s2 (persistent!
              (reduce (fn [t item] (disj! t item))
                      (transient s-with-counter)
                      (range 1 26)))]
      (is (pos? @call-count) "Processor should have been called during transient removes")
      (is (= 25 (count s2)))
      (is (= (vec (range 26 51)) (set-seq s2)))
      (is (validate-tree s2)))))

(deftest test-transient-compacting-processor-count-correct
  (testing "Count is correct after transient ops with compacting processor"
    (let [s (make-set (compacting-processor 3) 4 (range 1 51))
          entries (set-seq s)
          ;; Bulk remove via transient
          to-remove (take (quot (count entries) 2) entries)
          s2 (persistent!
              (reduce (fn [t item] (disj! t item))
                      (transient s)
                      to-remove))]
      ;; Count must match actual elements
      (let [result (set-seq s2)]
        (is (= (count result) (count s2))
            "count must match actual number of elements")
        (when (seq result)
          (is (apply < result) "Entries sorted"))
        (is (validate-tree s2))))))

(defspec transient-ops-with-compacting-processor 100
  (prop/for-all [elements (gen/vector gen-int 0 100)
                 ops (gen/vector gen-operation 0 100)]
                (let [^Comparator cmp (comparator <)
                      settings (Settings. (int 4) nil nil (compacting-processor 2))
                      pss (reduce pss-conj
                                  (PersistentSortedSet. nil cmp nil settings)
                                  elements)
                      final (persistent!
                             (reduce (fn [t [op v]]
                                       (case op
                                         :add (conj! t v)
                                         :remove (disj! t v)))
                                     (transient pss) ops))
                      result (set-seq final)]
                  (and (validate-tree final)
                       (= (count final) (count (or result [])))
                       (or (nil? result)
                           (empty? result)
                           (apply < result))))))

;; =============================================================================
;; Opening a diff-buf store WITH a leaf processor
;; =============================================================================
;;
;; A leaf processor rewrites a leaf's entries wholesale at materialization. A
;; diff-buf slot is a *replayable edit log* against an anchor blob. The two cannot
;; both be authoritative: replaying a diff onto a leaf the processor has already
;; rewritten reapplies edits the processor may have folded away or dropped.
;; `Settings.diffBufFor` therefore forces a processor-configured set's own budget
;; to 0 — such a set never buffers and never writes a slot.
;;
;; But `PersistentSortedSet.root()` ADOPTS the budget recorded in the restored
;; root's settings, so a processor-configured set opened against a store whose
;; nodes carry a budget would silently re-arm buffering. Refusing outright is
;; wrong too: a store can carry a non-zero budget and have nothing buffered — the
;; `:test` alias sets `-Dpss.diffBufSize=256`, so the bare `(Settings.)` that a
;; storage reconstructs nodes with picks 256 up, and 41 tests opened stores in
;; exactly that state. There was nothing to preserve in any of them.
;;
;; So the guard is keyed on what is actually BUFFERED, not on the declared budget:
;; nothing buffered means nothing to lose, stay at 0 and carry on; something
;; buffered means the two mechanisms genuinely conflict, and it must say so rather
;; than quietly return wrong contents. `bufEntries()` resolves from in-memory
;; slots and does no IO.

(defn- explicit-storage
  "A storage whose node Settings carry `dbs` explicitly, rather than inheriting the
   sysprop. Without this the fixture depends on the alias that runs it."
  [bf dbs]
  (tstore/storage-with-settings
   (Settings. (int bf) RefType/STRONG nil nil (int dbs))))

(deftest opening-a-budgeted-store-with-a-processor-is-allowed-when-nothing-is-buffered
  (testing "a non-zero budget on disk with no buffered entries is the common case
            — it must open, stay unbuffered, and read back correctly"
    (let [storage (explicit-storage 8 256)
          opts    {:comparator compare :branching-factor 8 :ref-type :strong
                   :leaf-processor (identity-processor)}
          s0      (into (set/sorted-set* opts) (range 200))
          _       (is (zero? (.diffBufSize (.-_settings ^PersistentSortedSet s0)))
                      "diffBufFor forces a processor-configured set to 0")
          addr    (set/store s0 storage)
          back    (set/restore-by compare addr storage opts)]
      (is (= (vec (range 200)) (vec (seq back)))
          "contents survive the round trip")
      (is (zero? (.diffBufSize (.-_settings ^PersistentSortedSet back)))
          "and the budget was NOT adopted — buffering stays off with a processor"))))

(deftest opening-a-store-with-buffered-entries-with-a-processor-is-refused
  (testing "when the store really does carry buffered edits the two mechanisms
            conflict, and silently dropping either one returns wrong contents"
    (let [storage (explicit-storage 16 256)
          plain   {:comparator compare :branching-factor 16 :ref-type :strong
                   :diff-buf-size 256}
          ;; build WITHOUT a processor, mutate a restored tree, store again — that
          ;; second store is what deposits slots. The SHAPE decides whether anything
          ;; is buffered at all: measured, `from-sorted-array` + 4 conjs gives
          ;; bufEntries 0 at (40,bf 8) and (200,bf 8) and 4 at (300,bf 16) and
          ;; (2000,bf 32). A fixture picked without measuring asserts nothing.
          s0      (set/from-sorted-array compare (object-array (range 0 3000 10)) 300
                                         (assoc plain :storage storage))
          a0      (set/store s0 storage)
          cold    (set/restore-by compare a0 storage plain)
          s1      (reduce #(set/conj %1 %2 compare) cold [5 15 25 35])
          a1      (set/store s1 storage)
          buffered (set/restore-by compare a1 storage plain)]
      (is (pos? (.bufEntries ^Branch (.root ^PersistentSortedSet buffered)))
          "precondition: the store really does carry buffered entries. If this
           ever stops holding the refusal below can never fire and the test is
           vacuous.")
      ;; `root()` is LAZY: restore-by only records the address, so the refusal
      ;; surfaces at the first access rather than at open. Resolving the root is
      ;; therefore part of the act being tested, not incidental setup — a try
      ;; around restore-by alone catches nothing and the test passes vacuously.
      ;; Open with a processor and NO :diff-buf-size — stratum's shape, and the
      ;; only shape that can reach this guard now. Naming a budget alongside a
      ;; processor is refused at CONSTRUCTION (see
      ;; an-explicit-budget-with-a-processor-is-refused), so the two refusals are
      ;; complementary: that one catches an incoherent request, this one catches a
      ;; coherent request meeting a store that turns out to carry buffered data.
      (let [e (try (let [s (set/restore-by compare a1 storage
                                          (-> plain
                                              (dissoc :diff-buf-size)
                                              (assoc :leaf-processor (identity-processor))))]
                     (.root ^PersistentSortedSet s)
                     nil)
                   (catch IllegalStateException e e))]
        (is (some? e) "opening it with a processor must be refused, not silently mis-read")
        ;; EVERY call, not just the first. `root()` used to publish `_root` BEFORE
        ;; running this check, and the `root == null` guard at the top means a second
        ;; entry takes the early return — so the refusal fired once and then stopped,
        ;; leaving the set at diffBufSize 0 over nodes carrying slots, which is exactly
        ;; the state it exists to prevent. Measured before the fix: 1st root() THREW,
        ;; 2nd root() NO-THROW. Every read and every write calls `root()`, so a single
        ;; `count` was enough to arm it.
        (let [again (try (let [s2 (set/restore-by compare a1 storage
                                                  (-> plain
                                                      (dissoc :diff-buf-size)
                                                      (assoc :leaf-processor (identity-processor))))]
                           (try (.root ^PersistentSortedSet s2) (catch IllegalStateException _ nil))
                           ;; second call on the SAME set
                           (.root ^PersistentSortedSet s2)
                           nil)
                         (catch IllegalStateException e2 e2))]
          (is (some? again)
              "the refusal must fire on the SECOND root() call too — a guard that
               disarms itself after one throw is worse than no guard, because the
               caller has already been told the store is unusable"))
        (is (re-find #"leafProcessor" (str (.getMessage ^IllegalStateException e)))
            (str "and the message must name the conflict, got: "
                 (some-> ^IllegalStateException e .getMessage)))))))

;; ---------------------------------------------------------------------------
;; Naming a diff-buf budget together with a processor
;;
;; The pairing corrupts (see Settings.diffBufFor), so it must not be accepted.
;; Silently zeroing it was also wrong: a caller who wrote `:diff-buf-size 256`
;; got neither an error nor buffering.
;;
;; But refusal cannot key on the VALUE, because a budget arrives from three
;; places and only one of them is a request. Measured under
;; `-Dpss.diffBufSize=256`, before the split, `(or (:diff-buf-size m)
;; (Settings/defaultDiffBufSize))` gave 256 for BOTH of these:
;;
;;     {:leaf-processor p}                      -> 256
;;     {:leaf-processor p :diff-buf-size 256}   -> 256
;;
;; So a value-keyed throw rejects stratum — which passes `:leaf-processor` and
;; never mentions diff-buf anywhere in its source — on any deployment that sets
;; the property, and rejects this file's own `(Settings. (int bf) nil nil
;; processor)` helpers, which is most of the tests above.
;;
;; The split: inheriting callers pass 0 before they reach the check (the 4-arg
;; ctor and `map->settings`), so a positive budget here means someone named one.
;; Both cases below are asserted with AND without the property, because a rule
;; about explicitness that changes with an ambient default is not one.

(defn- with-diff-buf-prop [v f]
  (let [old (System/getProperty "pss.diffBufSize")]
    (try (if v (System/setProperty "pss.diffBufSize" (str v))
             (System/clearProperty "pss.diffBufSize"))
         (f)
         (finally (if old (System/setProperty "pss.diffBufSize" old)
                      (System/clearProperty "pss.diffBufSize"))))))

(deftest an-explicit-budget-with-a-processor-is-refused
  (doseq [prop [nil 256]]
    (with-diff-buf-prop prop
      (fn []
        (testing (str "pss.diffBufSize=" prop ": naming both must fail loudly")
          (let [e (try (set/sorted-set* {:comparator compare :branching-factor 8
                                         :leaf-processor (identity-processor)
                                         :diff-buf-size 256})
                       nil
                       (catch IllegalArgumentException e e))]
            (is (some? e) "an explicit :diff-buf-size with a processor must be refused")
            (is (re-find #"leafProcessor" (str (some-> ^IllegalArgumentException e .getMessage)))
                "and the message must name the conflict")))
        (testing (str "pss.diffBufSize=" prop ": the same through the Java ctor")
          (is (thrown? IllegalArgumentException
                       (Settings. (int 8) nil nil (identity-processor) (int 256)))))))))

(deftest an-inherited-budget-with-a-processor-is-neutralized
  (doseq [prop [nil 256]]
    (with-diff-buf-prop prop
      (fn []
        (testing (str "pss.diffBufSize=" prop ": a caller who never named a budget —"
                      " stratum's shape — must keep working, unbuffered")
          (let [s (set/sorted-set* {:comparator compare :branching-factor 8
                                    :leaf-processor (identity-processor)})]
            (is (zero? (.diffBufSize (.-_settings ^PersistentSortedSet s)))
                "buffering off")
            (is (= (vec (range 50)) (vec (seq (into s (range 50)))))
                "and the set still works")))
        (testing (str "pss.diffBufSize=" prop ": the 4-arg Java ctor is an"
                      " inheriting caller too — most of this file uses it")
          (is (zero? (.diffBufSize (Settings. (int 8) nil nil (identity-processor))))))
        (testing (str "pss.diffBufSize=" prop ": an explicit 0 is not a request"
                      " for buffering and must be accepted")
          (is (zero? (.diffBufSize (Settings. (int 8) nil nil (identity-processor) (int 0))))))))))

(deftest a-budget-without-a-processor-is-untouched
  (doseq [prop [nil 256]]
    (with-diff-buf-prop prop
      (fn []
        (testing (str "pss.diffBufSize=" prop ": the no-processor path must be"
                      " unchanged in both directions")
          (is (= 256 (.diffBufSize (Settings. (int 8) nil nil nil (int 256)))))
          (is (= (or prop 0) (.diffBufSize (Settings. (int 8) nil nil nil)))
              "and an unnamed budget still follows the system property"))))))
