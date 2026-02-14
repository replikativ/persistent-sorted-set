(ns org.replikativ.persistent-sorted-set.test.leaf-processor
  (:require
   [clojure.test :as t :refer [is deftest testing]]
   [org.replikativ.persistent-sorted-set :as set])
  (:import
   [java.util Comparator List ArrayList]
   [org.replikativ.persistent_sorted_set ILeafProcessor Settings PersistentSortedSet]))

(set! *warn-on-reflection* true)

;; --- Processor implementations ---

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
  "When leaf reaches target-size entries, appends extra-count unique sentinels
   above the max entry. Simulates chunk splitting that increases entry count."
  [^long target-size ^long extra-count ^long gap]
  (reify ILeafProcessor
    (shouldProcess [_ leafSize _settings] (>= (long leafSize) target-size))
    (processLeaf [_ entries _storage _settings]
      (let [^List entries entries
            mx (long (.get entries (dec (.size entries))))
            result (ArrayList. ^java.util.Collection entries)]
        (dotimes [i extra-count]
          (.add result (+ mx gap (long i))))
        result))))

;; --- Helper ---

(defn make-set
  "Create a PersistentSortedSet with the given ILeafProcessor."
  ([^ILeafProcessor processor items]
   (make-set processor 8 items))
  ([^ILeafProcessor processor branching-factor items]
   (let [settings (Settings. (int branching-factor)
                             nil nil processor)
         ^Comparator cmp (comparator <)
         empty (PersistentSortedSet. nil cmp nil settings)]
     (reduce (fn [^PersistentSortedSet s item]
               (.cons s item))
             empty
             items))))

(defn set-seq [^PersistentSortedSet s]
  (when (pos? (.count s))
    (into [] (iterator-seq (.iterator s)))))

;; --- Tests ---

(deftest test-identity-processor
  (testing "Identity processor doesn't change tree behavior"
    (let [items (range 1 101)
          s (make-set (identity-processor) items)]
      (is (= (count items) (.count ^PersistentSortedSet s)))
      (is (= (vec items) (set-seq s))))))

(deftest test-identity-processor-with-removal
  (testing "Identity processor works with disj"
    (let [items (range 1 51)
          s (make-set (identity-processor) items)
          s2 (reduce (fn [^PersistentSortedSet s item]
                       (.disjoin s item))
                     s (range 10 30))]
      (is (= 30 (.count ^PersistentSortedSet s2)))
      (is (= (vec (concat (range 1 10) (range 30 51)))
             (set-seq s2))))))

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
      (is (= (vec (range 1 201)) (set-seq s2))))))

(deftest test-compacting-processor-triggers-merge
  (testing "Compaction that shrinks entries below minBF triggers merge/borrow"
    (let [;; BF=8, minBF=4. Compacting processor fires when leaf > 3 entries.
          ;; It keeps every other entry, halving the count.
          s (make-set (compacting-processor 3) 8 (range 1 51))
          ;; Tree should be valid and iterable (entries may be fewer due to compaction)
          result (set-seq s)]
      (is (seq result) "Set should have some entries")
      (is (apply < result) "Entries should be sorted")
      ;; Now remove some entries and verify tree stays valid
      (let [to-remove (take 5 result)
            s2 (reduce (fn [^PersistentSortedSet s item]
                         (.disjoin s item))
                       s to-remove)
            result2 (set-seq s2)]
        (is (or (nil? result2) (apply < result2)) "Entries should be sorted after removal")
        (is (every? (fn [x] (not (some #{x} to-remove))) (or result2 []))
            "Removed entries should be gone")))))

(deftest test-compacting-processor-remove-all
  (testing "Compaction during removal until set is empty"
    (let [s (make-set (compacting-processor 3) 8 (range 1 21))
          entries (set-seq s)
          ;; Remove all remaining entries one by one
          s2 (reduce (fn [^PersistentSortedSet s item]
                       (if (.contains s item)
                         (.disjoin s item)
                         s))
                     s entries)]
      (is (= 0 (.count ^PersistentSortedSet s2))))))

(deftest test-expanding-processor-causes-split
  (testing "Expansion that grows entries beyond BF triggers proper split"
    (let [;; BF=8. When leaf reaches 4 entries, add 6 more sentinels (total 10 > BF=8).
          ;; Gap of 10000 ensures sentinels don't collide with real entries.
          s (make-set (expanding-processor 4 6 10000) 8 (range 1 5))]
      ;; Adding 4 entries: leaf hits 4, processor adds 6 sentinels (total 10), splits
      (is (> (.count ^PersistentSortedSet s) 4)
          "Set should have more entries than added (processor expanded)")
      (let [result (set-seq s)]
        (is (apply < result) "Entries should be sorted")
        ;; Original entries should be present
        (doseq [i (range 1 5)]
          (is (.contains ^PersistentSortedSet s (long i))
              (str "Original entry " i " should be present")))))))

(deftest test-expanding-processor-with-removal
  (testing "Expanded set handles removal correctly"
    (let [s (make-set (expanding-processor 4 4 10000) 8 (range 1 5))
          entries (set-seq s)
          ;; Remove first 2 real entries
          s2 (reduce (fn [^PersistentSortedSet s item]
                       (.disjoin s item))
                     s [1 2])
          result (set-seq s2)]
      (is (not (.contains ^PersistentSortedSet s2 (long 1))))
      (is (not (.contains ^PersistentSortedSet s2 (long 2))))
      (is (.contains ^PersistentSortedSet s2 (long 3)))
      (is (apply < result) "Entries should remain sorted"))))

(deftest test-processor-preserves-tree-structure
  (testing "Large set with identity processor maintains structure"
    (let [items (range 1 1001)
          s (make-set (identity-processor) 32 items)]
      (is (= 1000 (.count ^PersistentSortedSet s)))
      (is (= 1 (long (first (set-seq s)))))
      (is (= 1000 (long (last (set-seq s))))))))

(deftest test-processor-with-interleaved-add-remove
  (testing "Interleaved add/remove with identity processor"
    (let [s (make-set (identity-processor) 8 (range 1 51))
          ;; Remove some
          s2 (reduce (fn [^PersistentSortedSet s item]
                       (.disjoin s item))
                     s (range 20 40))
          ;; Add back different values
          s3 (reduce (fn [^PersistentSortedSet s item]
                       (.cons s item))
                     s2 (range 60 80))]
      (is (= (count (distinct (concat (range 1 20) (range 40 51) (range 60 80))))
             (.count ^PersistentSortedSet s3))))))

(deftest test-no-processor-baseline
  (testing "Null processor has zero overhead (no change to behavior)"
    (let [settings (Settings. (int 8) nil nil nil)
          ^Comparator cmp (comparator <)
          s (reduce (fn [^PersistentSortedSet s item]
                      (.cons s item))
                    (PersistentSortedSet. nil cmp nil settings)
                    (range 1 51))]
      (is (= 50 (.count ^PersistentSortedSet s)))
      (is (= (vec (range 1 51)) (set-seq s))))))
