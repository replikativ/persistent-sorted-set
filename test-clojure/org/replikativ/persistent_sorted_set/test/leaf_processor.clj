(ns org.replikativ.persistent-sorted-set.test.leaf-processor
  (:require
   [clojure.test :as t :refer [is deftest testing]]
   [org.replikativ.persistent-sorted-set :as set])
  (:import
   [java.util Comparator List ArrayList]
   [org.replikativ.persistent_sorted_set ILeafProcessor IStorage Settings PersistentSortedSet]))

(set! *warn-on-reflection* true)

;; --- Processor implementations ---

(defn identity-processor
  "Always processes but returns entries unchanged."
  []
  (reify ILeafProcessor
    (shouldProcess [_ _leafSize _settings] true)
    (processLeaf [_ entries _storage _settings] entries)))

(defn drop-last-processor
  "Drops the last entry from leaves with more than 1 entry.
   Demonstrates compaction (reducing entry count)."
  []
  (reify ILeafProcessor
    (shouldProcess [_ leafSize _settings] (> leafSize 1))
    (processLeaf [_ entries _storage _settings]
      (.subList entries 0 (dec (.size entries))))))

(defn expanding-processor
  "Doubles each entry (adds entry+1 after each entry). Should throw."
  []
  (reify ILeafProcessor
    (shouldProcess [_ _leafSize _settings] true)
    (processLeaf [_ entries _storage _settings]
      (let [result (ArrayList.)]
        (doseq [e entries]
          (.add result e)
          (.add result (inc (long e))))
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

(deftest test-expanding-processor-throws
  (testing "Processor that expands beyond branchingFactor throws"
    (is (thrown-with-msg? IllegalStateException
                          #"exceeding branchingFactor"
                          ;; Use small branchingFactor so expansion exceeds it quickly
                          (make-set (expanding-processor) 4 (range 1 10))))))

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

(deftest test-drop-last-processor-on-remove
  (testing "Compaction processor that reduces entries during remove"
    (let [items (range 1 21)
          s (make-set (drop-last-processor) items)
          ;; The tree will have had entries compacted during add
          ;; Now remove some and verify tree stays valid
          s2 (reduce (fn [^PersistentSortedSet s item]
                       (.disjoin s item))
                     s
                     ;; Only remove entries we know are in the set
                     (filter #(.contains ^PersistentSortedSet s %) (range 1 10)))]
      ;; Tree should still be iterable and consistent
      (is (every? some? (set-seq s2))))))

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
