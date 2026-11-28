(ns org.replikativ.persistent-sorted-set.test.portable-nodes
  (:require [clojure.test :refer [deftest is]]
            [org.replikativ.persistent-sorted-set :as java]
            [org.replikativ.persistent-sorted-set.branch :as branch]
            [org.replikativ.persistent-sorted-set.impl.delta :as delta]
            [org.replikativ.persistent-sorted-set.impl.measure :as m]
            [org.replikativ.persistent-sorted-set.impl.node :as node]
            [org.replikativ.persistent-sorted-set.impl.range :as ranges]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage]
            [org.replikativ.persistent-sorted-set.leaf :as leaf]
            [org.replikativ.persistent-sorted-set.test.portable-storage :refer [backend]]
            [org.replikativ.persistent-sorted-set.test.stored-measures :as stored])
  (:import [org.replikativ.persistent_sorted_set IMeasure]))

(defrecord StoredRoot [address comparator])

(def opts {:sync? true})
(defn- weight-of [[_ i]] (mod i 4))

(def portable-measure
  (reify m/IMeasure
    (identity-measure [_] 0)
    (extract [_ k] (weight-of k))
    (merge-measure [_ a b] (+ a b))
    (remove-measure [_ v k _] (- v (weight-of k)))
    (weight [_ v] v)))

(def java-measure
  (reify IMeasure
    (identity [_] 0)
    (extract [_ k] (weight-of k))
    (merge [_ a b] (+ a b))
    (remove [_ v k _] (- v (weight-of k)))
    (weight [_ v] v)))

(defn- insert [root k]
  (if-let [pieces (node/add root nil k compare opts)]
    (if (= 1 (alength pieces))
      (aget pieces 0)
      (branch/promote-root pieces (.-settings root) compare -1))
    root))

(defn- remove-key [root k]
  (if-let [pieces (node/$remove root nil k nil nil compare opts)]
    (let [r (aget pieces 0)]
      (if (and (pos? (node/level r)) (= 1 (node/len r)))
        (branch/child r nil 0 opts)
        r))
    root))

(deftest retained-and-restored-roots
  (let [settings {:branching-factor 4 :diff-buf-size 0}
        entries (mapv (fn [i] [(quot i 8) i]) (range 80))
        shuffled (mapv #(nth entries (mod (* 17 %) 80)) (range 80))
        by-group #(compare (first %1) (first %2))
        java-settings (assoc settings :measure java-measure)
        java-store (stored/->MStorage (atom {}) (atom 0) (atom 0)
                                      (nodes/reader-context {:measure-ops java-measure :default-bf 4}))
        java-root (reduce conj (java/sorted-set* java-settings) shuffled)
        portable-root (reduce insert (leaf/empty-leaf (assoc settings :measure portable-measure)) shuffled)
        disk (atom {})
        reads (atom [])
        store (backend disk reads)
        removed (subvec entries 15 68)
        versions [[java-root portable-root]
                  [(reduce disj java-root removed) (reduce remove-key portable-root removed)]]
        saved (mapv (fn [[j p]] [(java/store j java-store) (node/store p store opts)]) versions)]
    (let [[[ja pa] [jb pb]] saved
          java-open #(java/restore-by compare % java-store java-settings)
          root-of (fn [tree opts] (storage/restore store (:address tree) opts))]
      (doseq [[from to p q] [[ja jb pa pb] [jb ja pb pa]]]
        (is (= (java/diff (java-open from) (java-open to))
               (delta/diff root-of (->StoredRoot p compare) (->StoredRoot q compare) store opts)))))
    (doseq [[java-address address] saved]
      (let [java-tree (java/restore-by compare java-address java-store java-settings)
            root (storage/restore store address opts)
            total (java/measure java-tree)]
        (reset! reads [])
        (is (= (java/get-nth java-tree 12) (branch/weighted-entry root store portable-measure 12 opts)))
        (is (= (node/level root) (count @reads)) "exactly one child per level")
        (doseq [i (range -1 (inc total))]
          (is (= (java/get-nth java-tree i) (branch/weighted-entry root store portable-measure i opts))))
        (doseq [[lo hi] [[nil nil] [[2] [6]] [[4] [4]] [nil [0]] [[9] nil]]]
          (is (= (java/slice java-tree lo hi by-group) (ranges/slice root store lo hi by-group)))
          (is (= (java/rslice java-tree hi lo by-group) (ranges/rslice root store hi lo by-group))))))))
