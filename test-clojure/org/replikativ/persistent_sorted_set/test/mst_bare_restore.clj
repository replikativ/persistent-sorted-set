(ns org.replikativ.persistent-sorted-set.test.mst-bare-restore
  "The first removal or replace on a content-defined (MST) tree restored WITHOUT `:boundary`.

   A stored node carries its boundary, and `root()` adopts it into the set's settings, so a
   caller need not repeat it at restore. But `disjoin` and `replace` asked the settings which
   algorithm to run BEFORE materializing the root. On a bare restore the settings still said
   count-based at that moment, so the first removal rebalanced by position: the right
   elements in a tree whose shape depends on history, which is what a content-defined
   boundary exists to rule out. Every later operation was fine, the root being loaded by then.

   The oracle is the same operation on the same stored tree restored WITH the boundary: the
   two results must have the same shape."
  (:require [clojure.test :refer [deftest is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.boundary :as b]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set Settings IBoundary ANode Branch PersistentSortedSet]))

(def ^:private cmp (fn [a b] (compare (first a) (first b))))

;; the STORAGE carries the boundary: restored nodes get its settings
(defn- storage [disk bf lz]
  (tstore/->Storage (atom {}) disk
                    (.withBoundary (Settings. (int bf) nil nil nil (int 0))
                                   ^IBoundary (b/mst-boundary lz))))

;; the tree as nested vectors of leaf contents: equal iff the shapes are
(defn- shape [^PersistentSortedSet s]
  (let [st (.-_storage s)]
    (letfn [(walk [^ANode n]
              (if (instance? Branch n)
                (mapv #(walk (.child ^Branch n st (int %))) (range (.len n)))
                (vec (take (.len n) (.-_keys n)))))]
      (walk (.root s)))))

(deftest the-first-write-after-a-bare-restore-keeps-the-canonical-shape
  (doseq [[bf n lz] [[8 200 2] [8 800 2] [32 800 3] [32 3000 2]]
          [label op] [[:disj-first #(set/disj % [0 0] cmp)]
                      [:disj-middle #(set/disj % [(quot n 2) 0] cmp)]
                      [:disj-many (fn [s] (reduce #(set/disj %1 [%2 0] cmp) s (range 0 n 3)))]
                      [:replace-same-key #(set/replace % [1 0] [1 1] cmp)]]]
    (let [full {:comparator cmp :branching-factor bf :boundary (b/mst-boundary lz)}
          bare {:comparator cmp :branching-factor bf}
          disk (atom {})
          s0   (reduce #(set/conj %1 [%2 0] cmp) (set/sorted-set* full) (range n))
          addr (set/store s0 (storage disk bf lz))
          with-boundary (op (set/restore-by cmp addr (storage disk bf lz) full))
          bare-restore  (op (set/restore-by cmp addr (storage disk bf lz) bare))
          lbl  (str label " bf=" bf " n=" n " lz=" lz)]
      (is (= (seq with-boundary) (seq bare-restore)) (str lbl ": same elements"))
      (is (= (shape with-boundary) (shape bare-restore))
          (str lbl ": and the same tree as a restore that named the boundary")))))
