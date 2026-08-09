(ns org.replikativ.persistent-sorted-set.test.diagnostics-reach
  "The oracle must reach the tree it is pointed at, and must not hand out slots that are not
   elements.

   Three defects, all of them shapes where a check reports confidently about something it
   never looked at.

   1. On ClojureScript `get-root` read `(.-root set)` — a raw FIELD that is nil until `-root`
      restores it — while the JVM called the `root()` METHOD, which materializes. So every
      entry point in `diagnostics` saw an empty tree on a cold set and pronounced it healthy.
      Measured on a 5000-element cold restore, identical for plain and MST trees:

          validate / validate-full / validate-navigation / validate-counts-known
          / validate-measures-known / validate-content   ALL true
          validate-content called the user's content-fn ZERO times
          tree-stats  {:element-count 0 :leaf-count 0 :branch-count 0 :counts-known? true}
          verification-coverage {:branches 0 :counts-verified 0 :counts-skipped 0}

      The coverage line is the worst of them: that function exists so a caller can tell a
      real pass from a vacuous one, and it reported NOTHING SKIPPED for a tree where nothing
      had been looked at. One `contains?` flipped the same set to `{:branches 6
      :counts-skipped 5}`.

   2. `check-sizes` asserted the B-tree bound `[bf/2, bf]` on MST trees, whose node sizes are
      decided by a content-defined boundary and not by `bf` at all. Over n = 1..299 it threw
      for 297 of 299 sizes at level-probability 2 and 3, and 248 of 299 at 4 — on HEALTHY
      trees. `validate-content` calls `validate-full` first, so the Datahike-facing entry
      point threw on MST too, and the only whole-tree oracle could not be aimed at MST at all.

   3. `leaf-keys-array` handed `validate-content` the RAW backing array. `ANode` documents it
      as valid only in `[0, _len)` and `ANode.keys()` truncates; this accessor did not. After
      20000 elements and 10000 random `disj!`: 14903 slots handed over for 10000 live
      elements at bf 8 — 49% surplus. Before tail-clearing those slots held the REMOVED
      elements, so a \"does every datom still exist?\" callback was given deleted datoms as
      live members; since tail-clearing they are nil, so the same callback NPEs instead."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.diagnostics :as diag]
            [org.replikativ.persistent-sorted-set.boundary :as b]
            #?(:clj  [org.replikativ.persistent-sorted-set.test.storage :as tstore]
               :cljs [org.replikativ.persistent-sorted-set.test.storage.util :as util]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set Settings IBoundary])))

(defn- elems [s] (vec #?(:clj (seq s) :cljs (set/seq s))))

(defn- storage [disk bf lz]
  #?(:clj  (tstore/->Storage (atom {}) disk
                             (cond-> (Settings. (int bf) nil nil nil (int 0))
                               lz (.withBoundary ^IBoundary (b/mst-boundary lz))))
     :cljs (util/storage (atom {}) disk (cond-> {:branching-factor bf :diff-buf-size 0}
                                          lz (assoc :boundary (b/mst-boundary lz))))))

(defn- restore* [addr st opts]
  #?(:clj (set/restore-by compare addr st opts) :cljs (set/restore addr st opts)))

(defn- build [bf n lz]
  (let [opts (cond-> {:comparator compare :branching-factor bf :diff-buf-size 0}
               lz (assoc :boundary (b/mst-boundary lz)))]
    (reduce #(set/conj %1 %2 compare) (set/sorted-set* opts) (range n))))

;; ---------------------------------------------------------------------------
;; 1. the oracle must MATERIALIZE the root before judging the tree

(deftest a-cold-tree-is-not-reported-as-an-empty-healthy-one
  (testing "reading the root as a raw field made every check vacuous on ClojureScript"
    (doseq [[bf n lz] [[8 2000 nil] [8 2000 2] [8 2000 4]]]
      (let [disk (atom {})
            opts (cond-> {:branching-factor bf :diff-buf-size 0}
                   lz (assoc :boundary (b/mst-boundary lz)))
            addr (set/store (build bf n lz) (storage disk bf lz))
            cold (restore* addr (storage disk bf lz) opts)
            stats (diag/tree-stats cold)
            cov   (diag/verification-coverage cold)
            label (str "bf=" bf " n=" n " lz=" lz)]
        ;; The tree really is cold: nothing has forced it yet beyond what diagnostics does.
        (is (pos? (:branch-count stats))
            (str label ": tree-stats must see a real tree, not an empty one: " stats))
        (is (pos? (:branches cov))
            (str label ": coverage must report the branches it looked at: " cov))
        (is (true? (diag/validate-full cold))
            (str label ": and the healthy tree must still validate"))))))

;; ---------------------------------------------------------------------------
;; 2. MST trees must be judged by an invariant they can satisfy

(deftest an-mst-tree-validates
  (testing "a content-defined boundary decides node size, so the [bf/2, bf] bound is simply
            the wrong invariant — it rejected 297 of 299 healthy sizes at lzpl 2"
    (doseq [lz [2 3 4]
            n  [3 20 60 200]]
      (let [s (build 32 n lz)]
        (is (true? (diag/validate s))
            (str "lzpl=" lz " n=" n ": a healthy MST tree must validate"))
        (is (true? (diag/validate-full s))
            (str "lzpl=" lz " n=" n ": and pass the full check"))
        (is (= (range n) (elems s))
            (str "lzpl=" lz " n=" n ": precondition — contents intact"))))))

(deftest the-b-tree-bound-still-applies-to-b-trees
  (testing "the control: relaxing MST must not relax ordinary trees. Corrupt a node's length
            and a plain B-tree must still be refused."
    (let [s (build 8 200 nil)]
      (is (true? (diag/validate s)) "healthy first")
      #?(:clj
         (let [^org.replikativ.persistent_sorted_set.Branch root
               (.root ^org.replikativ.persistent_sorted_set.PersistentSortedSet s)
               ^org.replikativ.persistent_sorted_set.ANode c0 (.child root nil (int 0))
               orig (.-_len c0)]
           (set! (.-_len c0) (int 1))            ; below bf/2, and not an MST tree
           (is (thrown? AssertionError (diag/validate s))
               "a B-tree node below bf/2 must still be caught")
           (set! (.-_len c0) (int orig)))))))

;; ---------------------------------------------------------------------------
;; 3. validate-content must be handed elements, not backing slots

(deftest validate-content-sees-only-live-elements
  (testing "the raw array is valid only in [0, _len); after transient churn roughly half of
            it is surplus, and the user's callback was given all of it"
    (doseq [bf [8 64]]
      (let [n 4000
            s0 (build bf n nil)
            victims (vec (take (quot n 2) (shuffle (range n))))
            s  #?(:clj  (persistent! (reduce disj! (transient s0) victims))
                  :cljs (reduce #(set/disj %1 %2 compare) s0 victims))
            seen (atom [])]
        (diag/validate-content s (fn [ks] (swap! seen into (vec ks)) nil))
        (is (= (clojure.core/count (elems s)) (clojure.core/count @seen))
            (str "bf=" bf ": the callback must see exactly as many slots as there are "
                 "elements, got " (clojure.core/count @seen) " for "
                 (clojure.core/count (elems s))))
        (is (not-any? nil? @seen)
            (str "bf=" bf ": and never a nil, which is what a cleared surplus slot is"))
        (is (= (elems s) (sort @seen))
            (str "bf=" bf ": and exactly the live elements"))))))
