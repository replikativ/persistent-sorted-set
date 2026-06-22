(ns org.replikativ.persistent-sorted-set.test.mst
  "Content-defined (Merkle Search Tree) boundary mode — the history-independence property
   that motivates the split-seam: same key set ⇒ identical node structure regardless of
   insert / merge / remove order. Runs on BOTH platforms (cljc) — the same battery exercising
   the JVM and the cljs implementations, which must produce identical structures. See
   .internal/SPLIT_SEAM_DESIGN.md."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [org.replikativ.persistent-sorted-set :as pss]
            [org.replikativ.persistent-sorted-set.boundary :as b]
            #?@(:cljs [[org.replikativ.persistent-sorted-set.leaf :refer [Leaf]]
                       [org.replikativ.persistent-sorted-set.branch :refer [Branch]]
                       [org.replikativ.persistent-sorted-set.arrays :as arrays]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set ANode Leaf Branch PersistentSortedSet])))

(def ^:const lzpl 4) ;; small fanout (~16) so trees branch at modest N

(defn- mkb [] (b/mst-boundary lzpl))
(defn- mst-set [ks] (reduce conj (pss/sorted-set* {:boundary (mkb)}) ks))

;; ---- portable node access (JVM Java fields vs cljs deftype fields) ---------------------
(defn- node-len [node]
  #?(:clj (.-_len ^ANode node) :cljs (arrays/alength (.-keys node))))
(defn- key-at [node i]
  #?(:clj (aget ^objects (.-_keys ^ANode node) i) :cljs (aget (.-keys node) i)))
(defn- node-level [node]
  #?(:clj (.-_level ^Branch node) :cljs (.-level node)))
(defn- node-child [node i]
  #?(:clj (aget ^objects (.-_children ^Branch node) i) :cljs (aget (.-children node) i)))
(defn- root-of [s]
  #?(:clj (.root ^PersistentSortedSet s) :cljs (.-root s)))

(defn shape
  "Canonical structural fingerprint: nested vectors of leaf key-runs + branch levels."
  [node]
  (if (instance? Leaf node)
    [:L (mapv #(key-at node %) (range (node-len node)))]
    [:B (node-level node) (mapv #(shape (node-child node %)) (range (node-len node)))]))

(defn root-shape [s] (shape (root-of s)))

(defn- leaves [node]
  (if (instance? Leaf node)
    [node]
    (mapcat #(leaves (node-child node %)) (range (node-len node)))))

;; ---------------------------------------------------------------------------------------

(deftest order-independence-conj
  (testing "same key set, different insert orders ⇒ identical structure"
    (let [n 2000
          ks (vec (range n))
          orders [ks (vec (reverse ks)) (shuffle ks) (shuffle ks)
                  (concat (shuffle (take (quot n 2) ks)) (shuffle (drop (quot n 2) ks)))]
          shapes (map (comp root-shape mst-set) orders)]
      (is (apply = shapes) "all insert orders yield the identical node tree")
      (is (every? #(= (seq (mst-set %)) (seq (range n))) orders) "elements correct & sorted"))))

(deftest bulk-equals-incremental
  (testing "from-sorted-array (bulk) builds the identical tree as incremental conj"
    (let [n 3000
          ks (range n)
          incremental (mst-set (shuffle ks))
          bulk (pss/from-sorted-array compare (object-array ks) n {:boundary (mkb)})]
      (is (= (root-shape bulk) (root-shape incremental)))
      (is (= (seq bulk) (seq (sort ks)))))))

(deftest remove-history-independence
  (testing "build-then-remove ≡ fresh build of the survivors (structure), any order"
    (let [n 2500
          uni (vec (range n))
          drop-set (set (take (quot n 3) (shuffle uni)))
          survivors (sort (remove drop-set uni))
          results (for [bo [(sort uni) (reverse (sort uni)) (shuffle uni)]
                        ro [(sort drop-set) (shuffle (vec drop-set))]]
                    (let [s (reduce disj (mst-set bo) ro)]
                      {:elems (seq s) :shape (root-shape s)}))
          fresh (mst-set (shuffle survivors))]
      (is (every? #(= (:elems %) survivors) results) "elements correct after removes")
      (is (apply = (map :shape results)) "all build/remove orders ⇒ identical structure")
      (is (= (:shape (first results)) (root-shape fresh))
          "removed tree is byte-identical to a fresh build of the survivors"))))

(deftest remove-stress
  ;; Many random (build-order, remove-order) trials at depth ≥3. The "boundary key alone in
  ;; its leaf, dropped, then the grandparent merges" case is rare (~1 in 15 trials), so a single
  ;; deterministic pass misses it — this stress reliably exercises mst-merge-with's junction guard.
  (testing "remove stays canonical (== fresh build of survivors) across many random trials"
    (let [n 1500
          fails (atom 0)]
      (dotimes [_ 60]
        (let [uni (vec (range n))
              drop-set (set (take (quot n 3) (shuffle uni)))
              survivors (sort (remove drop-set uni))
              removed (reduce disj (mst-set (shuffle uni)) (shuffle (vec drop-set)))
              fresh (mst-set (shuffle survivors))]
          (when (or (not= (seq removed) survivors)
                    (not= (root-shape removed) (root-shape fresh)))
            (swap! fails inc))))
      (is (zero? @fails) (str @fails "/60 random remove trials diverged from a fresh build")))))

(deftest remove-to-empty-and-readd
  (let [n 1500 uni (vec (range n))
        emptied (reduce disj (mst-set (shuffle uni)) (shuffle uni))]
    (is (= 0 (count emptied)))
    (is (= (seq (reduce conj emptied (shuffle uni))) (seq (sort uni))))))

(deftest leaf-boundary-invariant
  (testing "every non-rightmost leaf terminates on a key whose level ≥ 1 (a real boundary)"
    (let [s (mst-set (shuffle (range 4000)))
          ls (leaves (root-of s))
          interior (butlast ls)]
      (is (every? (fn [lf] (>= (b/key-level (key-at lf (dec (node-len lf))) lzpl) 1)) interior)
          "interior leaf terminators are all boundary keys"))))

(deftest count-mode-unaffected
  (testing "no :boundary ⇒ default count B-tree, all elements correct"
    (let [ks (shuffle (range 3000))
          a (reduce conj (pss/sorted-set* {}) ks)]
      (is (= (seq a) (seq (sort (range 3000))))))))

(defspec prop-order-independence 20
  (prop/for-all [ks (gen/such-that seq (gen/set gen/large-integer))]
    (let [v (vec ks)
          shapes (map (comp root-shape mst-set) [(sort v) (reverse (sort v)) (shuffle v)])]
      (and (apply = shapes)
           (= (seq (mst-set (shuffle v))) (seq (sort v)))))))

(defspec prop-remove-independence 20
  (prop/for-all [ks (gen/such-that #(> (count %) 2) (gen/set gen/large-integer))
                 seed gen/nat]
    (let [v (vec ks)
          drop-set (set (take (mod seed (count v)) (shuffle v)))
          survivors (sort (remove drop-set v))
          removed (reduce disj (mst-set (shuffle v)) (shuffle (vec drop-set)))
          fresh (mst-set (shuffle survivors))]
      (and (= (seq removed) survivors)
           (= (root-shape removed) (root-shape fresh))))))
