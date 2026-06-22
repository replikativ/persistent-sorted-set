(ns org.replikativ.persistent-sorted-set.test.mst
  "Content-defined (Merkle Search Tree) boundary mode — the history-independence property
   that motivates the split-seam: same key set ⇒ identical node structure regardless of
   insert / merge / remove order. Runs on BOTH platforms (cljc) — the same battery exercising
   the JVM and the cljs implementations, which must produce identical structures. See
   .internal/SPLIT_SEAM_DESIGN.md."
  (:require [clojure.test :refer [deftest is testing are]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [org.replikativ.persistent-sorted-set :as pss]
            [org.replikativ.persistent-sorted-set.boundary :as b]
            [hasch.core :as hasch]
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

(defn- merkle
  "The tree's content address, computed bottom-up exactly like a content-addressed store
   (hasch/uuid over each node's {:level :keys :addresses}). hasch is deterministic JVM↔cljs."
  [node]
  (if (instance? Leaf node)
    (hasch/uuid {:level 0 :keys (mapv #(key-at node %) (range (node-len node)))})
    (hasch/uuid {:level     (node-level node)
                 :keys      (mapv #(key-at node %) (range (node-len node)))
                 :addresses (mapv #(merkle (node-child node %)) (range (node-len node)))})))

(defn- node-max
  "A node's maximum key = its last key (leaf) / last separator (branch)."
  [node] (key-at node (dec (node-len node))))

(defn- leaf-depths [node d]
  (if (instance? Leaf node)
    [d]
    (mapcat #(leaf-depths (node-child node %) (inc d)) (range (node-len node)))))

(defn mst-violation
  "Structural-invariant validator for canonical MST (test B). Returns nil if the subtree
   rooted at `node` is a canonical Merkle Search Tree for the given `lz` (leadingZeros-per-
   level), else a map describing the first violation. Checks the MST-specific properties that
   element-equality cannot see:
     1. no MISSING split: every interior leaf key (all but the node's own max) is level-0 —
        a boundary key inside a leaf would mean the leaf should have been split there;
     2. no MISSING/EXTRA split at branches: every interior separator (all but the node's own
        max) is a boundary at the branch's level — i.e. key-level >= branch level, so its
        child is correctly boundary-terminated and the branch is split at exactly the boundaries;
     3. B+ shape: each separator equals its child's max key;
     4. height-balance: all leaves sit at the same depth.
   The node's own max key (last index) is exempt at every level — it is validated by the
   parent's separator check, or is the global rightmost spine max (no boundary required)."
  [node lz]
  (or
    ;; (4) height-balance — check once at the top
   (let [ds (leaf-depths node 0)]
     (when-not (apply = ds) {:unbalanced-leaf-depths (distinct ds)}))
   (letfn [(walk [node]
             (if (instance? Leaf node)
                ;; (1) interior leaf keys must NOT be boundaries
               (first (for [i (range (dec (node-len node)))
                            :when (>= (b/key-level (key-at node i) lz) 1)]
                        {:leaf-interior-boundary (key-at node i) :index i}))
               (let [n (node-len node) lvl (node-level node)]
                 (or
                    ;; (2) interior separators must be boundaries at this level
                  (first (for [i (range (dec n))
                               :when (< (b/key-level (key-at node i) lz) lvl)]
                           {:separator-not-boundary (key-at node i) :level lvl :index i}))
                    ;; (3) separator == child max
                  (first (for [i (range n)
                               :when (not= (key-at node i) (node-max (node-child node i)))]
                           {:separator-mismatch i :level lvl}))
                    ;; recurse
                  (first (keep #(walk (node-child node %)) (range n)))))))]
     (walk node))))

(defn- mst-set-lz [ks lz] (reduce conj (pss/sorted-set* {:boundary (b/mst-boundary lz)}) ks))

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

(deftest transient-add-canonical
  (testing "transient batch conj (the editable in-place path) still splits at boundaries"
    ;; yggdrasil's set-union uses (into a b), which conj!'s through a transient. The in-place
    ;; add shortcut must NOT be taken in MST mode or boundary keys wouldn't split.
    (let [n 4000
          ks (vec (range n))
          via-transient (persistent! (reduce conj! (transient (pss/sorted-set* {:boundary (mkb)})) (shuffle ks)))
          via-persistent (mst-set (shuffle ks))]
      (is (= (seq via-transient) (seq ks)) "elements correct")
      (is (= (root-shape via-transient) (root-shape via-persistent))
          "transient build is canonical (identical tree to persistent conj)"))))

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

(deftest cross-platform-structural-identity
  ;; The full tree's content address (Merkle hash of the whole structure) must equal the
  ;; JVM-computed reference on BOTH platforms — the gold-standard proof that JVM and cljs build
  ;; the byte-identical tree (not just matching key-levels) and hash it identically. A cljs
  ;; structural OR hashing divergence fails this. Built in shuffled order to also assert
  ;; order-independence reaches the same address.
  (testing "root Merkle address matches the JVM reference on this platform"
    (are [n lz expected]
         (= expected (str (merkle (root-of (mst-set-lz (shuffle (vec (range n))) lz)))))
      5000 4 "3763fc7d-cef1-5a60-a65d-46bce9dcb393"
      5000 6 "2c6ecf6a-0883-5da7-8dab-6309a819cedb"
      1000 5 "2c95d9db-32db-57a7-8b56-480de2cfd1b8")))

(deftest count-mode-unaffected
  (testing "no :boundary ⇒ default count B-tree, all elements correct"
    (let [ks (shuffle (range 3000))
          a (reduce conj (pss/sorted-set* {}) ks)]
      (is (= (seq a) (seq (sort (range 3000))))))))

(defspec prop-order-independence 20
  (prop/for-all [ks (gen/such-that seq (gen/set gen/large-integer))]
                (let [v (vec ks)
                      sets (map mst-set [(sort v) (reverse (sort v)) (shuffle v)])]
                  (and (apply = (map root-shape sets))
                       (every? #(nil? (mst-violation (root-of %) lzpl)) sets)
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
                       (nil? (mst-violation (root-of removed) lzpl))
                       (= (root-shape removed) (root-shape fresh))))))

;; =============================================================================
;; Model-based testing under INTERLEAVED operations (test A).
;;
;; The analog of generative.cljc's `operations-match-sorted-set`, specialized to MST.
;; A random INTERLEAVED sequence of conj/disj (not phased build-then-remove) drives the
;; MST in lock-step with clojure.core/sorted-set, then we assert all three guarantees at
;; once on the final tree:
;;   (a) ELEMENTS:      same contents as the sorted-set oracle;
;;   (b) INVARIANTS:    the tree is a canonical MST (mst-violation = nil);
;;   (c) CANONICALITY:  identical structure to a FRESH build of the survivors — i.e. full
;;                      history-independence, now over arbitrary interleaved op orders, the
;;                      regime the phased order/remove specs above never reach.
;; Uses a small fanout (lzpl 3 ⇒ B≈8) so even modest N produces multi-level trees that
;; exercise branch splits, merges, single-child spines and root grow/shrink. Runs on BOTH
;; platforms; the cljs and JVM trees must be byte-identical for (c) to hold cross-platform.
;; =============================================================================

(def ^:const ilz 3)
(defn- iset [ks] (mst-set-lz ks ilz))

(def ^:private gen-op
  (gen/tuple (gen/elements [:add :remove]) (gen/choose -400 400)))

(defspec prop-interleaved-ops-match-and-canonical 60
  (prop/for-all [init (gen/vector (gen/choose -400 400) 0 200)
                 ops  (gen/vector gen-op 0 400)]
                (let [[pss clj] (reduce (fn [[p c] [op v]]
                                          (case op
                                            :add    [(conj p v) (conj c v)]
                                            :remove [(disj p v) (disj c v)]))
                                        [(iset init) (into (sorted-set) init)]
                                        ops)
                      survivors (vec clj)]
                  (and (= (vec pss) survivors)                                  ; (a) elements
                       (nil? (mst-violation (root-of pss) ilz))                 ; (b) invariants
                       (= (root-shape pss)                                      ; (c) canonical
                          (root-shape (iset (shuffle survivors))))))))
