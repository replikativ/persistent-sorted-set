(ns org.replikativ.persistent-sorted-set.btset
  (:refer-clojure :exclude [iter sorted-set-by])
  (:require-macros [org.replikativ.persistent-sorted-set.macros :refer [async+sync]])
  (:require [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [is.simm.partial-cps.sequence :as aseq]
            [org.replikativ.persistent-sorted-set.arrays :as arrays]
            [org.replikativ.persistent-sorted-set.branch :as branch :refer [Branch]]
            [org.replikativ.persistent-sorted-set.leaf :as leaf :refer [Leaf]]
            [org.replikativ.persistent-sorted-set.impl.node :as node]
            [org.replikativ.persistent-sorted-set.impl.measure :as measure]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage]
            [org.replikativ.persistent-sorted-set.impl.boundary :as b]
            [org.replikativ.persistent-sorted-set.util :refer [rotate lookup-exact splice cut-n-splice binary-search-l binary-search-r return-array merge-n-split check-n-splice]]))

(declare BTSet)

(def ^:const UNINITIALIZED_HASH nil)

(declare -root)

(defn root-node
  "Materialize and return the root node, restoring it from storage if this set is cold.

   Exists for `diagnostics`, which must not read `(.-root set)` directly: that field is nil
   until `-root` restores it, so every check in that namespace silently saw an EMPTY tree and
   reported a cold set as healthy — `validate-full` true, `tree-stats` `:element-count 0`,
   and `verification-coverage` claiming nothing was even skipped. The JVM half calls the
   `root()` METHOD, which materializes, so this was a pure cross-runtime accident."
  [^BTSet set]
  (-root set {:sync? true}))

(defn- -root
  [^BTSet set {:keys [sync?] :or {sync? true} :as opts}]
  (assert (or (some? (.-address set)) (some? (.-root set))))
  (async+sync sync?
              (async
               (do
                 (when (and (nil? (.-root set)) (some? (.-address set)))
                   (assert (implements? storage/IStorage (.-storage set)))
                   (set! (.-root set) (await (storage/restore (.-storage set) (.-address set) opts)))
                   ;; self-describing BRANCHING FACTOR, mirroring the JVM's root(). A node
                   ;; knows the width it was written at and the caller cannot be expected to
                   ;; remember it, so reopening a store with a different (or defaulted)
                   ;; :branching-factor otherwise leaves the SET believing one width while its
                   ;; nodes have another. On the JVM that is loud — an -ea AssertionError, or
                   ;; ArrayIndexOutOfBounds in Stitch.copyAll without -ea. Here it is silent,
                   ;; because cljs arrays are exact-sized and split/rebalance read the width
                   ;; from the NODE's settings: nothing overflows, but a root grown by conjoin
                   ;; is built from the SET's settings, so a post-restore tree can carry a root
                   ;; at one width over children at another and diverge in SHAPE from the JVM
                   ;; for the same operations.
                   ;;
                   ;; Adopted UNCONDITIONALLY rather than only upward from a default, exactly
                   ;; as the JVM does: there is no safe way to run at a width the data
                   ;; contradicts. The boundary and diff-buf adoptions below are the same
                   ;; principle; this one was simply never ported (f3e977a is JVM-only).
                   (let [node-bf (:branching-factor (.-settings (.-root set)))]
                     (when (and (number? node-bf)
                                (not= node-bf (:branching-factor (.-settings set))))
                       (set! (.-settings set)
                             (assoc (.-settings set) :branching-factor node-bf))))
                   ;; self-describing boundary: a restored node carries its split strategy; adopt
                   ;; it so conj/disj use the right splitter even when restore opts omitted it.
                   (let [nb (b/content-boundary (.-settings (.-root set)))]
                     (when (and nb (not (b/content-boundary (.-settings set))))
                       ;; nb is content-defined ⇒ force diff-buf OFF too (mirrors the JVM root()
                       ;; adoption via Settings.withBoundary). Otherwise a set restored with
                       ;; :diff-buf-size > 0 would run MST + diff-buf together, breaking canonical
                       ;; addressing. See doc/merkle-search-tree.md (Incompatibilities).
                       (set! (.-settings set) (assoc (.-settings set) :boundary nb :diff-buf-size 0))))
                   ;; diff-buf: adopt the NODE's budget too, mirroring the JVM's
                   ;; PersistentSortedSet.root(). A node is self-describing, and a set
                   ;; restored WITHOUT :diff-buf-size ran at 0 over nodes at N: reads were
                   ;; fine (projection is driven by the node's own settings through child),
                   ;; but the next write rebuilt through the SET's settings and dropped
                   ;; every surviving sibling's buffered elements. The JVM records the
                   ;; measurement for this exact shape — bf 16, budget 512, 6000 elements,
                   ;; stored WITH slots then restored bare: 81 elements silently gone.
                   ;; ClojureScript adopted the boundary above and not this, so the bare-
                   ;; address restore path was exposed. (The root-BLOB path already carries
                   ;; the budget via impl.nodes; only a restore from a bare address was.)
                   ;;
                   ;; Skipped under a content-defined boundary: the branch above has just
                   ;; forced buffering off there, and MST + diff-buf is incompatible.
                   (let [node-dbs (:diff-buf-size (.-settings (.-root set)))
                         set-dbs  (:diff-buf-size (.-settings set))]
                     (when (and (number? node-dbs) (pos? node-dbs)
                                (not (pos? (or set-dbs 0)))
                                (not (b/content-boundary (.-settings set))))
                       (set! (.-settings set)
                             (assoc (.-settings set) :diff-buf-size node-dbs))))))
               ;; diff-buf: seed the projection comparator at the root; branch/child propagates it
               ;; down as nodes materialize, so a leaf-parent can project buffered leaves with the
               ;; set's stable comparator. Idempotent; a Leaf root has no buffered children.
               ;; Seed it when the root carries none; COPY when it already carries a
               ;; DIFFERENT one. The object in `root` is whatever the storage returned, and a
               ;; caching storage hands the same object to every set opened at that address,
               ;; so an unconditional write let the last set to call `-root` decide how every
               ;; other set's buffered leaves are ordered. The copy is published back into
               ;; `root`, so a conflicting pair costs one copy per node rather than one per
               ;; read. This is the ClojureScript half of the JVM fix; it was missing here,
               ;; leaving the durable data loss live on this runtime alone.
               (when (instance? Branch (.-root set))
                 (set! (.-root set)
                       (branch/stamp-proj-cmp (.-root set) (.-comparator set))))
               (.-root set))))

(def $root
  "Async-capable root accessor: materializes an address-rooted set's root (one
   restore) and adopts the restored node's settings, exactly as every internal
   reader does. Public for `org.replikativ.persistent-sorted-set.warm`, whose
   walk needs the root under BOTH `{:sync? true}` and `{:sync? false}` —
   `root-node` above is the sync-only diagnostic accessor."
  -root)

(defn $count
  [^BTSet set {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (do
                 (when (neg? (.-cnt set))
                   (let [root (await (-root set opts))]
                     (set! (.-cnt set) (await (node/$count root (.-storage set) opts)))))
                 (.-cnt set)))))

(defn $contains?
  [^BTSet set key {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root (await (-root set opts))]
                 (await (node/$contains? root (.-storage set) key (.-comparator set) opts))))))

;; ---- split-seam (MST / content-defined boundary) --------------------------------------
;; Parallel of the JVM PersistentSortedSet.growRoot + ANode.removeContent/mstMergeWith.
;; MST forces diff-buf off, so these build anchorless branches (no slots/addresses).

(defn- free-addr [storage addr] (when (and storage addr) (storage/markFreed storage addr)) nil)

(defn- mst-build-branch
  "Build a level-`lvl` Branch from a seq of child nodes. `addresses-seq` (optional) is the parallel
   durable addresses to PRESERVE (nil ⇒ anchorless, all children re-stored)."
  ([lvl children-seq settings projcmp] (mst-build-branch lvl children-seq settings projcmp nil))
  ([lvl children-seq settings projcmp addresses-seq]
   (let [carr          (arrays/into-array children-seq)
         child-counts  (map node/subtree-count children-seq)
         subtree-count (if (every? #(>= % 0) child-counts) (reduce + 0 child-counts) -1)
         measure-ops   (:measure settings)
         cmeas         (when measure-ops (map node/measure children-seq))
         measure       (when (and measure-ops (every? some? cmeas))
                         (reduce (fn [acc cs] (measure/merge-measure measure-ops acc cs))
                                 (measure/identity-measure measure-ops) cmeas))]
     (Branch. lvl (arrays/amap node/max-key carr) carr
              (when addresses-seq (arrays/into-array addresses-seq))
              subtree-count measure settings nil 0 projcmp))))

(defn- mst-children
  "Materialize a branch's children (storage-aware) → (async [children-vec addresses-vec]). Each
   address is the child's durable address (nil if unstored), preserved on rebuild."
  [^Branch node storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [n (arrays/alength (.-keys node))]
                 (loop [i 0 cs (transient []) as (transient [])]
                   (if (< i n)
                     (recur (inc i)
                            (conj! cs (await (branch/child node storage i opts)))
                            (conj! as (branch/address node i)))
                     [(persistent! cs) (persistent! as)]))))))

;; `disjoin` (defined below) is used by the MST replace path above it — forward-
;; declare it so cljs's single-pass analyzer doesn't warn :undeclared-var.
(declare mst-merge-with mst-remove-content disjoin)

(defn- mst-merge-with
  "Combine two same-level nodes whose separating (removed) boundary is gone. The junction —
   a's last child with b's first child — merges recursively ONLY when a's last child ended at
   that removed boundary. If a's last child is still terminated by a LIVE boundary, the two must
   NOT fuse — plain concatenation keeps it. Storage-aware + address-preserving (≡ JVM
   ANode.mstMergeWith). Returns (async node)."
  [a b projcmp bd storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (instance? Leaf a)
     ;; reached as a junction ⇒ a ended at the removed boundary (level 0) ⇒ concatenate
                 (let [settings    (.-settings a)
                       measure-ops (:measure settings)
                       lf          (Leaf. (arrays/aconcat (.-keys a) (.-keys b)) settings nil)]
                   (when measure-ops (node/try-compute-measure lf nil measure-ops {:sync? true}))
                   lf)
                 (let [[a-ch a-ad] (await (mst-children a storage opts))
                       [b-ch b-ad] (await (mst-children b storage opts))]
                   (if (>= (b/-key-level bd (node/max-key a)) (.-level a))
         ;; a's last child is a live boundary ⇒ keep it; concatenate (all addresses preserved).
                     (mst-build-branch (.-level a) (concat a-ch b-ch) (.-settings a) projcmp (concat a-ad b-ad))
         ;; a's last child ended at the removed boundary ⇒ merge the junction (consumed inputs freed).
                     (let [junction (await (mst-merge-with (last a-ch) (first b-ch) projcmp bd storage opts))]
                       (free-addr storage (last a-ad))
                       (free-addr storage (first b-ad))
                       (mst-build-branch (.-level a)
                                         (concat (butlast a-ch) [junction] (rest b-ch))
                                         (.-settings a) projcmp
                                         (concat (butlast a-ad) [nil] (rest b-ad))))))))))

(defn- mst-remove-content
  "MST remove without sibling-passing: returns (async <modified subtree, or empty Leaf>) or
   (async nil) if key absent. Storage-aware + address-preserving (≡ JVM ANode.removeContent)."
  [node storage key cmp projcmp {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (instance? Leaf node)
                 (let [keys (.-keys node)
                       idx  (lookup-exact cmp keys key)]
                   (when (>= idx 0)
                     (let [settings    (.-settings node)
                           measure-ops (:measure settings)
                           lf          (Leaf. (splice keys idx (inc idx) (arrays/array)) settings nil)]
                       (when measure-ops (node/try-compute-measure lf nil measure-ops {:sync? true}))
                       lf)))
                 (let [keys (.-keys node)
                       n    (arrays/alength keys)
                       idx  (binary-search-l cmp keys (dec n) key)]
                   (if (== idx n)
                     nil
                     (let [[children addrs] (await (mst-children node storage opts))
                           new-child        (await (mst-remove-content (nth children idx) storage key cmp projcmp opts))]
                       (if (nil? new-child)
                         nil
                         (let [bd          (b/content-boundary (.-settings node))
                               lvl         (.-level node)
                               settings    (.-settings node)
                               empty-leaf? (and (instance? Leaf new-child) (== 0 (arrays/alength (.-keys new-child))))
                               merge-right? (and (< idx (dec n)) (>= (b/-key-level bd key) lvl))]
                           (cond
                             empty-leaf?
                             (do (free-addr storage (nth addrs idx))
                                 (let [nc (concat (take idx children) (drop (inc idx) children))
                                       na (concat (take idx addrs) (drop (inc idx) addrs))]
                                   (if (seq nc) (mst-build-branch lvl nc settings projcmp na)
                                       (Leaf. (arrays/array) settings nil))))
                             merge-right?
                             (let [merged (await (mst-merge-with new-child (nth children (inc idx)) projcmp bd storage opts))]
                               (free-addr storage (nth addrs idx))
                               (free-addr storage (nth addrs (inc idx)))
                               (mst-build-branch lvl
                                                 (concat (take idx children) [merged] (drop (+ idx 2) children))
                                                 settings projcmp
                                                 (concat (take idx addrs) [nil] (drop (+ idx 2) addrs))))
                             :else
                             (do (free-addr storage (nth addrs idx))
                                 (mst-build-branch lvl
                                                   (concat (take idx children) [new-child] (drop (inc idx) children))
                                                   settings projcmp
                                                   (concat (take idx addrs) [nil] (drop (inc idx) addrs))))))))))))))

(defn- mst-grow-root
  "Promote sibling roots by boundary level until one remains (multi-level for high-level keys)."
  [bd nodes settings projcmp]
  (loop [nodes nodes]
    (let [n (arrays/alength nodes)]
      (if (< n 2)
        (arrays/aget nodes 0)
        (let [lvl    (inc (node/level (arrays/aget nodes 0)))
              thresh (inc lvl)
              branches
              (loop [i 0 start 0 out (transient [])]
                (if (>= i n)
                  (persistent! out)
                  (let [cut (and (< i (dec n))
                                 (>= (b/-key-level bd (node/max-key (arrays/aget nodes i))) thresh))]
                    (if (or cut (== i (dec n)))
                      (recur (inc i) (inc i)
                             (conj! out (mst-build-branch lvl (array-seq (.slice nodes start (inc i))) settings projcmp)))
                      (recur (inc i) start out)))))]
          (if (== 1 (count branches))
            (first branches)
            (recur (arrays/into-array branches))))))))

(defn conjoin
  ([^BTSet set key]
   (conjoin set key (.-comparator set) {:sync? true}))
  ([^BTSet set key arg]
   (if (fn? arg)
     (conjoin set key arg {:sync? true})
     (conjoin set key (.-comparator set) arg)))
  ([^BTSet set key cmp {:keys [sync?] :or {sync? true} :as opts}]
   ;; nil is not a storable value (matches upstream / the JVM PersistentSortedSet.cons check).
   (when (nil? key) (throw (ex-info "PersistentSortedSet cannot store nil" {:key key})))
   (async+sync sync?
               (async
                (let [root  (await (-root set opts))
                      roots (await (node/add root (.-storage set) key cmp opts))]
                  (if (nil? roots)
                    set
                    (do
                      ;; Mark old root address as freed if it exists
                      (when (and (.-storage set) (.-address set))
                        (storage/markFreed (.-storage set) (.-address set)))
                      ;; If count is unknown (-1), keep it unknown; will be computed lazily when needed
                      (let [current-cnt (.-cnt set)
                            new-cnt (if (neg? current-cnt) -1 (inc current-cnt))]
                        (if (== (arrays/alength roots) 1)
                          (BTSet. (arrays/aget roots 0)
                                  new-cnt
                                  (.-comparator set)
                                  (.-meta set)
                                  UNINITIALIZED_HASH
                                  (.-storage set)
                                  nil
                                  (.-settings set))
                          (if-let [bd (b/content-boundary (.-settings set))]
                            ;; split-seam (MST): multi-level boundary promotion (≡ JVM growRoot)
                            (BTSet. (mst-grow-root bd roots (.-settings set) (.-comparator set))
                                    new-cnt (.-comparator set) (.-meta set)
                                    UNINITIALIZED_HASH (.-storage set) nil (.-settings set))
                            (let [child0 (arrays/aget roots 0)
                                  lvl    (inc (node/level child0))
                                ;; Compute subtree count; propagate -1 (unknown) if any child is unknown
                                  child-counts (map node/subtree-count roots)
                                  subtree-count (if (every? #(>= % 0) child-counts)
                                                  (reduce + 0 child-counts)
                                                  -1)
                                ;; Compute measure from children if measure-ops available
                                  measure-ops (:measure (.-settings set))
                                ;; Only compute root-measure when all children have measure;
                                ;; otherwise keep nil to allow lazy recomputation
                                  child-measure (when measure-ops (map node/measure roots))
                                  root-measure (when (and measure-ops (every? some? child-measure))
                                                 (reduce (fn [acc cs]
                                                           (measure/merge-measure measure-ops acc cs))
                                                         (measure/identity-measure measure-ops)
                                                         child-measure))]
                            ;; diff-buf: new root on growth — _bufEntries = 0 (no buffered diff; this
                            ;; node is always the root ⇒ always written; if later mutated in a transient
                            ;; batch the first deposit poisons it correctly). Mirrors JVM makeBranchFromChildren.
                              (BTSet. (Branch. lvl (arrays/amap node/max-key roots) roots nil subtree-count root-measure (.-settings set) nil 0 (.-comparator set))
                                      new-cnt
                                      (.-comparator set)
                                      (.-meta set)
                                      UNINITIALIZED_HASH
                                      (.-storage set)
                                      nil
                                      (.-settings set)))))))))))))

(defn $replace
  ([^BTSet set old-key new-key]
   ($replace set old-key new-key (.-comparator set) {:sync? true}))
  ([^BTSet set old-key new-key arg]
   (if (fn? arg)
     ($replace set old-key new-key arg {:sync? true})
     ($replace set old-key new-key (.-comparator set) arg)))
  ([^BTSet set old-key new-key cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (let [bd (b/content-boundary (.-settings set))]
                  (if (and bd (not= (b/-key-level bd old-key) (b/-key-level bd new-key)))
                    ;; split-seam (MST): an in-place replace keeps the tree's boundary structure, which
                    ;; is canonical ONLY when old-key and new-key rise to the same level. When a partial
                    ;; comparator shifts the key hash across a level boundary, rebuild via disj+conj so
                    ;; the tree re-splits/merges into the history-independent shape (count mode is
                    ;; position-only ⇒ always in-place). See doc/merkle-search-tree.md.
                    (let [root (await (-root set opts))]
                      (if (await (node/$contains? root (.-storage set) old-key cmp opts))
                        (await (conjoin (await (disjoin set old-key cmp opts)) new-key cmp opts))
                        set))
                    (let [root  (await (-root set opts))
                          nodes (await (node/$replace root (.-storage set) old-key new-key cmp opts))]
                      (cond
                        (nil? nodes) set

                        ;; In-place update (transient) — root modified, just clear address
                        (= nodes :early-exit)
                        (do
                          (when (and (.-storage set) (.-address set))
                            (storage/markFreed (.-storage set) (.-address set)))
                          (BTSet. (.-root set)
                                  (.-cnt set)
                                  (.-comparator set)
                                  (.-meta set)
                                  UNINITIALIZED_HASH
                                  (.-storage set)
                                  nil
                                  (.-settings set)))

                        ;; New root node (persistent or maxKey changed)
                        :else
                        (do
                          (when (and (.-storage set) (.-address set))
                            (storage/markFreed (.-storage set) (.-address set)))
                          (BTSet. (arrays/aget nodes 0)
                                  (.-cnt set)
                                  (.-comparator set)
                                  (.-meta set)
                                  UNINITIALIZED_HASH
                                  (.-storage set)
                                  nil
                                  (.-settings set)))))))))))

(defn disjoin
  ([^BTSet set key]
   (disjoin set key (.-comparator set) {:sync? true}))
  ([^BTSet set key arg]
   (if (fn? arg)
     (disjoin set key arg {:sync? true})
     (disjoin set key (.-comparator set) arg)))
  ([^BTSet set key cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (let [root (await (-root set opts))
                      bd   (b/content-boundary (.-settings set))]
                  (if bd
                    ;; split-seam (MST): sibling-free removeContent + single-child root collapse
                    ;; (≡ JVM disjoin content branch). Storage-aware (await child materialization).
                    (let [new-root (await (mst-remove-content root (.-storage set) key cmp (.-comparator set) opts))]
                      (if (nil? new-root)
                        set
                        (do
                          (when (and (.-storage set) (.-address set))
                            (storage/markFreed (.-storage set) (.-address set)))
                          (let [new-root (loop [r new-root]
                                           (if (and (instance? Branch r)
                                                    (== 1 (arrays/alength (.-keys r))))
                                             (recur (await (branch/child r (.-storage set) 0 opts)))
                                             r))
                                current-cnt (.-cnt set)
                                new-cnt (if (neg? current-cnt) -1 (dec current-cnt))]
                            (BTSet. new-root new-cnt (.-comparator set) (.-meta set)
                                    UNINITIALIZED_HASH (.-storage set) nil (.-settings set))))))
                    ;; count path (unchanged)
                    (let [new-roots (await (node/$remove root (.-storage set) key nil nil cmp opts))]
                      (if (nil? new-roots)
                        set
                        (do
                          ;; Mark old root address as freed if it exists
                          (when (and (.-storage set) (.-address set))
                            (storage/markFreed (.-storage set) (.-address set)))
                          (let [new-root (arrays/aget new-roots 0)
                                new-root (if (and (instance? Branch new-root)
                                                  (== 1 (arrays/alength (.-children new-root))))
                                           (await (branch/child new-root (.-storage set) 0 opts))
                                           new-root)
                                ;; If count is unknown (-1), keep it unknown; will be computed lazily when needed
                                current-cnt (.-cnt set)
                                new-cnt (if (neg? current-cnt) -1 (dec current-cnt))]
                            (BTSet. new-root
                                    new-cnt
                                    (.-comparator set)
                                    (.-meta set)
                                    UNINITIALIZED_HASH
                                    (.-storage set)
                                    nil
                                    (.-settings set))))))))))))

(defn store
  ([^BTSet set arg]
   (if (implements? storage/IStorage arg)
     (store set arg {:sync? true})
     (store set (.-storage set) arg)))
  ([^BTSet set storage {:keys [sync?] :or {sync? true} :as opts}]
   (assert (instance? BTSet set))
   (assert (implements? storage/IStorage storage) "BTSet/store requires IStorage in second argument")
   (async+sync sync?
               (async
                (do
                  (set! (.-storage set) storage)
                  (when (nil? (.-address set))
                    (set! (.-address set) (await (node/store (.-root set) storage opts))))
                  (.-address set))))))

(defn walk-addresses
  [^BTSet set on-address {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (some? (.-address set))
                 (when (on-address (.-address set))
                   (await (node/walk-addresses (await (-root set opts))
                                               (.-storage set)
                                               on-address
                                               opts)))
                 (await (node/walk-addresses (await (-root set opts))
                                             (.-storage set)
                                             on-address
                                             opts))))))

;; ---- diff -------------------------------------------------------------------
;; Mirrors the JVM frontier walk in persistent_sorted_set.clj: same pruning rule,
;; same candidate differencing, so a diff computed here agrees with one computed
;; there on the same pair of trees.
;;
;; A frontier entry names a node WITHOUT loading it:
;;
;;     [node parent idx addr prunable?]
;;
;; `node` is nil until materialized, `addr` is the stored address (nil for a node
;; never stored), and `prunable?` says whether that address can be trusted to
;; stand for the contents.
;;
;; Each round intersects the two frontiers' addresses, drops what both sides
;; hold, and loads only the remainder. The addresses come from the parents, which
;; are already loaded, so pruning itself costs no IO. That shape matters more
;; here than on the JVM: every load is an async round trip, so the count of them
;; is what the caller feels, and it is bounded by the CHANGE rather than by the
;; tree — measured on the JVM at 3-4 node reads for a two-element delta whether
;; the set holds a thousand elements or a hundred thousand.

(defn- diff-child-refs
  "Frontier entries for every child of `branches` (already materialized). Pure."
  [branches]
  (into []
        (mapcat (fn [[node]]
                  (let [^Branch b node
                        addrs (.-addresses b)
                        ;; diff-buf: a branch buffers a child's changes in its OWN
                        ;; slots and leaves the child's ADDRESS untouched, so for a
                        ;; buffered child an address match no longer proves the
                        ;; subtrees are equal. The guard is per CHILD: a nil slot
                        ;; means that child has no buffered diff. A non-nil slot is
                        ;; unprunable whatever its shape — for a BRANCH child :diff
                        ;; is nil and the real diff lives in the subtree, so a nil
                        ;; :diff does not mean no change.
                        slots (.-_slots b)]
                    (map (fn [i]
                           [nil b i
                            (when addrs (arrays/aget addrs i))
                            (or (nil? slots) (nil? (arrays/aget slots i)))])
                         (range (arrays/alength (.-keys b)))))))
        branches))

(defn- diff-prune
  "Drop from each frontier the entries the other side holds at the same address —
   identical subtrees, which cannot contain a difference. Pure, no IO."
  [fa fb]
  (let [addrs   (fn [f] (into #{} (keep (fn [[_ _ _ addr prunable?]]
                                          (when (and (some? addr) prunable?) addr)))
                              f))
        sa      (addrs fa)
        sb      (addrs fb)
        shared? (fn [other] (fn [[_ _ _ addr prunable?]]
                              (and (some? addr) prunable? (contains? other addr))))]
    ;; an entry the OTHER side marked unprunable never entered its address set, so
    ;; neither side prunes against a buffered branch.
    [(into [] (remove (shared? sb)) fa)
     (into [] (remove (shared? sa)) fb)]))

(defn- diff-level [node]
  (if (instance? Branch node) (.-level ^Branch node) 0))

(defn- diff-sorted
  "Elements of `xs` absent from `ys`. Both ascending under `cmp`; O(n+m), no IO."
  [cmp xs ys]
  (loop [xs (seq xs) ys (seq ys) out (transient [])]
    (cond
      (nil? xs) (persistent! out)
      (nil? ys) (persistent! (reduce conj! out xs))
      :else     (let [c (cmp (first xs) (first ys))]
                  (cond
                    (neg? c) (recur (next xs) ys (conj! out (first xs)))
                    (pos? c) (recur xs (next ys) out)
                    :else    (recur (next xs) (next ys) out))))))

(defn- diff-descend
  "One level down. At level 0 the entries are leaves and contribute their keys to
   `cand`; above it they contribute their children to the next frontier.

   Loads are sequential rather than overlapped: a frontier holds only the nodes
   that actually changed, so there is little to overlap, and issuing a whole
   frontier at once would make fan-out unbounded on a large delta."
  [storage refs level cand {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [n (count refs)]
                 (loop [i 0 nodes []]
                   (if (< i n)
                     (let [[node parent idx :as ref] (nth refs i)]
                       (if (some? node)
                         (recur (inc i) (conj nodes ref))
                         (recur (inc i)
                                (conj nodes (assoc ref 0 (await (branch/child parent storage idx opts)))))))
                     (if (zero? level)
                       [[] (reduce (fn [c [nd]] (reduce conj c (.-keys nd))) cand nodes)]
                       [(diff-child-refs nodes) cand])))))))

(defn diff
  "Keys added and removed between two sets that SHARE STRUCTURE. See the JVM
   `org.replikativ.persistent-sorted-set/diff` for the full contract; this is the
   same algorithm with the loads awaited.

   Returns `{:added [...] :removed [...]}`, or a continuation yielding it when
   `{:sync? false}`."
  [^BTSet a ^BTSet b storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [addr-a (.-address a)
                     addr-b (.-address b)]
                 (if (and (some? addr-a) (= addr-a addr-b))
                   {:added [] :removed []}          ; same root: zero reads
                   (let [cmp    (.-comparator b)
                         ;; roots go through -root, which stamps the projection
                         ;; comparator that diff-buf needs on descent
                         root-a (await (-root a opts))
                         root-b (await (-root b opts))]
                     (loop [fa [[root-a nil nil addr-a true]] la (diff-level root-a)
                            fb [[root-b nil nil addr-b true]] lb (diff-level root-b)
                            ca [] cb []]
                       (let [la (if (seq fa) la -1)
                             lb (if (seq fb) lb -1)]
                         (if (and (neg? la) (neg? lb))
                           {:added (diff-sorted cmp cb ca) :removed (diff-sorted cmp ca cb)}
                           ;; addresses only mean the same thing at the same level,
                           ;; and a shared node keeps its level, so pruning across
                           ;; unequal levels would find nothing. Walk the deeper
                           ;; side down until they meet.
                           (let [[fa fb]   (if (== la lb) (diff-prune fa fb) [fa fb])
                                 la        (if (seq fa) la -1)
                                 lb        (if (seq fb) lb -1)
                                 down-a?   (and (>= la 0) (>= la lb))
                                 down-b?   (and (>= lb 0) (>= lb la))
                                 [fa' ca'] (if down-a?
                                             (await (diff-descend storage fa la ca opts))
                                             [fa ca])
                                 [fb' cb'] (if down-b?
                                             (await (diff-descend storage fb lb cb opts))
                                             [fb cb])]
                             (recur fa' (if down-a? (dec la) la)
                                    fb' (if down-b? (dec lb) lb)
                                    ca' cb')))))))))))

(defn- walk-delta-descend
  "Materialize one frontier and return its children without retaining keys.
   `on-address` is nil on the old side and observational on the new side."
  [storage refs level on-address {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [n (count refs)
                     nodes
                     (loop [i 0 nodes []]
                       (if (< i n)
                         (let [[nd parent idx :as ref] (nth refs i)]
                           (if (some? nd)
                             (recur (inc i) (conj nodes ref))
                             (recur (inc i)
                                    (conj nodes
                                          (assoc ref 0
                                                 (await (branch/child parent storage idx opts)))))))
                         nodes))]
                 (when on-address
                   (doseq [[_ _ _ address] nodes]
                     (when (some? address)
                       (on-address address))))
                 (if (zero? level) [] (diff-child-refs nodes))))))

(defn walk-delta
  "Restore and visit the stored nodes in `b` that cannot be pruned as subtrees
   shared with `a`.

   Uses the same paired-frontier and diff-buffer-aware pruning rules as `diff`,
   but does not retain or compare elements. `on-address` observes successfully
   materialized new-side addresses; its return value is ignored so callback return
   values cannot accidentally prune an incomplete hydration. Old-side restores
   are not reported.

   Returns nil when synchronous, or a continuation yielding nil under
   `{:sync? false}`."
  [^BTSet a ^BTSet b storage on-address {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [addr-a (.-address a)
                     addr-b (.-address b)]
                 (when-not (and (some? addr-a) (= addr-a addr-b))
                   (let [root-a (await (-root a opts))
                         root-b (await (-root b opts))]
                     (loop [fa [[root-a nil nil addr-a true]] la (diff-level root-a)
                            fb [[root-b nil nil addr-b true]] lb (diff-level root-b)]
                       (let [la (if (seq fa) la -1)
                             lb (if (seq fb) lb -1)]
                         (when-not (and (neg? la) (neg? lb))
                           (let [[fa fb] (if (== la lb) (diff-prune fa fb) [fa fb])
                                 la      (if (seq fa) la -1)
                                 lb      (if (seq fb) lb -1)
                                 down-a? (and (>= la 0) (>= la lb))
                                 down-b? (and (>= lb 0) (>= lb la))
                                 fa'     (if down-a?
                                           (await (walk-delta-descend storage fa la nil opts))
                                           fa)
                                 fb'     (if down-b?
                                           (await (walk-delta-descend storage fb lb on-address opts))
                                           fb)]
                             (recur fa' (if down-a? (dec la) la)
                                    fb' (if down-b? (dec lb) lb))))))))))))

(defn lookup
  [^BTSet set key cmp {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root (await (-root set opts))
                     cmp  (or cmp (.-comparator set))]
                 (await (node/lookup root (.-storage set) key cmp opts))))))

(defn lookup-ge
  "Look up the first element >= key (ceiling/GE lookup).
   Returns nil if no element >= key exists. O(log n)."
  [^BTSet set key cmp {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root (await (-root set opts))
                     cmp  (or cmp (.-comparator set))
                     storage (.-storage set)]
                 (loop [node root]
                   (let [keys (.-keys node)
                         len  (arrays/alength keys)]
                     (if (== len 0)
                       nil
                       (let [idx (binary-search-l cmp keys (dec len) key)]
                         (if (>= idx len)
                           nil
                           (if (instance? Branch node)
                             (let [child-node (await (branch/child node storage idx opts))]
                               (recur child-node))
                             ;; Leaf — return the key at idx
                             (arrays/aget keys idx)))))))))))

(defn measure
  "Get the aggregated statistics for the entire set."
  [^BTSet set {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root (await (-root set opts))
                     measure-ops (:measure (.-settings set))]
                 (if (nil? measure-ops)
                   nil
                   (or (node/measure root)
                       (await (node/force-compute-measure root (.-storage set) measure-ops opts))))))))

(defn restore
  [root-address-or-info storage opts]
  (let [;; Handle both old format (bare UUID) and new format (map with metadata)
        address (if (map? root-address-or-info)
                  (:root-address root-address-or-info)
                  root-address-or-info)
        _       (assert (some? address))
        meta    (or (and (map? root-address-or-info) (:meta root-address-or-info))
                    (:meta opts))
        cmp     (if (map? root-address-or-info)
                  (or (:comparator root-address-or-info) compare)
                  (or (:comparator opts) (:cmp opts) compare))
        settings (select-keys (merge (when (map? root-address-or-info) root-address-or-info) opts)
                              ;; :diff-buf-size and :boundary were dropped here. The JVM's
                              ;; `map->settings` keeps both, so a restored cljs set silently
                              ;; disagreed with the same call on the JVM: ask for
                              ;; {:diff-buf-size 256} and get 0. `-root` recovers :boundary
                              ;; from the root NODE (self-describing), which is why the MST
                              ;; half never showed; nothing recovers :diff-buf-size, so
                              ;; freshly created nodes stopped buffering after a restore
                              ;; while restored ones kept doing it.
                              [:branching-factor :measure :boundary :diff-buf-size])]
    (BTSet. nil -1 cmp meta UNINITIALIZED_HASH storage address settings)))

#!------------------------------------------------------------------------------

(def ^:const EMPTY_PATH (js* "0n"))

(defn- bits-per-level [^BTSet _set]
  ;; The iterator packs one index PER TREE LEVEL into a single BigInt path,
  ;; `bits-per-level` bits each (`path-get`/`path-set` shift+mask by this width).
  ;; A COUNT B-tree node is always ≤ branching-factor, so `ceil(log2 bf)` bits
  ;; sufficed historically. But an MST content-defined node can EXCEED
  ;; branching-factor (the boundary cuts at content keys, not by size — with no
  ;; boundary key in a range a node simply grows). Sizing the field by
  ;; `ceil(log2 bf)` then MASKS any index ≥ bf on read (e.g. `199 & 63 = 7`),
  ;; truncating the path and silently dropping the tail of `seq`/`slice`/`rseq`.
  ;; Use a fixed 32-bit field per level — matching the JVM's `int[]`-per-level
  ;; path — so an index never overflows for any realistically sized node (a node
  ;; with > 2^32 entries is physically impossible). BigInt absorbs the wider path.
  32)

;; These take SETTINGS, not a set. They used to take a `BTSet`, which forced
;; `from-sorted-array-count` to fabricate a throwaway one just to ask its fanout
;; (`(BTSet. nil 0 cmp nil nil nil nil settings)`), and would have forced the
;; streaming builder to do the same before it has a set to speak of.
(defn- max-len [settings]
  (get settings :branching-factor))

(defn- min-len [settings]
  ;; `half`, not `/`: the JVM spells this `_branchingFactor >>> 1`, and plain
  ;; division yields 256.5 for an odd branching factor where the JVM yields 256.
  ;; Every fanout decision downstream is then off by half an element on one
  ;; runtime and not the other.
  (arrays/half (max-len settings)))

(defn- avg-len [settings]
  (arrays/half (+ (max-len settings) (min-len settings))))

(defn- path-inc [path]
  (+ path (js* "1n")))

(defn- path-dec [path]
  (- path (js* "1n")))

(defn- path-cmp [path1 path2]
  (- path1 path2))

(defn- path-lt [path1 path2]
  (< path1 path2))

(defn- path-lte [path1 path2]
  (<= path1 path2))

(defn- path-eq [path1 path2]
  (== path1 path2))

(defn- path-get ^number [set path ^number level]
  (let [bpl (bits-per-level set)
        shift (js/BigInt (* level bpl))
        mask (js* "~{} - 1n" (bit-shift-left (js* "1n") (js/BigInt bpl)))]
    (js/Number (bit-and (bit-shift-right path shift) mask))))

(defn- path-set [set path ^number level ^number idx]
  (let [bpl   (bits-per-level set)
        shift (js/BigInt (* level bpl))
        mask  (js* "~{} - 1n" (bit-shift-left (js* "1n") (js/BigInt bpl)))
        old   (bit-and (bit-shift-right path shift) mask)]
    (-> path
        (- (bit-shift-left old shift))
        (+ (bit-shift-left (js/BigInt idx) shift)))))

(defn- rpath
  [set node path ^number level]
  (if (pos? level)
    (let [last-idx (dec (arrays/alength (.-children node)))]
      (recur
       set
       (arrays/aget (.-children node) last-idx)
       (path-set set path level last-idx)
       (dec level)))
    (path-set set path 0 (dec (arrays/alength (.-keys node))))))

(defn- -rpath
  [set node path ^number level {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (pos? level)
                 (let [last-idx (dec (node/len node))
                       child-node (await (branch/child node (.-storage set) last-idx opts))]
                   (await (-rpath set child-node (path-set set path level last-idx) (dec level) opts)))
                 (path-set set path 0 (dec (arrays/alength (.-keys node))))))))

(defn- $$_next-path
  [set node path ^number level {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (some? node) (implements? node/INode node)))
  (async+sync sync?
              (async
               (let [idx (path-get set path level)]
                 (if (pos? level)
                   (let [child-node (await (branch/child node (.-storage set) idx opts))
                         sub-path (await ($$_next-path set child-node path (dec level) opts))]
                     (if (nil? sub-path)
                       (if (< (inc idx) (arrays/alength (.-keys node)))
                         (path-set set EMPTY_PATH level (inc idx))
                         nil)
                       (path-set set sub-path level idx)))
                   (if (< (inc idx) (arrays/alength (.-keys node)))
                     (path-set set EMPTY_PATH 0 (inc idx)) ;; advance leaf idx
                     nil))))))

(defn- -next-path
  "Returns path representing next item after `path` in natural traversal order.
   Will overflow at leaf if at the end of the tree"
  [set path {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (< path (js* "0n"))
                 EMPTY_PATH
                 (let [root (await (-root set opts))
                       lvl  (node/level root)]
                   (or
                    (await ($$_next-path set root path lvl opts))
                    (path-inc (if (.-storage set)
                                (await (-rpath set root EMPTY_PATH lvl opts))
                                (rpath set root EMPTY_PATH lvl)))))))))

(defn- $$_prev-path
  [set node path ^number level {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (some? node) (implements? node/INode node)))
  (async+sync sync?
              (async
               (let [idx (path-get set path level)]
                 (if (and (== 0 level) (== 0 idx))
                   nil ;; leaf overflow
                   (if (== 0 level)
                     (path-set set EMPTY_PATH 0 (dec idx)) ;; leaf
                     (if (>= idx (node/len node))
                       (if (.-storage set) ;; branch that was overflow before
                         (await (-rpath set node path level opts))
                         (rpath set node path level))
                       (let [child-node (await (branch/child node (.-storage set) idx opts))
                             path' (await ($$_prev-path set child-node path (dec level) opts))]
                         (if (some? path')
                           (path-set set path' level idx) ;; no sub-overflow, keep current idx
                           (if (== 0 idx)
                             nil ;; nested overflow + this node overflow
                             (let [;; nested overflow, advance current idx, reset subsequent indexes
                                   child-node (await (branch/child node (.-storage set) (dec idx) opts))
                                   path' (if (.-storage set)
                                           (await (-rpath set child-node path (dec level) opts))
                                           (rpath set child-node path (dec level)))]
                               (path-set set path' level (dec idx)))))))))))))

(defn- -prev-path
  "Returns path representing previous item before `path` in natural traversal order.
   Will overflow at leaf if at beginning of tree"
  [set path {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root (await (-root set opts))
                     lvl  (node/level root)]
                 (if (> (path-get set path (inc lvl)) 0) ;; overflow
                   (if (.-storage set)
                     (await (-rpath set root path lvl opts))
                     (rpath set root path lvl))
                   (or (await ($$_prev-path set root path lvl opts))
                       (path-dec EMPTY_PATH)))))))

(defn- path-same-leaf ^boolean [set path1 path2]
  (let [bpl (bits-per-level set)]
    (== (bit-shift-right path1 (js/BigInt bpl)) (bit-shift-right path2 (js/BigInt bpl)))))

(defn- path-str [set path]
  (let [ml (js/BigInt (max-len (.-settings set)))]
    (loop [res ()
           path path]
      (if (not= path (js* "0n"))
        (recur (conj res (js/Number (mod path ml))) (quot path ml))
        (vec res)))))

(defn- -keys-for
  "Returns keys array for the leaf node at the given path."
  [set path {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root (await (-root set opts))
                     lvl  (node/level root)]
                 (loop [level lvl
                        node  root]
                   (if (pos? level)
                     (recur
                      (dec level)
                      (await (branch/child node (.-storage set) (path-get set path level) opts)))
                     (.-keys node)))))))

;;------------------------------------------------------------------------------

;; replace with cljs.core/ArrayChunk after https://dev.clojure.org/jira/browse/CLJS-2470
(deftype Chunk [arr off end]
  ICounted
  (-count [_] (- end off))

  IIndexed
  (-nth [this i] (aget arr (+ off i)))

  (-nth [this i not-found]
    (if (and (>= i 0) (< i (- end off)))
      (aget arr (+ off i))
      not-found))

  IChunk
  (-drop-first [this]
    (if (== off end)
      (throw (js/Error. "-drop-first of empty chunk"))
      (Chunk. arr (inc off) end)))

  IReduce
  (-reduce [this f]
    (if (== off end)
      (f)
      (-reduce (-drop-first this) f (aget arr off))))

  (-reduce [this f start]
    (loop [val start, n off]
      (if (< n end)
        (let [val' (f val (aget arr n))]
          (if (reduced? val')
            @val'
            (recur val' (inc n))))
        val))))

#!------------------------------------------------------------------------------

(defprotocol IIter (-copy [this left right]))

(defprotocol ISeek (-seek [this key] [this key comparator]))

(defn- -seek-path
  "Returns path to first element >= key, or nil if all elements in a set < key."
  [^BTSet set key comparator {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (nil? key)
                 EMPTY_PATH
                 (loop [node  (await (-root set opts))
                        path  EMPTY_PATH
                        level (node/level node)]
                   (let [keys-l (node/len node)]
                     (if (== 0 level)
                       (let [keys (.-keys node)
                             idx  (binary-search-l comparator keys (dec keys-l) key)]
                         (if (== keys-l idx)
                           nil
                           (path-set set path 0 idx)))
                       (let [keys (.-keys node)
                             idx  (binary-search-l comparator keys (- keys-l 2) key)
                             child-node (await (branch/child node (.-storage set) idx opts))]
                         (recur
                          child-node
                          (path-set set path level idx)
                          (dec level))))))))))

(declare ReverseIter seek)

(deftype Iter [^BTSet set left right keys idx]
  IIter
  (-copy [_ l r] (Iter. set l r (-keys-for set l {:sync? true}) (path-get set l 0)))

  IEquiv
  (-equiv [this other]
    (equiv-sequential this other))

  ISequential
  ISeqable
  (-seq [this]
    (when keys this))

  ISeq
  (-first [_]
    (when keys (arrays/aget keys idx)))

  (-rest [this]
    (or (-next this) ()))

  INext
  (-next [this]
    (when keys
      (if (< (inc idx) (arrays/alength keys))
        ;; can use cached array to move forward
        (let [left' (path-inc left)]
          (when (path-lt left' right)
            (Iter. set left' right keys (inc idx))))
        (let [left' (-next-path set left {:sync? true})]
          (when (path-lt left' right)
            (-copy this left' right))))))

  IChunkedSeq
  (-chunked-first [this]
    (let [end-idx (if (path-same-leaf set left right)
                    ;; right is in the same node
                    (path-get set right 0)
                    ;; right is in a different node
                    (arrays/alength keys))]
      (Chunk. keys idx end-idx)))

  (-chunked-rest [this]
    (or (-chunked-next this) ()))

  IChunkedNext
  (-chunked-next [this]
    (let [last  (path-set set left 0 (dec (arrays/alength keys)))
          left' (-next-path set last {:sync? true})]
      (when (path-lt left' right)
        (-copy this left' right))))

  IReduce
  (-reduce [this f]
    (if (nil? keys)
      (f)
      (let [first (-first this)]
        (if-some [next (-next this)]
          (-reduce next f first)
          first))))

  (-reduce [this f start]
    (loop [left left
           keys keys
           idx  idx
           acc  start]
      (if (nil? keys)
        acc
        (let [new-acc (f acc (arrays/aget keys idx))]
          (cond
            (reduced? new-acc)
            @new-acc

            (< (inc idx) (arrays/alength keys)) ;; can use cached array to move forward
            (let [left' (path-inc left)]
              (if (path-lt left' right)
                (recur left' keys (inc idx) new-acc)
                new-acc))

            :else
            (let [left' (-next-path set left {:sync? true})]
              (if (path-lt left' right)
                (recur left' (-keys-for set left' {:sync? true}) (path-get set left' 0) new-acc)
                new-acc)))))))

  IReversible
  (-rseq [this]
    (when keys
      (let [left' (-prev-path set left {:sync? true})
            right' (-prev-path set right {:sync? true})]
        (ReverseIter. set left' right'
                      (-keys-for set right' {:sync? true})
                      (path-get set right' 0)))))

  ISeek
  (-seek [this key]
    (-seek this key (.-comparator set)))

  ;; Two things were wrong here, and they pulled in opposite directions.
  ;;
  ;; The `(nat-int? (cmp current key)) => this` arm made a BACKWARD seek a no-op: positioned
  ;; at 5000, `(seek it 2500)` returned the iterator unchanged, so the answer began at 5000
  ;; rather than 2500. That arm is only sound under forward-only semantics; the docstring
  ;; states the postcondition unconditionally, and `-seek-path` re-descends from the root, so
  ;; the general answer costs the same O(log n) a forward seek already paid.
  ;;
  ;; And the `:else` arm built the new Iter with NO check that `left'` is still before
  ;; `right`, so seeking past a slice's upper bound emitted elements outside the slice:
  ;;
  ;;     (seek (slice s 2500 7500) 9000)   cljs => [9000]   JVM => nil
  ;;
  ;; `right` is this Iter's retained end and every other method here guards on it
  ;; (`-next`, `-chunked-next`, `-reduce` all test `path-lt`); only `-seek` did not.
  ;; `ReverseIter` already had the mirror guard.
  (-seek [this key cmp]
    (if (nil? key)
      (throw (js/Error. "seek can't be called with a nil key!"))
      (when-some [left' (-seek-path set key cmp {:sync? true})]
        (when (path-lt left' right)
          (Iter. set left' right (-keys-for set left' {:sync? true}) (path-get set left' 0))))))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts (seq this))))

#!------------------------------------------------------------------------------

(defn- -rseek
  "Returns path to the first element that is > key.
   If all elements in a set are <= key, returns `(-rpath set) + 1`.
   It's a virtual path that is bigger than any path in a tree."
  [^BTSet set key comparator {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root (await (-root set opts))
                     lvl  (node/level root)]
                 (if (nil? key)
                   (path-inc (if (.-storage set)
                               (await (-rpath set root EMPTY_PATH lvl opts))
                               (rpath set root EMPTY_PATH lvl)))
                   (loop [node  root
                          path  EMPTY_PATH
                          level lvl]
                     (let [keys-l (node/len node)]
                       (if (== 0 level)
                         (let [keys (.-keys node)
                               idx  (binary-search-r comparator keys (dec keys-l) key)]
                           (path-set set path 0 idx))
                         (let [keys       (.-keys node)
                               idx        (binary-search-r comparator keys (- keys-l 2) key)
                               res        (path-set set path level idx)
                               child-node (await (branch/child node (.-storage set) idx opts))]
                           (recur
                            child-node
                            res
                            (dec level)))))))))))

;;------------------------------------------------------------------------------

(deftype AsyncSeq [^BTSet set left right ^:mutable keys ^:mutable idx]
  aseq/PAsyncSeq
  (anext [this]
    (async
     (when (and left (path-lt left right))
       (when (nil? keys)
         (set! keys (await (-keys-for set left {:sync? false})))
         (set! idx (path-get set left 0)))
       [(arrays/aget keys idx)
        (if (< (inc idx) (arrays/alength keys))
          (AsyncSeq. set (path-inc left) right keys (inc idx))
          (let [next-path (await (-next-path set left {:sync? false}))]
            (when (and next-path (path-lt next-path right))
              (AsyncSeq. set next-path right nil nil))))])))
  Object
  (toString [this]
    (str "AsyncSeq[" (path-str set left) " -> " (path-str set right) "]"))
  IPrintWithWriter
  (-pr-writer [this writer opts]
    (-write writer (str this))))

(defprotocol PAsyncChunkedSeq
  "Chunk-level traversal of an async seq: one await per LEAF instead of one
   per element (element-wise anext allocates a continuation per element —
   measured ~4x slower than a chunk loop on warm data). `achunk-next`
   resolves to `[keys-array start end next]`: consume `(arrays/aget keys i)`
   for start <= i < end in a synchronous loop, then continue with `next`
   (an async seq positioned at the next leaf, or nil) — or resolves nil when
   the seq is exhausted. Boundary semantics mirror the sync Iter's
   IChunkedSeq exactly (-chunked-first / -chunked-next)."
  (achunk-next [this]))

(extend-type AsyncSeq
  PAsyncChunkedSeq
  (achunk-next [this]
    (async
     (let [set   (.-set this)
           left  (.-left this)
           right (.-right this)]
       (when (and left (path-lt left right))
         (when (nil? (.-keys this))
           (set! (.-keys this) (await (-keys-for set left {:sync? false})))
           (set! (.-idx this) (path-get set left 0)))
         (let [keys  (.-keys this)
               start (.-idx this)
               end   (if (path-same-leaf set left right)
                       ;; right is in the same node (exclusive bound)
                       (path-get set right 0)
                       ;; right is in a different node
                       (arrays/alength keys))
               last-path (path-set set left 0 (dec (arrays/alength keys)))
               next-path (await (-next-path set last-path {:sync? false}))]
           [keys start end
            (when (and next-path (path-lt next-path right))
              (AsyncSeq. set next-path right nil nil))]))))))

#!------------------------------------------------------------------------------

(declare -iter)

(deftype ReverseIter [^BTSet set left right keys idx]
  IIter
  (-copy [_ l r] (ReverseIter. set l r (-keys-for set r {:sync? true}) (path-get set r 0)))

  IEquiv
  (-equiv [this other] (equiv-sequential this other))

  ISequential
  ISeqable
  (-seq [this] (when keys this))

  ISeq
  (-first [this]
    (when keys
      (arrays/aget keys idx)))

  (-rest [this]
    (or (-next this) ()))

  INext
  (-next [this]
    (when keys
      (if (> idx 0)
        (let [right' (path-dec right)]
          (when (path-lt left right')
            (ReverseIter. set left right' keys (dec idx))))
        (let [right' (-prev-path set right {:sync? true})]
          (when (path-lt left right')
            (-copy this left right'))))))

  IReversible
  (-rseq [this]
    (when keys
      (-iter set
             (-next-path set left {:sync? true})
             (-next-path set right {:sync? true})
             {:sync? true})))

  ISeek
  (-seek [this key]
    (-seek this key (.-comparator set)))

  ;; Mirror of `Iter -seek` above, for the same two reasons.
  ;;
  ;; The `this` arm made a seek back UP a descending iterator a no-op. And `(path-lt right'
  ;; right)` was not a bound check but a "never move above where you started" restriction:
  ;; for a ReverseIter, `left` is the retained end (the `to` of `rslice`) and `right` is
  ;; merely the current position. Keeping `path-lte left right'` preserves the bound; dropping
  ;; the other restores the same absolute-reposition semantics the ascending side and the JVM
  ;; now have, so `(-> (rslice s 9999 nil) (seek 5000) (seek 7500))` answers from 7500.
  (-seek [this key cmp]
    (if (nil? key)
      (throw (js/Error. "seek can't be called with a nil key!"))
      (let [right' (-prev-path set (-rseek set key cmp {:sync? true}) {:sync? true})]
        (when (and right' (>= right' (js* "0n"))
                   (path-lte left right'))
          (ReverseIter. set left right' (-keys-for set right' {:sync? true}) (path-get set right' 0))))))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts (seq this))))

#!------------------------------------------------------------------------------

(deftype AsyncReverseSeq [^BTSet set left right ^:mutable keys ^:mutable idx]
  aseq/PAsyncSeq
  (anext [this]
    ;; PAsyncSeq termination contract: an exhausted seq resolves nil — the
    ;; guard must enclose the WHOLE tuple. A bare two-slot vector of `when`
    ;; forms yields a truthy [nil nil] at exhaustion, which consumers
    ;; written against AsyncSeq's contract treat as one more (nil) element.
    (async
     (when (and right (path-lt left right))
       (when (nil? keys)
         (set! keys (await (-keys-for set right {:sync? false})))
         (let [i (path-get set right 0)
               n (arrays/alength keys)]
           (set! idx (if (< i n) i (dec n)))))
       [(arrays/aget keys idx)
        (if (> idx 0)
          (let [right' (path-dec right)]
            (when (path-lt left right')
              (AsyncReverseSeq. set left right' keys (dec idx))))
          (let [right' (await (-prev-path set right {:sync? false}))]
            (when (path-lt left right')
              (let [ks (await (-keys-for set right' {:sync? false}))]
                (AsyncReverseSeq. set left right' ks (path-get set right' 0))))))])))
  Object
  (toString [_] (str "AsyncReverseSeq[" (path-str set right) " <- " (path-str set left) "]"))
  IPrintWithWriter
  (-pr-writer [this w _] (-write w (str this))))

(defn -iter
  ([^BTSet set {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (let [root (await (-root set opts))
                      lvl  (node/level root)]
                  (when (pos? (node/len root))
                    (let [left  EMPTY_PATH
                          rpth  (if (.-storage set)
                                  (await (-rpath set root EMPTY_PATH lvl opts))
                                  (rpath set root EMPTY_PATH lvl))
                          right (await (-next-path set rpth opts))
                          ks (await (-keys-for set left opts))]
                      (if sync?
                        (Iter.     set left right ks (path-get set left 0))
                        (AsyncSeq. set left right ks (path-get set left 0)))))))))
  ([^BTSet set left right {:keys [sync?] :or {sync? true} :as opts}]
   (if sync?
     (Iter. set left right (-keys-for set left {:sync? true}) (path-get set left 0))
     (async
      (let [root (await (-root set {:sync? false}))]
        (when (pos? (node/len root))
          (let [cmp  (.-comparator set)
                left' (await (-seek-path set left cmp {:sync? false}))
                right' (await (-rseek set right cmp {:sync? false}))]
            (when (and left' (path-lt left' right'))
              (let [ks (await (-keys-for set left' {:sync? true}))]
                (AsyncSeq. set left' right' ks (path-get set left' 0)))))))))))

(defn slice
  ([^BTSet set key-from key-to]
   (slice set key-from key-to (.-comparator set) {:sync? true}))
  ([^BTSet set key-from key-to arg]
   (if (fn? arg)
     (slice set key-from key-to arg {:sync? true})
     (slice set key-from key-to (.-comparator set) arg)))
  ([^BTSet set key-from key-to cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (when-some [left (await (-seek-path set key-from cmp opts))]
                  (let [right (await (-rseek set key-to cmp opts))]
                    (when (path-lt left right)
                      (let [ks (await (-keys-for set left opts))]
                        (if sync?
                          (Iter.     set left right ks (path-get set left 0))
                          (AsyncSeq. set left right ks (path-get set left 0)))))))))))

(defn rslice
  ([^BTSet set key-from key-to]
   (rslice set key-from key-to (.-comparator set) {:sync? true}))
  ([^BTSet set key-from key-to arg]
   (if (fn? arg)
     (rslice set key-from key-to arg {:sync? true})
     (rslice set key-from key-to (.-comparator set) arg)))
  ([^BTSet set key-from key-to cmp {:keys [sync?] :or {sync? true} :as opts}]
   (if sync?
     (when-some [iter (slice set key-to key-from cmp opts)]
       (rseq iter))
     (async
      (when-some [from-path (await (-seek-path set key-to cmp opts))]
        (let [to-path (await (-rseek set key-from cmp opts))]
          (when (path-lt from-path to-path)
            (let [left-bound (await (-prev-path set from-path opts))
                  start-path (await (-prev-path set to-path   opts))
                  ks         (await (-keys-for set start-path opts))
                  idx        (path-get set start-path 0)]
              (AsyncReverseSeq. set left-bound start-path ks idx)))))))))

(defn- count-slice-leaf
  "Count elements in range [from, to] within a leaf node."
  [leaf from to cmp]
  (let [keys (.-keys leaf)
        len  (arrays/alength keys)
        from-idx (if from
                   (binary-search-l cmp keys (dec len) from)
                   0)
        to-idx   (if to
                   (binary-search-r cmp keys (dec len) to)
                   len)]
    (if (or (>= from-idx len) (<= to-idx 0))
      0
      (max 0 (- (min to-idx len) (max from-idx 0))))))

(defn- count-slice-node
  "Recursively count elements in range [from, to] within a node."
  [node storage from to cmp {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (instance? Leaf node)
                 (count-slice-leaf node from to cmp)
        ;; Branch node
                 (let [keys (.-keys node)
                       len  (arrays/alength keys)
                       from-idx (if from
                                  (let [idx (binary-search-l cmp keys (- len 2) from)]
                                    (min idx (dec len)))
                                  0)
                       to-idx   (if to
                                  (let [idx (binary-search-r cmp keys (- len 2) to)]
                                    (min idx (dec len)))
                                  (dec len))]
                   (cond
            ;; Empty range
                     (> from-idx to-idx)
                     0

            ;; Same child, recurse into it
                     (== from-idx to-idx)
                     (let [child (await (branch/child node storage from-idx opts))]
                       (await (count-slice-node child storage from to cmp opts)))

            ;; Spans multiple children
                     :else
                     (let [;; Count partial from first child
                           first-child (await (branch/child node storage from-idx opts))
                           first-count (await (count-slice-node first-child storage from nil cmp opts))
                  ;; Count partial in last child
                           last-child  (await (branch/child node storage to-idx opts))
                           last-count  (await (count-slice-node last-child storage nil to cmp opts))
                  ;; Count fully contained children in between
                           middle-count (loop [i (inc from-idx)
                                               acc 0]
                                          (if (>= i to-idx)
                                            acc
                                            (let [child (await (branch/child node storage i opts))
                                                  child-count (node/subtree-count child)
                                                  cnt (if (>= child-count 0)
                                                        child-count
                                                        (await (node/$count child storage opts)))]
                                              (recur (inc i) (+ acc cnt)))))]
                       (+ first-count middle-count last-count))))))))

(defn count-slice
  "Count elements in the range [from, to] inclusive.
   Uses O(log n) algorithm when subtree counts are available.
   If from is nil, counts from the beginning.
   If to is nil, counts to the end."
  ([^BTSet set from to]
   (count-slice set from to (.-comparator set) {:sync? true}))
  ([^BTSet set from to arg]
   (if (fn? arg)
     (count-slice set from to arg {:sync? true})
     (count-slice set from to (.-comparator set) arg)))
  ([^BTSet set from to cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (if (and from to (pos? (cmp from to)))
                  0 ;; Empty range
                  (let [root (await (-root set opts))]
                    (if (zero? (node/len root))
                      0
                      (await (count-slice-node root (.-storage set) from to cmp opts)))))))))

(defn- measure-slice-leaf
  "Compute measure for keys in range [from, to] within a leaf."
  [^Leaf node measure-ops from to cmp]
  (let [keys (.-keys node)
        len  (arrays/alength keys)]
    (loop [i 0
           acc (measure/identity-measure measure-ops)]
      (if (>= i len)
        acc
        (let [key (arrays/aget keys i)
              in-range? (and (or (nil? from) (>= (cmp key from) 0))
                             (or (nil? to) (<= (cmp key to) 0)))]
          (recur (inc i)
                 (if in-range?
                   (measure/merge-measure measure-ops acc (measure/extract measure-ops key))
                   acc)))))))

(defn- measure-slice-node
  "Recursively compute measure for elements in range [from, to] within a node."
  [node storage measure-ops from to cmp {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (instance? Leaf node)
                 (measure-slice-leaf node measure-ops from to cmp)
        ;; Branch node
                 (let [keys (.-keys node)
                       len  (arrays/alength keys)
                       from-idx (if from
                                  (let [idx (binary-search-l cmp keys (- len 2) from)]
                                    (min idx (dec len)))
                                  0)
                       to-idx   (if to
                                  (let [idx (binary-search-r cmp keys (- len 2) to)]
                                    (min idx (dec len)))
                                  (dec len))]
                   (cond
            ;; Empty range
                     (> from-idx to-idx)
                     (measure/identity-measure measure-ops)

            ;; Same child, recurse into it
                     (== from-idx to-idx)
                     (let [child (await (branch/child node storage from-idx opts))]
                       (await (measure-slice-node child storage measure-ops from to cmp opts)))

            ;; Spans multiple children
                     :else
                     (let [;; Measure from partial first child
                           first-child (await (branch/child node storage from-idx opts))
                           first-measure (await (measure-slice-node first-child storage measure-ops from nil cmp opts))
                  ;; Measure from partial last child
                           last-child  (await (branch/child node storage to-idx opts))
                           last-measure  (await (measure-slice-node last-child storage measure-ops nil to cmp opts))
                  ;; Measure from fully contained children in between
                           middle-measure (loop [i (inc from-idx)
                                                 acc (measure/identity-measure measure-ops)]
                                            (if (>= i to-idx)
                                              acc
                                              (let [child (await (branch/child node storage i opts))
                                                    child-measure (or (node/measure child)
                                                                      (await (node/force-compute-measure child storage measure-ops opts)))]
                                                (recur (inc i)
                                                       (measure/merge-measure measure-ops acc child-measure)))))]
                       (measure/merge-measure measure-ops
                                              (measure/merge-measure measure-ops first-measure middle-measure)
                                              last-measure))))))))

(defn measure-slice
  "Compute measure for elements in the range [from, to] inclusive.
   Uses O(log n) algorithm when subtree measure is available.
   If from is nil, computes from the beginning.
   If to is nil, computes to the end.
   Returns nil if no measure-ops configured."
  ([^BTSet set from to]
   (measure-slice set from to (.-comparator set) {:sync? true}))
  ([^BTSet set from to arg]
   (if (fn? arg)
     (measure-slice set from to arg {:sync? true})
     (measure-slice set from to (.-comparator set) arg)))
  ([^BTSet set from to cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (let [measure-ops (:measure (.-settings set))]
                  (if (nil? measure-ops)
                    nil
                    (if (and from to (pos? (cmp from to)))
                      (measure/identity-measure measure-ops) ;; Empty range
                      (let [root (await (-root set opts))]
                        (if (zero? (node/len root))
                          (measure/identity-measure measure-ops)
                          (await (measure-slice-node root (.-storage set) measure-ops from to cmp opts)))))))))))

(defn get-nth
  "Find the entry at weighted rank `n`.
   Navigation uses cached subtree measure and IMeasure weight for
   O(log entries) performance.

   Returns [entry local-offset] where local-offset is the rank
   within the found entry, or nil if out of bounds.

   Requires measure with weight to be configured on the set."
  ([^BTSet set n]
   (get-nth set n {:sync? true}))
  ([^BTSet set n {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (let [root (await (-root set opts))
                      measure-ops (:measure (.-settings set))]
                  (when (nil? measure-ops)
                    (throw (js/Error. "get-nth requires measure to be configured")))
                  (when (pos? (node/len root))
                    (let [root-measure (or (node/measure root)
                                           (await (node/force-compute-measure root (.-storage set) measure-ops opts)))]
                      (when root-measure
                        (let [total-weight (measure/weight measure-ops root-measure)]
                          (when (and (>= n 0) (< n total-weight))
                            ;; Navigate tree
                            (loop [cur-node root
                                   rank n]
                              (if (instance? Branch cur-node)
                            ;; Branch: find child by weight
                                (let [len (node/len cur-node)
                                      result (loop [i 0
                                                    r rank]
                                               (when (< i len)
                                                 (let [child (await (branch/child cur-node (.-storage set) i opts))
                                                       child-measure (or (node/measure child)
                                                                         (await (node/force-compute-measure child (.-storage set) measure-ops opts)))
                                                       child-weight (measure/weight measure-ops child-measure)]
                                                   (if (< r child-weight)
                                                     [child r]
                                                     (recur (inc i) (- r child-weight))))))]
                                  (when result
                                    (recur (nth result 0) (nth result 1))))
                            ;; Leaf: iterate keys by weight
                                (let [keys (.-keys cur-node)
                                      len (arrays/alength keys)]
                                  (loop [i 0
                                         r rank]
                                    (when (< i len)
                                      (let [key (arrays/aget keys i)
                                            key-measure (measure/extract measure-ops key)
                                            key-weight (measure/weight measure-ops key-measure)]
                                        (if (< r key-weight)
                                          [key r]
                                          (recur (inc i) (- r key-weight)))))))))))))))))))

(defn equivalent?
  [^BTSet set other {:keys [sync?] :or {sync? true} :as opts}]
  (if sync?
    (if-not (set? other)
      false
      (and (= (count set) (count other))
           (every? #($contains? set % opts) other)))
    (async
     (if-not (set? other)
       false
       (if (instance? BTSet other)
         ;; NOTE we are assuming both have async-storage (if any)!!
         (and (= (await ($count set opts)) (await ($count other opts)))
              (loop [items (await (-iter other opts))]
                (let [item (and items (await (aseq/first items)))]
                  (if (nil? item)
                    true
                    (if-not (await ($contains? set item opts))
                      false
                      (recur (await (aseq/rest items))))))))
         (and (= (await ($count set opts)) (count other))
              (loop [items (seq other)]
                (let [item (first items)]
                  (if (nil? item)
                    true
                    (if-not (await ($contains? set item opts))
                      false
                      (recur (rest items))))))))))))

(defn equivalent-sequential?
  [xs ys {:keys [sync?] :or {sync? true} :as opts}]
  (if sync?
    (cljs.core/equiv-sequential xs ys)
    (async
     (cond
        ;; BTSet X BTSet
       (and (instance? BTSet xs) (instance? BTSet ys))
       (let [cnt-x (await ($count xs opts))
             cnt-y (await ($count ys opts))]
         (if (not= cnt-x cnt-y)
           false
           (loop [xiter (await (-iter xs opts))
                  yiter (await (-iter ys opts))]
             (let [x (await (aseq/first xiter))
                   y (await (aseq/first yiter))]
               (cond
                 (nil? x) (nil? y)
                 (not= x y) false
                 :else (recur (await (aseq/rest xiter))
                              (await (aseq/rest yiter))))))))

        ;; BTSet X AsyncSeq
       (and (instance? BTSet xs) (satisfies? aseq/PAsyncSeq ys))
       (loop [xiter (await (-iter xs opts))
              yiter ys]
         (let [x (await (aseq/first xiter))
               y (await (aseq/first yiter))]
           (cond
             (nil? x) (nil? y)
             (not= x y) false
             :else (recur (await (aseq/rest xiter))
                          (await (aseq/rest yiter))))))

        ;; AsyncSeq X BTSet
       (and (satisfies? aseq/PAsyncSeq xs) (instance? BTSet ys))
       (loop [xiter xs
              yiter (await (-iter ys opts))]
         (let [x (await (aseq/first xiter))
               y (await (aseq/first yiter))]
           (cond
             (nil? x) (nil? y)
             (not= x y) false
             :else (recur (await (aseq/rest xiter))
                          (await (aseq/rest yiter))))))

        ;; AsyncSeq X AsyncSeq
       (and (satisfies? aseq/PAsyncSeq xs) (satisfies? aseq/PAsyncSeq ys))
       (loop [xiter xs
              yiter ys]
         (let [x (await (aseq/first xiter))
               y (await (aseq/first yiter))]
           (cond
             (nil? x) (nil? y)
             (not= x y) false
             :else (recur (await (aseq/rest xiter))
                          (await (aseq/rest yiter))))))

        ;; BTSet X Seqable
       (and (instance? BTSet xs) (or (seqable? ys) (array? ys)))
       (let [s (if (array? ys) (array-seq ys 0) (seq ys))
             cnt-x (await ($count xs opts))
             cnt-y (count s)]
         (if (not= cnt-x cnt-y)
           false
           (loop [xiter (await (-iter xs opts))
                  z     s]
             (let [x (await (aseq/first xiter))]
               (if (nil? x)
                 (nil? z)
                 (if (= x (first z))
                   (recur (await (aseq/rest xiter)) (next z))
                   false))))))

        ;; AsyncSeq X Seqable
       (and (satisfies? aseq/PAsyncSeq xs) (or (seqable? ys) (array? ys)))
       (let [s (if (array? ys) (array-seq ys 0) (seq ys))]
         (loop [xiter xs
                z     s]
           (let [x (await (aseq/first xiter))]
             (cond
               (nil? x) (nil? z)
               (nil? z) false
               (= x (first z)) (recur (await (aseq/rest xiter)) (next z))
               :else false))))

       :else false))))

(defn async-reduce
  [arf set from]
  (assert (instance? BTSet set) "async-reduce expects a BTSet in second arg")
  (if (instance? BTSet from)
    (async
     (loop [acc set
            items (await (-iter from {:sync? false}))]
       (if-some [x (await (aseq/first items))]
         (let [acc' (await (arf acc x))]
           (if (reduced? acc')
             (await (arf (unreduced acc')))
             (recur acc' (await (aseq/rest items)))))
         (await (arf acc)))))
    (if (satisfies? aseq/PAsyncSeq from)
      (async
       (loop [acc set
              items from]
         (if-some [x (await (aseq/first items))]
           (let [acc' (await (arf acc x))]
             (if (reduced? acc')
               (await (arf (unreduced acc')))
               (recur acc' (await (aseq/rest items)))))
           (await (arf acc)))))
      (async
       (loop [acc set
              items from]
         (if (seq items)
           (let [v (first items)]
             (if (some? v)
               (let [acc' (await (arf acc v))]
                 (if (reduced? acc')
                   (await (arf (unreduced acc')))
                   (recur acc' (rest items))))
               (await (arf acc))))
           (await (arf acc))))))))

(defn- xf-driver
  "Returns {:step (fn [x] {:out <vector> :done? <bool>})
            :complete (fn [] <vector>)}.
   Internally applies `xform` to a synchronous collecting rf, preserving
   early-termination semantics via `reduced`."
  [xform]
  (let [buf  (volatile! [])
        step (fn
               ([] nil)
               ([acc] acc)
               ([acc x] (vswap! buf conj x) acc))
        xf   (xform step)]
    {:step
     (fn [x]
       (let [start (count @buf)
             ret   (xf nil x)
             v     @buf
             out   (if (> (count v) start) (subvec v start (count v)) [])
             done? (reduced? ret)]
         (when done?
           ;; finalize the transducer and collect any final outputs
           (let [s2 (count @buf)
                 _  (xf (unreduced ret))
                 v2 @buf
                 tail (if (> (count v2) s2) (subvec v2 s2 (count v2)) [])]
             (vreset! buf [])
             {:out (if (seq tail) (into out tail) out)
              :done? true}))
         (when-not done?
           (vreset! buf [])
           {:out out :done? false})))

     :complete
     (fn []
       (let [start (count @buf)
             _     (xf nil)
             v     @buf
             out   (if (> (count v) start) (subvec v start (count v)) [])]
         (vreset! buf [])
         out))}))

(defn async-transduce
  "xform: synchronous transducer (core map/filter/comp/etc)
   arf:   MUST BE ASYNC reducing fn with arities ([acc] ...) and ([acc x] ...)
   init:  initial accumulator
   from:  BTSet | aseq/PAsyncSeq | sequential"
  [xform arf init from]
  (let [{:keys [step complete]} (xf-driver xform)
        apply-outs
        (fn [acc outs]
          (async
           (loop [a acc, i 0, n (count outs)]
             (if (< i n)
               (let [a' (await (arf a (nth outs i)))]
                 (if (reduced? a')
                   (reduced (unreduced a'))
                   (recur a' (unchecked-inc i) n)))
               a))))]

    (cond
      (instance? BTSet from)
      (async
       (loop [acc init
              items (await (-iter from {:sync? false}))]
         (if-some [x (await (aseq/first items))]
           (let [{:keys [out done?]} (step x)
                 acc' (await (apply-outs acc out))]
             (if (reduced? acc')
               (await (arf (unreduced acc')))
               (if done?
                 (await (arf acc'))
                 (recur acc' (await (aseq/rest items))))))
           (let [tail (complete)
                 acc' (await (apply-outs acc tail))]
             (await (arf acc'))))))

      (satisfies? aseq/PAsyncSeq from)
      (async
       (loop [acc init
              items from]
         (if-some [x (await (aseq/first items))]
           (let [{:keys [out done?]} (step x)
                 acc' (await (apply-outs acc out))]
             (if (reduced? acc')
               (await (arf (unreduced acc')))
               (if done?
                 (await (arf acc'))
                 (recur acc' (await (aseq/rest items))))))
           (let [tail (complete)
                 acc' (await (apply-outs acc tail))]
             (await (arf acc'))))))

      (sequential? from)
      (async
       (loop [acc init
              xs  (seq from)]
         (if (seq xs)
           (let [{:keys [out done?]} (step (first xs))
                 acc' (await (apply-outs acc out))]
             (if (reduced? acc')
               (await (arf (unreduced acc')))
               (if done?
                 (await (arf acc'))
                 (recur acc' (next xs)))))
           (let [tail (complete)
                 acc' (await (apply-outs acc tail))]
             (await (arf acc'))))))

      :else
      (throw (js/Error. (str "async-transduce: unsupported input type " (type from)))))))

(defn $reduce
  ([rf init from]
   ($reduce rf init from {:sync? true}))
  ([rf init from {:keys [sync?] :or {sync? true}}]
   (assert (instance? BTSet init))
   (if sync?
     (reduce rf init from)
     (async-reduce rf init from))))

(defn $transduce
  ([xform arf init from]
   ($transduce xform arf init from {:sync? true}))
  ([xform arf init from {:keys [sync?] :or {sync? true}}]
   (assert (instance? BTSet init))
   (if sync?
     (transduce xform arf init from)
     (async-transduce xform arf init from))))

(defn $into
  ([to from]
   ($into to (map identity) from {:sync? true}))
  ([to arg0 arg1]
   (if (fn? arg0)
     ($into to arg0 arg1 {:sync? true})
     ($into to (map identity) arg0 arg1)))
  ([to xform from {:keys [sync?] :or {sync? true}}]
   (assert (instance? BTSet to))
   (if sync?
     (into to xform from)
     (async-transduce xform
                      (fn
                        ([acc] (async acc))
                        ([acc item]
                         (if (instance? BTSet acc)
                           (conjoin acc item (.-comparator acc) {:sync? false})
                           (async (conj acc item)))))
                      to
                      from))))

(defn $seq
  ([set]
   ($seq set {:sync? true}))
  ([set opts]
   (-iter set opts)))

(defn $rseq
  ([set]
   ($rseq set {:sync? true}))
  ([set {:keys [sync?] :or {sync? true} :as opts}]
   (if sync?
     (rseq (-iter set {:sync? true}))
     (async
      (let [i (await (-iter set opts))]
        (when (.-keys i)
          (let [l' (await (-prev-path set (.-left i) opts))
                r' (await (-prev-path set (.-right i) opts))
                ks (await (-keys-for set r' opts))
                idx (path-get set r' 0)]
            (AsyncReverseSeq. set l' r' ks idx))))))))

(defn seek
  ([seq key]
   (seek seq key {:sync? true}))
  ([seq key arg]
   (assert (some? seq))
   (if (fn? arg)
     (seek seq key arg {:sync? true})
     (seek seq key (.-comparator ^BTSet (.-set ^js seq)) arg)))
  ([seq key cmp {:keys [sync?] :or {sync? true} :as opts}]
   (assert (some? seq))
   (assert (fn? cmp))
   (if sync?
     (-seek seq key cmp)
     (let [set (.-set seq)]
       (assert (instance? BTSet set))
       (if (instance? AsyncSeq seq)
         (if (nat-int? (cmp (arrays/aget (.-keys seq) (.-idx seq)) key))
           (async seq)
           (async
            (when-some [left' (await (-seek-path set key cmp opts))]
              (let [ks (await (-keys-for set left' opts))]
                (AsyncSeq. set left' (.-right seq) ks (path-get set left' 0))))))
         (if (instance? AsyncReverseSeq seq)
           (if (nat-int? (cmp key (arrays/aget (.-keys seq) (.-idx seq))))
             (async seq)
             (async
              (let [k (await (-rseek set key cmp opts))
                    right' (await (-prev-path set k opts))]
                (when (and right' (>= right' (js* "0n"))
                           (path-lte (.-left seq) right')
                           (path-lt  right' (.-right seq)))
                  (AsyncReverseSeq. set (.-left seq) right' (await (-keys-for set right' opts)) (path-get set right' 0))))))
           (throw (js/Error. (str "unsupported type: '" (type seq) "'")))))))))

#!------------------------------------------------------------------------------

(defn- empty-leaf
  "The empty root leaf, carrying the measure IDENTITY when a measure is configured.

   Every measure-maintenance site in leaf.cljs and branch.cljs is guarded on the SOURCE
   node already having a measure — correct for maintenance, but it means nothing ever
   BOOTSTRAPS one. The empty leaf was built with nil, so on the incremental path no node
   ever acquired a measure at all, and the library's own `diagnostics/validate-measures-known`
   failed on ClojureScript for every conj-built set (measured: 12 / 129 / 5 / 56 violations at
   bf 8 n 40 / bf 8 n 400 / bf 16 n 40 / bf 16 n 400 — leaves as well as branches). It also
   made cljs internally inconsistent, since the bulk builders compute eagerly, so bulk and
   incremental builds of the SAME set produced different `:measure` in their blobs.

   Seeding the identity here is enough: from a non-nil measure the existing incremental
   maintenance carries it through every add, split, merge and borrow, so the whole tree
   bootstraps from the empty set outward. The identity is what an empty leaf's measure IS,
   so this is also the correct value rather than a convenient one."
  [settings]
  (let [measure-ops (:measure settings)]
    (Leaf. (arrays/array) settings (when measure-ops (measure/identity-measure measure-ops)))))

(deftype BTSet [^:mutable root cnt comparator meta ^:mutable _hash storage ^:mutable address ^:mutable settings]
  Object
  (toString [this] (pr-str* this))

  ICloneable
  (-clone [_] (BTSet. root cnt comparator meta _hash storage address settings))

  IWithMeta
  (-with-meta [_ new-meta] (BTSet. root cnt comparator new-meta _hash storage address settings))

  IMeta
  (-meta [_] meta)

  IEmptyableCollection
  ;; KEEP the storage. This passed `nil` for it, so `(store (empty s))` threw "BTSet/store
  ;; requires IStorage in second argument" while the JVM twin (which carries `_storage`
  ;; through) returns an address — the same class of defect already fixed for `compact`, and
  ;; the same failure shape: a crash on the next write, well away from the `empty` that
  ;; caused it. The ADDRESS is still dropped, correctly: an empty set shares no nodes with
  ;; the one it came from, so it has nothing durable to point at until it is stored.
  (-empty [_] (BTSet. (empty-leaf settings) 0 comparator meta UNINITIALIZED_HASH storage nil settings))

  IEquiv
  (-equiv [this other]
    (equivalent? this other {:sync? true}))

  IHash
  (-hash [this] (caching-hash this hash-unordered-coll _hash))

  ICollection
  (-conj [this key] (conjoin this key comparator {:sync? true}))

  ISet
  (-disjoin [this key] (disjoin this key comparator {:sync? true}))

  ILookup
  (-lookup [this k] (lookup this k nil {:sync? true}))
  (-lookup [this k not-found]
    (let [result (lookup this k nil {:sync? true})]
      (if (some? result) result not-found)))

  ISeqable
  (-seq [this]
    (-iter this {:sync? true}))

  IReduce
  (-reduce [this f] (if-let [i (-iter this {:sync? true})] (-reduce i f) (f)))
  (-reduce [this f start] (if-let [i (-iter this {:sync? true})] (-reduce i f start) start))

  IReversible
  (-rseq [this] (when-some [i (-iter this {:sync? true})] (rseq i)))   ; empty set ⇒ nil (not a nil-iter error)

  ; ISorted
  ; (-sorted-seq [this ascending?])
  ; (-sorted-seq-from [this k ascending?])
  ; (-entry-key [this entry] entry)
  ; (-comparator [this] comparator)

  ICounted
  (-count [this] ($count this {:sync? true}))

  IEditableCollection
  (-as-transient [this] this)

  ITransientCollection
  (-conj! [this key] (conjoin this key comparator {:sync? true}))
  (-persistent! [this] this)

  ITransientSet
  (-disjoin! [this key] (disjoin this key comparator {:sync? true}))

  IFn
  (-invoke [this k] (-lookup this k))
  (-invoke [this k not-found] (-lookup this k not-found))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "#{" " " "}" opts (seq this))))

#!------------------------------------------------------------------------------
#! Constructors

(defn- arr-partition-approx
  "Splits `arr` into arrays of size between min-len and max-len,
   trying to stick to (min+max)/2.

   The three cases, and their ORDER, are `persistent-sorted-set/split`'s on the
   JVM. They used to differ, and the difference was not cosmetic: this asked
   whether `chunk-len + min-len` remained where the JVM asks whether `2*avg`
   does, and tested `<= max-len` first rather than second. Since
   `2*avg = min+max > avg+min`, the two disagreed for every remainder in
   `[avg+min, 2*avg)` — at the default branching factor of 512, measured on both
   runtimes rather than reasoned about:

       n     JVM              ClojureScript
       640   [320 320]        [384 256]
       700   [350 350]        [384 316]
       767   [383 384]        [384 383]

   Same elements, same settings, different trees. Under content-addressed
   storage that means different node addresses and so a different merkle root
   for the same data, so a set built on Node could not share nodes with one
   built on the JVM — which is the whole point of addressing them by content.
   Sizes outside that window (512, 513, 768, 900) already agreed, which is why
   it went unnoticed.

   The `:else` branch recurs where the JVM emits both halves and stops; that is
   the same partition, because after taking `half rest` the remainder is at most
   `max-len` and the next iteration takes it whole."
  [settings arr]
  (let [chunk-len (avg-len settings)
        max-len   (max-len settings)
        len       (arrays/alength arr)
        acc       (transient [])]
    (when (pos? len)
      (loop [pos 0]
        (let [rest (- len pos)]
          (cond
            (>= rest (* 2 chunk-len))
            (do
              (conj! acc (.slice arr pos (+ pos chunk-len)))
              (recur (+ pos chunk-len)))
            (<= rest max-len)
            (conj! acc (.slice arr pos))
            :else
            (let [piece-len (arrays/half rest)]
              (conj! acc (.slice arr pos (+ pos piece-len)))
              (recur (+ pos piece-len)))))))
    (to-array (persistent! acc))))

(defn- sorted-arr-distinct? [arr cmp]
  (let [al (arrays/alength arr)]
    (if (<= al 1)
      true
      (loop [i 1
             p (arrays/aget arr 0)]
        (if (>= i al)
          true
          (let [e (arrays/aget arr i)]
            (if (== 0 (cmp e p))
              false
              (recur (inc i) e))))))))

(defn- sorted-arr-distinct
  "Filter out repetitive values in a sorted array.
   Optimized for no-duplicates case"
  [arr cmp]
  (if (sorted-arr-distinct? arr cmp)
    arr
    (let [al (arrays/alength arr)]
      (loop [acc (transient [(arrays/aget arr 0)])
             i   1
             p   (arrays/aget arr 0)]
        (if (>= i al)
          (into-array (persistent! acc))
          (let [e (arrays/aget arr i)]
            (if (== 0 (cmp e p))
              (recur acc (inc i) e)
              (recur (conj! acc e) (inc i) e))))))))

(defn- arr-map-inplace [f arr]
  (let [len (arrays/alength arr)]
    (loop [i 0]
      (when (< i len)
        (arrays/aset arr i (f (arrays/aget arr i)))
        (recur (inc i))))
    arr))

(declare from-sorted-array-count)

(defn- mst-partition
  "Bulk MST partition: cut arr[0..n) into JS-array segments after each index i<n-1 whose
   (key-of arr[i]) rises to thresh (= node-level+1). Mirrors the JVM mst-split."
  [bd arr key-of thresh]
  (let [n (arrays/alength arr)
        last-i (dec n)]
    (loop [i 0 start 0 out (transient [])]
      (if (>= i n)
        (persistent! (if (> n start) (conj! out (.slice arr start n)) out))
        (if (and (< i last-i) (>= (b/-key-level bd (key-of (aget arr i))) thresh))
          (recur (inc i) (inc i) (conj! out (.slice arr start (inc i))))
          (recur (inc i) start out))))))

;; ---------------------------------------------------------------------------
;; Supported branching factors — must agree with Settings.MIN_BRANCHING_FACTOR
;; on the JVM (4) and with `Settings.checkBranchingFactor`'s treatment of an
;; unset value.
;;
;; Below 4 the minimum fill (`bf >>> 1`) is 1, so a branch of length 1 counts as
;; full, its only child has no sibling to rebalance with, and a removal leaves a
;; length-0 leaf. The JVM then throws (`maxKey()` reads index -1); ClojureScript
;; does not — it keeps the empty leaf and starts yielding `nil` ELEMENTS out of a
;; set whose constructor refuses nil, with `count` disagreeing with `seq`.
;; Measured, bf 2, one random seed: `count` 41 for 39 real elements, 5 leaves of
;; length 0, and 11 structural violations reported by `diagnostics/validate`.
;;
;; A non-positive or absent value means UNSET and takes the default, matching the
;; JVM where `Settings()` delegates with 0. Only an explicit 1, 2 or 3 is refused.
(def ^:const MIN-BRANCHING-FACTOR 4)
(def ^:const DEFAULT-BRANCHING-FACTOR 512)

(defn check-branching-factor
  "Normalize an unset branching factor to the default and refuse an unsupported one."
  [bf]
  (let [bf (if (or (nil? bf) (not (pos? bf))) DEFAULT-BRANCHING-FACTOR bf)]
    (when (< bf MIN-BRANCHING-FACTOR)
      (throw (ex-info (str "branching-factor " bf " is not supported: the minimum is "
                           MIN-BRANCHING-FACTOR ". Below it the minimum fill (bf >>> 1) is 1, "
                           "so a branch of length 1 is considered full, its only child has no "
                           "sibling to rebalance with, and a removal leaves a length-0 leaf. "
                           "Measured: this yields nil elements and a count that disagrees with "
                           "seq in ClojureScript, and throws ArrayIndexOutOfBoundsException on "
                           "the JVM. Omit :branching-factor for the default of "
                           DEFAULT-BRANCHING-FACTOR ".")
                      {:branching-factor bf :min MIN-BRANCHING-FACTOR})))
    bf))

(defn- node-settings
  "The settings a node carries, normalized and validated exactly as the JVM's
   `Settings` constructor does."
  [opts]
  (-> (select-keys opts [:branching-factor :measure :boundary :diff-buf-size])
      (update :branching-factor check-branching-factor)
      (update :diff-buf-size (fn [d] (if (or (nil? d) (neg? d)) 0 d)))))

(defn ^BTSet from-sorted-array
  "Build from the first `len` elements of `arr`.

   `len` used to be ignored outright — the parameter was spelled `_len` and both branches
   below partitioned the WHOLE array and took the count from `(arrays/alength arr)`. A caller
   passing a reusable buffer with only its first `len` slots valid got the buffer's stale tail
   as set members, silently:

       (from-sorted-array compare #js [1 2 3 4 5] 3)
       JVM  => [1 2 3]      count 3
       cljs => [1 2 3 4 5]  count 5

   Truncating here rather than threading `len` through both builders keeps the two branches
   (MST and count) honest by construction — neither can forget it again — and costs one array
   slice only when the caller actually passed a shorter length."
  [cmp arr len opts]
  (let [full     (arrays/alength arr)
        len      (if (nil? len) full len)
        _        (when (or (neg? len) (> len full))
                   (throw (ex-info "from-sorted-array: len out of range"
                                   {:len len :array-length full})))
        arr      (if (< len full) (.slice arr 0 len) arr)
        settings (node-settings opts)
        measure-ops (:measure settings)
        storage  (:storage opts)
        bd       (b/content-boundary settings)]
    (if bd
      ;; split-seam (MST): chunk by boundary level so bulk == incremental (≡ JVM mst-split).
      (let [leaves (mapv #(let [leaf (Leaf. % settings nil)]
                            (when measure-ops (node/try-compute-measure leaf nil measure-ops {:sync? true}))
                            leaf)
                         (mst-partition bd arr identity 1))]
        (loop [nodes leaves, lvl 1]
          (case (count nodes)
            0 (BTSet. (empty-leaf settings) 0 cmp (:meta opts) UNINITIALIZED_HASH storage nil settings)
            1 (BTSet. (first nodes) (arrays/alength arr) cmp (:meta opts) UNINITIALIZED_HASH storage nil settings)
            (recur (mapv #(mst-build-branch lvl (array-seq %) settings cmp)
                         (mst-partition bd (arrays/into-array nodes) node/max-key (inc lvl)))
                   (inc lvl)))))
      (from-sorted-array-count cmp arr len settings storage measure-ops (:meta opts)))))

(defn- ^BTSet from-sorted-array-count
  ;; `meta-val` is threaded explicitly: this fn takes `settings` (a select-keys subset), not
  ;; `opts`, so `:meta` is not reachable here otherwise. Dropping it was the cljs half of the
  ;; defect fixed on the JVM — `from-sorted-array`/`from-sequential`/`sorted-set` all lost the
  ;; metadata the wire codec resolves `:pss/storage-id` from.
  ;; `arr` arrives already truncated to `len` by `from-sorted-array`, so the count taken from
  ;; `(arrays/alength arr)` below is the caller's length. `len` is kept in the signature only
  ;; to keep the two call sites symmetric.
  [cmp arr len settings storage measure-ops meta-val]
  (let [leaves   (->> arr
                      (arr-partition-approx settings)
                      (arr-map-inplace #(let [leaf (Leaf. % settings nil)]
                                          ;; Compute measure for leaf if measure-ops available
                                          (when measure-ops
                                            (node/try-compute-measure leaf nil measure-ops {:sync? true}))
                                          leaf)))]
    (loop [current-level leaves
           shift 0]
      (case (count current-level)
        0 (BTSet. (Leaf. (arrays/array) settings nil) 0 cmp meta-val UNINITIALIZED_HASH storage nil settings)
        1 (BTSet. (first current-level) (arrays/alength arr) cmp meta-val UNINITIALIZED_HASH storage nil settings)
        (recur
         (->> current-level
              (arr-partition-approx settings)
              (arr-map-inplace #(let [subtree-count (reduce + 0 (map node/subtree-count %))
                                      ;; Compute measure from children if measure-ops available
                                      measure-ops (:measure settings)
                                      child-measure (when measure-ops
                                                      (reduce (fn [acc child]
                                                                (if (nil? acc)
                                                                  (reduced nil)
                                                                  (let [cs (node/measure child)]
                                                                    (if cs
                                                                      (measure/merge-measure measure-ops acc cs)
                                                                      (reduced nil)))))
                                                              (measure/identity-measure measure-ops)
                                                              %))]
                                  (Branch. (inc shift)
                                           (arrays/amap node/max-key %)
                                           %
                                           nil
                                           subtree-count
                                           child-measure
                                           ;; diff-buf: bulk-built branch — _bufEntries = 0 (no slots,
                                           ;; anchorless ⇒ written wholesale at store). See branch.cljs.
                                           settings nil 0 cmp))))
         (inc shift))))))

(defn ^BTSet from-sequential [cmp seq opts]
  (when (some nil? seq) (throw (ex-info "PersistentSortedSet cannot store nil" {})))
  (let [arr (-> (into-array seq) (arrays/asort cmp) (sorted-arr-distinct cmp))]
    (from-sorted-array cmp arr (alength arr) opts)))

;; ---------------------------------------------------------------------------
;; streaming bulk build

;; One per level, as a JS array so the hot loop does no allocation:
;;   [keys addrs counts measures emitted last-address last-count]
;; At level 0 `keys` holds raw ELEMENTS and slots 1-3 stay empty.
;;
;; JS arrays and not vectors, deliberately. `.splice` and `.push` copy, so the
;; head handed to a node does not alias what remains. The JVM builder had to
;; write `(into [] (subvec buf 0 avg))` for exactly this reason — a SubVector
;; shares its base, and `conj` on one does `base.assocN(end, o)`, so the base
;; grows without bound and every element ever consumed stays reachable, making
;; the build silently O(n). ClojureScript's `Subvec` retains identically
;; (`-conj` is `(-assoc-n v end o)`), so "simplifying" this to vectors would
;; reintroduce that bug on a second runtime.
;; The driver's four states, as INTEGERS.
;;
;; Keywords with `identical?` would be the obvious spelling and are a trap: it
;; compiles to `===`, which holds only where the compiler has hoisted keyword
;; literals into shared constants. That depends on build options — it held in
;; this project's own `:advanced` build and NOT in datahike's, where every
;; comparison silently returned false, every iteration fell through to the drain
;; branch, and an 800-element build produced an empty set with no error. Ints
;; compare the same way under every optimization setting.
(def ^:private ^:const SBB-INPUT 0)
(def ^:private ^:const SBB-PUSH 1)
(def ^:private ^:const SBB-CUT 2)
(def ^:private ^:const SBB-DRAIN 3)

(def ^:private ^:const SBB-KEYS 0)
(def ^:private ^:const SBB-ADDRS 1)
(def ^:private ^:const SBB-CNTS 2)
(def ^:private ^:const SBB-MEAS 3)
(def ^:private ^:const SBB-EMITTED 4)
(def ^:private ^:const SBB-LAST-ADDR 5)
(def ^:private ^:const SBB-LAST-CNT 6)

(defn- sbb-level [bufs lvl]
  (or (aget bufs lvl)
      (let [lv #js [#js [] #js [] #js [] #js [] 0 nil 0]]
        (aset bufs lvl lv)
        lv)))

(defn- ^Leaf sbb-leaf [ks settings measure-ops]
  (let [leaf (Leaf. ks settings nil)]
    ;; `{:sync? true}` is right even in the async arm: Leaf/try-compute-measure
    ;; ignores `storage` entirely, folds the keys and `set!`s the result. No IO.
    ;; Same call `from-sorted-array-count` makes.
    (when measure-ops
      (node/try-compute-measure leaf nil measure-ops {:sync? true}))
    leaf))

(defn- ^Branch sbb-branch [lvl ks as cs ms settings measure-ops cmp]
  (let [n    (arrays/alength ks)
        cnt  (loop [i 0 acc 0]
               (if (< i n) (recur (inc i) (+ acc (aget cs i))) acc))
        ;; Folded here rather than by the node, because Branch/try-compute-measure
        ;; is guarded by `(when (some? children) ...)` — for an address-only
        ;; branch it returns nil and sets NOTHING. `from-sorted-array-count` and
        ;; `mst-build-branch` fold the same way; the nil short-circuit is theirs.
        meas (when measure-ops
               (loop [i 0 acc (measure/identity-measure measure-ops)]
                 (if (< i n)
                   (when-some [m (aget ms i)]
                     (recur (inc i) (measure/merge-measure measure-ops acc m)))
                   acc)))]
    ;; children = nil: the parent holds an ADDRESS and never a child pointer,
    ;;   which is the whole reason peak memory is O(depth x branching-factor).
    ;; _bufEntries = 0, not -2: -2 is `branch/from-map`'s LAZY marker for a node
    ;;   whose slots the storage layer will back-fill. We built this one and it
    ;;   has no slots, so it is clean and written wholesale.
    ;; _projCmp = cmp: matches every other bulk-built branch (from-sorted-array-count).
    (Branch. lvl ks nil as cnt meas settings nil 0 cmp)))

(defn ^BTSet from-sorted-seq
  "Bulk-build a set from a SORTED, DISTINCT seq, storing every node as it fills.

   The ClojureScript counterpart of the JVM's `from-sorted-seq`, and it builds
   the SAME tree: same cuts, same levels, same node contents, so a node address
   means the same thing whichever runtime produced it.

   ## Why this is a push machine and not the JVM's lazy seqs

   The JVM version is `(map (fn [ks] ... store-node! ...) (streaming-split ...))`
   — IO inside a function literal. partial-cps refuses that outright: `await`
   inside a `fn` can never suspend, because closures are opaque to the CPS
   transform. A literal port would not compile in the async arm.

   So `streaming-split`'s lookahead buffer is turned inside out. It asks only
   whether `2*avg` elements remain, so pushing one element at a time and cutting
   when a level reaches `2*avg` answers the same question, yields the identical
   cut sequence, and keeps the identical O(depth x branching-factor) bound —
   while putting every `node/store` at a statement position inside one `async`
   block. `async+sync` then emits both arms from this one source.

   A cut hands its entry up by pushing at `lvl+1`, so cascades happen by
   themselves. That is what a naive 'finish each level, then move up' version
   gets wrong: it would store every leaf before any branch.

   ## Options

   `:storage` is required — without somewhere to put nodes there is nothing to
   stream to, and `from-sorted-array` is the right call instead.

   `:flush-fn` is optional, called after each node is stored and AWAITED. The
   await is the point: it is backpressure, so a caller buffering writes can
   drain without the buffer growing without bound. Return a value under
   `{:sync? true}`, a continuation under `{:sync? false}`.

   Input MUST be sorted and distinct under `cmp`; this is checked, because the
   alternative is a silently corrupt tree."
  [cmp xs {:keys [sync? storage flush-fn] :or {sync? true} :as opts}]
  (let [settings    (node-settings opts)
        measure-ops (:measure settings)
        max-bf      (max-len settings)
        avg-bf      (avg-len settings)
        need        (* 2 avg-bf)]
    ;; Eagerly, outside async+sync, so misuse throws at the CALL SITE in both
    ;; arms instead of becoming a rejected continuation nobody looks at.
    (when (nil? storage)
      (throw (ex-info "from-sorted-seq requires :storage — use from-sorted-array for an in-memory build"
                      {:type :pss/requires-storage})))
    (when (b/content-boundary settings)
      (throw (ex-info "from-sorted-seq does not support content-defined (MST) boundaries"
                      {:type :pss/unsupported-boundary})))
    ;; `throw`, not `assert`: :advanced elides asserts in a consumer release, and
    ;; an elided guard here means a branching factor of 2 spins until it OOMs
    ;; instead of failing — with avg = 1 every branch gets one child, so a level
    ;; of k nodes produces k nodes again and the tree grows upward forever. The
    ;; JVM can afford `assert` here; ClojureScript cannot.
    (when (< avg-bf 2)
      (throw (ex-info (str "branching-factor must be >= 4 for a streaming build (got avg fanout "
                           avg-bf "); a fanout of 1 never reduces the level count")
                      {:type :pss/branching-factor-too-small :avg avg-bf})))
    (async+sync
     sync?
     (async
      (let [bufs #js []]
        ;; `cond` + `identical?` rather than `case`: `if` is unambiguously
        ;; inverted by partial-cps, so the state machine cannot depend on how
        ;; cljs `case` expands inside a CPS'd loop.
        (loop [s (seq xs), prev nil, seen? false
               op SBB-INPUT, lvl 0, width 0, k nil, addr nil, cnt 0, meas nil]
          (cond
            ;; ---- pull one element, checking the input contract as we go ----
            (== op SBB-INPUT)
            (if (nil? s)
              (recur s prev seen? SBB-DRAIN 0 0 nil nil 0 nil)
              (let [x (first s)]
                (when (nil? x)
                  (throw (ex-info "PersistentSortedSet cannot store nil" {:type :pss/nil-key})))
                (when (and seen? (>= 0 (cmp x prev)))
                  (throw (ex-info (str "from-sorted-seq requires strictly ascending input; "
                                       (pr-str prev) " was followed by " (pr-str x))
                                  {:type :pss/unsorted :prev prev :next x})))
                (recur (next s) x true SBB-PUSH 0 0 x nil 0 nil)))

            ;; ---- append at `lvl`; a full level owes a cut of exactly `avg` ----
            (== op SBB-PUSH)
            (let [lv (sbb-level bufs lvl)]
              (.push (aget lv SBB-KEYS) k)
              (when (pos? lvl)
                (.push (aget lv SBB-ADDRS) addr)
                (.push (aget lv SBB-CNTS) cnt)
                (.push (aget lv SBB-MEAS) meas))
              (if (>= (arrays/alength (aget lv SBB-KEYS)) need)
                (recur s prev seen? SBB-CUT lvl avg-bf nil nil 0 nil)
                (recur s prev seen? SBB-INPUT 0 0 nil nil 0 nil)))

            ;; ---- the ONE IO site: build the node, store it, hand its entry up ----
            (== op SBB-CUT)
            (let [lv   (sbb-level bufs lvl)
                  hks  (.splice (aget lv SBB-KEYS) 0 width)
                  node (if (zero? lvl)
                         (sbb-leaf hks settings measure-ops)
                         (sbb-branch lvl hks
                                     (.splice (aget lv SBB-ADDRS) 0 width)
                                     (.splice (aget lv SBB-CNTS) 0 width)
                                     (.splice (aget lv SBB-MEAS) 0 width)
                                     settings measure-ops cmp))
                  a    (await (node/store node storage opts))
                  _    (when flush-fn (await (flush-fn)))
                  c    (node/subtree-count node)]
              (aset lv SBB-EMITTED (inc (aget lv SBB-EMITTED)))
              (aset lv SBB-LAST-ADDR a)
              (aset lv SBB-LAST-CNT c)
              (recur s prev seen? SBB-PUSH (inc lvl) 0
                     (node/max-key node) a c (node/measure node)))

            ;; ---- input exhausted: finish levels bottom-up ----
            ;; A level that ends having emitted exactly ONE node is the root.
            :else
            (let [lv (sbb-level bufs lvl)
                  n  (arrays/alength (aget lv SBB-KEYS))]
              (cond
                ;; streaming-split's two terminal cases, in its order. `half`,
                ;; not `quot`, to match arr-partition-approx exactly.
                (pos? n)
                (recur s prev seen? SBB-CUT lvl (if (<= n max-bf) n (arrays/half n)) nil nil 0 nil)

                (zero? (aget lv SBB-EMITTED))
                (BTSet. (Leaf. (arrays/array) settings nil) 0 cmp (:meta opts)
                        UNINITIALIZED_HASH storage nil settings)

                (== 1 (aget lv SBB-EMITTED))
                ;; Address-rooted, exactly as a restore is: the root loads on
                ;; first access. `cnt` is exact, so `count` never has to walk —
                ;; an address-rooted set with cnt -1 restores the whole tree on
                ;; the first `count`.
                (BTSet. nil (aget lv SBB-LAST-CNT) cmp (:meta opts)
                        UNINITIALIZED_HASH storage (aget lv SBB-LAST-ADDR) settings)

                :else
                (recur s prev seen? SBB-DRAIN (inc lvl) 0 nil nil 0 nil))))))))))

(defn ^BTSet from-opts
  "Create a set with options map containing:
   - :storage  Storage implementation
   - :comparator  Custom comparator (defaults to compare)
   - :measure  Measure implementation (IMeasure protocol)
   - :meta     Metadata"
  [opts]
  (let [settings (node-settings opts)]
    (BTSet. (empty-leaf settings) 0 (or (:comparator opts) (:cmp opts) compare)
            (:meta opts) UNINITIALIZED_HASH (:storage opts) nil settings)))

(defn ^BTSet sorted-set-by
  ([cmp]
   (from-opts {:comparator cmp}))
  ([cmp & keys]
   (from-sequential cmp keys {})))
