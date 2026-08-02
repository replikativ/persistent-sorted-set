(ns ^{:author "Nikita Prokopov"
      :doc "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can’t store nil."}
 org.replikativ.persistent-sorted-set
  (:refer-clojure :exclude [conj disj sorted-set sorted-set-by replace])
  (:require
   [org.replikativ.persistent-sorted-set.arrays :as arrays])
  (:import
   [clojure.lang RT]
   [java.lang.ref SoftReference]
   [java.util Comparator Arrays]
   [java.util.function BiConsumer]
   [org.replikativ.persistent_sorted_set ANode ArrayUtil Branch IBoundary IMeasure IStorage ISubtreeCount Leaf PersistentSortedSet RefType Settings Seq]))

(set! *warn-on-reflection* true)

(defn conj
  "Analogue to [[clojure.core/conj]] but with comparator that overrides the one stored in set."
  [^PersistentSortedSet set key ^Comparator cmp]
  (.cons set key cmp))

(defn disj
  "Analogue to [[clojure.core/disj]] with comparator that overrides the one stored in set."
  [^PersistentSortedSet set key ^Comparator cmp]
  (.disjoin set key cmp))

(defn slice
  "An iterator for part of the set with provided boundaries.
   `(slice set from to)` returns iterator for all Xs where from <= X <= to.
   `(slice set from nil)` returns iterator for all Xs where X >= from.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^PersistentSortedSet set from to]
   (.slice set from to))
  ([^PersistentSortedSet set from to ^Comparator cmp]
   (.slice set from to cmp)))

(defn rslice
  "A reverse iterator for part of the set with provided boundaries.
   `(rslice set from to)` returns backwards iterator for all Xs where from <= X <= to.
   `(rslice set from nil)` returns backwards iterator for all Xs where X <= from.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^PersistentSortedSet set from to]
   (.rslice set from to))
  ([^PersistentSortedSet set from to ^Comparator cmp]
   (.rslice set from to cmp)))

(defn count-slice
  "Count elements in the range [from, to] inclusive.
   Uses O(log n) algorithm when subtree counts are available.
   If from is nil, counts from the beginning.
   If to is nil, counts to the end.
   Optionally pass in comparator that will override the one that set uses."
  ([^PersistentSortedSet set from to]
   (.countSlice set from to))
  ([^PersistentSortedSet set from to ^Comparator cmp]
   (.countSlice set from to cmp)))

(defn has-subtree-counts?
  "Check whether all nodes in this tree have precomputed subtree counts.
   When true, count-slice is guaranteed O(log n).
   When false, count-slice may degrade to O(n) for subtrees missing counts."
  [^PersistentSortedSet set]
  (.hasSubtreeCounts set))

(defn seek
  "An efficient way to seek to a specific key in a seq (either returned by [[clojure.core.seq]] or a slice.)
  `(seek (seq set) to)` returns iterator for all Xs where to <= X.
  Optionally pass in comparator that will override the one that set uses."
  ([seq to]
   (when seq (.seek ^Seq seq to)))                 ; seq is nil for an empty set ⇒ nothing to seek
  ([seq to cmp]
   (when seq (.seek ^Seq seq to ^Comparator cmp))))

(defn lookup
  "Look up a key and return the actual stored element.
  Unlike get/valAt which return the search key, this returns the
  stored element - useful when using custom comparators that only
  compare part of the key (e.g., [id value] tuples compared by id).

  O(log n) traversal with no allocations (unlike slice).

  Returns nil if not found."
  ([^PersistentSortedSet set key]
   (.lookup set key))
  ([^PersistentSortedSet set key ^Comparator cmp]
   (.lookup set key cmp)))

(defn replace
  "Replace an existing key with a new key at the same logical position.
  The comparator must return 0 for both old-key and new-key.
  This is a single-traversal update - much faster than disj + conj.

  O(log n) traversal with minimal allocations.

  Returns the updated set, or the original set if old-key not found."
  ([^PersistentSortedSet set old-key new-key]
   (.replace set old-key new-key))
  ([^PersistentSortedSet set old-key new-key ^Comparator cmp]
   (.replace set old-key new-key cmp)))

(defn- array-from-indexed [coll type from to]
  (cond
    (instance? clojure.lang.Indexed coll)
    (ArrayUtil/indexedToArray type coll from to)

    (arrays/array? coll)
    (Arrays/copyOfRange coll from to (arrays/array-type type))))

(defn- split
  ([coll to type avg max]
   (persistent! (split (transient []) 0 coll to type avg max)))
  ([res from coll to type avg max]
   (let [len (- to from)]
     (cond
       (== 0 len)
       res

       (>= len (* 2 avg))
       (recur (conj! res (array-from-indexed coll type from (+ from avg))) (+ from avg) coll to type avg max)

       (<= len max)
       (conj! res (array-from-indexed coll type from to))

       :else
       (-> res
           (conj! (array-from-indexed coll type from (+ from (quot len 2))))
           (conj! (array-from-indexed coll type (+ from (quot len 2)) to)))))))

(defn- elem-at [coll i]
  (if (arrays/array? coll) (aget ^objects coll i) (nth coll i)))

(defn- mst-split
  "Bulk MST partition (matches incremental conj): cut coll[0..len) AFTER index i (i<len-1)
   whose `key-of` value rises to `thresh` (= node-level+1). `key-of` maps a coll element to
   the key to hash (identity for leaf keys, .maxKey for child nodes). Produces arrays via
   array-from-indexed, exactly like the count `split`."
  [^IBoundary boundary ^Settings settings coll len type key-of thresh]
  (let [len (long len)
        thresh (long thresh)
        last-i (dec len)]
    (loop [i 0, start 0, acc (transient [])]
      (if (>= i len)
        (persistent! (if (> len (long start))
                       (conj! acc (array-from-indexed coll type start len))
                       acc))
        (if (and (< i last-i)
                 (>= (.keyLevel boundary (key-of (elem-at coll i)) settings) thresh))
          (recur (inc i) (inc i) (conj! acc (array-from-indexed coll type start (inc i))))
          (recur (inc i) start acc))))))

(defn- map->settings ^Settings [m]
  (let [boundary (:boundary m)
        s (Settings.
           (int (or (:branching-factor m) 0))
           (case (:ref-type m)
             :strong RefType/STRONG
             :soft   RefType/SOFT
             :weak   RefType/WEAK
             nil)
           ^IMeasure (:measure m)
           (:leaf-processor m)
           ;; diff-buf: fall back to the shared Settings default (Settings/defaultDiffBufSize,
           ;; 0/off unless the pss.diffBufSize sysprop is set) when the caller doesn't specify.
           ;; 0 = baseline (I0). See doc/diff-buffering.md. The MST incompatibility (a buffered
           ;; spine node is addressed by hash(anchor+diff), not its canonical content hash, which
           ;; breaks the cross-peer dedup MST exists for) is enforced in ONE place — `.withBoundary`
           ;; below forces diff-buf OFF for a *content-defined* boundary (a non-content boundary is
           ;; left untouched). See .internal/SPLIT_SEAM_DESIGN.md two-hash note.
           (int (or (:diff-buf-size m) (Settings/defaultDiffBufSize))))
        ;; split-seam: opt into a content-defined boundary (e.g. MST) per store. nil ⇒ the
        ;; default count B-tree (byte-identical baseline). See .internal/SPLIT_SEAM_DESIGN.md.
        s (if boundary (.withBoundary s ^IBoundary boundary) s)]
    ;; diff-buf: the comparator is NOT stored on Settings — it lives on the PersistentSortedSet
    ;; (_cmp) and is propagated to Branch nodes (Branch._projCmp) for leaf projection.
    s))

(defn- settings->map [^Settings s]
  {:branching-factor (.branchingFactor s)
   :ref-type         (condp identical? (.refType s)
                       RefType/STRONG :strong
                       RefType/SOFT   :soft
                       RefType/WEAK   :weak)
   :measure          ^IMeasure (.measure s)
   :leaf-processor   (.leafProcessor s)
   :diff-buf-size      (.diffBufSize s)})

(defn- assert-sorted!
  "Under `*assert*` only: verify strictly ascending order.

   Unsorted input does not fail here, it produces a tree whose invariants are
   quietly false — lookups miss, slices return the wrong range. RocksDB's
   `SstFileWriter`, the same bulk-build pattern, refuses outright with \"Keys must
   be added in strict ascending order\" rather than accepting it, and this is the
   same hazard.

   Behind `assert` because it is O(n) comparisons on a path documented as a fast
   path, so it is on in dev and test and compiled out in a production build.
   `from-sorted-seq` checks unconditionally instead: it is streaming, so the check
   is a fold it is doing anyway."
  [^Comparator cmp keys len]
  (assert (loop [i 1]
            (cond
              (>= i len) true
              (>= 0 (.compare cmp (arrays/aget keys i) (arrays/aget keys (dec i)))) false
              :else (recur (inc i))))
          "from-sorted-array requires strictly ascending, distinct input"))

(defn from-sorted-array
  "Fast path to create a set if you already have a sorted array of elements on your hands."
  ([^Comparator cmp keys]
   (from-sorted-array cmp keys (arrays/alength keys) (Settings.)))
  ([^Comparator cmp keys len]
   (from-sorted-array cmp keys len (Settings.)))
  ([^Comparator cmp keys len opts]
   (assert-sorted! cmp keys len)
   (let [settings             (map->settings opts)
         max-branching-factor (.branchingFactor settings)
         avg-branching-factor (-> (.minBranchingFactor settings) (+ max-branching-factor) (quot 2))
         ;; avg >= 2 or the level count never reduces and this loops forever.
         ;; `min = bf >>> 1`, so branching factors 1 and 2 both yield avg 1.
         ;; Verified: bf=2 ran to OutOfMemoryError rather than failing. The
         ;; streaming builder got this guard first; the arithmetic is shared.
         _                    (assert (>= avg-branching-factor 2)
                                      (str "branching-factor must be >= 3 (got avg fanout "
                                           avg-branching-factor "); a fanout of 1 never "
                                           "reduces the level count"))
         storage              (:storage opts)
         ^IMeasure measure-ops  (.measure settings)
         ->Leaf               (fn [keys]
                                (let [^Leaf leaf (Leaf. (count keys) ^objects keys settings)]
                                  (when measure-ops
                                    (set! (.-_measure leaf) (.tryComputeMeasure leaf nil)))
                                  leaf))
         ->Branch             (fn [level ^objects children]
                                (let [subtree-count (reduce + 0 (map #(if (instance? ISubtreeCount %)
                                                                        (.subtreeCount ^ISubtreeCount %)
                                                                        (.count ^ANode % nil))
                                                                     children))
                                      measure       (when measure-ops
                                                      (reduce (fn [acc ^ANode child]
                                                                (let [child-measure (.-_measure child)]
                                                                  (if child-measure
                                                                    (.merge measure-ops acc child-measure)
                                                                    acc)))
                                                              (.identity measure-ops)
                                                              children))]
                                  (Branch.
                                   (int level)
                                   (int (count children))
                                   ^objects (arrays/amap #(.maxKey ^ANode %) Object children)
                                   nil
                                   children
                                   (long subtree-count)
                                   measure
                                   cmp
                                   settings)))]
     (if (.contentDefined (.boundary settings))
       ;; MST bulk: chunk by boundary level so a freshly-built tree matches an incrementally
       ;; grown one for the same key set (history-independence). key-of = identity for leaf
       ;; keys, .maxKey for child nodes; thresh = node-level+1.
       (let [^IBoundary boundary (.boundary settings)]
         (loop [level 1
                nodes (mapv ->Leaf (mst-split boundary settings keys len Object identity 1))]
           (case (count nodes)
             0 (PersistentSortedSet. {} cmp storage settings)
             1 (PersistentSortedSet. {} cmp nil storage (first nodes) len settings 0)
             (recur (inc level)
                    (mapv #(->Branch level %)
                          (mst-split boundary settings nodes (count nodes) Object
                                     (fn [^ANode n] (.maxKey n)) (inc level)))))))
       (loop [level 1
              nodes (mapv ->Leaf (split keys len Object avg-branching-factor max-branching-factor))]
         (case (count nodes)
           0 (PersistentSortedSet. {} cmp storage settings)
           1 (PersistentSortedSet. {} cmp nil storage (first nodes) len settings 0)
           (recur (inc level) (mapv #(->Branch level %) (split nodes (count nodes) Object avg-branching-factor max-branching-factor)))))))))

(defn- streaming-split
  "Lazy seq of vectors, reproducing `split`'s distribution over a SEQ rather than
   an indexed collection, buffering at most `(* 2 avg)` elements.

   `split` needs the remaining count to choose a cut, which looks like it needs
   the whole collection — but its rule only ever asks whether at least `2*avg`
   remain, so a `2*avg` lookahead answers it exactly. That is what makes a
   streaming build possible at all, and it is why the output is IDENTICAL to
   `from-sorted-array`'s rather than merely similar: same cuts, same tree.

   The three cases are `split`'s, in order:
     >= 2*avg remaining  take exactly avg
     <= max remaining    take all of it as the last node
     otherwise           halve it, so the last two nodes are both >= min"
  [coll avg max]
  ;; avg >= 2 or the build never terminates: with avg = 1 every branch gets one
  ;; child, so a level with k nodes produces k nodes again and the tree grows
  ;; upward forever. avg = (min+max)/2 with min = bf>>>1, so this means bf >= 3.
  ;; Checked rather than left to hang — bf=2 spun until OOM.
  (assert (>= avg 2)
          (str "branching-factor must be >= 3 for a streaming build (got avg fanout "
               avg "); a fanout of 1 never reduces the level count"))
  (let [need (* 2 avg)
        fill (fn [buf s]
               (loop [buf buf s s]
                 (if (or (nil? s) (>= (count buf) need))
                   [buf s]
                   (recur (clojure.core/conj buf (first s)) (next s)))))]
    ((fn step [buf s]
       (lazy-seq
        (let [[buf s] (fill buf s)
              n (count buf)]
          (cond
            (zero? n) nil
            ;; `(into [] (subvec ...))` and NOT the bare subvec: a SubVector shares
            ;; its base, and `conj` on one does `base.assocN(end, o)` — so the base
            ;; grows without bound and every element ever consumed stays reachable.
            ;; That silently makes this O(n), which is the one thing the function
            ;; exists not to be. Measured: 4M elements OOM'd at -Xmx128m before
            ;; this copy, and complete comfortably after it.
            (>= n need) (cons (into [] (subvec buf 0 avg))
                              (step (into [] (subvec buf avg)) s))
            (<= n max) (list buf)
            :else (let [h (quot n 2)]
                    (list (into [] (subvec buf 0 h)) (into [] (subvec buf h))))))))
     [] (seq coll))))

(defn from-sorted-seq
  "Bulk-build a set from a SORTED, DISTINCT seq, flushing every node to `:storage`
   as it is filled. Returns the set; its root address is `(store set)`.

   ## Why this exists next to `from-sorted-array`

   `from-sorted-array` materialises everything: the element array, then every
   leaf, then every branch level, each in a Clojure vector, with storage attached
   only afterwards. That is fine for a set that fits in memory and impossible for
   a database restore — the whole point of a bulk build is the case where the
   data does not fit.

   This walks the same structure bottom-up but keeps only ONE partial node per
   level, storing each node as it is completed, which is the shape PostgreSQL's
   `nbtsort.c` uses for exactly this reason. Peak memory is
   O(depth × branching-factor), independent of the element count.

   ## The tree it builds is a RESTORED tree

   Each node is stored on completion, so its parent references it by ADDRESS and
   holds no child pointer — the same form `Branch(level, keys, addresses,
   settings)` produces on restore, and the ctor that backs both. Children load
   lazily on first access. That is a deliberate difference from
   `from-sorted-array`, whose result is fully resident.

   ## Equivalence

   For the same elements and settings the two produce the same SHAPE: identical
   cuts, identical levels (see `streaming-split`). Asserted in the test suite by
   building both ways and comparing structure, not just contents — contents alone
   would pass on a tree with the right elements and the wrong fanout.

   Requires `:storage`; without somewhere to put nodes there is nothing to stream
   to, and `from-sorted-array` is the right call instead.

   Input MUST be sorted and distinct under `cmp`. This is checked, because the
   alternative is a silently corrupt tree: RocksDB's `SstFileWriter`, the same
   pattern, refuses unsorted input with \"Keys must be added in strict ascending
   order\" rather than accepting it."
  ([^Comparator cmp xs opts]
   (let [settings (map->settings opts)
         max-bf   (.branchingFactor settings)
         avg-bf   (-> (.minBranchingFactor settings) (+ max-bf) (quot 2))
         storage  (:storage opts)
         ^IMeasure measure-ops (.measure settings)
         _ (when (nil? storage)
             (throw (IllegalArgumentException.
                     "from-sorted-seq requires :storage — use from-sorted-array for an in-memory build")))
         _ (when (.contentDefined (.boundary settings))
             (throw (IllegalArgumentException.
                     "from-sorted-seq does not support content-defined (MST) boundaries")))
         ;; ---- element stream, order-checked as it is consumed ----
         checked (fn checked [prev s]
                   (lazy-seq
                    (when-let [s (seq s)]
                      (let [x (first s)]
                        (when (nil? x)
                          (throw (IllegalArgumentException. "PersistentSortedSet cannot store nil")))
                        (when (and (not (identical? ::none prev))
                                   (>= 0 (.compare cmp x prev)))
                          (throw (IllegalArgumentException.
                                  (str "from-sorted-seq requires strictly ascending input; "
                                       (pr-str prev) " was followed by " (pr-str x)))))
                        (cons x (checked x (rest s)))))))
         ;; ---- level 0: leaves ----
         store-node! (fn [^ANode node]
                       (let [addr (.store node ^IStorage storage)]
                         {:key (.maxKey node)
                          :address addr
                          :count (if (instance? ISubtreeCount node)
                                   (.subtreeCount ^ISubtreeCount node)
                                   (.count node nil))
                          :measure (.-_measure node)}))
         leaves (map (fn [ks]
                       (let [^objects arr (to-array ks)
                             ^Leaf leaf (Leaf. (alength arr) arr settings)]
                         (when measure-ops
                           (set! (.-_measure ^ANode leaf) (.tryComputeMeasure leaf nil)))
                         (store-node! leaf)))
                     (streaming-split (checked ::none xs) avg-bf max-bf))
         ;; ---- level n>0: branches over the level below ----
         branch-level (fn [level entries]
                        (map (fn [es]
                               (let [n (count es)
                                     ^objects keys (to-array (map :key es))
                                     ^objects addrs (to-array (map :address es))
                                     cnt (reduce + 0 (map :count es))
                                     ^Branch b (Branch. (int level) (int n) keys addrs nil
                                                        (long cnt) settings)]
                                 (when measure-ops
                                   (set! (.-_measure ^ANode b)
                                         (reduce (fn [acc m]
                                                   (if m (.merge measure-ops acc m) acc))
                                                 (.identity measure-ops)
                                                 (map :measure es))))
                                 (store-node! b)))
                             (streaming-split entries avg-bf max-bf)))]
     ;; Climb one level at a time. `(next s)` realises only the SECOND entry, so
     ;; the check for "is this level the root" costs two nodes, not a level.
     (loop [level 1
            entries leaves]
       (let [s (seq entries)]
         (cond
           (nil? s)
           (PersistentSortedSet. {} cmp storage settings)

           (nil? (next s))
           (let [{:keys [address count]} (first s)]
             ;; root is referenced by address and loaded on demand, like a restore
             (PersistentSortedSet. {} cmp address storage nil (int count) settings 0))

           :else
           (recur (inc level) (branch-level level s))))))))

(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you don’t want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  ([^Comparator cmp keys]
   (from-sequential cmp keys (Settings.)))
  ([^Comparator cmp keys opts]
   (when (some nil? keys) (throw (IllegalArgumentException. "PersistentSortedSet cannot store nil")))
   (let [arr (to-array keys)
         _   (arrays/asort arr cmp)
         len (ArrayUtil/distinct cmp arr)]
     (from-sorted-array cmp arr len opts))))

(defn sorted-set*
  "Create a set with custom comparator, metadata and settings.
   Options:
     :comparator  Custom comparator (defaults to compare)
     :storage     IStorage implementation
     :measure     IMeasure implementation for aggregate measures
     :meta        Metadata map
     :branching-factor  B-tree branching factor (default 512)
     :ref-type    Reference type for cached nodes (:strong, :soft, :weak)"
  [opts]
  (PersistentSortedSet.
   (:meta opts)
   ^Comparator (or (:comparator opts) (:cmp opts) compare)
   (:storage opts)
   (map->settings opts)))

(defn sorted-set-by
  "Create a set with custom comparator."
  ([cmp] (PersistentSortedSet. ^Comparator cmp))
  ([cmp & keys] (from-sequential cmp keys)))

(defn sorted-set
  "Create a set with default comparator."
  ([] (PersistentSortedSet/EMPTY))
  ([& keys] (from-sequential compare keys)))

(defn restore-by
  "Constructs lazily-loaded set from storage, root address and custom comparator.
   Supports all operations that normal in-memory impl would,
   will fetch missing nodes by calling IStorage::restore when needed"
  ([cmp address ^IStorage storage]
   (restore-by cmp address storage {}))
  ([cmp address ^IStorage storage opts]
   (PersistentSortedSet. nil cmp address storage nil -1 (map->settings opts) 0)))

(defn restore
  "Constructs lazily-loaded set from storage and root address.
   Supports all operations that normal in-memory impl would,
   will fetch missing nodes by calling IStorage::restore when needed"
  ([address storage]
   (restore-by RT/DEFAULT_COMPARATOR address storage {}))
  ([address ^IStorage storage opts]
   (restore-by RT/DEFAULT_COMPARATOR address storage opts)))

(defn walk-addresses
  "Visit each address used by this set. Usable for cleaning up
   garbage left in storage from previous versions of the set"
  [^PersistentSortedSet set consume-fn]
  (.walkAddresses set consume-fn))

(defn store
  "Store each not-yet-stored node by calling IStorage::store and remembering
   returned address. Incremental, won’t store same node twice on subsequent calls.
   Returns root address. Remember it and use it for restore"
  ([^PersistentSortedSet set]
   (.store set))
  ([^PersistentSortedSet set ^IStorage storage]
   (.store set storage)))

(defn settings [^PersistentSortedSet set]
  (settings->map (.-_settings set)))

(defn measure
  "Get the aggregated measure for the entire set.
   Returns the measure object computed by the measure-ops provided when creating the set.
   Returns nil if no measure-ops were provided."
  [^PersistentSortedSet set]
  (let [^ANode root (.root set)
        settings (.-_settings set)
        measure-ops (.measure settings)]
    (when (and measure-ops root)
      (or (.-_measure root)
          (.forceComputeMeasure root (.-_storage set))))))

(defn- measure-slice-node
  [^ANode node ^IStorage storage ^IMeasure measure-ops from to ^java.util.Comparator cmp]
  (if (instance? Leaf node)
    ;; Leaf: iterate keys in range
    (let [^Leaf leaf node
          keys (.-_keys leaf)
          len (.-_len leaf)]
      (loop [i 0
             acc (.identity measure-ops)]
        (if (>= i len)
          acc
          (let [key (aget ^objects keys i)
                in-range? (and (or (nil? from) (>= (.compare cmp key from) 0))
                               (or (nil? to) (<= (.compare cmp key to) 0)))]
            (recur (inc i)
                   (if in-range?
                     (.merge measure-ops acc (.extract measure-ops key))
                     acc))))))
    ;; Branch: recurse
    (let [^Branch branch node
          len (int (.-_len branch))
          from-idx (int (if from
                          (let [idx (.searchFirst branch from cmp)]
                            (if (>= idx len) (dec len) idx))
                          0))
          to-idx (int (if to
                        (let [idx (inc (.searchLast branch to cmp))]
                          (min (max idx 0) (dec len)))
                        (dec len)))]
      (cond
        (> from-idx to-idx)
        (.identity measure-ops)

        (== from-idx to-idx)
        (let [child (.child branch storage from-idx)]
          (measure-slice-node child storage measure-ops from to cmp))

        :else
        (let [first-child (.child branch storage from-idx)
              first-measure (measure-slice-node first-child storage measure-ops from nil cmp)
              last-child (.child branch storage to-idx)
              last-measure (measure-slice-node last-child storage measure-ops nil to cmp)
              middle-measure (loop [i (int (inc from-idx))
                                    acc (.identity measure-ops)]
                               (if (>= i to-idx)
                                 acc
                                 (let [^ANode child (.child branch storage i)
                                       child-measure (or (.-_measure child)
                                                         (.forceComputeMeasure child storage))]
                                   (recur (inc i)
                                          (.merge measure-ops acc child-measure)))))]
          (.merge measure-ops
                  (.merge measure-ops first-measure middle-measure)
                  last-measure))))))

(defn get-nth
  "Find the entry at weighted rank `n`.
   Navigation uses cached subtree measure and IMeasure.weight() for
   O(log entries) performance.

   Returns `[entry local-offset]` where `local-offset` is the rank
   within the found entry, or nil if out of bounds.

   Requires measure with weight() to be configured on the set."
  [^PersistentSortedSet set ^long n]
  (let [offset (long-array 1)]
    (when-let [entry (.getNth set n offset)]
      [entry (aget offset 0)])))

(defn measure-slice
  "Compute measure for elements in the range [from, to] inclusive.
   Uses O(log n + k) algorithm where k is keys in boundary leaves.
   If from is nil, computes from the beginning.
   If to is nil, computes to the end.
   Returns nil if no measure-ops configured."
  [^PersistentSortedSet set from to]
  (let [^ANode root (.root set)
        settings (.-_settings set)
        measure-ops (.measure settings)
        ^Comparator cmp (.comparator set)]
    (when measure-ops
      (if (and from to (pos? (.compare cmp from to)))
        (.identity measure-ops)
        (if (zero? (.count root (.-_storage set)))
          (.identity measure-ops)
          (measure-slice-node root (.-_storage set) measure-ops from to cmp))))))

(defn compact
  "Rebuild the tree with optimal fill factors from the current elements.
   Useful after heavy insert/delete churn that may have degraded node
   fill ratios. Preserves comparator, settings, and metadata.
   Returns a new set with the same elements in a freshly built tree.

   Note: currently materializes all elements in memory. For large
   IStorage-backed sets, ensure sufficient heap space."
  [^PersistentSortedSet set]
  (let [arr   (to-array (clojure.core/seq set))
        len   (alength arr)
        opts  (settings->map (.-_settings set))]
    (from-sorted-array (.comparator set) arr len opts)))
