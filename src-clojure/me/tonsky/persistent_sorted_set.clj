(ns ^{:author "Nikita Prokopov"
      :doc "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can’t store nil."}
 me.tonsky.persistent-sorted-set
  (:refer-clojure :exclude [conj disj sorted-set sorted-set-by replace])
  (:require
   [me.tonsky.persistent-sorted-set.arrays :as arrays])
  (:import
   [clojure.lang RT]
   [java.lang.ref SoftReference]
   [java.util Comparator Arrays]
   [java.util.function BiConsumer]
   [me.tonsky.persistent_sorted_set ANode ArrayUtil Branch IMeasure IStorage ISubtreeCount Leaf PersistentSortedSet RefType Settings Seq]))

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

(defn seek
  "An efficient way to seek to a specific key in a seq (either returned by [[clojure.core.seq]] or a slice.)
  `(seek (seq set) to)` returns iterator for all Xs where to <= X.
  Optionally pass in comparator that will override the one that set uses."
  ([seq to]
   (.seek ^Seq seq to))
  ([seq to cmp]
   (.seek ^Seq seq to ^Comparator cmp)))

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

(defn- map->settings ^Settings [m]
  (Settings.
   (int (or (:branching-factor m) 0))
   (case (:ref-type m)
     :strong RefType/STRONG
     :soft   RefType/SOFT
     :weak   RefType/WEAK
     nil)
   (:measure m)
   (:leaf-processor m)))

(defn- settings->map [^Settings s]
  {:branching-factor (.branchingFactor s)
   :ref-type         (condp identical? (.refType s)
                       RefType/STRONG :strong
                       RefType/SOFT   :soft
                       RefType/WEAK   :weak)
   :measure          ^IMeasure (.measure s)
   :leaf-processor   (.leafProcessor s)})

(defn from-sorted-array
  "Fast path to create a set if you already have a sorted array of elements on your hands."
  ([^Comparator cmp keys]
   (from-sorted-array cmp keys (arrays/alength keys) (Settings.)))
  ([^Comparator cmp keys len]
   (from-sorted-array cmp keys len (Settings.)))
  ([^Comparator cmp keys len opts]
   (let [settings             (map->settings opts)
         max-branching-factor (.branchingFactor settings)
         avg-branching-factor (-> (.minBranchingFactor settings) (+ max-branching-factor) (quot 2))
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
                                   settings)))]
     (loop [level 1
            nodes (mapv ->Leaf (split keys len Object avg-branching-factor max-branching-factor))]
       (case (count nodes)
         0 (PersistentSortedSet. {} cmp storage settings)
         1 (PersistentSortedSet. {} cmp nil storage (first nodes) len settings 0)
         (recur (inc level) (mapv #(->Branch level %) (split nodes (count nodes) Object avg-branching-factor max-branching-factor))))))))

(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you don’t want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  ([^Comparator cmp keys]
   (from-sequential cmp keys (Settings.)))
  ([^Comparator cmp keys opts]
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
