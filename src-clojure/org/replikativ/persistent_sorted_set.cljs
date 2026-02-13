(ns ^{:doc
      "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can't store nil."
      :author "Nikita Prokopov"}
 org.replikativ.persistent-sorted-set
  (:refer-clojure :exclude [conj count disj sorted-set sorted-set-by contains?
                            seq rseq into transduce reduce replace])
  (:require [org.replikativ.persistent-sorted-set.arrays :as arrays]
            [org.replikativ.persistent-sorted-set.btset :as btset :refer [BTSet]]))

(def ^:private default-opts
  {:branching-factor 512})

(defn- with-defaults [opts]
  (merge default-opts opts))

(defn from-sorted-array
  "Fast path to create a set if you already have a sorted array of elements on your hands."
  ([cmp arr]
   (from-sorted-array cmp arr (arrays/alength arr)))
  ([cmp arr _len]
   (from-sorted-array cmp arr _len {}))
  ([cmp arr _len opts]
   (btset/from-sorted-array cmp arr _len (with-defaults opts))))

(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you't want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  ([cmp seq]
   (from-sequential cmp seq {}))
  ([cmp seq opts]
   (btset/from-sequential cmp seq (with-defaults opts))))

(defn sorted-set-by
  ([cmp]
   (btset/from-opts (with-defaults {:comparator cmp})))
  ([cmp & keys]
   (from-sequential cmp keys)))

(defn sorted-set
  ([] (sorted-set-by compare))
  ([& keys] (from-sequential compare keys)))

(defn sorted-set*
  "Create a set with options map containing:
   - :storage  Storage implementation
   - :comparator  Custom comparator (defaults to compare)
   - :meta     Metadata"
  [opts]
  (btset/from-opts (with-defaults opts)))

#!------------------------------------------------------------------------------

(defn count
  "O(n) when restoring root address, otherwise O(1)
   returns number by default
   returns continuation yeilding number when {:sync? false}"
  ([set] (btset/$count set {:sync? true}))
  ([set opts] (btset/$count set opts)))

(defn contains?
  "returns boolean by default
   returns continuation yeilding boolean when {:sync? false}"
  ([^BTSet set key] (btset/$contains? set key {:sync? true}))
  ([^BTSet set key opts] (btset/$contains? set key opts)))

(defn lookup
  "Look up key in the set. Returns key if present, else nil.
   3-arity version accepts a custom comparator."
  ([^BTSet set key]
   (btset/lookup set key nil {:sync? true}))
  ([^BTSet set key cmp]
   (btset/lookup set key nil {:sync? true :comparator cmp})))

(defn equiv?
  "Is _other_ a set with the same items?
   returns boolean by default
   returns continuation yeilding boolean when {:sync? false}"
  ([set other] (btset/equivalent? set other {:sync? true}))
  ([set other opts] (btset/equivalent? set other opts)))

(defn seq
  "returns btset/Iter by default
   returns continuation yielding btset/AsyncSeq when {:sync? false}"
  ([set] (btset/$seq set))
  ([set opts] (btset/$seq set opts)))

(defn rseq
  "returns btset/ReverseIter by default
   returns continuation yielding btset/AsyncRSeq when {:sync? false}"
  ([set] (btset/$rseq set))
  ([set opts] (btset/$rseq set opts)))

(defn equiv-sequential?
  "Test items in sequential order.
   returns boolean by default
   returns continuation yeilding boolean when {:sync? false}"
  ([set other] (btset/equivalent-sequential? set other {:sync? true}))
  ([set other opts] (btset/equivalent-sequential? set other opts)))

(defn conj
  "Analogue to [[clojure.core/conj]] but with comparator that overrides the one stored in set.
   returns BTSet by default
   returns continuation yeilding BTSet when {:sync? false}"
  ([^BTSet set key]          (btset/conjoin set key))
  ([^BTSet set key arg]      (btset/conjoin set key arg))
  ([^BTSet set key cmp opts] (btset/conjoin set key cmp opts)))

(defn disj
  "Analogue to [[clojure.core/disj]] with comparator that overrides the one stored in set.
   returns BTSet by default
   returns continuation yeilding BTSet when {:sync? false}"
  ([^BTSet set key]          (btset/disjoin set key))
  ([^BTSet set key arg]      (btset/disjoin set key arg))
  ([^BTSet set key cmp opts] (btset/disjoin set key cmp opts)))

(defn replace
  "Replace an existing key with a new key at the same logical position.
   The comparator must return 0 for both old-key and new-key.
   This is a single-traversal update - faster than disj + conj.
   returns BTSet by default
   returns continuation yielding BTSet when {:sync? false}"
  ([^BTSet set old-key new-key]          (btset/$replace set old-key new-key))
  ([^BTSet set old-key new-key arg]      (btset/$replace set old-key new-key arg))
  ([^BTSet set old-key new-key cmp opts] (btset/$replace set old-key new-key cmp opts)))

(defn slice
  "An iterator for part of the set with provided boundaries.
   `(slice set from to)` returns iterator for all Xs where from <= X <= to.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^BTSet set key-from key-to]
   (btset/slice set key-from key-to))
  ([^BTSet set key-from key-to arg]
   (btset/slice set key-from key-to arg))
  ([^BTSet set key-from key-to comparator opts]
   (btset/slice set key-from key-to comparator opts)))

(defn rslice
  "A reverse iterator for part of the set with provided boundaries.
   `(rslice set from to)` returns backwards iterator for all Xs where from <= X <= to.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^BTSet set key]
   (btset/rslice set key key (.-comparator set) {:sync? true}))
  ([^BTSet set key-from key-to]
   (btset/rslice set key-from key-to (.-comparator set) {:sync? true}))
  ([^BTSet set key-from key-to arg]
   (btset/rslice set key-from key-to arg))
  ([^BTSet set key-from key-to cmp opts]
   (btset/rslice set key-from key-to cmp opts)))

(defn count-slice
  "Count elements in the range [from, to] inclusive.
   Uses O(log n) algorithm when subtree counts are available.
   If from is nil, counts from the beginning.
   If to is nil, counts to the end.
   Optionally pass in comparator that will override the one that set uses.
   Returns number by default.
   Returns continuation yielding number when {:sync? false}."
  ([^BTSet set from to]
   (btset/count-slice set from to))
  ([^BTSet set from to arg]
   (btset/count-slice set from to arg))
  ([^BTSet set from to cmp opts]
   (btset/count-slice set from to cmp opts)))

(defn get-nth
  "Find the entry at weighted rank `n`.
   Navigation uses cached subtree measure and IMeasure weight for
   O(log entries) performance.

   Returns [entry local-offset] where local-offset is the rank
   within the found entry, or nil if out of bounds.

   Requires measure with weight to be configured on the set.
   Returns continuation yielding result when {:sync? false}."
  ([^BTSet set n]
   (btset/get-nth set n))
  ([^BTSet set n opts]
   (btset/get-nth set n opts)))

(defn seek
  "An efficient way to seek to a specific key in a seq (either returned by [[clojure.core.seq]] or a slice.)
   `(seek (seq set) to)` returns iterator for all Xs where to <= X.
   Optionally pass in comparator that will override the one that set uses."
  ([seq to]
   (btset/seek seq to))
  ([seq to arg]
   (btset/seek seq to arg))
  ([seq to cmp opts]
   (btset/seek seq to cmp opts)))

(defn walk-addresses
  "Visit each address used by this set. Usable for cleaning up
   garbage left in storage from previous versions of the set.

   returns nil when the walk completes
   returns a continuation yielding nil when {:sync? false}"
  ([^BTSet set consume-fn]
   (btset/walk-addresses set consume-fn {:sync? true}))
  ([^BTSet set consume-fn opts]
   (btset/walk-addresses set consume-fn opts)))

(defn store
  "Flush set to storage. sync calls must be used with sync storage
   and async calls must be used with async storage.

   returns address by default
   returns continuation yeilding address when {:sync? false}"
  ([^BTSet set] (btset/store set {:sync? true}))
  ([^BTSet set arg] (btset/store set arg))
  ([^BTSet set storage opts] (btset/store set storage opts)))

(defn restore
  "Restore a set from storage given root-address-or-info and storage.
   This operation is always synchronous and does not initiate io.
   + First arg can be either:
     - A root address (UUID) - requires opts with :shift and :count
     - A map from store-set with :root-address :comparator

   returns BTSet, **always synchronously**"
  ([root-address-or-info storage]
   (restore root-address-or-info storage {}))
  ([root-address-or-info storage opts]
   (btset/restore root-address-or-info storage (with-defaults opts))))

(defn reduce
  "reducing function is fn<acc,item> and _must_ return a continuation
   returns result by default
   returns continuation yielding result when {:sync? false}"
  ([arf set from]
   (btset/$reduce arf set from {:sync? true}))
  ([arf set from opts]
   (btset/$reduce arf set from opts)))

(defn transduce
  "xforms must be synchronous
   reducing function is fn<acc,item> and _must_ return a continuation
   returns result by default
   returns continuation yielding result when {:sync? false}"
  ([xform arf set from]
   (btset/$transduce xform arf set from {:sync? true}))
  ([xform arf set from opts]
   (btset/$transduce xform arf set from opts)))

(defn into
  "xforms must be synchronous
   returns collection by default
   returns continuation yielding collection when {:sync? false}"
  ([set arg]
   (btset/$into set arg))
  ([set arg0 arg1]
   (btset/$into set arg0 arg1))
  ([set xform from opts]
   (btset/$into set xform from opts)))

(defn measure
  "Get the aggregated measure for the entire set.
   Returns the measure object computed by the measure-ops provided when creating the set.
   Returns nil if no measure-ops were provided or the set is empty.
   Returns continuation yielding measure when {:sync? false}."
  ([^btset/BTSet set]
   (measure set {:sync? true}))
  ([^btset/BTSet set opts]
   (btset/measure set opts)))

(defn measure-slice
  "Compute measure for elements in the range [from, to] inclusive.
   Uses O(log n + k) algorithm where k is keys in boundary leaves.
   If from is nil, computes from the beginning.
   If to is nil, computes to the end.
   Returns nil if no measure-ops configured.
   Returns continuation yielding measure when {:sync? false}."
  ([set from to]
   (btset/measure-slice set from to))
  ([set from to cmp]
   (btset/measure-slice set from to cmp))
  ([set from to cmp opts]
   (btset/measure-slice set from to cmp opts)))

(defn compact
  "Rebuild the tree with optimal fill factors from the current elements.
   Useful after heavy insert/delete churn that may have degraded node
   fill ratios. Preserves comparator, settings, and metadata.
   Returns a new set with the same elements in a freshly built tree.

   Note: currently materializes all elements in memory."
  [^BTSet set]
  (let [arr (into-array (btset/$seq set))
        len (alength arr)
        opts (.-settings set)]
    (btset/from-sorted-array (.-comparator set) arr len opts)))

