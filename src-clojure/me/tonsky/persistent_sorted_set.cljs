(ns ^{:doc
      "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can't store nil."
      :author "Nikita Prokopov"}
  me.tonsky.persistent-sorted-set
  (:refer-clojure :exclude [conj count disj sorted-set sorted-set-by contains?])
  (:require [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [me.tonsky.persistent-sorted-set.btset :as btset]))

; B+ tree
; -------

; Leaf:     keys[]     :: array of values

; Node:     children[] :: links to children nodes
;           keys[]     :: max value for whole subtree
;                         node.keys[i] == max(node.children[i].keys)
; All arrays are 16..32 elements, inclusive

; BTSet:    root       :: Node or Leaf
;           shift      :: depth - 1
;           cnt        :: size of a set, integer, rolling
;           comparator :: comparator used for ordering
;           meta       :: clojure meta map
;           _hash      :: hash code, same as for clojure collections, on-demand, cached

; Path: conceptually a vector of indexes from root to leaf value, but encoded in a single number.
;       E.g. we have path [7 30 11] representing root.children[7].children[30].keys[11].
;       In our case level-shift is 5, meaning each index will take 5 bits:
;       (7 << 10) | (30 << 5) | (11 << 0) = 8139
;         00111       11110       01011

; Iter:     set       :: Set this iterator belongs to
;           left      :: Current path
;           right     :: Right bound path (exclusive)
;           keys      :: Cached ref for keys array for a leaf
;           idx       :: Cached idx in keys array
; Keys and idx are cached for fast iteration inside a leaf"

(defn count
  ([set] (btset/$count set {:sync? true}))
  ([set opts] (btset/$count set opts)))

(defn contains?
  ([^BTSet set key] (btset/$contains? set key {:sync? true}))
  ([^BTSet set key opts] (btset/$contains? set key opts)))

(defn equivalent?
  ([set other] (btset/$equivalent? set other {:sync? true}))
  ([set other opts] (btset/$equivalent? set other opts)))

(defn conj
  "Analogue to [[clojure.core/conj]] but with comparator that overrides the one stored in set.
   Accepts optional opts map with {:sync? true/false} (defaults to true)."
  ([^BTSet set key]          (btset/$conjoin set key (.-comparator set) {:sync? true}))
  ([^BTSet set key arg]      (btset/$conjoin set key arg))
  ([^BTSet set key cmp opts] (btset/$conjoin set key cmp opts)))

(defn disj
  "Analogue to [[clojure.core/disj]] with comparator that overrides the one stored in set.
   Accepts optional opts map with {:sync? true/false} (defaults to true)."
  ([^BTSet set key] (btset/$disjoin set key (.-comparator set) {:sync? true}))
  ([^BTSet set key arg] (btset/$disjoin set key arg))
  ([^BTSet set key cmp opts] (btset/$disjoin set key cmp opts)))

#!------------------------------------------------------------------------------

(defn slice
  "An iterator for part of the set with provided boundaries.
   `(slice set from to)` returns iterator for all Xs where from <= X <= to.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^BTSet set key-from key-to]
   (btset/slice set key-from key-to (.-comparator set)))
  ([^BTSet set key-from key-to comparator]
   (btset/slice set key-from key-to comparator)))

(defn async-slice
  "Async version of slice that returns a Promise resolving to a vector of elements.
   Returns a Promise that resolves to a vector containing all elements in the range [key-from, key-to)."
  ([^BTSet set key-from key-to]
   (btset/async-slice set key-from key-to (.-comparator set)))
  ([^BTSet set key-from key-to comparator]
   (btset/async-slice set key-from key-to comparator)))

(defn rslice
  "A reverse iterator for part of the set with provided boundaries.
   `(rslice set from to)` returns backwards iterator for all Xs where from <= X <= to.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^BTSet set key]
   (some-> (btset/slice set key key (.-comparator set)) rseq))
  ([^BTSet set key-from key-to]
   (some-> (btset/slice set key-to key-from (.-comparator set)) rseq))
  ([^BTSet set key-from key-to comparator]
   (some-> (btset/slice set key-to key-from comparator) rseq)))

(defn seek
  "An efficient way to seek to a specific key in a seq (either returned by [[clojure.core.seq]] or a slice.)
  `(seek (seq set) to)` returns iterator for all Xs where to <= X.
  Optionally pass in comparator that will override the one that set uses."
  ([seq to]
   (btset/-seek seq to))
  ([seq to cmp]
   (btset/-seek seq to cmp)))

#!------------------------------------------------------------------------------

(defn lookup-async
  ([^BTSet set key]
   (btset/$lookup set key nil {:sync? false}))
  ([^BTSet set key not-found]
   (btset/$lookup set key not-found {:sync? false})))

(defn async-seq ;;-------------------------------------------------------------- TODO
  "Create an async sequence from a BTSet and path range"
  [set path till-path]
  (btset/async-seq set path till-path))

(defn async-iter
  "Async full-order iteration over `set` without pre-known bounds.
   returns cont(IAsyncSeq | nil)"
  [set] (btset/async-iter set))

(defn afirst [set](btset/afirst set));;-----------------------------------------TODO

(defn arest [set] (btset/arest set));;------------------------------------------TODO

(def async-into btset/async-into)

; (defn asequence [])
; (defn atransduce [])

#!------------------------------------------------------------------------------

(defn store
  "Accepts optional opts map with {:sync? true/false} (defaults to true).
   returns address specified by storage"
  ([^BTSet set] (btset/$store set {:sync? true}))
  ([^BTSet set arg] (btset/$store set arg))
  ([^BTSet set storage opts] (btset/$store set storage opts)))

(defn restore
  "Restore a set from storage given root-address-or-info and storage.
   + First arg can be either:
     - A root address (UUID) - requires opts with :shift and :count
     - A map from store-set with :root-address, :shift, :count, :comparator
   + Storage operations will use the provided opts for sync/async mode.
   + This operation is always synchronous and does not initiate io."
  ([root-address-or-info storage]
   (btset/restore root-address-or-info storage {}))
  ([root-address-or-info storage opts]
   (btset/restore root-address-or-info storage opts)))

#!------------------------------------------------------------------------------

(defn from-sorted-array
  "Fast path to create a set if you already have a sorted array of elements on your hands."
  ([cmp arr]
   (from-sorted-array cmp arr (arrays/alength arr)))
  ([cmp arr _len]
   (from-sorted-array cmp arr _len {}))
  ([cmp arr _len opts]
   (btset/from-sorted-array cmp arr _len opts)))

(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you don't want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  [cmp seq]
  (let [arr (-> (into-array seq) (arrays/asort cmp) (btset/sorted-arr-distinct cmp))]
    (from-sorted-array cmp arr)))

(def sorted-set-by btset/sorted-set-by)

(defn sorted-set
  ([] (btset/sorted-set-by compare))
  ([& keys] (btset/from-sequential compare keys)))

(defn sorted-set*
  "Create a set with options map containing:
   - :storage  Storage implementation
   - :comparator  Custom comparator (defaults to compare)
   - :meta     Metadata"
  [opts]
  (btset/from-opts opts))

