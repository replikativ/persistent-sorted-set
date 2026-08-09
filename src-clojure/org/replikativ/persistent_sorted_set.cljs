(ns ^{:doc
      "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can't store nil."
      :author "Nikita Prokopov"}
 org.replikativ.persistent-sorted-set
  (:refer-clojure :exclude [conj count disj sorted-set sorted-set-by contains?
                            seq rseq into transduce reduce replace])
  (:require [org.replikativ.persistent-sorted-set.arrays :as arrays]
            [org.replikativ.persistent-sorted-set.btset :as btset :refer [BTSet]]))

;; diff-buf: diff-buffering is OFF by default (0 ⇒ byte-identical baseline) so existing
;; storages (which don't serialize Branch._slots) are unaffected — enabling it without a
;; slots-aware storage would silently drop buffered diffs on write. Mirrors the JVM default
;; (Settings.defaultDiffBufSize). Consumers serializing :slots (e.g. datahike) pass an
;; explicit :diff-buf-size; the cljs test build overrides this define to 256 to gate the path.
(goog-define default-diff-buf-size 0)

(def ^:private default-opts
  {:branching-factor 512
   :diff-buf-size default-diff-buf-size})

(defn- with-defaults [opts]
  ;; nil-AWARE: a plain `merge` lets an explicit nil beat the default, which the JVM's
  ;; `map->settings` never does — it uses `(or (:diff-buf-size m) (default))` and normalizes
  ;; a non-positive branching factor to 512. Measured before this, `{:branching-factor nil}`
  ;; gave a 3-element set on the JVM and exhausted the Node heap here (`arr-partition-approx`
  ;; loops forever with a chunk length of 0), and `{:diff-buf-size nil}` gave the JVM default
  ;; but 0 here.
  (reduce-kv (fn [acc k v] (if (nil? v) acc (assoc acc k v)))
             default-opts opts))

(defn- assert-sorted!
  "Under `*assert*` only: verify strictly ascending order, as the JVM half does.

   Unsorted input does not fail on its own — it produces a tree whose invariants are quietly
   false, so lookups miss and slices return the wrong range while `count` and `seq` both look
   perfect. This runtime had NO check at all, while the JVM has had one since the beginning,
   so the same call was refused on one runtime and silently accepted on the other. Measured on
   ClojureScript before this:

       (from-sorted-array compare #js [3 1 2] 3)
       seq => [3 1 2]   count => 3   but contains? 1 => false, contains? 3 => false
       1000 shuffled elements => count 1000, only 3 of them findable by contains?

   Duplicates were accepted too (`#js [1 2 2 3]` => count 4), and `#js [1 nil 3]` made nil a
   durable member of a set whose own namespace docstring says it \"can't store nil\" — the one
   hole, since `conj` and `from-sequential` both refuse nil here. An ascending check closes
   that case as well, because nil compares below any number.

   Behind `assert` for the same reason as the JVM: O(n) comparisons on a documented fast
   path, so it is live in dev and test and elided by `:elide-asserts` in a production build.
   `from-sorted-seq` checks unconditionally instead — it is streaming, so the check is a fold
   it performs anyway."
  [cmp arr len]
  (assert (loop [i 1]
            (cond
              (>= i len) true
              (>= 0 (cmp (arrays/aget arr i) (arrays/aget arr (dec i)))) false
              :else (recur (inc i))))
          "from-sorted-array requires strictly ascending, distinct input"))

(defn from-sorted-array
  "Fast path to create a set if you already have a sorted array of elements on your hands.

   Only the first `len` elements are used; the rest of `arr` is ignored. (`len` was formerly
   accepted and then discarded here, so a caller passing a reusable buffer got its stale tail
   as set members — see `btset/from-sorted-array`.)

   Input MUST be strictly ascending and distinct under `cmp`; checked under `*assert*`."
  ([cmp arr]
   (from-sorted-array cmp arr (arrays/alength arr)))
  ([cmp arr len]
   (from-sorted-array cmp arr len {}))
  ([cmp arr len opts]
   (assert-sorted! cmp arr (min len (arrays/alength arr)))
   (btset/from-sorted-array cmp arr len (with-defaults opts))))

(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you't want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  ([cmp seq]
   (from-sequential cmp seq {}))
  ([cmp seq opts]
   (btset/from-sequential cmp seq (with-defaults opts))))

(defn from-sorted-seq
  "Bulk-build a set from a SORTED, DISTINCT seq, storing every node to `:storage`
   as it fills. Peak memory is O(depth x branching-factor), independent of the
   element count — which is what makes it the right builder for a restore, where
   the data does not fit in memory and `from-sorted-array` therefore cannot run.

   The result is address-rooted, the same shape a restore produces: nodes hold
   child ADDRESSES, not child pointers, and load lazily. `(store set)` returns
   the root address without re-storing anything.

   Builds the same tree as the JVM's `from-sorted-seq` — same cuts, same node
   contents — so an address means the same thing on either runtime.

   Options beyond the usual: `:storage` (required), `:flush-fn` (called and
   awaited after each node is stored, for backpressure), `:sync?`.

   Three-arity only, matching the JVM: `:storage` is mandatory, so a call
   without opts is always an error."
  ([cmp xs opts]
   (btset/from-sorted-seq cmp xs (with-defaults opts))))

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
   (btset/lookup set key cmp {:sync? true})))

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

   PRECONDITION, and it is the caller's to uphold: under the comparator passed to
   THIS call, no element other than old-key itself may compare equal to old-key.
   The single-traversal update rewrites in place at the position it finds, so if a
   comparator-EQUAL sibling exists, the rewrite can order the two wrongly and leave
   the node unsorted. The sibling then still appears in iteration but is no longer
   reachable by binary search, and the disorder becomes durable the moment the set
   is stored — after which every later search on that node is arbitrary.

   This matters specifically when the comparator is COARSER than the set's own —
   the `[id value]`-compared-by-id pattern these docstrings advertise. A set built
   under a full comparator may legitimately hold two elements that a coarser one
   cannot tell apart; `replace` under that coarser comparator is then outside
   contract.

   It is checked by assertions only, so it is NOT checked in ordinary production
   builds. That is deliberate — the check costs a scan on a hot write path — but it
   means violating this silently corrupts the structure rather than throwing. If you
   cannot guarantee uniqueness under the comparator, use disj + conj.

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
   (when seq (btset/seek seq to)))                 ; seq is nil for an empty set ⇒ nothing to seek
  ([seq to arg]
   (when seq (btset/seek seq to arg)))
  ([seq to cmp opts]
   (when seq (btset/seek seq to cmp opts))))

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

(defn diff
  "Keys added and removed between two sets that SHARE STRUCTURE.

   Returns `{:added [...] :removed [...]}`, both in the sets' sort order, or a
   continuation yielding it when `{:sync? false}`.

   Cost is proportional to what CHANGED, not to set size, in NODES READ — which
   here means async round trips. Two versions of a persistent set share every
   node they have in common, so a subtree whose address appears on both sides
   cannot contain a difference and is dropped without being loaded. Measured on
   the JVM against a serializing storage, sets one two-element transaction
   apart: 3-4 nodes read whether the set holds 1 000 elements or 100 000. Two
   identical stored roots are answered without touching storage at all.

   Both sets must come from the same lineage (one derived from the other, or
   both from a common ancestor) and must be STORED for pruning to work — an
   in-memory set has no addresses, so every node is walked. Diffing unrelated
   sets is CORRECT but degrades to a full walk of both.

   Membership is decided by the set's comparator, so two keys that compare equal
   are treated as the same key even if they are not `=`.

   Same algorithm and same answers as the JVM `diff`."
  ([^BTSet a ^BTSet b]
   (btset/diff a b (.-storage b) {:sync? true}))
  ([^BTSet a ^BTSet b storage]
   (btset/diff a b storage {:sync? true}))
  ([^BTSet a ^BTSet b storage opts]
   (btset/diff a b storage opts)))

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

   Note: currently materializes all elements in memory.

   `(.-settings set)` is the NODE settings — `[:branching-factor :measure :boundary
   :diff-buf-size]` only — so passing it alone dropped both the storage and the metadata that
   the docstring promises and that the wire codec resolves `:pss/storage-id` from. Measured
   before the fix: `(meta (compact s))` nil for `(meta s)` `{:x 1}`, `(.-storage (compact s))`
   nil, and `(store (compact s))` throwing \"BTSet/store requires IStorage in second
   argument\". This is the ClojureScript half of the defect fixed on the JVM."
  [^BTSet set]
  (let [arr (into-array (btset/$seq set))
        len (alength arr)
        opts (assoc (.-settings set)
                    :storage (.-storage set)
                    :meta (meta set))]
    (btset/from-sorted-array (.-comparator set) arr len opts)))

