(ns org.replikativ.persistent-sorted-set.branch
  (:refer-clojure :exclude [await])
  #?(:cljs (:require-macros [org.replikativ.persistent-sorted-set.macros :refer [async+sync]]
                            [org.replikativ.persistent-sorted-set.impl.tree-macros :refer [read-field write-field!]]))
  (:require #?(:clj [org.replikativ.persistent-sorted-set.impl.tree-macros :refer [read-field write-field!]])
            #?(:clj [org.replikativ.persistent-sorted-set.macros :refer [async+sync]])
            [is.simm.partial-cps.async #?@(:cljs [:refer [await] :refer-macros [async]]
                                           :clj [:refer [async await]])]
            [org.replikativ.persistent-sorted-set.arrays :as arrays]
            [org.replikativ.persistent-sorted-set.impl.node :as node :refer [INode]]
            [org.replikativ.persistent-sorted-set.impl.measure :as measure]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage]
            [org.replikativ.persistent-sorted-set.impl.boundary :as b]
            [org.replikativ.persistent-sorted-set.leaf :as leaf]
            [org.replikativ.persistent-sorted-set.util :as util]))

(declare ->Branch diff-size child fold-measures ensure-child-measures ensure-children
         ensure-addresses install-measures! concat-slots try-compute-subtree-count-from-children
         add $contains? $count lookup $remove $replace store walk-addresses)

#?(:jolt nil
   :clj
   (definterface BranchState
     (children []) (children [v])
     (addresses []) (addresses [v])
     (subtreeCount []) (subtreeCount [v])
     (nodeMeasure []) (nodeMeasure [v])
     (slots []) (slots [v])
     (bufferEntries []) (bufferEntries [v])
     (projectionComparator []) (projectionComparator [v])
     (childMeasures []) (childMeasures [v])
     (lazyMeasureRecord []) (lazyMeasureRecord [v])))

;; diff-buf (mirrors JVM Branch):
;;   `_slots`      — per-child buffered diff (nil unless diff-buf-size > 0). Read/projection
;;                   parity with clj; written-back on restore by the storage layer.
;;   `_bufEntries` — this node's subtree buffered-diff size in *entries* (the budget-B unit),
;;                   maintained by delta on the deposit return path (mirrors JVM Branch._bufEntries
;;                   and subtree-count). >= 0 content-only size; -1 must-WRITE (a split/merge/borrow
;;                   in this subtree — propagates up the deposit sum, so the store gate is O(1));
;;                   -2 LAZY (restored, derived from _slots on first read). Cleared to 0 at store().
;;   `_projCmp`    — the SET's stable comparator, used to PROJECT a buffered leaf (project-leaf
;;                   rebuilds it in stored order) — NOT any per-operation/navigation comparator.
;;                   Internally-created branches inherit it from the creating node via the ctor;
;;                   the root (-root) and freshly-restored branches (child) are stamped on
;;                   descent — the storage layer has no comparator. Matches JVM Branch._projCmp.
;;   `_childMeasures` — the measure of each child's subtree, an array as long as `keys`, or
;;                   nil when unknown (a blob written before the record existed). Never
;;                   mutated once installed. See the helpers at the top of this namespace and
;;                   JVM `Branch._childMeasures`.
;;   `_cmLazy`      — true while this node's BLOB has no array: filled by a read, or the
;;                   content-only successor of such a node (it may be buffered in its parent
;;                   instead of written). Cleared only where the node itself is written.
;;                   `measure-census` and `remeasure` have to know.
(deftype Branch
         #?(:cljs [^number level keys ^:mutable children ^:mutable addresses ^:mutable ^number subtree-count
                   ^:mutable _measure settings ^:mutable _slots ^:mutable _bufEntries ^:mutable _projCmp
                   ^:mutable _childMeasures ^:mutable _cmLazy]
            :clj [level keys ^:unsynchronized-mutable children ^:unsynchronized-mutable addresses
                  ^:unsynchronized-mutable subtree-count ^:unsynchronized-mutable _measure settings
                  ^:unsynchronized-mutable _slots ^:unsynchronized-mutable _bufEntries
                  ^:unsynchronized-mutable _projCmp ^:unsynchronized-mutable _childMeasures
                  ^:unsynchronized-mutable _cmLazy])
  #?@(:jolt []
      :clj [BranchState
            (children [_] children)
            (children [_ v] (set! children v))
            (addresses [_] addresses)
            (addresses [_ v] (set! addresses v))
            (subtreeCount [_] subtree-count)
            (subtreeCount [_ v] (set! subtree-count v))
            (nodeMeasure [_] _measure)
            (nodeMeasure [_ v] (set! _measure v))
            (slots [_] _slots)
            (slots [_ v] (set! _slots v))
            (bufferEntries [_] _bufEntries)
            (bufferEntries [_ v] (set! _bufEntries v))
            (projectionComparator [_] _projCmp)
            (projectionComparator [_ v] (set! _projCmp v))
            (childMeasures [_] _childMeasures)
            (childMeasures [_ v] (set! _childMeasures v))
            (lazyMeasureRecord [_] _cmLazy)
            (lazyMeasureRecord [_ v] (set! _cmLazy v))])
  Object
  (toString [_] (#?(:cljs pr-str* :clj pr-str) {:level level :keys (vec keys)}))
  INode
  (len [_] (arrays/alength keys))
  (level [_] level)
  (max-key [_] (arrays/alast keys))
  (subtree-count [_] subtree-count)
  (measure [_] _measure)
  (try-compute-measure [this storage measure-ops {:keys [sync?] :or {sync? true} :as opts}]
    ;; Try to compute measure from in-memory children only; postpone if any child not loaded
    (if sync?
      (when measure-ops
        (when (some? children)
          (let [result (loop [i 0
                              acc (measure/identity-measure measure-ops)]
                         (if (< i (arrays/alength keys))
                           (let [child (when (some? children) (aget children i))]
                             (if (nil? child)
                               nil ;; child not in memory, postpone
                               (let [child-measure (node/measure child)]
                                 (if (some? child-measure)
                                   (recur (inc i)
                                          (measure/merge-measure measure-ops acc child-measure))
                                   nil)))) ;; child measure unavailable, postpone
                           acc))]
            (when result
              (write-field! this :_measure result))
            result)))
      (async
       (when measure-ops
         (when (some? children)
           (let [result (loop [i 0
                               acc (measure/identity-measure measure-ops)]
                          (if (< i (arrays/alength keys))
                            (let [child (when (some? children) (aget children i))]
                              (if (nil? child)
                                nil
                                (let [child-measure (node/measure child)]
                                  (if (some? child-measure)
                                    (recur (inc i)
                                           (measure/merge-measure measure-ops acc child-measure))
                                    nil))))
                            acc))]
             (when result
               (write-field! this :_measure result))
             result))))))
  (force-compute-measure [this storage measure-ops {:keys [sync?] :or {sync? true} :as opts}]
    ;; The fold of the record, which is filled by reading this node's children if missing.
    (async+sync sync?
                (async
                 (when measure-ops
                   (let [cm     (await (ensure-child-measures this storage measure-ops opts))
                         result (fold-measures measure-ops cm)]
                     (write-field! this :_measure result)
                     result)))))
  (merge [this next]
    (let [sc1 subtree-count
          sc2 (read-field next :subtree-count)
          new-sc (if (and (>= sc1 0) (>= sc2 0)) (+ sc1 sc2) -1)
          ;; Merge measure if both have them
          new-measure (when (and _measure (read-field next :_measure))
                        (when-let [measure-ops (:measure settings)]
                          (measure/merge-measure measure-ops _measure (read-field next :_measure))))
          ;; Ensure children arrays exist (may be arrays of nulls for lazy branches)
          c1 (ensure-children this)
          c2 (ensure-children next)
          ;; Merge addresses too if present
          new-addrs (when (or addresses (read-field next :addresses))
                      (arrays/aconcat (or (ensure-addresses this) (arrays/make-array (arrays/alength keys)))
                                      (or (ensure-addresses next) (arrays/make-array (arrays/alength (.-keys next))))))
          nb (->Branch level
                       (arrays/aconcat keys (.-keys next))
                       (arrays/aconcat c1 c2)
                       new-addrs
                       new-sc
                       new-measure
                       settings nil 0 _projCmp nil false)]
      (when (and (some? _childMeasures) (some? (read-field next :_childMeasures)))
        (install-measures! nb (:measure settings)
                           (arrays/aconcat _childMeasures (read-field next :_childMeasures)) new-measure))
      ;; diff-buf: a merged node's structure differs from any anchor ⇒ it must be WRITTEN
      ;; (-1), but it still buffers the surviving children's slots (concatenated).
      (when (pos? (or (:diff-buf-size settings) 0))
        (write-field! nb :_bufEntries -1)
        (write-field! nb :_slots (concat-slots this next)))
      nb))
  (merge-split [this next]
    (let [;; Ensure children arrays exist
          c1 (ensure-children this)
          c2 (ensure-children next)
          ks (util/merge-n-split keys (.-keys next))
          ps (util/merge-n-split c1 c2)
          ;; Also merge-split addresses if present
          as (when (or addresses (read-field next :addresses))
               (util/merge-n-split (or (ensure-addresses this) (arrays/make-array (arrays/alength keys)))
                                   (or (ensure-addresses next) (arrays/make-array (arrays/alength (.-keys next))))))]
      (let [p0 (arrays/aget ps 0)
            p1 (arrays/aget ps 1)
            sc0 (try-compute-subtree-count-from-children p0 (arrays/alength p0))
            sc1 (try-compute-subtree-count-from-children p1 (arrays/alength p1))
            measure-ops (:measure settings)
            m0 (when (and measure-ops _measure (read-field next :_measure))
                 (reduce (fn [acc child]
                           (if (nil? acc)
                             (reduced nil)
                             (let [cs (when (some? child) (node/measure child))]
                               (if (some? cs)
                                 (measure/merge-measure measure-ops acc cs)
                                 (reduced nil)))))
                         (measure/identity-measure measure-ops)
                         p0))
            m1 (when (and measure-ops _measure (read-field next :_measure))
                 (reduce (fn [acc child]
                           (if (nil? acc)
                             (reduced nil)
                             (let [cs (when (some? child) (node/measure child))]
                               (if (some? cs)
                                 (measure/merge-measure measure-ops acc cs)
                                 (reduced nil)))))
                         (measure/identity-measure measure-ops)
                         p1))
            b0 (->Branch level (arrays/aget ks 0) p0 (when as (arrays/aget as 0)) sc0 m0 settings nil 0 _projCmp nil false)
            b1 (->Branch level (arrays/aget ks 1) p1 (when as (arrays/aget as 1)) sc1 m1 settings nil 0 _projCmp nil false)]
        ;; the two records, cut where the children were cut
        (when (and (some? _childMeasures) (some? (read-field next :_childMeasures)))
          (let [cms (util/merge-n-split _childMeasures (read-field next :_childMeasures))]
            (install-measures! b0 measure-ops (arrays/aget cms 0) m0)
            (install-measures! b1 measure-ops (arrays/aget cms 1) m1)))
        ;; diff-buf: redistribution is structural on both halves (-1); split the
        ;; concatenated slots at the same child boundary so each half buffers its own children.
        (when (pos? (or (:diff-buf-size settings) 0))
          (write-field! b0 :_bufEntries -1)
          (write-field! b1 :_bufEntries -1)
          (when-let [all (concat-slots this next)]
            (let [n0 (arrays/alength p0)]
              (write-field! b0 :_slots (arrays/slice all 0 n0))
              (write-field! b1 :_slots (arrays/slice all n0)))))
        (util/return-array b0 b1))))
  (add [this storage key cmp opts]
    (add this storage key cmp opts))
  ($contains? [this storage key cmp opts]
    ($contains? this storage key cmp opts))
  ($count [this storage opts]
    ($count this storage opts))
  (lookup [this storage key cmp opts]
    (lookup this storage key cmp opts))
  ($remove [this storage key left right cmp opts]
    ($remove this storage key left right cmp opts))
  ($replace [this storage old-key new-key cmp opts]
    ($replace this storage old-key new-key cmp opts))
  (store [this storage opts]
    (store this storage opts))
  (walk-addresses [this storage on-address opts]
    (walk-addresses this storage on-address opts)))


;; ---------------------------------------------------------------------------
;; The record of the children's measures (mirrors JVM Branch._childMeasures).
;;
;; A branch holds the measure of each child's subtree in `_childMeasures`. Its own measure
;; is the fold of that array, so a write below child i needs only child i's new measure --
;; which the child that came back up always has. Nothing is asked of the measure but
;; `extract` and `merge`, so min/max and order-sensitive measures are kept like a sum, and
;; a reader can pick a child by position or fold a range without restoring the children.
;;
;; nil means UNKNOWN (a blob written before the record existed). Unknown is safe: the arms
;; fall back to what they did before, and `ensure-child-measures` fills it with one read of
;; this node's children. A stale array would be a wrong answer, so the array is never
;; mutated once installed and every arm installs one for the children it leaves behind.
;; ---------------------------------------------------------------------------

(defn fold-measures
  "Fold `cm`. nil in, or a nil entry, gives nil."
  [measure-ops cm]
  (when (and measure-ops (some? cm))
    (let [n (arrays/alength cm)]
      (loop [i 0, acc (measure/identity-measure measure-ops)]
        (if (< i n)
          (let [m (arrays/aget cm i)]
            (if (some? m)
              (recur (inc i) (measure/merge-measure measure-ops acc m))
              nil))
          acc)))))

(defn measure-of
  "The measure of a RESIDENT node, without IO: its cached one, else a leaf's keys folded,
   else a branch's array folded. nil when a branch knows neither."
  [node measure-ops]
  (when (and measure-ops (some? node))
    (let [m (node/measure node)]
      (cond
        (some? m)             m
        (instance? #?(:cljs leaf/Leaf :clj org.replikativ.persistent_sorted_set.leaf.Leaf) node) (node/try-compute-measure node nil measure-ops {:sync? true})
        :else                 (fold-measures measure-ops (read-field node :_childMeasures))))))

(defn- splice-measures
  "`cm` with the entries [from, to) replaced by the measures of `nodes`. `keep`, when
   given, holds the entry to use for nodes[k] if that node cannot say (an untouched
   sibling). nil if `cm` is unknown or a node's measure is."
  [measure-ops cm from to nodes keep]
  (when (and measure-ops (some? cm))
    (let [n  (arrays/alength nodes)
          ms (arrays/make-array n)]
      (loop [k 0]
        (if (< k n)
          (let [m0 (measure-of (arrays/aget nodes k) measure-ops)
                m  (if (some? m0) m0 (when keep (arrays/aget keep k)))]
            (if (some? m)
              (do (aset ms k m) (recur (inc k)))
              nil))
          (util/splice cm from to ms))))))

(defn- install-measures!
  "Install `cm` on `b` with the measure it folds to. With `cm` unknown the measure is
   `fallback` (what the arm computed before the record existed) and the array is cleared."
  [^Branch b measure-ops cm fallback]
  (when measure-ops
    (let [m (fold-measures measure-ops cm)]
      (if (some? m)
        (do (write-field! b :_childMeasures cm)
            (write-field! b :_measure m))
        (do (write-field! b :_childMeasures nil)
            (write-field! b :_measure fallback)))))
  b)

(defn with-child-measures!
  "Give a branch that was just BUILT over resident children its record. Leaves it without
   one if a child cannot say."
  [^Branch b]
  (when-some [measure-ops (:measure (.-settings b))]
    (when-some [kids (read-field b :children)]
      (let [n   (arrays/alength (.-keys b))
            out (arrays/make-array n)]
        (loop [i 0]
          (if (< i n)
            (let [m (measure-of (aget kids i) measure-ops)]
              (if (some? m)
                (do (aset out i m) (recur (inc i)))
                nil))
            (install-measures! b measure-ops out (read-field b :_measure)))))))
  b)

(defn ensure-child-measures
  "The array, filled if it is missing by reading THIS node's children once. A child that
   stored no measure of its own is forced, which reads below it. nil only without a
   measure, or when a child cannot produce one."
  [^Branch node storage measure-ops {:keys [sync?] :or {sync? true} :as opts}]
  ;; `measure-ops` is the caller's: a restored node's own settings come from the storage
  ;; and need not carry the measure the set was opened with.
  (async+sync sync?
              (async
               (let [cm (read-field node :_childMeasures)]
                 (cond
                   (some? cm)         cm
                   (nil? measure-ops) nil
                   :else
                   (let [n   (arrays/alength (.-keys node))
                         out (arrays/make-array n)]
                     (loop [i 0]
                       (if (< i n)
                         (let [c  (await (child node storage i opts))
                               ;; A leaf is folded from its keys, not trusted: before the
                               ;; record existed a leaf insert here could store a measure
                               ;; merged in insertion order.
                               m0 (when-not (instance? #?(:cljs leaf/Leaf :clj org.replikativ.persistent_sorted_set.leaf.Leaf) c) (node/measure c))
                               m  (cond
                                    (some? m0)         m0
                                    (instance? #?(:cljs leaf/Leaf :clj org.replikativ.persistent_sorted_set.leaf.Leaf) c) (node/try-compute-measure c nil measure-ops {:sync? true})
                                    :else              (await (node/force-compute-measure c storage measure-ops opts)))]
                           (if (some? m)
                             (do (aset out i m) (recur (inc i)))
                             nil))
                         (do (write-field! node :_childMeasures out)
                             (write-field! node :_cmLazy true)
                             out)))))))))

(defn- projected-measures
  "The base's array describes its children as of the base's own blob; a slot carries the
   current measure of a grandchild that changed since. One of them missing leaves the copy
   without an array, which is the safe direction."
  [^Branch base slots]
  (when-some [cm (read-field base :_childMeasures)]
    (let [out (arrays/aclone cm)
          n   (arrays/alength cm)]
      (loop [i 0]
        (if (< i n)
          (let [sl (aget slots i)]
            (if (nil? sl)
              (recur (inc i))
              (let [m (:measure sl)]
                (if (some? m)
                  (do (aset out i m) (recur (inc i)))
                  nil))))
          out)))))

;; diff-buf read/projection parity with the JVM Branch.java.
;;
;; cljs `_slots` mirrors the JVM Branch._slots: a JS array indexed by child idx whose
;; entries are nil or a slot-map {:diff D :count N :measure M :anchor A}. Slots are
;; reconstructed on restore by the storage/serializer layer (datahike's cljs read
;; handler) and projected here, lazily, when a child is first materialized in `child`.
;;
;; `:diff` is one of:
;;   - a leaf-diff map {cmp-key -> element (Present) | ABSENT (remove)} — a LEAF child;
;;   - a nested branch-diff {idx -> {:count :measure :diff :max-key}} — a BRANCH child.

;; Marks a removed key in a leaf-diff (an <em>Absent</em> entry). Must match the JVM
;; Slot.ABSENT keyword exactly so JVM-written diffs project identically on cljs frontends.
(def ^:const ABSENT :org.replikativ.persistent-sorted-set/absent)

(defn- leaf-diff->storage
  "Convert a live leaf-diff (sorted-map {element -> element|ABSENT}, keyed by the set comparator)
  into the COMPARATOR-AGNOSTIC storage form {:absent [el…] :present [el…]} (mirrors JVM
  leafDiffForStorage). Storage carries no comparator, so the wire form must not be a map keyed by
  the element: the element's own equality can be coarser than the set comparator (e.g. datahike
  Datom = e,a,v vs e,a,v,tx), which would collapse two entries the diff distinguishes (a tx-only
  replace's Absent+Present → one, losing the removal). Two vectors are lossless. Pass-through if
  already storage form (restored, re-emitted) or nil."
  [diff]
  (if-not (sorted? diff)
    diff
    (reduce (fn [acc [k v]]
              (if (= v ABSENT) (update acc :absent conj k) (update acc :present conj v)))
            {:absent [] :present []}
            diff)))

(defn- project-leaf
  "Apply a leaf-diff to a durable leaf in one pass: merge the durable keys with the diff
  (Present upserts the element, Absent removes) under the set's comparator, emitting the
  result elements in key order. Mirrors JVM Branch.projectLeaf. IO-free (no split/merge).
  Handles both the live form (sorted-map {element -> element|ABSENT}) and the restored storage
  form {:absent [el…] :present [el…]}."
  [base diff cmp]
  (let [bkeys (.-keys base)
        n     (arrays/alength bkeys)
        m0    (loop [i 0, acc (sorted-map-by cmp)]
                (if (< i n)
                  (recur (inc i) (assoc acc (arrays/aget bkeys i) (arrays/aget bkeys i)))
                  acc))
        m1    (if (sorted? diff)
                (reduce (fn [acc e]
                          (let [k (key e), v (val e)]
                            (if (= v ABSENT) (dissoc acc k) (assoc acc v v))))
                        m0 (seq diff))
                (as-> m0 acc
                  (reduce (fn [a el] (dissoc a el)) acc (:absent diff))
                  (reduce (fn [a el] (assoc a el el)) acc (:present diff))))]
    (leaf/->Leaf (into-array (vals m1)) (.-settings base) nil)))

(defn- project-branch
  "Push one level down onto a COPY — never mutate `base`. Mirrors JVM
  Branch.projectBranch, whose comment is the reference explanation for why.

  Caching IStorage impls (datahike's CachedStorage, and this project's own test
  storage) return restored nodes SHARED BY ADDRESS across tree versions, and this
  projection is VERSION-SPECIFIC: consecutive commits buffer against the same
  durable anchor with different accumulated diffs, so version N's exact child at
  address B is the very object version N+1 projects {B, δ} onto. This used to
  install slots, rewrite separators, count and measure directly on `base` and
  return it, which leaked one version's projection into every other version's
  reads. Measured on cljs before the fix (bf 8, 4000 elements, diff-buf 512, one
  cache shared across two versions): v2 missing 115-121 elements, v1 missing
  64-65, in both read orders; zero at diff-buf 0 and zero with a non-caching
  storage. The JVM had and fixed the same bug (#19).

  `children` stays nil on the copy: a grandchild with a nested slot must be
  projected by the copy's own descent, and a passthrough grandchild re-restores
  through the (pristine) cache. Leaving base's children in place handed version
  N's already-projected grandchild to version N+1.

  The copy aliases base's `addresses` — read-only by the shared-snapshot
  contract — and takes the PARENT's projection comparator, since that is whose
  diff is being projected."
  [^Branch base sl proj-cmp]
  (let [diff      (:diff sl)
        base-keys (.-keys base)
        base-addr (read-field base :addresses)
        new-keys  (arrays/aclone base-keys)
        slots     (arrays/make-array (arrays/alength base-keys))]
    (doseq [[k entry] (seq diff)]
      (let [i  (int k)
            mk (:max-key entry)]
        (aset slots i {:diff    (:diff entry)
                       :count   (long (:count entry))
                       :measure (:measure entry)
                       :anchor  (arrays/aget base-addr i)})   ; anchor = grandchild's durable address
        ;; Restore the separator ON THE COPY: base came from the anchor whose _keys[i] is
        ;; the PRE-diff max; the diff changed child i's max, so fix the separator here
        ;; (the verified read bug) — without writing through to the shared node.
        (when (some? mk)
          (arrays/aset new-keys i mk))))
    ;; _bufEntries -2 = LAZY, derived from the slots on first read (mirrors from-map).
    ;; COPY the addresses array rather than aliasing base's. The JVM twin took this trade for
    ;; the reason recorded at `Branch.projectBranch`: a caching IStorage returns the same
    ;; object per address across versions, so any in-place write through this array would be
    ;; seen by every other version. On this runtime the write-through is reachable — not
    ;; under a consistent budget (a projected copy's `pass` equals the nested diff the parent
    ;; buffered, and the parent only buffered it because it fit), but under a BUDGET
    ;; MISMATCH: `install-slots!` refuses only a non-positive budget and `-root` adopts a
    ;; node's budget only upward from 0, so a store written at `:diff-buf-size 512` whose
    ;; storage rebuilds nodes at 8 gives a copy `pass` > `budget`, and the D3 over-budget
    ;; flush writes `(aset addrs i ...)` in place. This function already allocates two arrays
    ;; of this length; a third removes the reasoning chain entirely.
    (->Branch (.-level base) new-keys nil (arrays/aclone base-addr)
              (long (:count sl))                      ; ĝ.count — no child summing
              (:measure sl)                           ; ĝ.measure
              (.-settings base) slots -2 proj-cmp
              (projected-measures base slots) (boolean (read-field base :_cmLazy)))))

(defn with-proj-cmp
  "A copy of `base` that projects under `proj-cmp` instead of its own `_projCmp`.

   The ClojureScript half of the JVM's `Branch.withProjCmp`. `_projCmp` is a FIELD on a node
   that a caching storage shares by address, so two sets over one storage whose comparators
   order ties differently were overwriting each other's stamp, and whichever read last won.
   `project-leaf` then rebuilt a buffered leaf's key array in the OTHER set's order.

   Measured on the JVM before the equivalent fix, and the same shape applies here: count
   correct, `seq` NOT sorted under the first set's comparator, `contains?` false for elements
   present in `seq`, and — after one `conj` into the mis-sorted cached leaf plus a `store` —
   an element permanently unfindable on a cold reload, because the branch separator no longer
   bounds it.

   The children array is NOT shared: it holds children already projected under the other
   comparator, which is the whole thing being escaped. Children with NO durable address are
   the exception and must be carried over — such a child is reachable only through this
   array, and dropping it loses the subtree. Keys and addresses are copied; `_slots` and
   `_bufEntries` are shared, since slots are immutable snapshots.

   Copy rather than refuse: two semantically identical comparators are routinely distinct
   objects (any caller building `(fn [a b] ...)` per restore) and they order the leaf
   identically, so throwing there would break correct code."
  [^Branch base proj-cmp]
  (let [ks    (.-keys base)
        n     (arrays/alength ks)
        addrs (read-field base :addresses)
        kids  (read-field base :children)
        addr-copy (when (some? addrs) (arrays/aclone addrs))
        kid-copy  (when (some? kids)
                    (let [out (arrays/make-array (arrays/alength kids))]
                      (dotimes [i n]
                        (when (and (or (nil? addr-copy) (nil? (arrays/aget addr-copy i)))
                                   (some? (arrays/aget kids i)))
                          (aset out i (arrays/aget kids i))))
                      out))]
    (->Branch (.-level base) (arrays/aclone ks) kid-copy addr-copy
              (read-field base :subtree-count) (read-field base :_measure) (.-settings base)
              (read-field base :_slots) (read-field base :_bufEntries) proj-cmp
             ;; Without buffered edits the projection comparator touches nothing and the
             ;; record stands. With them, a comparator that orders ties differently replays
             ;; a buffered leaf in another order, and an order-sensitive measure with it:
             ;; the copy starts unknown and refills.
              (when (nil? (read-field base :_slots)) (read-field base :_childMeasures))
              (boolean (read-field base :_cmLazy)))))

(defn stamp-proj-cmp
  "Seed `node`'s projection comparator, or return a COPY when it already carries a different
   one. Returns the node to use. Both stamp sites — `btset/-root` and `branch/child`'s
   restore arm — used to `set!` unconditionally onto the object the storage handed back."
  [node proj-cmp]
  (if-not (instance? Branch node)
    node
    (let [^Branch b node
          cur (read-field b :_projCmp)]
      (cond
        (nil? cur) (do (write-field! b :_projCmp proj-cmp) b)
        (identical? cur proj-cmp) b
        :else (with-proj-cmp b proj-cmp)))))

(defn- project-child
  "Project a freshly-restored child against this parent's buffered slot (if any). Returns
  `base` unchanged when there is no diff to apply. Mirrors the JVM Branch.child push-down."
  [^Branch node storage idx base]
  (let [sl (when (some? (read-field node :_slots)) (aget (read-field node :_slots) idx))]
    (if (and sl (some? (:diff sl)))
      ;; project with THIS leaf-parent's _projCmp (the set's stable comparator), never the
      ;; operation/navigation comparator that drove the descent. Mirrors JVM Branch.child.
      (if (instance? #?(:cljs leaf/Leaf :clj org.replikativ.persistent_sorted_set.leaf.Leaf) base)
        (project-leaf base (:diff sl) (read-field node :_projCmp))
        (project-branch base sl (read-field node :_projCmp)))
      base)))

(defn ensure-children
  [^Branch node]
  (when (nil? (read-field node :children))
    (write-field! node :children (arrays/make-array (alength (.-keys node)))))
  (read-field node :children))

(defn ensure-addresses
  [^Branch this]
  (when (nil? (read-field this :addresses))
    (write-field! this :addresses (arrays/make-array (alength (.-keys this)))))
  (read-field this :addresses))

(defn install-slots!
  "diff-buf restore: install reconstructed slots on a not-yet-published node. The single
   entry point for a storage/codec handing buffered diffs back to a restored Branch —
   the ClojureScript twin of JVM `Branch.installSlots`, including its refusal.

   A node carrying buffered diffs whose SETTINGS say buffering is off is an incoherent
   reconstruction: the storage persisted `:slots` and is now rebuilding the node declaring
   there is no buffer. Every read of it is then subtly wrong rather than loudly broken —
   `store` takes the baseline path at budget 0 and writes the node WITHOUT its slots, so
   the buffered element-changes are silently dropped. Measured on cljs before this check,
   a restored root with 7 buffered slots whose contents were exact beforehand: one conj,
   then store and cold restore, lost 13 previously committed elements.

   The JVM has refused this since `Branch.java`'s installSlots; ClojureScript accepted it
   and lost the data, so the same storage-contract violation was loud on one runtime and
   silent on the other.

   Refused rather than accommodated, for the same reason as on the JVM: the settings are
   the storage's to get right — they come from the blob the storage itself wrote — and a
   library that silently returns the wrong set is worse than one that says which half of
   the contract was broken."
  [node slots]
  (when (and (some? slots)
             (not (pos? (or (:diff-buf-size (.-settings node)) 0))))
    (throw (ex-info (str "diff-buf: a node reconstructed with diff-buf-size "
                         (pr-str (:diff-buf-size (.-settings node)))
                         " was handed buffered slots. The storage persisted this node's "
                         "diff buffer and must reconstruct it with the same budget — see "
                         "IStorage restore and :diff-buf-size.")
                    {:diff-buf-size (:diff-buf-size (.-settings node))
                     :slot-count    (count (remove nil? (seq slots)))})))
  (write-field! node :_slots slots)
  node)

(defn child
  [^Branch node storage idx {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (some? idx) (number? idx)))
  (assert (or (and (some? (read-field node :children))
                   (some? (aget (read-field node :children) idx)))
              (and (some? (read-field node :addresses))
                   (some? (aget (read-field node :addresses) idx)))))
  (async+sync sync?
              (async
               (let [*child (atom nil)]
                 (when (some? (read-field node :children))
                   (reset! *child (aget (read-field node :children) idx)))
                 (if (nil? @*child)
                   (let [addr (aget (read-field node :addresses) idx)
                         _    (assert (some? addr) "expected address to restore child")
                         _    (assert (some? storage) "expected storage")
                         base0 (await (storage/restore storage addr opts))
                         ;; diff-buf: propagate the set's projection comparator down to the
                         ;; restored branch (the storage layer has no comparator), so a
                         ;; leaf-parent projects its buffered leaves with the set's own
                         ;; comparator — independent of the op that drove this descent.
                         ;; SEED it when the node carries none; COPY when it carries a
                         ;; different one, because `base0` is the object the storage returned
                         ;; and a caching storage shares it by address across sets. Writing
                         ;; unconditionally is what let one set's comparator decide another
                         ;; set's leaf order — see `with-proj-cmp`.
                         base (stamp-proj-cmp base0 (read-field node :_projCmp))
                         ;; diff-buf: project this parent's buffered diff onto the freshly
                         ;; loaded child (leaf: rebuild keys; branch: install nested _slots).
                         c    (project-child node storage idx base)]
                     (reset! *child c)
                     (aset (ensure-children node) idx c))
                   (when (and (some? (read-field node :addresses)) (some? (aget (read-field node :addresses) idx)))
                     (assert (some? storage) "expected storage")
                     (storage/accessed storage (aget (read-field node :addresses) idx))))
                 @*child))))

(defn address
  ([^Branch this idx]
   (assert (and (<= 0 idx) (< idx (alength (.-keys this)))))
   (when-some [addrs (read-field this :addresses)]
     (aget addrs idx)))
  ([^Branch this idx address]
   (assert (and (<= 0 idx) (< idx (alength (.-keys this)))))
   (when (or (some? (read-field this :addresses)) (some? address))
     (ensure-addresses this)
     (aset (read-field this :addresses) idx address))
   address))

(defn- $count
  "Element count of this subtree.

   A branch already KNOWS this most of the time: `subtree-count` is maintained by the
   write paths and round-trips through the blob (`impl.nodes/branch->blob` writes it,
   `blob->branch` reads it back), so a restored root can answer in O(1) without touching
   storage. This used to recurse unconditionally, which made `count` on a stored set load
   the ENTIRE tree — where the JVM (`Branch.count`, which returns `_subtreeCount` when it
   is >= 0) loads one node. `-1` means genuinely unknown and is the only case that walks.

   Deliberately NOT caching the walked result back into `subtree-count`, unlike the JVM.
   The JVM can, because its in-place transient arms maintain the field; the ClojureScript
   Branch has no in-place arms at all, so nothing here would keep a cached value honest
   through a later mutation, and `btset/$count` already memoizes at the SET level (`.-cnt`)
   which is where the repeated-call win actually comes from."
  [^Branch node storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [known (read-field node :subtree-count)]
                 (if (>= known 0)
                   known
                   (let [*cnt (atom 0)]
                     (dotimes [i (alength (.-keys node))]
                       (let [c (await (child node storage i opts))]
                         (swap! *cnt + (await (node/$count c storage opts)))))
                     @*cnt))))))

(defn- $contains?
  [^Branch node storage key cmp {:keys [sync?] :or {sync? true} :as opts}]
  (let [idx (util/binary-search cmp (.-keys node) key)]
    (async+sync sync?
                (async
                 (if (<= 0 idx)
                   true
                   (let [ins (dec (- idx))]
                     (if (== ins (alength (.-keys node)))
                       false
                       (do
                         (assert (and (<= 0 ins) (< ins (alength (.-keys node)))))
                         (let [c (await (child node storage ins opts))]
                           (await (node/$contains? c storage key cmp opts)))))))))))

(defn- lookup
  [^Branch node storage key cmp {:keys [sync?] :or {sync? true} :as opts}]
  (let [idx (util/binary-search cmp (.-keys node) key)]
    (async+sync sync?
                (async
                 (let [ins (if (<= 0 idx) idx (dec (- idx)))]
                   (when (< ins (alength (.-keys node)))
                     (let [c (await (child node storage ins opts))]
                       (await (node/lookup c storage key cmp opts)))))))))

(defn- try-compute-subtree-count-from-children
  "Compute subtree count from in-memory children. Returns -1 if any child unavailable."
  [children len]
  (if (nil? children)
    -1
    (loop [i 0, cnt 0]
      (if (>= i len)
        cnt
        (let [child (aget children i)]
          (if (nil? child)
            -1
            (let [cc (node/subtree-count child)]
              (if (>= cc 0)
                (recur (inc i) (+ cnt cc))
                -1))))))))

;; ---- diff-buf write-side diff tracking (mirrors JVM Branch) ----

(defn- diff-buf?
  "Is diff-buf buffering enabled for this node's set? (settings :diff-buf-size > 0)."
  [^Branch node]
  (pos? (or (:diff-buf-size (.-settings node)) 0)))

(defn- slot-at
  "This branch's buffered slot for child i (nil if none). Mirrors JVM slotAt."
  [^Branch node i]
  (when-some [slots (read-field node :_slots)] (aget slots i)))

(defn- slot-be
  "Buffered-entry count this slot contributes: its cached :buf-entries, or — for a slot
  reconstructed from storage (no :buf-entries) — derived from the diff blob (IO-free). A restored
  slot simply lacks :buf-entries (nil ⇒ derive), the cljs analogue of JVM Slot.LAZY (-2). The
  slot's child is at child-level. Mirrors JVM Branch.slotBE."
  [sl child-level]
  (let [be (:buf-entries sl)]
    (if (nil? be) (diff-size (:diff sl) child-level) be)))

(defn- buf-entries
  "This branch's subtree buffered-diff size in entries (mirrors JVM bufEntries()). Resolves a
  restored node's LAZY (-2) value from its slots once (IO-free — the diffs are already in
  memory), caches it, then returns; -1 (must-write) passes through. O(1) once resolved."
  [^Branch node]
  (let [be (read-field node :_bufEntries)]
    (if (== be -2)
      (let [slots (read-field node :_slots)
            clvl  (dec (.-level node))
            s     (if (nil? slots)
                    0
                    (areduce slots j acc 0
                             (+ acc (if-some [sl (aget slots j)] (slot-be sl clvl) 0))))]
        (write-field! node :_bufEntries s)
        s)
      be)))

(defn- child-count
  "Exact subtree count of in-memory child i (cheap; maintained by add/remove). Mirrors
  JVM childCount. Async because cljs `child` is async."
  [^Branch node storage i {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [c  (await (child node storage i opts))
                     sc (node/subtree-count c)]
                 (if (>= sc 0)
                   sc
                   ;; An unknown branch (a tree not yet `recount`ed): this reads its
                   ;; children. `c` is the node the write in progress just built, so it is
                   ;; written — or buffered — by the coming store, and takes the count with
                   ;; it instead of being flushed unknown under a slot that knows.
                   (let [n (await (node/$count c storage opts))]
                     (when (instance? Branch c) (write-field! c :subtree-count n))
                     n))))))

(defn- deposit-kv
  "Core leaf-diff deposit (mirrors JVM depositKV): accumulate the given [k v] pairs onto this
  branch's slot i, keyed by the SET's comparator (.-_projCmp) — the comparator the durable leaf is
  sorted by and the only one project-leaf uses. Keying by _projCmp (not the operation comparator)
  is what makes the diff a self-contained remove/upsert language. anchor0 is child i's pre-mutation
  durable address; the slot keeps a previously-captured anchor (first capture of the txn wins).
  Leaf child ⇒ accumulate net-latest-wins; branch child ⇒ nil marker."
  [^Branch node storage i kvs anchor0 {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (when (nil? (read-field node :_slots))
                 (write-field! node :_slots (arrays/make-array (arrays/alength (.-keys node)))))
               (let [slots  (read-field node :_slots)
                     prev   (aget slots i)
                     anchor (if (and prev (some? (:anchor prev))) (:anchor prev) anchor0)
                     c      (await (child node storage i opts))
                     pcmp   (read-field node :_projCmp)
                     diff   (cond
                              ;; no durable base to diff against (never-stored tree / child split
                              ;; this txn): at store such a child is written wholesale and its diff
                              ;; is discarded, so building one is pure waste. Skip it (mirrors JVM
                              ;; depositKV); count/measure stay for op-buf-aware aggregation.
                              (nil? anchor) nil
                              (== (.-level node) 1)
                              ;; leaf child: accumulate net-latest-wins, rebuilding a _projCmp-sorted
                              ;; map from a restored storage-form diff {:absent [..] :present [..]}.
                              (let [pd (when prev (:diff prev))
                                    d  (cond
                                         (sorted? pd) pd
                                         (map? pd)    (as-> (sorted-map-by pcmp) d
                                                        (reduce (fn [d el] (assoc d el ABSENT)) d (:absent pd))
                                                        (reduce (fn [d el] (assoc d el el)) d (:present pd)))
                                         :else        (sorted-map-by pcmp))]
                                (reduce (fn [d [k v]] (assoc d k v)) d kvs))
                              ;; branch child: anchor marker (nested diff derived at store)
                              :else nil)
                     cnt     (await (child-count node storage i opts))
                     ;; worked out if the child has none cached: a restore rebuilds the
                     ;; child's place in this node's record of measures from it
                     measure (measure-of c (:measure (.-settings node)))
                     ;; this slot's new buffered-entry count (subtree total for a branch child)
                     new-be  (cond
                               (nil? anchor)        0                 ; written wholesale ⇒ no budget
                               (== (.-level node) 1) (count diff)      ; leaf-diff entry count
                               :else                (buf-entries c))   ; child subtree total (-1 if it rebalanced)
                     ;; resolve our running total and the old slot's contribution BEFORE overwriting
                     ;; slot i (buf-entries may sum over the slots, which still hold prev here).
                     cur     (buf-entries node)
                     old-be  (if prev (slot-be prev (dec (.-level node))) 0)]
                 (aset slots i {:diff diff :count cnt :measure measure :anchor anchor :buf-entries new-be})
                 ;; maintain this node's running total by delta (mirrors JVM depositKV / subtree-count).
                 ;; A -1 (must-write) child poisons us → the must-write signal climbs the deposit sum
                 ;; with no extra propagation code (this is what replaced the store-time subtree walk).
                 (when-not (== cur -1)
                   (write-field! node :_bufEntries (if (== new-be -1) -1 (+ (- cur old-be) new-be))))
                 nil))))

(defn- deposit-into
  "Deposit a single leaf-op (Present(map-key=val) or Absent(map-key→ABSENT)). Mirrors JVM depositInto."
  [^Branch node storage i map-key val anchor0 opts]
  (deposit-kv node storage i [[map-key val]] anchor0 opts))

(defn- deposit-replace
  "Deposit a replace as Absent(old-key)+Present(new-key) (mirrors JVM depositReplace). When old-key
  and new-key are equal under _projCmp (in-place replace) the Present overwrites the Absent at the
  same key ⇒ net Present; when they differ (a coarser-comparator replace, e.g. a tx-only value
  change), both survive so project-leaf removes old and adds new with no comparator logic."
  [^Branch node storage i old-key new-key anchor0 opts]
  (deposit-kv node storage i [[old-key ABSENT] [new-key new-key]] anchor0 opts))

(defn- stitch-slots
  "Carry _slots through a structural rebuild where child `ins` was replaced by `n-nodes` new
  nodes, producing a slots array of length new-len laid out like the rebuilt children: surviving
  siblings keep their slot; the new nodes get none. nil if this node has no slots. Mirrors JVM
  stitchSlots."
  [^Branch node ins n-nodes new-len]
  (when-some [slots (read-field node :_slots)]
    (let [out (arrays/make-array new-len)
          len (arrays/alength (.-keys node))]
      (dotimes [i ins] (aset out i (aget slots i)))   ; [0, ins)
      ;; the n-nodes new slots stay nil (make-array)
      (loop [src (inc ins), dst (+ ins n-nodes)]      ; [ins+1, len)
        (when (< src len)
          (aset out dst (aget slots src))
          (recur (inc src) (inc dst))))
      out)))

(defn- carry-slots
  "Carry the source branch's slots into this freshly-built branch (1-for-1 child replacement, so
  indices align). Mirrors the slot-copy half of JVM carryAndDeposit."
  [^Branch node src-slots]
  (when (some? src-slots)
    (let [n   (arrays/alength (.-keys node))
          arr (arrays/make-array n)
          m   (min n (alength src-slots))]
      (dotimes [k m] (aset arr k (aget src-slots k)))
      (write-field! node :_slots arr))))

(defn- carry-and-deposit
  "Carry source slots then deposit a single leaf-op at i. Mirrors JVM carryAndDeposit."
  [^Branch node storage src-slots i map-key val anchor0 opts]
  (async+sync (:sync? opts true)
              (async
               (carry-slots node src-slots)
               (await (deposit-into node storage i map-key val anchor0 opts)))))

(defn- carry-and-deposit-replace
  "Carry source slots then deposit a replace (Absent(old)+Present(new)) at i. Mirrors JVM
  carryAndDepositReplace."
  [^Branch node storage src-slots i old-key new-key anchor0 opts]
  (async+sync (:sync? opts true)
              (async
               (carry-slots node src-slots)
               (await (deposit-replace node storage i old-key new-key anchor0 opts)))))

(defn- concat-slots
  "Concatenate two branches' _slots (this ++ next), 1-per-child, padding nils for a slot-less
  branch. Returns nil if neither has slots. Used by merge/merge-split to carry buffered children
  through a structural rebalance (the cljs analogue of JVM's per-case slot Stitch)."
  [^Branch a ^Branch b]
  (let [sa (read-field a :_slots) sb (read-field b :_slots)]
    (when (or sa sb)
      (let [na  (arrays/alength (.-keys a))
            nb  (arrays/alength (.-keys b))
            out (arrays/make-array (+ na nb))]
        (when sa (dotimes [i na] (aset out i (aget sa i))))
        (when sb (dotimes [i nb] (aset out (+ na i) (aget sb i))))
        out))))

(defn- remove-stitch-slots
  "Build the slots array for the spliced center node of a STRUCTURAL $remove (a borrow/merge
  happened one level down): carry the surviving siblings outside the rebuilt range, keep an
  unchanged left/right sibling's slot, and null the center + any changed sibling. Mirrors the
  JVM remove slot Stitch (Branch.java 679-683)."
  [^Branch this left-idx right-idx alen left-child right-child left-unchanged right-unchanged new-len]
  (let [src (read-field this :_slots)
        out (arrays/make-array new-len)
        len (arrays/alength (.-keys this))]
    (when src
      (dotimes [i left-idx] (aset out i (aget src i)))                 ; prefix [0, left-idx)
      (when (and left-child left-unchanged)                            ; unchanged left sibling keeps its slot
        (aset out left-idx (aget src left-idx)))
      (when (and right-child right-unchanged)                          ; unchanged right sibling keeps its slot
        (aset out (+ left-idx (dec alen)) (aget src (dec right-idx))))
      (loop [i right-idx, dst (+ left-idx alen)]                       ; suffix [right-idx, len) shifted
        (when (< i len)
          (aset out dst (aget src i))
          (recur (inc i) (inc dst)))))
    out))

(defn- free-dropped-child
  "diff-buf: free child i's durable blob when a STRUCTURAL rebuild (merge/borrow/split) drops
  it. Such a child is materialized into a new node and its old blob is never re-pointed as a
  buffered anchor (unlike the content-only case, which IS deferred to store), so it is dead
  now. The live durable address is addresses[i] if set, else the anchor parked in slot i
  (content-buffered earlier this txn). Read i from THIS node before the rebuild overwrites it.
  Mirrors JVM Branch.freeDroppedChild. No-op when storage is nil / diff-buf is off (callers gate)."
  [^Branch this storage i]
  (when storage
    (let [addrs (read-field this :addresses)
          a     (when addrs (aget addrs i))]
      (if (some? a)
        (storage/markFreed storage a)
        (let [slots (read-field this :_slots)
              sl    (when slots (aget slots i))]
          (when (and sl (some? (:anchor sl)))
            (storage/markFreed storage (:anchor sl))))))))

;; ---- counts that need the children: split, borrow, recount (mirrors JVM Branch) ----

(defn- children-to-read
  "How many of this node's children cannot say their count from memory: not resident, and
   with no diff-buf slot carrying it. The number of reads `count-children-reading` would do."
  [^Branch node]
  (let [children (read-field node :children)
        n        (arrays/alength (.-keys node))]
    (loop [i 0, acc 0]
      (if (>= i n)
        acc
        (let [child (when children (aget children i))
              sl    (slot-at node i)]
          (recur (inc i)
                 (if (or (some? child) (and sl (>= (:count sl -1) 0))) acc (inc acc))))))))

(defn- count-children-reading
  "The number of elements below this node, as the sum of its children's counts — READING
   the children that are not in memory. -1 when a child cannot say (a branch whose own count
   is unknown, or a child with neither a node nor an address); it never descends further, so
   the cost is bounded by this node's fanout.

   Per child, in this order:
     resident — its own count (a leaf's length, a branch's subtree-count);
     a slot   — the slot's count. Under diff-buf the slot is the ONLY right answer for a
                child that is not resident: its durable blob predates the buffered changes,
                so its stored count is stale by exactly those.
     neither  — restore the blob and read it. The node is not kept: this is one number."
  [^Branch node storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [children (read-field node :children)
                     addrs    (read-field node :addresses)
                     n        (arrays/alength (.-keys node))]
                 (loop [i 0, total 0]
                   (if (>= i n)
                     total
                     (let [child (when children (aget children i))
                           sl    (slot-at node i)
                           c0    (if (some? child) (node/subtree-count child) -1)
                           c1    (if (and (neg? c0) sl) (:count sl -1) c0)
                           addr  (when addrs (aget addrs i))
                           c     (if (and (neg? c1) (nil? child) (nil? sl) (some? storage) (some? addr))
                                   (node/subtree-count (await (storage/restore storage addr opts)))
                                   c1)]
                       (if (neg? c)
                         -1
                         (recur (inc i) (+ total c))))))))))

(defn- divide-count!
  "`a` and `b` are two freshly built nodes that together hold `total` elements. Where
   neither could be counted from memory, read the children of the one that needs fewer
   reads; give the other the remainder.

   This is the one place a write reads beyond its path, and it is the format that makes it
   necessary: a branch stores its total and nothing about how the total is spread over its
   children, so a node that DIVIDES — a split, a borrow — finds the per-child numbers only
   in the children. Leaving both unknown instead (what this did) is durable: the unknown is
   serialized, no write heals it, and every later write through the node re-stores it
   unknown. See JVM `Branch.divideCount` for the measurements."
  [^Branch a ^Branch b total storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [ca (read-field a :subtree-count)
                     cb (read-field b :subtree-count)]
                 (cond
                   (neg? total) nil
                   (and (>= ca 0) (>= cb 0)) nil
                   ;; one half was resident: the other is the remainder
                   (>= ca 0) (when (<= ca total) (write-field! b :subtree-count (- total ca)))
                   (>= cb 0) (when (<= cb total) (write-field! a :subtree-count (- total cb)))
                   :else
                   (let [read  (if (<= (children-to-read a) (children-to-read b)) a b)
                         other (if (identical? read a) b a)
                         c     (await (count-children-reading read storage opts))]
                     (when (>= c 0)
                       (write-field! read :subtree-count c)
                       ;; a total smaller than one of its parts was already wrong; do not
                       ;; turn it into a second wrong number
                       (when (<= c total)
                         (write-field! other :subtree-count (- total c))))))
                 nil))))

(defn count-census
  "Walk every branch below (and including) `node`; returns [branches unknown], `unknown`
   being the branches whose count is not known. A child restored through a slot that carries
   a diff takes its count from the slot, so what is seen here is what a restore finds.
   Reads every branch, no leaf."
  [^Branch node storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [n    (arrays/alength (.-keys node))
                     self (if (neg? (read-field node :subtree-count)) 1 0)]
                 (if (<= (.-level node) 1)
                   [1 self]
                   (loop [i 0, branches 1, unknown self]
                     (if (>= i n)
                       [branches unknown]
                       (let [c  (await (child node storage i opts))
                             r  (await (count-census c storage opts))]
                         (recur (inc i) (+ branches (nth r 0)) (+ unknown (nth r 1)))))))))))

(defn measure-census
  "Walk every branch below (and including) `node`; returns [branches unknown], `unknown`
   being the branches whose STORED form has no record of its children's measures: no
   array, or one only filled in memory by a read. Reads every branch, no leaf."
  [^Branch node storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [n    (arrays/alength (.-keys node))
                     self (if (or (nil? (read-field node :_childMeasures)) (read-field node :_cmLazy)) 1 0)]
                 (if (<= (.-level node) 1)
                   [1 self]
                   (loop [i 0, branches 1, unknown self]
                     (if (>= i n)
                       [branches unknown]
                       (let [c (await (child node storage i opts))
                             r (await (measure-census c storage opts))]
                         (recur (inc i) (+ branches (nth r 0)) (+ unknown (nth r 1)))))))))))

(defn remeasure
  "This subtree with every branch holding its children's measures, and every branch that
   did not hold them on disk made dirty so the next `store` writes it. Returns `node`
   itself when nothing below needs it. Content and counts are unchanged. Mirrors JVM
   `Branch.remeasure`."
  [^Branch node storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [measure-ops (:measure (.-settings node))
                     n       (arrays/alength (.-keys node))
                     changed (arrays/make-array n)
                     any?    (if (or (nil? measure-ops) (<= (.-level node) 1))
                               false
                               (loop [i 0, any? false]
                                 (if (>= i n)
                                   any?
                                   (let [c  (await (child node storage i opts))
                                         nc (await (remeasure c storage opts))]
                                     (if (identical? c nc)
                                       (recur (inc i) any?)
                                       (do (aset changed i nc)
                                           (recur (inc i) true)))))))]
                 (if (or (nil? measure-ops)
                         (and (not any?) (some? (read-field node :_childMeasures)) (not (read-field node :_cmLazy))))
                   node
                   (let [addrs     (read-field node :addresses)
                         slots     (read-field node :_slots)
                         children  (read-field node :children)
                         new-kids  (if children (arrays/aclone children) (arrays/make-array n))
                         new-addrs (when addrs (arrays/aclone addrs))
                         new-slots (when slots (arrays/aclone slots))]
                     (dotimes [i n]
                       (when-some [nc (aget changed i)]
                         (free-dropped-child node storage i)
                         (when new-addrs (aset new-addrs i nil))
                         (aset new-kids i nc)
                         (when new-slots (aset new-slots i nil))))
                     (let [nb (->Branch (.-level node) (.-keys node) new-kids new-addrs
                                        (read-field node :subtree-count) (read-field node :_measure) (.-settings node)
                                        new-slots (if (diff-buf? node) -1 0) (read-field node :_projCmp)
                                       ;; a remeasured child holds the same elements
                                        (read-field node :_childMeasures) false)
                           cm (await (ensure-child-measures nb storage measure-ops opts))
                           m  (fold-measures measure-ops cm)]
                       (write-field! nb :_cmLazy false)       ; this node is written, record and all
                       (when (some? m) (write-field! nb :_measure m))
                       nb)))))))

(defn recount
  "This subtree with every branch counted and every branch that was not counted made dirty,
   so the next `store` writes it. Returns `node` itself when nothing below needs it.

   A rewritten child is handled as a structural replacement: its address and its slot are
   dropped, its old blob is freed, and this node becomes one that must be written — the
   same shape a split leaves behind. Content is unchanged: same keys, same children, same
   measures. Mirrors JVM `Branch.recount`."
  [^Branch node storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [n       (arrays/alength (.-keys node))
                     changed (arrays/make-array n)
                     any?    (if (<= (.-level node) 1)
                               false
                               (loop [i 0, any? false]
                                 (if (>= i n)
                                   any?
                                   (let [c  (await (child node storage i opts))
                                         nc (await (recount c storage opts))]
                                     (if (identical? c nc)
                                       (recur (inc i) any?)
                                       (do (aset changed i nc)
                                           (recur (inc i) true)))))))]
                 (if (and (not any?) (>= (read-field node :subtree-count) 0))
                   node
                   (let [addrs     (read-field node :addresses)
                         slots     (read-field node :_slots)
                         children  (read-field node :children)
                         new-kids  (if children (arrays/aclone children) (arrays/make-array n))
                         new-addrs (when addrs (arrays/aclone addrs))
                         new-slots (when slots (arrays/aclone slots))]
                     (dotimes [i n]
                       (when-some [nc (aget changed i)]
                         (free-dropped-child node storage i)
                         (when new-addrs (aset new-addrs i nil))
                         (aset new-kids i nc)
                         (when new-slots (aset new-slots i nil))))
                     (let [;; The copy keeps this node's settings: only the count changes.
                           nb (->Branch (.-level node) (.-keys node) new-kids new-addrs
                                        (read-field node :subtree-count) (read-field node :_measure) (.-settings node)
                                        new-slots (if (diff-buf? node) -1 0) (read-field node :_projCmp)
                                       ;; a recounted child holds the same elements
                                        (read-field node :_childMeasures) (boolean (read-field node :_cmLazy)))]
                       ;; The children are counted now (below a leaf-parent they are leaves,
                       ;; which always are), so this is a sum; it reads the leaves of a
                       ;; leaf-parent that was unknown.
                       (when (neg? (read-field nb :subtree-count))
                         (write-field! nb :subtree-count (await (count-children-reading nb storage opts))))
                       nb)))))))

(defn- mst-branch-add
  "split-seam (MST): given the merged separators/children after absorbing a child split,
   cut at boundary keys (≤2-way for one incremental insert). Mirrors the JVM Branch.add seam
   path. MST forces diff-buf off, so no slots/addresses bookkeeping (anchorless re-store)."
  [^Branch this bd new-keys new-children new-addrs ins key cm-ins]
  (let [settings    (.-settings this)
        lvl         (.-level this)
        measure-ops (:measure settings)
        total       (arrays/alength new-children)
        ;; O(1): the one promoted separator (the split child's new max) is at `ins`.
        lens        (b/-split-on-insert bd new-keys total ins lvl)]
    (if (nil? lens)
      (let [old-sc (read-field this :subtree-count)
            new-sc (if (>= old-sc 0) (inc old-sc) -1)
            m      (when (and measure-ops (some? (read-field this :_measure)))
                     (measure/merge-measure measure-ops (read-field this :_measure) (measure/extract measure-ops key)))]
        (arrays/array (install-measures! (->Branch lvl new-keys new-children new-addrs new-sc m settings nil 0 (read-field this :_projCmp) nil (boolean (read-field this :_cmLazy)))
                                         ;; unknown rather than `m` when the record cannot be spliced: appending
                                         ;; the key's measure is right only for a commutative merge
                                         measure-ops cm-ins nil)))
      (loop [out (transient []), pos 0, ls lens]
        (if (seq ls)
          (let [l    (first ls)
                kseg (arrays/slice new-keys pos (+ pos l))
                cseg (arrays/slice new-children pos (+ pos l))
                aseg (when new-addrs (arrays/slice new-addrs pos (+ pos l)))
                m    (when (and measure-ops (some? (read-field this :_measure)))
                       (reduce (fn [acc child]
                                 (if (nil? acc)
                                   (reduced nil)
                                   (let [cs (when (some? child) (node/measure child))]
                                     (if (some? cs) (measure/merge-measure measure-ops acc cs) (reduced nil)))))
                               (measure/identity-measure measure-ops) cseg))
                sc   (try-compute-subtree-count-from-children cseg l)]
            (recur (conj! out (install-measures! (->Branch lvl kseg cseg aseg sc m settings nil 0 (read-field this :_projCmp) nil false)
                                                 measure-ops (when cm-ins (arrays/slice cm-ins pos (+ pos l))) m))
                   (+ pos l) (next ls)))
          (arrays/into-array (persistent! out)))))))

(defn- mst-divide!
  "The pieces `mst-branch-add` cut (at most two for one insert) hold this node's elements
   plus the inserted one. Same settlement as the count-mode split."
  [^Branch this pieces storage opts]
  (if (== 2 (arrays/alength pieces))
    (let [old-sc (read-field this :subtree-count)]
      (divide-count! (arrays/aget pieces 0) (arrays/aget pieces 1)
                     (if (>= old-sc 0) (inc old-sc) -1) storage opts))
    (let [sync? (not (false? (:sync? opts)))]
      (async+sync sync? (async nil)))))

(defn add
  [^Branch this storage key cmp opts]
  (let [{:keys [sync?] :or {sync? true}} opts
        keys  (.-keys this)
        addrs (read-field this :addresses)
        diff-buf? (diff-buf? this)
        idx   (util/binary-search-l cmp keys (- (arrays/alength keys) 2) key)
        ;; diff-buf: capture child idx's durable address BEFORE the mutation nulls it,
        ;; so a deposit at this level can record it as the buffer anchor.
        anchor0 (when (and diff-buf? addrs) (aget addrs idx))]
    (async+sync sync?
                (async
                 (let [child-node (await (child this storage idx opts))
                       nodes      (await (node/add child-node storage key cmp opts))]
                   (when nodes
                     (let [branching-factor (:branching-factor (.-settings this))
                           bd               (b/content-boundary (.-settings this))
                           children         (ensure-children this)
                           new-keys         (util/check-n-splice cmp keys idx (inc idx) (arrays/amap node/max-key nodes))
                           new-children     (util/splice children idx (inc idx) nodes)
                           nodes-len        (arrays/alength nodes)
                           measure-ops0     (:measure (.-settings this))
                           ;; the record after this insert: ours, with the child that was
                           ;; written replaced by what came back up (one node, or two)
                           cm-ins           (when measure-ops0
                                              (splice-measures measure-ops0
                                                               (await (ensure-child-measures this storage measure-ops0 opts))
                                                               idx (inc idx) nodes nil))]
                       (if bd
                         ;; PROBE FIX: preserve unchanged siblings' durable addresses across the
                         ;; MST rebuild, mirroring the JVM's allAddresses Stitch (copyAll +
                         ;; copyOne(null) per new node). Also free the split child's old blob.
                         (let [mst-addrs (when addrs
                                           (util/splice addrs idx (inc idx)
                                                        (arrays/make-array nodes-len)))]
                           (when (and storage addrs (aget addrs idx))
                             (storage/markFreed storage (aget addrs idx)))
                           (let [pieces (mst-branch-add this bd new-keys new-children mst-addrs idx key cm-ins)]
                             (await (mst-divide! this pieces storage opts))
                             pieces))
                         (if (<= (arrays/alength new-children) branching-factor)
                           (let [new-addrs
                                 (when addrs
                                   (if (= nodes-len 1)
                                     (let [na (arrays/make-array (arrays/alength addrs))]
                                       (arrays/acopy addrs 0 (arrays/alength addrs) na 0)
                                     ;; Mark old child address as freed before clearing. diff-buf:
                                     ;; under diff-buf the old address may be re-pointed as a buffered
                                     ;; anchor at store, so freeing is DEFERRED to store.
                                       (when (and (not diff-buf?) storage (aget addrs idx))
                                         (storage/markFreed storage (aget addrs idx)))
                                       (aset na idx nil)
                                       na)
                                     (let [old-addr (aget addrs idx)]
                                       (when (and (not diff-buf?) storage old-addr)
                                         (storage/markFreed storage old-addr))
                                       (util/splice addrs idx (inc idx) (arrays/array nil nil)))))
                               ;; After adding one element, increment count if known
                                 old-sc (read-field this :subtree-count)
                                 new-sc (if (>= old-sc 0) (inc old-sc) -1)
                               ;; Update measure incrementally only if already computed
                                 measure-ops (:measure (.-settings this))
                                 new-measure (when (and measure-ops (some? (read-field this :_measure)))
                                               (measure/merge-measure measure-ops (read-field this :_measure) (measure/extract measure-ops key)))
                                 nb (install-measures!
                                     (->Branch (.-level this) new-keys new-children new-addrs new-sc new-measure (.-settings this) nil 0 (read-field this :_projCmp) nil (boolean (read-field this :_cmLazy)))
                                     ;; unknown, not the appended `new-measure`, when the record cannot be spliced
                                     measure-ops cm-ins nil)]
                           ;; diff-buf: nodes-len==1 ⇒ content-only ⇒ carry the source slots and
                           ;; deposit Present(key) (matches JVM persistent path). nodes-len>=2 ⇒ a child
                           ;; split was absorbed ⇒ structural: mark rebalanced + stitch surviving siblings.
                             (when diff-buf?
                               (if (= nodes-len 1)
                                 (do (write-field! nb :_bufEntries (buf-entries this)) ; carry running total (or -1) onto successor
                                     (await (carry-and-deposit nb storage (read-field this :_slots) idx key key anchor0 opts)))
                                 (do (write-field! nb :_bufEntries -1) ; absorbed a child split: structural → must write
                                     (free-dropped-child this storage idx) ; diff-buf: free the split child's old blob
                                     (write-field! nb :_slots (stitch-slots this idx nodes-len (arrays/alength new-children))))))
                             (arrays/array nb))
                           (let [middle      (arrays/half (arrays/alength new-children))
                                 tmp-addrs   (when addrs
                                               (let [old-addr (aget addrs idx)]
                                               ;; Mark old child address as freed before clearing (deferred under diff-buf)
                                                 (when (and (not diff-buf?) storage old-addr)
                                                   (storage/markFreed storage old-addr))
                                                 (util/splice addrs idx (inc idx) (arrays/array nil nil))))
                                 left-addrs  (when tmp-addrs (arrays/slice tmp-addrs 0 middle))
                                 right-addrs (when tmp-addrs (arrays/slice tmp-addrs middle))
                                 left-children (arrays/slice new-children 0 middle)
                                 right-children (arrays/slice new-children middle)
                                 measure-ops (:measure (.-settings this))
                               ;; Compute measure for split branches from their children only if already computed
                               ;; Return nil if any child measure is nil (don't silently undercount)
                                 left-measure (when (and measure-ops (some? (read-field this :_measure)))
                                                (reduce (fn [acc child]
                                                          (if (nil? acc)
                                                            (reduced nil)
                                                            (let [cs (when (some? child) (node/measure child))]
                                                              (if (some? cs)
                                                                (measure/merge-measure measure-ops acc cs)
                                                                (reduced nil)))))
                                                        (measure/identity-measure measure-ops)
                                                        left-children))
                                 right-measure (when (and measure-ops (some? (read-field this :_measure)))
                                                 (reduce (fn [acc child]
                                                           (if (nil? acc)
                                                             (reduced nil)
                                                             (let [cs (when (some? child) (node/measure child))]
                                                               (if (some? cs)
                                                                 (measure/merge-measure measure-ops acc cs)
                                                                 (reduced nil)))))
                                                         (measure/identity-measure measure-ops)
                                                         right-children))
                                 new-len (arrays/alength new-children)
                                 left-b  (->Branch (.-level this)
                                                   (arrays/slice new-keys 0 middle)
                                                   left-children
                                                   left-addrs
                                                   (try-compute-subtree-count-from-children left-children (arrays/alength left-children))
                                                   left-measure
                                                   (.-settings this) nil 0 (read-field this :_projCmp) nil false)
                                 right-b (->Branch (.-level this)
                                                   (arrays/slice new-keys middle)
                                                   right-children
                                                   right-addrs
                                                   (try-compute-subtree-count-from-children right-children (arrays/alength right-children))
                                                   right-measure
                                                   (.-settings this) nil 0 (read-field this :_projCmp) nil false)]
                             (install-measures! left-b measure-ops (when cm-ins (arrays/slice cm-ins 0 middle)) left-measure)
                             (install-measures! right-b measure-ops (when cm-ins (arrays/slice cm-ins middle)) right-measure)
                           ;; diff-buf: a child split overflowed this branch → split: structural on both
                           ;; halves → written, but each still buffers its surviving siblings' slots.
                             (when diff-buf?
                               (write-field! left-b :_bufEntries -1) ; split: structural on both halves → must write
                               (write-field! right-b :_bufEntries -1)
                               (free-dropped-child this storage idx) ; diff-buf: free the split child's old blob
                               (when-let [all (stitch-slots this idx nodes-len new-len)]
                                 (write-field! left-b :_slots  (arrays/slice all 0 middle))
                                 (write-field! right-b :_slots (arrays/slice all middle))))
                             ;; After the slots are in place: under diff-buf a slot is where
                             ;; a non-resident child's count lives. One new element was added
                             ;; below this node, so the halves share its old total + 1.
                             (let [old-sc (read-field this :subtree-count)]
                               (await (divide-count! left-b right-b (if (>= old-sc 0) (inc old-sc) -1) storage opts)))
                             (arrays/array left-b right-b)))))))))))

(defn $remove
  [^Branch this storage key left right cmp {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [keys (.-keys this)
                     idx  (let [arr-l (arrays/alength keys)
                                i     (util/binary-search-l cmp keys (dec arr-l) key)]
                            (if (== i arr-l) -1 i))
                     diff-buf? (diff-buf? this)
                     ;; diff-buf: capture child idx's durable address before the mutation nulls it.
                     anchor0 (when (and diff-buf? (read-field this :addresses) (not= -1 idx)) (aget (read-field this :addresses) idx))]
                 (when-not (== -1 idx)
                   (let [children    (ensure-children this)
                         addrs       (read-field this :addresses)
                         left-child  (when (> idx 0)
                                       (await (child this storage (dec idx) opts)))
                         right-child (when (< idx (dec (arrays/alength keys)))
                                       (await (child this storage (inc idx) opts)))
                         child       (await (child this storage idx opts))
                         ;; Ask the child to REPORT the element it removed. This branch
                         ;; subtracts that element's contribution from its cached measure
                         ;; below, and the caller's `key` is not it whenever `cmp` is
                         ;; coarser than the set's comparator — the same search-key-is-not-
                         ;; the-stored-element defect as the leaf's, one level up. The JVM
                         ;; branch does not have it because it RECOMPUTES from children
                         ;; (`tryComputeMeasure`) instead of subtracting.
                         ;; Needed by BOTH the measure subtraction below and the diff-buf
                         ;; Absent deposit — the deposit records the element the leaf really
                         ;; held, not the caller's search key, which differ under a coarse
                         ;; operation comparator. Allocating this only when a measure was
                         ;; configured left the deposit falling back to `key`, which is the
                         ;; JVM defect reproduced in ANode's six-arg `remove`.
                         ;; NB `diff-buf?` is a LOCAL boolean here (bound above), not the
                         ;; predicate fn of the same name — calling it threw TypeError.
                         removed-out (when (or (:measure (.-settings this))
                                               (and diff-buf? (== 1 (.-level this))))
                                       (arrays/make-array 1))
                         disjoined   (await (node/$remove child storage key left-child right-child cmp
                                                          (if removed-out
                                                            (assoc opts :removed-out removed-out)
                                                            opts)))
                         ;; `if-some`, not `or`: `false` is a legal ELEMENT, and `or` fell
                         ;; through to the caller's comparator-equivalent probe whenever the
                         ;; stored element was `false`. The in-memory result was right and the
                         ;; DIFF recorded the wrong key, so a cold restore applied an absence
                         ;; for something that was never there and `false` came back from the
                         ;; dead. Measured at bf 4 / budget 128: removing `false` through an
                         ;; equivalent probe gave count 5 in memory and count 6 after restore;
                         ;; a shape sweep over bf [4 6 8 10 16] failed 1489 of 1721 cells.
                         ;; Correct at budget 0 (no diff is recorded) and on the JVM, which
                         ;; tests the array slot for null. Same class as the `false`-measure
                         ;; defect fixed in 071038c — that pass converted the measure sites
                         ;; and missed these two element sites.
                         removed-element (if-some [r (some-> removed-out (arrays/aget 0))]
                                           r key)]
                     (when disjoined
                       (let [left-idx  (if left-child  (dec idx) idx)
                             right-idx (if right-child (+ idx 2) (inc idx))
                             alen      (arrays/alength disjoined)
                             ;; diff-buf: a surviving sibling is "unchanged" when the child's remove
                             ;; returned it identical (no borrow/merge touched it).
                             left-unchanged  (and left-child (> alen 1)
                                                  (identical? (arrays/aget disjoined 0) left-child))
                             right-unchanged (and right-child (> alen 1)
                                                  (identical? (arrays/aget disjoined (dec alen)) right-child))
                             ;; content-only ⇔ this node's child count unchanged (no merge: alen == range)
                             ;; AND no sibling borrowed (both unchanged) ⇒ only child idx's content changed.
                             content-only? (and (== alen (- right-idx left-idx))
                                                (or (nil? left-child)  left-unchanged)
                                                (or (nil? right-child) right-unchanged))
                             new-keys  (util/check-n-splice cmp keys left-idx right-idx
                                                            (arrays/amap node/max-key disjoined))
                             new-kids  (util/splice children left-idx right-idx disjoined)
                             new-addrs (when addrs
                                         (let [repl  (arrays/make-array alen)
                                               laddr (when left-child  (arrays/aget addrs left-idx))
                                               raddr (when right-child (arrays/aget addrs (dec right-idx)))]
                                           ;; Mark freed addresses before clearing. diff-buf: under diff-buf
                                           ;; the old addresses may be re-pointed as buffered anchors at store,
                                           ;; so freeing is DEFERRED to store.
                                           (when (and (not diff-buf?) storage)
                                             (dotimes [i (- right-idx left-idx)]
                                               (let [addr-idx (+ left-idx i)
                                                     old-addr (arrays/aget addrs addr-idx)]
                                                 (when (and old-addr
                                                            (not (and (= addr-idx left-idx) left-unchanged))
                                                            (not (and (= addr-idx (dec right-idx)) right-unchanged)))
                                                   (storage/markFreed storage old-addr)))))
                                           (when left-unchanged
                                             (aset repl 0 laddr))
                                           (when right-unchanged
                                             (aset repl (dec alen) raddr))
                                           (util/splice addrs left-idx right-idx repl)))
                             ;; After removing one element, decrement count if known
                             old-sc (read-field this :subtree-count)
                             new-sc (if (>= old-sc 0) (dec old-sc) -1)
                             ;; Update measure only if already computed
                             measure-ops (:measure (.-settings this))
                             ;; RECOMPUTE from the (already rebuilt) children — never subtract.
                             ;; This mirrors the JVM's `Branch.remove`, which assigns
                             ;; `tryComputeMeasure(storage)` and offers no branch-level
                             ;; subtraction at all.
                             ;;
                             ;; Subtracting was wrong twice over. It presumed INVERTIBILITY: a
                             ;; measure is a monoid, not a group, so min/max — which stratum
                             ;; uses — cannot be un-merged, and `remove-measure`'s recompute-fn
                             ;; exists precisely because the implementation must decide. And it
                             ;; is arithmetic on a total that a structural rebalance may already
                             ;; have changed by more than the one removed element.
                             ;;
                             ;; This was tried once before and made every cljs measure nil. That
                             ;; was a SYMPTOM of the leaf bug fixed alongside it: `Leaf/merge`
                             ;; and `merge-split` built successors with a nil measure, and a
                             ;; branch above a nil-measure child can only postpone. With the
                             ;; leaves carrying their measures again, folding them is both
                             ;; possible and exact.
                             new-measure (when (and measure-ops (some? (read-field this :_measure)))
                                           (node/try-compute-measure
                                            (->Branch (.-level this) new-keys new-kids new-addrs new-sc nil (.-settings this) nil 0 (read-field this :_projCmp) nil false)
                                            storage measure-ops {:sync? true}))
                             cm0    (when measure-ops (await (ensure-child-measures this storage measure-ops opts)))
                             ;; an untouched sibling keeps the entry it had
                             keep   (when cm0
                                      (let [k (arrays/make-array alen)]
                                        (when left-unchanged (aset k 0 (arrays/aget cm0 left-idx)))
                                        (when right-unchanged (aset k (dec alen) (arrays/aget cm0 (dec right-idx))))
                                        k))
                             cm-c   (splice-measures measure-ops cm0 left-idx right-idx disjoined keep)
                             center (install-measures!
                                     (->Branch (.-level this) new-keys new-kids new-addrs new-sc new-measure (.-settings this) nil 0 (read-field this :_projCmp) nil (boolean (read-field this :_cmLazy)))
                                     measure-ops cm-c new-measure)
                             ;; A join or a borrow combines the records of both nodes, and
                             ;; `merge` / `merge-split` have no storage to fill a missing one.
                             _      (when (and (some? cm-c)
                                               (< (arrays/alength new-keys)
                                                  (quot (:branching-factor (.-settings this) 32) 2)))
                                      (when left (await (ensure-child-measures left storage measure-ops opts)))
                                      (when right (await (ensure-child-measures right storage measure-ops opts))))]
                         ;; diff-buf: install the center's slots BEFORE rotate (so a subsequent
                         ;; rotate merge/merge-split with this node's siblings carries them).
                         ;; content-only ⇒ carry source slots + deposit Absent(key) at idx (1-for-1).
                         ;; structural ⇒ _bufEntries=-1 (must write) + stitch surviving siblings (rebuilt range nulled).
                         (when diff-buf?
                           (if content-only?
                             (do (write-field! center :_bufEntries (buf-entries this)) ; carry running total (or -1) onto successor
                                 (await (carry-and-deposit center storage (read-field this :_slots) idx removed-element ABSENT anchor0 opts)))
                             (do (write-field! center :_bufEntries -1) ; child merged/borrowed: structural → must write
                                 ;; diff-buf: free this node's dropped (merged/borrowed) children — the
                                 ;; range [left-idx, right-idx) minus unchanged surviving siblings (the
                                 ;; diff-buf twin of the baseline markFreed loop above). Never re-pointed
                                 ;; as anchors ⇒ free now (store has no slot/anchor for the structural child).
                                 (when storage
                                   (dotimes [i (- right-idx left-idx)]
                                     (let [ci (+ left-idx i)]
                                       (when-not (or (and (= ci left-idx) left-unchanged)
                                                     (and (= ci (dec right-idx)) right-unchanged))
                                         (free-dropped-child this storage ci)))))
                                 (write-field! center :_slots
                                       (remove-stitch-slots this left-idx right-idx alen left-child right-child
                                                            left-unchanged right-unchanged (arrays/alength new-kids))))))
                         (let [res (util/rotate center
                                                (and (nil? left) (nil? right))
                                                left
                                                right
                                                (.-settings this))]
                           ;; A BORROW redistributes two nodes' elements between two new
                           ;; ones, and `merge-split` (no storage, synchronous) counts them
                           ;; from resident children only. The pair's total is known whenever
                           ;; both sources knew theirs; settle the halves here, where storage
                           ;; is. A join has a nil in the triple and sums; nothing to do.
                           (let [n     (arrays/alength res)
                                 has?  (fn [x] (loop [i 0] (cond (>= i n) false
                                                                 (identical? x (arrays/aget res i)) true
                                                                 :else (recur (inc i)))))
                                 fresh (loop [i 0, acc []]
                                         (if (>= i n)
                                           acc
                                           (let [x (arrays/aget res i)]
                                             (recur (inc i)
                                                    (if (or (identical? x left) (identical? x right) (identical? x center))
                                                      acc
                                                      (conj acc x))))))]
                             ;; `rotate` drops nils, so the arms are told apart by identity:
                             ;; two nodes that are none of left/center/right are a borrow's
                             ;; two halves, and the sibling that is gone is the one they
                             ;; were made from.
                             (when (== 2 (count fresh))
                               (let [sibling (cond (and (some? left) (not (has? left))) left
                                                   (and (some? right) (not (has? right))) right)]
                                 (when sibling
                                   (await (divide-count! (nth fresh 0) (nth fresh 1)
                                                         (if (and (>= (read-field sibling :subtree-count) 0) (>= new-sc 0))
                                                           (+ (read-field sibling :subtree-count) new-sc) -1)
                                                         storage opts))))))
                           res)))))))))

(defn- replace-measure
  "Try to recompute measure after replace (postpone if children unavailable)."
  [branch storage measure-ops]
  (when measure-ops
    (node/try-compute-measure branch storage measure-ops {:sync? true})))

(defn- no-equal-sibling-across-boundary?
  "-ea only (see `$replace`): does no ADJACENT leaf hold an element `cmp` calls
   equal to `old-key`?

   `leaf.cljs` checks the neighbours inside the leaf; those are invisible to it.
   The two cmp-equal elements are not reliably in one leaf — measured at bf 4, a
   set of [k 0] for k in 0..39 plus [5 7] splits as ... [[4 0] [5 0]] |
   [[5 7] [6 0] [7 0]] ..., either side of a boundary — so a leaf-local check
   alone silently passes the very case the precondition exists for.

   Best-effort by design: it reads only children that are already RESIDENT, so it
   stays synchronous inside an assert and never provokes a restore. A cold
   sibling is simply not checked."
  [^Branch this child idx old-key cmp]
  (let [children (read-field this :children)
        n        (arrays/alength (.-keys this))
        resident (fn [i] (when (and children (<= 0 i) (< i n)) (aget children i)))
        ks       (.-keys child)
        j        (util/binary-search cmp ks old-key)]
    (if (neg? j)
      true
      (let [left  (when (== j 0) (resident (dec idx)))
            right (when (== j (dec (arrays/alength ks))) (resident (inc idx)))
            eq?   (fn [node pick]
                    (when node
                      (let [nks (.-keys node)
                            len (arrays/alength nks)]
                        (when (pos? len)
                          (== 0 (cmp (arrays/aget nks (pick len)) old-key))))))]
        (not (or (eq? left (fn [len] (dec len)))
                 (eq? right (fn [_] 0))))))))

(defn $replace
  [^Branch this storage old-key new-key cmp {:keys [sync?] :or {sync? true} :as opts}]
  (assert (== 0 (cmp old-key new-key)) "old-key and new-key must compare as equal (cmp must return 0)")
  (async+sync sync?
              (async
               (let [keys (.-keys this)
                     settings (.-settings this)
                     measure-ops (:measure settings)
                     idx  (let [arr-l (arrays/alength keys)
                                i     (util/binary-search-l cmp keys (dec arr-l) old-key)]
                            (if (== i arr-l) -1 i))
                     diff-buf? (diff-buf? this)
                     ;; diff-buf: capture child idx's durable address before the mutation nulls it.
                     anchor0 (when (and diff-buf? (read-field this :addresses) (not= -1 idx)) (aget (read-field this :addresses) idx))]
                 (when-not (== -1 idx)
                   (let [child  (await (child this storage idx opts))
                         ;; diff-buf: the element this replace will actually REMOVE. NOT the
                         ;; same as `old-key`, which is the caller's SEARCH key under a possibly
                         ;; COARSER operation comparator. The leaf-diff is keyed by the SET's
                         ;; comparator, so Absent(old-key) only cancels the leaf's element while
                         ;; the leaf still holds the element the caller searched for — after one
                         ;; buffered replace it does not, and the new Present is added ALONGSIDE
                         ;; the old. Mirrors the JVM fix in Branch.replace; see
                         ;; test/diff_buf_restore_cycle.clj for the measured shape.
                         ;;
                         ;; The leaf REPORTS it (via `:removed-out`) rather than being searched
                         ;; again here. Both for cost — the comparator-bound binary search runs
                         ;; once, not twice, worth +18% at bf 512 on the JVM — and because the
                         ;; two searches must agree: `binarySearch` is arbitrary among elements
                         ;; equal under the operation comparator, so a second search could name
                         ;; a different element than the one the leaf overwrote, recording
                         ;; Absent for one while replacing another.
                         ;;
                         ;; Only at level 1 — above it the slot is a branch anchor whose diff is
                         ;; null, so the key is unused.
                         removed-out (when (and diff-buf? (== 1 (.-level this)))
                                       (arrays/make-array 1))
                         ;; PRECONDITION (assertion only; release builds elide it): the
                         ;; cross-leaf half of replace's no-equal-sibling rule — `leaf.cljs`
                         ;; checks the in-leaf half. Best-effort: only inspects siblings already
                         ;; RESIDENT, so it stays synchronous and never triggers a restore.
                         ;; Mirrors Branch.noEqualSiblingAcrossBoundary on the JVM.
                         _ (assert (or (not= 1 (.-level this))
                                       (no-equal-sibling-across-boundary? this child idx old-key cmp))
                                   (str "replace(" (pr-str old-key) " -> " (pr-str new-key) "): the"
                                        " ADJACENT leaf holds another element the operation comparator"
                                        " calls equal, so which element is replaced is arbitrary and the"
                                        " result may be UNSORTED. `replace` requires at most one"
                                        " cmp-equal element; use disj+conj instead."))
                         nodes  (await (node/$replace child storage old-key new-key cmp
                                                      (if removed-out
                                                        (assoc opts :removed-out removed-out)
                                                        opts)))
                         ;; `if-some`, not `or` — see the remove path above. Replacing a
                         ;; stored `false` recorded the probe instead, so a cold restore kept
                         ;; BOTH: count 16 in memory, 17 after restore, with `false` and the
                         ;; replacement both present.
                         removed-key (if-some [r (some-> removed-out (arrays/aget 0))]
                                       r old-key)]
                     (cond
                       ;; Not found in child
                       (nil? nodes)
                       nil

                       ;; Early exit from child (transient, no maxKey change). diff-buf: still
                       ;; deposit Present(new-key) at this level (mirrors JVM EARLY_EXIT path).
                       (= nodes :early-exit)
                       (do
                         (when diff-buf? (await (deposit-replace this storage idx removed-key new-key anchor0 opts)))
                         :early-exit)

                       ;; Child returned updated node
                       :else
                       (let [new-node      (arrays/aget nodes 0)
                             new-max-key   (node/max-key new-node)
                             children      (ensure-children this)
                             addrs         (read-field this :addresses)
                             ;; split-seam (MST): the separator keys[idx] must equal the child's max for
                             ;; canonical content-addressing, so ANY value change (even at the same
                             ;; comparator position, and for a non-rightmost child) must rebuild keys[idx]
                             ;; and propagate up the spine. Count mode is routing-only (by cmp), so its
                             ;; original last-child/cmp test is preserved byte-for-byte. Mirrors JVM
                             ;; Branch.replace, which always writes _keys[idx] = newMaxKey.
                             ;; VALUE equality, and for EVERY child — not `cmp`, and not only
                             ;; the last one.
                             ;;
                             ;; Asking the OPERATION comparator whether the max moved is wrong
                             ;; whenever that comparator is coarser than the one routing uses:
                             ;; datahike's value-changing upsert searches [e a _ _], so `cmp`
                             ;; returns 0 for a datom whose v changed, this arm took the
                             ;; "reuse keys array" branch, and `keys[idx]` kept naming the OLD
                             ;; element while the child held the new one. A later descent
                             ;; comparing the new element against that stale separator routes
                             ;; past the child that holds it.
                             ;;
                             ;; The JVM writes `_keys[idx] = newMaxKey` UNCONDITIONALLY and its
                             ;; persistent path always returns the successor, so only its
                             ;; transient path was affected. ClojureScript gated the keys
                             ;; rebuild on this flag too, which is why its default (persistent)
                             ;; path was. Testing the value for every child makes the rebuild
                             ;; and the propagation match the JVM persistent path exactly.
                             ;;
                             ;; `not=` (Clojure `=`, i.e. -equiv) rather than identity: a Datom
                             ;; implements equiv but not reference equality, so this propagates
                             ;; exactly when the element really changed.
                             ;; Mirrors the JVM's `separatorMoved` (Branch.java:1596). VALUE
                             ;; equality alone is not enough: it answers "is this the same
                             ;; element?", and the question here is "does the separator still
                             ;; sit where the SET's comparator puts it?". Those come apart for
                             ;; any type whose `=` is COARSER than the set comparator —
                             ;; datahike's Datom, whose `equiv-datom` compares e/a/v while
                             ;; `cmp-datoms-eavt` orders by e/a/v/tx. `not=` then says
                             ;; "unchanged", the separator is left naming the OLD element, and
                             ;; a later descent comparing the new element against that stale
                             ;; separator routes past the child that holds it.
                             ;;
                             ;; Measured with an element type whose `=` ignores a field the set
                             ;; orders by, value-changing upsert over the whole set, elements
                             ;; that `seq` still lists but `lookup` cannot find:
                             ;;
                             ;;     bf  4 n   40   JVM 0   cljs 19
                             ;;     bf  8 n  400   JVM 0   cljs 99
                             ;;     bf 16 n 3000   JVM 0   cljs 374
                             ;;
                             ;; A control with plain vectors (exact `=`) is 0 on both, so the
                             ;; probe is sound. This was fixed on the JVM and not ported here.
                             max-key-changed
                             (let [old-sep (arrays/aget keys idx)
                                   pcmp    (read-field this :_projCmp)]
                               (or (not= new-max-key old-sep)
                                   (nil? pcmp)
                                   (not (zero? (pcmp new-max-key old-sep)))))]
                         (if max-key-changed
                           ;; maxKey changed - update keys array
                           ;; Clone arrays. There is no in-place arm: see
                           ;; .internal/transient-support-cljs.md — cljs has no node
                           ;; ownership, so mutating `this` would edit a node other
                           ;; versions share.
                           (let [new-keys     (arrays/aclone keys)
                                 new-children (arrays/aclone children)
                                 new-addrs    (when addrs
                                                (let [na (arrays/aclone addrs)]
                                                    ;; Mark old child address as freed before clearing (deferred under diff-buf)
                                                  (when (and (not diff-buf?) storage (aget addrs idx))
                                                    (storage/markFreed storage (aget addrs idx)))
                                                  (aset na idx nil)
                                                  na))
                                 _            (aset new-keys idx new-max-key)
                                 _            (aset new-children idx new-node)
                                 new-branch   (->Branch (.-level this) new-keys new-children new-addrs (read-field this :subtree-count) nil (.-settings this) nil 0 (read-field this :_projCmp) nil (boolean (read-field this :_cmLazy)))
                                 new-measure    (when (and measure-ops (some? (read-field this :_measure)))
                                                  (replace-measure new-branch storage measure-ops))
                                 cm-r           (when measure-ops
                                                  (splice-measures measure-ops
                                                                   (await (ensure-child-measures this storage measure-ops opts))
                                                                   idx (inc idx) nodes nil))]
                             (install-measures! new-branch measure-ops cm-r new-measure)
                             ;; read AFTER the fill above, which is what sets it on `this`
                             (write-field! new-branch :_cmLazy (boolean (read-field this :_cmLazy)))
                               ;; diff-buf: content-only replace ⇒ carry source slots + deposit Present(new-key).
                             (when diff-buf?
                               (write-field! new-branch :_bufEntries (buf-entries this)) ; carry running total (or -1) onto successor
                               (await (carry-and-deposit-replace new-branch storage (read-field this :_slots) idx removed-key new-key anchor0 opts)))
                             (arrays/array new-branch))
                           ;; maxKey unchanged - reuse keys array
                           ;; Clone ALL arrays — sharing any of them would let a future
                           ;; in-place path corrupt the original.
                           (let [new-keys     (arrays/aclone keys)
                                 new-children (arrays/aclone children)
                                 new-addrs    (when addrs
                                                (let [na (arrays/aclone addrs)]
                                                    ;; Mark old child address as freed before clearing (deferred under diff-buf)
                                                  (when (and (not diff-buf?) storage (aget addrs idx))
                                                    (storage/markFreed storage (aget addrs idx)))
                                                  (aset na idx nil)
                                                  na))
                                 _            (aset new-children idx new-node)
                                 new-branch   (->Branch (.-level this) new-keys new-children new-addrs (read-field this :subtree-count) nil (.-settings this) nil 0 (read-field this :_projCmp) nil (boolean (read-field this :_cmLazy)))
                                 new-measure    (when (and measure-ops (some? (read-field this :_measure)))
                                                  (replace-measure new-branch storage measure-ops))
                                 cm-r           (when measure-ops
                                                  (splice-measures measure-ops
                                                                   (await (ensure-child-measures this storage measure-ops opts))
                                                                   idx (inc idx) nodes nil))]
                             (install-measures! new-branch measure-ops cm-r new-measure)
                             ;; read AFTER the fill above, which is what sets it on `this`
                             (write-field! new-branch :_cmLazy (boolean (read-field this :_cmLazy)))
                               ;; diff-buf: content-only replace ⇒ carry source slots + deposit Present(new-key).
                             (when diff-buf?
                               (write-field! new-branch :_bufEntries (buf-entries this)) ; carry running total (or -1) onto successor
                               (await (carry-and-deposit-replace new-branch storage (read-field this :_slots) idx removed-key new-key anchor0 opts)))
                             (arrays/array new-branch)))))))))))

;; ---- diff-buf store-side helpers (mirror JVM Branch) ----

(defn- diff-size
  "Entry count (buffered element-changes) in a slot's diff blob. CONTRACT: `child-level` is the
  level of the NODE whose diff this is — for a slot in a node at level L describing its child
  (level L-1), call (diff-size slot-diff (dec L)). child-level 0 ⇒ a leaf-diff (live sorted-map or
  storage form {:absent :present}) ⇒ element count; child-level >= 1 ⇒ a branch-diff {idx ->
  {:diff …}} ⇒ sum over children at child-level-1, recursing to the leaf-diffs. Counts only leaf
  entries (interior levels add nothing ⇒ linear in buffered ops, not exponential in depth).
  Discriminated by child-level, not by probing values (a leaf-diff's values are the set's
  ELEMENTS, which may themselves be maps — e.g. Datoms). Mirrors JVM diffSize."
  [diff child-level]
  (if-not (map? diff)
    0
    (if (== child-level 0)
      (if (sorted? diff)                                         ; live form: one entry per element
        (count diff)
        (+ (count (:absent diff)) (count (:present diff))))      ; storage form {:absent :present}
      (let [n (count diff)]
        (if (zero? n)
          0
          (reduce (fn [t v] (+ t (diff-size (:diff v) (dec child-level)))) 0 (vals diff)))))))

;; (The recursive content-only-diff-size walk that classified a dirty branch child was replaced
;; by the O(1) _bufEntries aggregate: a child's subtree size is (buf-entries child) and its
;; must-write status is the -1 poison that already climbed the deposit sum — see store/deposit-kv.)

(defn- refresh-marker-slots!
  "Mirrors JVM `Branch.refreshMarkerSlots`. A branch-MARKER slot (`:diff` nil, `:anchor`
  non-nil) caches the child's whole-subtree buffered total as of DEPOSIT time, and carries no
  diff of its own — the diff is derived from the LIVE child at store time by `assemble-nested`.

  `store` settles that child IN PLACE (`set! (read-field child :_bufEntries) embedded`, and its slots are
  rewritten), and nodes are SHARED between versions, so every other version's parent slot keeps
  both the pre-settle total AND the pre-settle `:anchor`. Nothing is overwritten — blobs are
  immutable and a re-store gets a fresh address — but the slot's two halves now straddle the
  settle: the derived diff moved forward while the anchor did not, and the flush already
  `markFreed` that anchor.

  Correcting the NUMBER is the wrong fix and was measured wrong on the JVM: 25/35/37 content
  mismatches per config at B in {1,4}. The inflated stale value is accidentally protective —
  it forces `embedded + csz > budget`, so the child is FLUSHED; correcting it lets the child be
  BUFFERED against the stale anchor and the restore loses everything the child flushed.

  So detect the settle and poison the slot to -1 (must-write): Pass 1 then writes the child
  wholesale and it gets a fresh, coherent anchor. On the JVM this changed write counts by zero
  — the flush was already happening, for the wrong reason.

  Only detects staleness for a RESIDENT child; a child settled and then evicted is invisible
  here. Instrumented on the JVM across the diff-buf namespaces: 0 non-resident markers out of
  4382 examined, so that hole is real but unobserved."
  [^Branch node]
  (let [slots (read-field node :_slots)]
    (when (some? slots)
      (let [children (read-field node :children)
            len      (arrays/alength (.-keys node))]
        (when (some? children)
          (loop [i 0, poisoned? false]
            (if (>= i len)
              (when poisoned? (write-field! node :_bufEntries -1))
              (let [sl (aget slots i)]
                (if (or (nil? sl) (some? (:diff sl)) (nil? (:anchor sl))
                        (== -1 (:buf-entries sl)))
                  (recur (inc i) poisoned?)                    ; not a live marker slot
                  (let [c (aget children i)]
                    (if-not (instance? Branch c)
                      (recur (inc i) poisoned?)                ; absent or a leaf
                      (do
                        (refresh-marker-slots! c)              ; post-order
                        (if (== (buf-entries c) (slot-be sl (dec (.-level node))))
                          (recur (inc i) poisoned?)
                          (do (aset slots i (assoc sl :buf-entries -1))
                              (recur (inc i) true)))))))))))))))

(defn- assemble-nested
  "Assemble c's serializable nested diff {idx -> {:count :measure :diff :max-key}}, recursing
  markers into the (resident) live subtree. Mirrors JVM assembleNested."
  [storage ^Branch c {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [slots (read-field c :_slots)]
                 (if (nil? slots)
                   {}
                   (let [len (arrays/alength (.-keys c))]
                     (loop [j 0, m {}]
                       (if (>= j len)
                         m
                         (let [sl (aget slots j)]
                           (if (or (nil? sl) (nil? (:anchor sl)))
                             ;; Skip an ANCHORLESS slot, mirroring JVM assembleNested. A null
                             ;; anchor means the child has no durable base to diff against, so
                             ;; store() writes it WHOLESALE and `deposit-kv` leaves its diff nil
                             ;; for that reason — there is no buffered difference to assemble,
                             ;; at any level.
                             ;;
                             ;; Without the skip a nil diff was read as "branch marker" and this
                             ;; recursed: it awaited a restore the JVM never performs, and for a
                             ;; LEAF child `(read-field c :_slots)` is undefined so the recursion returned
                             ;; {} — emitting an entry anchored at `(aget base-addr j)`, an
                             ;; address belonging to a DIFFERENT, older child. The JVM hit the
                             ;; same shape as a ClassCastException out of a plain store
                             ;; (`c.level=1 j=22 childClass=Leaf slotDiffNull=true
                             ;; slotAnchor=false`), which is what put the guard there.
                             (recur (inc j) m)
                             (let [gc     (when (nil? (:diff sl)) (await (child c storage j opts)))
                                   d      (if (some? (:diff sl))
                                            (if (== (.-level c) 1) (leaf-diff->storage (:diff sl)) (:diff sl)) ; leaf child ⇒ storage form
                                            (await (assemble-nested storage gc opts)))
                                   entry  {:count   (:count sl)
                                           :measure (:measure sl)
                                           :diff    d
                                           :max-key (arrays/aget (.-keys c) j)}]
                               (recur (inc j) (assoc m j entry)))))))))))))

(defn slots-for-storage
  "Serializable slots for THIS node (diffs already assembled during store); nil if none.
  Storage backends call this to persist the per-child buffered diffs alongside addresses.
  Mirrors JVM slotsForStorage."
  [^Branch node]
  (when-some [slots (read-field node :_slots)]
    (let [len   (arrays/alength (.-keys node))
          leaf? (== (.-level node) 1)]   ; this node's children are leaves ⇒ leaf-diffs
      (loop [i 0, m {}]
        (if (>= i len)
          (when (pos? (count m)) m)
          (let [sl (aget slots i)]
            (if (nil? sl)
              (recur (inc i) m)
              (recur (inc i) (assoc m i {:count   (:count sl)
                                         :measure (:measure sl)
                                         ;; leaf child ⇒ comparator-agnostic storage form; branch child ⇒ nested map as-is
                                         :diff    (if leaf? (leaf-diff->storage (:diff sl)) (:diff sl))
                                         :max-key (arrays/aget (.-keys node) i)})))))))))

(defn store
  [^Branch this storage {:keys [sync?] :or {sync? true} :as opts}]
  (ensure-addresses this)
  (async+sync sync?
              (async
               (if-not (diff-buf? this)
                 ;; baseline ⇒ byte-identical (I0): store every dirty child, then this node.
                 (let [keys-l (arrays/alength (.-keys this))]
                   (loop [i 0]
                     (when (< i keys-l)
                       (when (nil? (aget (read-field this :addresses) i))
                         (assert (some? (read-field this :children)))
                         (assert (some? (aget (read-field this :children) i)))
                         (assert (satisfies? node/INode (aget (read-field this :children) i)))
                         (let [child-address (await (node/store (aget (read-field this :children) i) storage opts))]
                           (address this i child-address)))
                       (recur (inc i))))
                   (do (write-field! this :_cmLazy false)   ; written with its record
                       (await (storage/store storage this opts))))
                 ;; diff-buf: buffer content-only dirty children (re-point to the child's durable
                 ;; anchor) up to budget B; flush the rest BIGGEST-FIRST — the largest diffs are
                 ;; written and the small ones kept buffered, so a slot that regularly fills a big
                 ;; share of the budget is written proportionally often and can't jam the buffer.
                 ;; Only dirty (resident) children are flushed (no read); clean passthrough consumes
                 ;; budget but is left untouched. Mirrors JVM Branch.store (see doc/diff-buffering.md).
                 (let [_      (refresh-marker-slots! this)   ; see the fn: detect a settled child
                       addrs  (read-field this :addresses)
                       slots  (read-field this :_slots)
                       budget (or (:diff-buf-size (.-settings this)) 0)
                       len    (arrays/alength (.-keys this))
                       level  (.-level this)
                       ;; Pass 1: account clean passthrough; classify dirty children. Both are O(1)
                       ;; per child now: size = the slot's cached :buf-entries (slot-be resolves a
                       ;; restored slot from its diff), must-write = the -1 poison the deposit sum
                       ;; already lifted from the rebalance point (no subtree walk).
                       classified
                       (loop [i 0, pass 0, buf [], wl []]
                         (if-not (< i len)
                           {:pass pass :buf buf :wl wl}
                           (let [sl (when slots (aget slots i))]
                             (if (some? (aget addrs i))
                               (recur (inc i) (if sl (+ pass (slot-be sl (dec level))) pass) buf wl)  ; clean passthrough subtree total
                               (let [child (aget (read-field this :children) i)]
                                 (cond
                                   (or (nil? sl) (nil? (:anchor sl)))         ; no anchor ⇒ must write
                                   (recur (inc i) pass buf (conj wl i))
                                   (== (:buf-entries sl) -1)                   ; subtree rebalanced (poison) ⇒ must write
                                   (recur (inc i) pass buf (conj wl i))
                                   :else                                       ; content-only ⇒ bufferable, size O(1)
                                   ;; RESOLVE with slot-be, don't read `:buf-entries` raw — the
                                   ;; passthrough arm above already calls it for exactly this
                                   ;; quantity. A slot reconstructed from storage carries no
                                   ;; `:buf-entries` (the cljs analogue of JVM Slot.LAZY), and the
                                   ;; gate above only rejects the -1 poison, so a restored slot on a
                                   ;; dirty child fell through to here and was sized as nil — which
                                   ;; `+` coerces to 0, so the budget test always passes, the running
                                   ;; total never advances, and nil is then written back into the
                                   ;; slot as its settled size. The JVM closed this in 31e41a6; the
                                   ;; two arms must not disagree about how to read the same field.
                                   (let [nested (if (some? (:diff sl)) (:diff sl) (await (assemble-nested storage child opts)))]
                                     (recur (inc i) pass (conj buf {:i i :sz (slot-be sl (dec level)) :nested nested}) wl))))))))
                       ;; Pass 2: buffer SMALLEST-first while running total ≤ budget; flush the rest.
                       [embedded flushed]
                       (reduce (fn [[emb wl] {:keys [i sz nested]}]
                                 (if (<= (+ emb sz) budget)
                                   (let [sl (aget slots i)]
                                     (aset addrs i (:anchor sl))
                                     (aset slots i {:diff nested :count (:count sl) :measure (:measure sl) :anchor (:anchor sl) :buf-entries sz})
                                     [(+ emb sz) wl])
                                   [emb (conj wl i)]))
                               [(:pass classified) (:wl classified)]
                               (sort-by :sz (:buf classified)))
                       ;; D3: the merge/borrow arms (`concat-slots`, `merge`, `merge-split`)
                       ;; concatenate two nodes' slot arrays without re-checking the budget, so the
                       ;; passthrough total alone can start above B — and Pass 2, which only ever
                       ;; flushes DIRTY children, can never bring it back down. Measured over ~157k
                       ;; written blobs before this: worst case ≈ 2B (B=1→2, 2→4, 4→8, 8→12,
                       ;; 16→26) on ~0.1% of blobs. It does not compound, and content was always
                       ;; correct — but it is a budget the code claims to enforce and did not.
                       ;;
                       ;; Flush already-settled children (clean passthrough and newly buffered
                       ;; alike — both now carry an address AND a slot) biggest-first until the
                       ;; total fits. Candidate test and write order both mirror the JVM arm: the
                       ;; child is stored HERE, before Pass 3, so both runtimes issue the same
                       ;; `store` call sequence and a backend that assigns addresses in call order
                       ;; produces the same disk image.
                       embedded
                       (loop [emb   embedded
                              cands (when (and slots (> embedded budget))
                                      (->> (range len)
                                           (filter (fn [i] (and (some? (aget addrs i))
                                                                (some? (aget slots i)))))
                                           (sort-by (fn [i] (- (slot-be (aget slots i) (dec level)))))
                                           seq))]
                         (if (or (<= emb budget) (nil? cands))
                           emb
                           (let [i  (first cands)
                                 sl (aget slots i)
                                 ;; resident if we still hold it, else restore+project — the
                                 ;; settled child was re-pointed at its anchor, so this is
                                 ;; restore(anchor) + project(slot), never a wrong node. Measured
                                 ;; on the JVM: every flushed child was already resident under
                                 ;; :ref-type strong, soft AND weak, so this is a fallback rather
                                 ;; than a read in practice.
                                 c  (or (when-some [cs (read-field this :children)] (aget cs i))
                                        (await (child this storage i opts)))]
                             (storage/markFreed storage (aget addrs i))
                             (aset addrs i (await (node/store c storage opts)))
                             (aset slots i nil)
                             (recur (- emb (slot-be sl (dec level))) (next cands)))))]
                   ;; Pass 3: write flushed/structural children (all resident ⇒ no read).
                   (loop [ws (seq flushed)]
                     (when ws
                       (let [i     (first ws)
                             child (aget (read-field this :children) i)
                             sl    (when slots (aget slots i))]
                         (when (and sl (:anchor sl)) (storage/markFreed storage (:anchor sl)))
                         (aset addrs i (await (node/store child storage opts)))
                         (when slots (aset slots i nil))
                         (recur (next ws)))))
                   (let [a (do (write-field! this :_cmLazy false)   ; written with its record
                               (await (storage/store storage this opts)))]
                     ;; Written ⇒ this node equals its durable object, whose remaining slots are the
                     ;; children we BUFFERED (passthrough + newly buffered); flushed ones were nulled.
                     ;; So the settled total is `embedded`, not 0 (also clears any -1 poison — the new
                     ;; structure is now materialized on disk). A later commit deltas from here.
                     (write-field! this :_bufEntries embedded)
                     a))))))

(defn walk-addresses
  [^Branch this storage on-address {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [keys-l (arrays/alength (.-keys this))]
                 (loop [i 0]
                   (when (< i keys-l)
                     (let [addr (when (read-field this :addresses)
                                  (arrays/aget (read-field this :addresses) i))]
                       (when (or (nil? addr) (on-address addr))
                         (let [child (await (child this storage i opts))]
                           (when (instance? Branch child)
                             (await (node/walk-addresses child storage on-address opts)))))
                       (recur (inc i)))))))))

(defn ^Branch from-map
  [{:keys [level keys addresses subtree-count measure child-measures settings]}]
  ;; cold restore (storage deserializer): no comparator here — stamped on descent by -root/child.
  ;; _bufEntries = -2 (LAZY): derived from _slots on first read (the storage layer attaches them
  ;; after construction), mirroring subtree-count = -1. See buf-entries.
  ;; Only a record that fits: one of another length is not this node's.
  (->Branch level keys nil addresses (or subtree-count -1) measure settings nil -2 nil
            (when (and (some? child-measures) (== (count child-measures) (arrays/alength keys)))
              (to-array child-measures))
            false))

(defn promote-root
  "Build the parent of a count-based root split. root-total is the fallback
   when a child's count is unknown."
  [roots settings cmp root-total]
  (let [counts (map node/subtree-count roots)
        total (if (every? #(>= % 0) counts) (reduce + 0 counts) root-total)
        ops (:measure settings)
        measures (when ops (map node/measure roots))
        m (when (and ops (every? some? measures))
            (reduce #(measure/merge-measure ops %1 %2)
                    (measure/identity-measure ops) measures))]
    (with-child-measures!
      (->Branch (inc (node/level (arrays/aget roots 0)))
                (arrays/amap node/max-key roots) roots nil total m settings nil 0 cmp nil false))))

(defn weighted-entry
  "Look up a weighted rank from a materialized root. A stored child-measure
   record lets the descent restore just the chosen child at each level."
  [root storage measure-ops n {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (when-not measure-ops
                 (throw (ex-info "get-nth requires measure to be configured" {})))
               (when (and (not (neg? n)) (pos? (node/len root)))
                 (let [m (node/measure root)
                       total (if (some? m) m (await (node/force-compute-measure root storage measure-ops opts)))]
                   (when (and (some? total) (< n (measure/weight measure-ops total)))
                     (loop [cur root rank n]
                       (if (instance? Branch cur)
                         (when-some [ms (await (ensure-child-measures cur storage measure-ops opts))]
                           (let [hit (loop [i 0 r rank]
                                       (when (< i (node/len cur))
                                         (let [w (measure/weight measure-ops (arrays/aget ms i))]
                                           (if (< r w) [i r] (recur (inc i) (- r w))))))]
                             (when hit
                               (let [next-node (await (child cur storage (first hit) opts))]
                                 (recur next-node (second hit))))))
                         (let [keys (.-keys cur)]
                           (loop [i 0 r rank]
                             (when (< i (arrays/alength keys))
                               (let [key (arrays/aget keys i)
                                     w (measure/weight measure-ops (measure/extract measure-ops key))]
                                 (if (< r w) [key r] (recur (inc i) (- r w)))))))))))))))
