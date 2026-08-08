(ns org.replikativ.persistent-sorted-set.branch
  (:require-macros [org.replikativ.persistent-sorted-set.macros :refer [async+sync]])
  (:require [goog.array :as garr]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [org.replikativ.persistent-sorted-set.arrays :as arrays]
            [org.replikativ.persistent-sorted-set.impl.node :as node :refer [INode]]
            [org.replikativ.persistent-sorted-set.impl.measure :as measure]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage]
            [org.replikativ.persistent-sorted-set.impl.boundary :as b]
            [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]]
            [org.replikativ.persistent-sorted-set.util :as util]))

(declare Branch diff-size)

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
    (Leaf. (into-array (vals m1)) (.-settings base) nil)))

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
        base-addr (.-addresses base)
        new-keys  (arrays/aclone base-keys)
        slots     (make-array (arrays/alength base-keys))]
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
    (Branch. (.-level base) new-keys nil base-addr
             (long (:count sl))                      ; ĝ.count — no child summing
             (:measure sl)                           ; ĝ.measure
             (.-settings base) slots -2 proj-cmp)))

(defn- project-child
  "Project a freshly-restored child against this parent's buffered slot (if any). Returns
  `base` unchanged when there is no diff to apply. Mirrors the JVM Branch.child push-down."
  [^Branch node storage idx base]
  (let [sl (when (some? (.-_slots node)) (aget (.-_slots node) idx))]
    (if (and sl (some? (:diff sl)))
      ;; project with THIS leaf-parent's _projCmp (the set's stable comparator), never the
      ;; operation/navigation comparator that drove the descent. Mirrors JVM Branch.child.
      (if (instance? Leaf base)
        (project-leaf base (:diff sl) (.-_projCmp node))
        (project-branch base sl (.-_projCmp node)))
      base)))

(defn ensure-children
  [^Branch node]
  (when (nil? (.-children node))
    (set! (.-children node) (make-array (alength (.-keys node)))))
  (.-children node))

(defn ensure-addresses
  [^Branch this]
  (when (nil? (.-addresses this))
    (set! (.-addresses this) (make-array (alength (.-keys this)))))
  (.-addresses this))

(defn child
  [^Branch node storage idx {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (some? idx) (number? idx)))
  (assert (or (and (some? (.-children node))
                   (some? (aget (.-children node) idx)))
              (and (some? (.-addresses node))
                   (some? (aget (.-addresses node) idx)))))
  (async+sync sync?
              (async
               (let [*child (atom nil)]
                 (when (some? (.-children node))
                   (reset! *child (aget (.-children node) idx)))
                 (if (nil? @*child)
                   (let [addr (aget (.-addresses node) idx)
                         _    (assert (some? addr) "expected address to restore child")
                         _    (assert (some? storage) "expected storage")
                         base (await (storage/restore storage addr opts))
                         ;; diff-buf: propagate the set's projection comparator down to the
                         ;; restored branch (the storage layer has no comparator), so a
                         ;; leaf-parent projects its buffered leaves with the set's own
                         ;; comparator — independent of the op that drove this descent.
                         _    (when (instance? Branch base)
                                (set! (.-_projCmp base) (.-_projCmp node)))
                         ;; diff-buf: project this parent's buffered diff onto the freshly
                         ;; loaded child (leaf: rebuild keys; branch: install nested _slots).
                         c    (project-child node storage idx base)]
                     (reset! *child c)
                     (aset (ensure-children node) idx c))
                   (when (and (some? (.-addresses node)) (some? (aget (.-addresses node) idx)))
                     (assert (some? storage) "expected storage")
                     (storage/accessed storage (aget (.-addresses node) idx))))
                 @*child))))

(defn address
  ([^Branch this idx]
   (assert (and (<= 0 idx) (< idx (alength (.-keys this)))))
   (when-some [addrs (.-addresses this)]
     (aget addrs idx)))
  ([^Branch this idx address]
   (assert (and (<= 0 idx) (< idx (alength (.-keys this)))))
   (when (or (some? (.-addresses this)) (some? address))
     (ensure-addresses this)
     (aset (.-addresses this) idx address))
   address))

(defn- $count
  [^Branch node storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [*cnt (atom 0)]
                 (dotimes [i (alength (.-keys node))]
                   (let [c (await (child node storage i opts))]
                     (swap! *cnt + (await (node/$count c storage opts)))))
                 @*cnt))))

(defn- $contains?
  [^Branch node storage key cmp {:keys [sync?] :or {sync? true} :as opts}]
  (let [idx (garr/binarySearch (.-keys node) key cmp)]
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
  (let [idx (garr/binarySearch (.-keys node) key cmp)]
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
  (when-some [slots (.-_slots node)] (aget slots i)))

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
  (let [be (.-_bufEntries node)]
    (if (== be -2)
      (let [slots (.-_slots node)
            clvl  (dec (.-level node))
            s     (if (nil? slots)
                    0
                    (areduce slots j acc 0
                             (+ acc (if-some [sl (aget slots j)] (slot-be sl clvl) 0))))]
        (set! (.-_bufEntries node) s)
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
                 (if (>= sc 0) sc (await (node/$count c storage opts)))))))

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
               (when (nil? (.-_slots node))
                 (set! (.-_slots node) (make-array (arrays/alength (.-keys node)))))
               (let [slots  (.-_slots node)
                     prev   (aget slots i)
                     anchor (if (and prev (some? (:anchor prev))) (:anchor prev) anchor0)
                     c      (await (child node storage i opts))
                     pcmp   (.-_projCmp node)
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
                     measure (node/measure c)
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
                   (set! (.-_bufEntries node) (if (== new-be -1) -1 (+ (- cur old-be) new-be))))
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
  (when-some [slots (.-_slots node)]
    (let [out (make-array new-len)
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
          arr (make-array n)
          m   (min n (alength src-slots))]
      (dotimes [k m] (aset arr k (aget src-slots k)))
      (set! (.-_slots node) arr))))

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
  (let [sa (.-_slots a) sb (.-_slots b)]
    (when (or sa sb)
      (let [na  (arrays/alength (.-keys a))
            nb  (arrays/alength (.-keys b))
            out (make-array (+ na nb))]
        (when sa (dotimes [i na] (aset out i (aget sa i))))
        (when sb (dotimes [i nb] (aset out (+ na i) (aget sb i))))
        out))))

(defn- remove-stitch-slots
  "Build the slots array for the spliced center node of a STRUCTURAL $remove (a borrow/merge
  happened one level down): carry the surviving siblings outside the rebuilt range, keep an
  unchanged left/right sibling's slot, and null the center + any changed sibling. Mirrors the
  JVM remove slot Stitch (Branch.java 679-683)."
  [^Branch this left-idx right-idx alen left-child right-child left-unchanged right-unchanged new-len]
  (let [src (.-_slots this)
        out (make-array new-len)
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
    (let [addrs (.-addresses this)
          a     (when addrs (aget addrs i))]
      (if (some? a)
        (storage/markFreed storage a)
        (let [slots (.-_slots this)
              sl    (when slots (aget slots i))]
          (when (and sl (some? (:anchor sl)))
            (storage/markFreed storage (:anchor sl))))))))

(defn- mst-branch-add
  "split-seam (MST): given the merged separators/children after absorbing a child split,
   cut at boundary keys (≤2-way for one incremental insert). Mirrors the JVM Branch.add seam
   path. MST forces diff-buf off, so no slots/addresses bookkeeping (anchorless re-store)."
  [^Branch this bd new-keys new-children new-addrs ins key]
  (let [settings    (.-settings this)
        lvl         (.-level this)
        measure-ops (:measure settings)
        total       (arrays/alength new-children)
        ;; O(1): the one promoted separator (the split child's new max) is at `ins`.
        lens        (b/-split-on-insert bd new-keys total ins lvl)]
    (if (nil? lens)
      (let [old-sc (.-subtree-count this)
            new-sc (if (>= old-sc 0) (inc old-sc) -1)
            m      (when (and measure-ops (.-_measure this))
                     (measure/merge-measure measure-ops (.-_measure this) (measure/extract measure-ops key)))]
        (arrays/array (Branch. lvl new-keys new-children new-addrs new-sc m settings nil 0 (.-_projCmp this))))
      (loop [out (transient []), pos 0, ls lens]
        (if (seq ls)
          (let [l    (first ls)
                kseg (.slice new-keys pos (+ pos l))
                cseg (.slice new-children pos (+ pos l))
                aseg (when new-addrs (.slice new-addrs pos (+ pos l)))
                m    (when (and measure-ops (.-_measure this))
                       (reduce (fn [acc child]
                                 (if (nil? acc)
                                   (reduced nil)
                                   (let [cs (node/measure child)]
                                     (if cs (measure/merge-measure measure-ops acc cs) (reduced nil)))))
                               (measure/identity-measure measure-ops) cseg))
                sc   (try-compute-subtree-count-from-children cseg l)]
            (recur (conj! out (Branch. lvl kseg cseg aseg sc m settings nil 0 (.-_projCmp this)))
                   (+ pos l) (next ls)))
          (arrays/into-array (persistent! out)))))))

(defn add
  [^Branch this storage key cmp opts]
  (let [{:keys [sync?] :or {sync? true}} opts
        keys  (.-keys this)
        addrs (.-addresses this)
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
                           nodes-len        (arrays/alength nodes)]
                       (if bd
                         ;; PROBE FIX: preserve unchanged siblings' durable addresses across the
                         ;; MST rebuild, mirroring the JVM's allAddresses Stitch (copyAll +
                         ;; copyOne(null) per new node). Also free the split child's old blob.
                         (let [mst-addrs (when addrs
                                           (util/splice addrs idx (inc idx)
                                                        (arrays/make-array nodes-len)))]
                           (when (and storage addrs (aget addrs idx))
                             (storage/markFreed storage (aget addrs idx)))
                           (mst-branch-add this bd new-keys new-children mst-addrs idx key))
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
                                 old-sc (.-subtree-count this)
                                 new-sc (if (>= old-sc 0) (inc old-sc) -1)
                               ;; Update measure incrementally only if already computed
                                 measure-ops (:measure (.-settings this))
                                 new-measure (when (and measure-ops (.-_measure this))
                                               (measure/merge-measure measure-ops (.-_measure this) (measure/extract measure-ops key)))
                                 nb (Branch. (.-level this) new-keys new-children new-addrs new-sc new-measure (.-settings this) nil 0 (.-_projCmp this))]
                           ;; diff-buf: nodes-len==1 ⇒ content-only ⇒ carry the source slots and
                           ;; deposit Present(key) (matches JVM persistent path). nodes-len>=2 ⇒ a child
                           ;; split was absorbed ⇒ structural: mark rebalanced + stitch surviving siblings.
                             (when diff-buf?
                               (if (= nodes-len 1)
                                 (do (set! (.-_bufEntries nb) (buf-entries this)) ; carry running total (or -1) onto successor
                                     (await (carry-and-deposit nb storage (.-_slots this) idx key key anchor0 opts)))
                                 (do (set! (.-_bufEntries nb) -1) ; absorbed a child split: structural → must write
                                     (free-dropped-child this storage idx) ; diff-buf: free the split child's old blob
                                     (set! (.-_slots nb) (stitch-slots this idx nodes-len (arrays/alength new-children))))))
                             (arrays/array nb))
                           (let [middle      (arrays/half (arrays/alength new-children))
                                 tmp-addrs   (when addrs
                                               (let [old-addr (aget addrs idx)]
                                               ;; Mark old child address as freed before clearing (deferred under diff-buf)
                                                 (when (and (not diff-buf?) storage old-addr)
                                                   (storage/markFreed storage old-addr))
                                                 (util/splice addrs idx (inc idx) (arrays/array nil nil))))
                                 left-addrs  (when tmp-addrs (.slice tmp-addrs 0 middle))
                                 right-addrs (when tmp-addrs (.slice tmp-addrs middle))
                                 left-children (.slice new-children 0 middle)
                                 right-children (.slice new-children middle)
                                 measure-ops (:measure (.-settings this))
                               ;; Compute measure for split branches from their children only if already computed
                               ;; Return nil if any child measure is nil (don't silently undercount)
                                 left-measure (when (and measure-ops (.-_measure this))
                                                (reduce (fn [acc child]
                                                          (if (nil? acc)
                                                            (reduced nil)
                                                            (let [cs (node/measure child)]
                                                              (if cs
                                                                (measure/merge-measure measure-ops acc cs)
                                                                (reduced nil)))))
                                                        (measure/identity-measure measure-ops)
                                                        left-children))
                                 right-measure (when (and measure-ops (.-_measure this))
                                                 (reduce (fn [acc child]
                                                           (if (nil? acc)
                                                             (reduced nil)
                                                             (let [cs (node/measure child)]
                                                               (if cs
                                                                 (measure/merge-measure measure-ops acc cs)
                                                                 (reduced nil)))))
                                                         (measure/identity-measure measure-ops)
                                                         right-children))
                                 new-len (arrays/alength new-children)
                                 left-b  (Branch. (.-level this)
                                                  (.slice new-keys 0 middle)
                                                  left-children
                                                  left-addrs
                                                  (try-compute-subtree-count-from-children left-children (arrays/alength left-children))
                                                  left-measure
                                                  (.-settings this) nil 0 (.-_projCmp this))
                                 right-b (Branch. (.-level this)
                                                  (.slice new-keys middle)
                                                  right-children
                                                  right-addrs
                                                  (try-compute-subtree-count-from-children right-children (arrays/alength right-children))
                                                  right-measure
                                                  (.-settings this) nil 0 (.-_projCmp this))]
                           ;; diff-buf: a child split overflowed this branch → split: structural on both
                           ;; halves → written, but each still buffers its surviving siblings' slots.
                             (when diff-buf?
                               (set! (.-_bufEntries left-b) -1) ; split: structural on both halves → must write
                               (set! (.-_bufEntries right-b) -1)
                               (free-dropped-child this storage idx) ; diff-buf: free the split child's old blob
                               (when-let [all (stitch-slots this idx nodes-len new-len)]
                                 (set! (.-_slots left-b)  (.slice all 0 middle))
                                 (set! (.-_slots right-b) (.slice all middle))))
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
                     anchor0 (when (and diff-buf? (.-addresses this) (not= -1 idx)) (aget (.-addresses this) idx))]
                 (when-not (== -1 idx)
                   (let [children    (ensure-children this)
                         addrs       (.-addresses this)
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
                         removed-element (or (some-> removed-out (arrays/aget 0)) key)]
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
                             old-sc (.-subtree-count this)
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
                             new-measure (when (and measure-ops (.-_measure this))
                                           (node/try-compute-measure
                                            (Branch. (.-level this) new-keys new-kids new-addrs new-sc nil (.-settings this) nil 0 (.-_projCmp this))
                                            storage measure-ops {:sync? true}))
                             center (Branch. (.-level this) new-keys new-kids new-addrs new-sc new-measure (.-settings this) nil 0 (.-_projCmp this))]
                         ;; diff-buf: install the center's slots BEFORE rotate (so a subsequent
                         ;; rotate merge/merge-split with this node's siblings carries them).
                         ;; content-only ⇒ carry source slots + deposit Absent(key) at idx (1-for-1).
                         ;; structural ⇒ _bufEntries=-1 (must write) + stitch surviving siblings (rebuilt range nulled).
                         (when diff-buf?
                           (if content-only?
                             (do (set! (.-_bufEntries center) (buf-entries this)) ; carry running total (or -1) onto successor
                                 (await (carry-and-deposit center storage (.-_slots this) idx removed-element ABSENT anchor0 opts)))
                             (do (set! (.-_bufEntries center) -1) ; child merged/borrowed: structural → must write
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
                                 (set! (.-_slots center)
                                       (remove-stitch-slots this left-idx right-idx alen left-child right-child
                                                            left-unchanged right-unchanged (arrays/alength new-kids))))))
                         (util/rotate center
                                      (and (nil? left) (nil? right))
                                      left
                                      right
                                      (.-settings this))))))))))

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
  (let [children (.-children this)
        n        (arrays/alength (.-keys this))
        resident (fn [i] (when (and children (<= 0 i) (< i n)) (aget children i)))
        ks       (.-keys child)
        j        (garr/binarySearch ks old-key cmp)]
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
                     anchor0 (when (and diff-buf? (.-addresses this) (not= -1 idx)) (aget (.-addresses this) idx))]
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
                         removed-key (or (some-> removed-out (arrays/aget 0)) old-key)]
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
                             addrs         (.-addresses this)
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
                                   pcmp    (.-_projCmp this)]
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
                                 new-branch   (Branch. (.-level this) new-keys new-children new-addrs (.-subtree-count this) nil (.-settings this) nil 0 (.-_projCmp this))
                                 new-measure    (when (and measure-ops (.-_measure this))
                                                  (replace-measure new-branch storage measure-ops))]
                             (set! (.-_measure new-branch) new-measure)
                               ;; diff-buf: content-only replace ⇒ carry source slots + deposit Present(new-key).
                             (when diff-buf?
                               (set! (.-_bufEntries new-branch) (buf-entries this)) ; carry running total (or -1) onto successor
                               (await (carry-and-deposit-replace new-branch storage (.-_slots this) idx removed-key new-key anchor0 opts)))
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
                                 new-branch   (Branch. (.-level this) new-keys new-children new-addrs (.-subtree-count this) nil (.-settings this) nil 0 (.-_projCmp this))
                                 new-measure    (when (and measure-ops (.-_measure this))
                                                  (replace-measure new-branch storage measure-ops))]
                             (set! (.-_measure new-branch) new-measure)
                               ;; diff-buf: content-only replace ⇒ carry source slots + deposit Present(new-key).
                             (when diff-buf?
                               (set! (.-_bufEntries new-branch) (buf-entries this)) ; carry running total (or -1) onto successor
                               (await (carry-and-deposit-replace new-branch storage (.-_slots this) idx removed-key new-key anchor0 opts)))
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

  `store` settles that child IN PLACE (`set! (.-_bufEntries child) embedded`, and its slots are
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
  (let [slots (.-_slots node)]
    (when (some? slots)
      (let [children (.-children node)
            len      (arrays/alength (.-keys node))]
        (when (some? children)
          (loop [i 0, poisoned? false]
            (if (>= i len)
              (when poisoned? (set! (.-_bufEntries node) -1))
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
               (let [slots (.-_slots c)]
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
                             ;; LEAF child `(.-_slots c)` is undefined so the recursion returned
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
  (when-some [slots (.-_slots node)]
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
                       (when (nil? (aget (.-addresses this) i))
                         (assert (some? (.-children this)))
                         (assert (some? (aget (.-children this) i)))
                         (assert (implements? node/INode (aget (.-children this) i)))
                         (let [child-address (await (node/store (aget (.-children this) i) storage opts))]
                           (address this i child-address)))
                       (recur (inc i))))
                   (await (storage/store storage this opts)))
                 ;; diff-buf: buffer content-only dirty children (re-point to the child's durable
                 ;; anchor) up to budget B; flush the rest BIGGEST-FIRST — the largest diffs are
                 ;; written and the small ones kept buffered, so a slot that regularly fills a big
                 ;; share of the budget is written proportionally often and can't jam the buffer.
                 ;; Only dirty (resident) children are flushed (no read); clean passthrough consumes
                 ;; budget but is left untouched. Mirrors JVM Branch.store (see doc/diff-buffering.md).
                 (let [_      (refresh-marker-slots! this)   ; see the fn: detect a settled child
                       addrs  (.-addresses this)
                       slots  (.-_slots this)
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
                               (let [child (aget (.-children this) i)]
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
                                 c  (or (when-some [cs (.-children this)] (aget cs i))
                                        (await (child this storage i opts)))]
                             (storage/markFreed storage (aget addrs i))
                             (aset addrs i (await (node/store c storage opts)))
                             (aset slots i nil)
                             (recur (- emb (slot-be sl (dec level))) (next cands)))))]
                   ;; Pass 3: write flushed/structural children (all resident ⇒ no read).
                   (loop [ws (seq flushed)]
                     (when ws
                       (let [i     (first ws)
                             child (aget (.-children this) i)
                             sl    (when slots (aget slots i))]
                         (when (and sl (:anchor sl)) (storage/markFreed storage (:anchor sl)))
                         (aset addrs i (await (node/store child storage opts)))
                         (when slots (aset slots i nil))
                         (recur (next ws)))))
                   (let [a (await (storage/store storage this opts))]
                     ;; Written ⇒ this node equals its durable object, whose remaining slots are the
                     ;; children we BUFFERED (passthrough + newly buffered); flushed ones were nulled.
                     ;; So the settled total is `embedded`, not 0 (also clears any -1 poison — the new
                     ;; structure is now materialized on disk). A later commit deltas from here.
                     (set! (.-_bufEntries this) embedded)
                     a))))))

(defn walk-addresses
  [^Branch this storage on-address {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [keys-l (arrays/alength (.-keys this))]
                 (loop [i 0]
                   (when (< i keys-l)
                     (let [addr (when (.-addresses this)
                                  (arrays/aget (.-addresses this) i))]
                       (when (or (nil? addr) (on-address addr))
                         (let [child (await (child this storage i opts))]
                           (when (instance? Branch child)
                             (await (node/walk-addresses child storage on-address opts)))))
                       (recur (inc i)))))))))

(defn ^Branch from-map
  [{:keys [level keys addresses subtree-count measure settings]}]
  ;; cold restore (storage deserializer): no comparator here — stamped on descent by -root/child.
  ;; _bufEntries = -2 (LAZY): derived from _slots on first read (the storage layer attaches them
  ;; after construction), mirroring subtree-count = -1. See buf-entries.
  (Branch. level keys nil addresses (or subtree-count -1) measure settings nil -2 nil))

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
(deftype Branch [^number level keys ^:mutable children ^:mutable addresses ^:mutable ^number subtree-count ^:mutable _measure settings ^:mutable _slots ^:mutable _bufEntries ^:mutable _projCmp]
  Object
  (toString [_] (pr-str* {:level level :keys (vec keys)}))
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
                                 (if child-measure
                                   (recur (inc i)
                                          (measure/merge-measure measure-ops acc child-measure))
                                   nil)))) ;; child measure unavailable, postpone
                           acc))]
            (when result
              (set! _measure result))
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
                                  (if child-measure
                                    (recur (inc i)
                                           (measure/merge-measure measure-ops acc child-measure))
                                    nil))))
                            acc))]
             (when result
               (set! _measure result))
             result))))))
  (force-compute-measure [this storage measure-ops {:keys [sync?] :or {sync? true} :as opts}]
    ;; Force compute measure, recursively descending if needed
    (async+sync sync?
                (async
                 (when measure-ops
                   (let [result (loop [i 0
                                       acc (measure/identity-measure measure-ops)]
                                  (if (< i (arrays/alength keys))
                                    (let [child (await (child this storage i opts))
                                          child-measure (or (node/measure child)
                                                            (await (node/force-compute-measure child storage measure-ops opts)))]
                                      (recur (inc i)
                                             (if child-measure
                                               (measure/merge-measure measure-ops acc child-measure)
                                               acc)))
                                    acc))]
                     (set! _measure result)
                     result)))))
  (merge [this ^Branch next]
    (let [sc1 subtree-count
          sc2 (.-subtree-count next)
          new-sc (if (and (>= sc1 0) (>= sc2 0)) (+ sc1 sc2) -1)
          ;; Merge measure if both have them
          new-measure (when (and _measure (.-_measure next))
                        (when-let [measure-ops (:measure settings)]
                          (measure/merge-measure measure-ops _measure (.-_measure next))))
          ;; Ensure children arrays exist (may be arrays of nulls for lazy branches)
          c1 (ensure-children this)
          c2 (ensure-children next)
          ;; Merge addresses too if present
          new-addrs (when (or addresses (.-addresses next))
                      (arrays/aconcat (or (ensure-addresses this) (arrays/make-array (arrays/alength keys)))
                                      (or (ensure-addresses next) (arrays/make-array (arrays/alength (.-keys next))))))
          nb (Branch. level
                      (arrays/aconcat keys (.-keys next))
                      (arrays/aconcat c1 c2)
                      new-addrs
                      new-sc
                      new-measure
                      settings nil 0 _projCmp)]
      ;; diff-buf: a merged node's structure differs from any anchor ⇒ it must be WRITTEN
      ;; (-1), but it still buffers the surviving children's slots (concatenated).
      (when (pos? (or (:diff-buf-size settings) 0))
        (set! (.-_bufEntries nb) -1)
        (set! (.-_slots nb) (concat-slots this next)))
      nb))
  (merge-split [this ^Branch next]
    (let [;; Ensure children arrays exist
          c1 (ensure-children this)
          c2 (ensure-children next)
          ks (util/merge-n-split keys (.-keys next))
          ps (util/merge-n-split c1 c2)
          ;; Also merge-split addresses if present
          as (when (or addresses (.-addresses next))
               (util/merge-n-split (or (ensure-addresses this) (arrays/make-array (arrays/alength keys)))
                                   (or (ensure-addresses next) (arrays/make-array (arrays/alength (.-keys next))))))]
      (let [p0 (arrays/aget ps 0)
            p1 (arrays/aget ps 1)
            sc0 (try-compute-subtree-count-from-children p0 (arrays/alength p0))
            sc1 (try-compute-subtree-count-from-children p1 (arrays/alength p1))
            measure-ops (:measure settings)
            m0 (when (and measure-ops _measure (.-_measure next))
                 (reduce (fn [acc child]
                           (if (nil? acc)
                             (reduced nil)
                             (let [cs (node/measure child)]
                               (if cs
                                 (measure/merge-measure measure-ops acc cs)
                                 (reduced nil)))))
                         (measure/identity-measure measure-ops)
                         p0))
            m1 (when (and measure-ops _measure (.-_measure next))
                 (reduce (fn [acc child]
                           (if (nil? acc)
                             (reduced nil)
                             (let [cs (node/measure child)]
                               (if cs
                                 (measure/merge-measure measure-ops acc cs)
                                 (reduced nil)))))
                         (measure/identity-measure measure-ops)
                         p1))
            b0 (Branch. level (arrays/aget ks 0) p0 (when as (arrays/aget as 0)) sc0 m0 settings nil 0 _projCmp)
            b1 (Branch. level (arrays/aget ks 1) p1 (when as (arrays/aget as 1)) sc1 m1 settings nil 0 _projCmp)]
        ;; diff-buf: redistribution is structural on both halves (-1); split the
        ;; concatenated slots at the same child boundary so each half buffers its own children.
        (when (pos? (or (:diff-buf-size settings) 0))
          (set! (.-_bufEntries b0) -1)
          (set! (.-_bufEntries b1) -1)
          (when-let [all (concat-slots this next)]
            (let [n0 (arrays/alength p0)]
              (set! (.-_slots b0) (.slice all 0 n0))
              (set! (.-_slots b1) (.slice all n0)))))
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
