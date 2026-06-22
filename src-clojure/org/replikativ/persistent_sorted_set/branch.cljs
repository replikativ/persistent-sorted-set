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
  "Push one level down: install the nested diff as base's own _slots (each grandchild's
  diff + ĝ, anchored at base's durable child address) and set base's aggregates from ĝ.
  Grandchildren project lazily on their own descent. Mirrors JVM Branch.projectBranch."
  [^Branch base sl]
  (let [diff      (:diff sl)
        base-keys (.-keys base)
        base-addr (.-addresses base)
        slots     (make-array (arrays/alength base-keys))]
    (doseq [[k entry] (seq diff)]
      (let [i  (int k)
            mk (:max-key entry)]
        (aset slots i {:diff    (:diff entry)
                       :count   (long (:count entry))
                       :measure (:measure entry)
                       :anchor  (arrays/aget base-addr i)})   ; anchor = grandchild's durable address
        ;; Restore the separator: base came from the anchor whose _keys[i] is the PRE-diff
        ;; max; the diff changed child i's max, so fix the separator here (the verified read bug).
        (when (some? mk)
          (arrays/aset base-keys i mk))))
    (set! (.-_slots base) slots)
    (set! (.-subtree-count base) (long (:count sl)))   ; ĝ.count — no child summing
    (set! (.-_measure base) (:measure sl))             ; ĝ.measure
    base))

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
        (project-branch base sl))
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
  [^Branch this bd new-keys new-children key]
  (let [settings    (.-settings this)
        lvl         (.-level this)
        measure-ops (:measure settings)
        total       (arrays/alength new-children)]
    (if (b/-overflows? bd new-keys total lvl)
      (let [lens (b/-split-lengths bd new-keys total lvl)]
        (loop [out (transient []), pos 0, ls lens]
          (if (seq ls)
            (let [l    (first ls)
                  kseg (.slice new-keys pos (+ pos l))
                  cseg (.slice new-children pos (+ pos l))
                  m    (when (and measure-ops (.-_measure this))
                         (reduce (fn [acc child]
                                   (if (nil? acc)
                                     (reduced nil)
                                     (let [cs (node/measure child)]
                                       (if cs (measure/merge-measure measure-ops acc cs) (reduced nil)))))
                                 (measure/identity-measure measure-ops) cseg))
                  sc   (try-compute-subtree-count-from-children cseg l)]
              (recur (conj! out (Branch. lvl kseg cseg nil sc m settings nil 0 (.-_projCmp this)))
                     (+ pos l) (next ls)))
            (arrays/into-array (persistent! out)))))
      (let [old-sc (.-subtree-count this)
            new-sc (if (>= old-sc 0) (inc old-sc) -1)
            m      (when (and measure-ops (.-_measure this))
                     (measure/merge-measure measure-ops (.-_measure this) (measure/extract measure-ops key)))]
        (arrays/array (Branch. lvl new-keys new-children nil new-sc m settings nil 0 (.-_projCmp this)))))))

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
                         (mst-branch-add this bd new-keys new-children key)
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
                         disjoined   (await (node/$remove child storage key left-child right-child cmp opts))]
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
                             new-measure (when (and measure-ops (.-_measure this))
                                           (measure/remove-measure measure-ops (.-_measure this) key
                                                                   #(node/try-compute-measure
                                                                     (Branch. (.-level this) new-keys new-kids new-addrs new-sc nil (.-settings this) nil 0 (.-_projCmp this))
                                                                     storage measure-ops {:sync? true})))
                             center (Branch. (.-level this) new-keys new-kids new-addrs new-sc new-measure (.-settings this) nil 0 (.-_projCmp this))]
                         ;; diff-buf: install the center's slots BEFORE rotate (so a subsequent
                         ;; rotate merge/merge-split with this node's siblings carries them).
                         ;; content-only ⇒ carry source slots + deposit Absent(key) at idx (1-for-1).
                         ;; structural ⇒ _bufEntries=-1 (must write) + stitch surviving siblings (rebuilt range nulled).
                         (when diff-buf?
                           (if content-only?
                             (do (set! (.-_bufEntries center) (buf-entries this)) ; carry running total (or -1) onto successor
                                 (await (carry-and-deposit center storage (.-_slots this) idx key ABSENT anchor0 opts)))
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

(defn $replace
  [^Branch this storage old-key new-key cmp {:keys [sync?] :or {sync? true} :as opts}]
  (assert (== 0 (cmp old-key new-key)) "old-key and new-key must compare as equal (cmp must return 0)")
  (async+sync sync?
              (async
               (let [keys (.-keys this)
                     settings (.-settings this)
                     editable? (:edit settings)
                     measure-ops (:measure settings)
                     idx  (let [arr-l (arrays/alength keys)
                                i     (util/binary-search-l cmp keys (dec arr-l) old-key)]
                            (if (== i arr-l) -1 i))
                     diff-buf? (diff-buf? this)
                     ;; diff-buf: capture child idx's durable address before the mutation nulls it.
                     anchor0 (when (and diff-buf? (.-addresses this) (not= -1 idx)) (aget (.-addresses this) idx))]
                 (when-not (== -1 idx)
                   (let [child  (await (child this storage idx opts))
                         nodes  (await (node/$replace child storage old-key new-key cmp opts))]
                     (cond
                       ;; Not found in child
                       (nil? nodes)
                       nil

                       ;; Early exit from child (transient, no maxKey change). diff-buf: still
                       ;; deposit Present(new-key) at this level (mirrors JVM EARLY_EXIT path).
                       (= nodes :early-exit)
                       (do
                         (when diff-buf? (await (deposit-replace this storage idx old-key new-key anchor0 opts)))
                         :early-exit)

                       ;; Child returned updated node
                       :else
                       (let [new-node      (arrays/aget nodes 0)
                             new-max-key   (node/max-key new-node)
                             children      (ensure-children this)
                             addrs         (.-addresses this)
                             last-child?   (== idx (dec (arrays/alength keys)))
                             max-key-changed (and last-child? (not (== 0 (cmp new-max-key (arrays/aget keys idx)))))]
                         (if max-key-changed
                           ;; maxKey changed - update keys array
                           (if editable?
                             ;; Transient: mutate in place
                             (do
                               (aset keys idx new-max-key)
                               (aset children idx new-node)
                               (when addrs
                                 ;; Mark old child address as freed before clearing (deferred under diff-buf)
                                 (when (and (not diff-buf?) storage (aget addrs idx))
                                   (storage/markFreed storage (aget addrs idx)))
                                 (aset addrs idx nil))
                               (when (and measure-ops (.-_measure this))
                                 (set! (.-_measure this)
                                       (replace-measure this storage measure-ops)))
                               (when diff-buf? (await (deposit-replace this storage idx old-key new-key anchor0 opts)))
                               (arrays/array this))
                             ;; Persistent: clone arrays
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
                                 (await (carry-and-deposit-replace new-branch storage (.-_slots this) idx old-key new-key anchor0 opts)))
                               (arrays/array new-branch)))
                           ;; maxKey unchanged - reuse keys array
                           (if editable?
                             ;; Transient: mutate in place
                             (do
                               (aset children idx new-node)
                               (when addrs
                                 ;; Mark old child address as freed before clearing (deferred under diff-buf)
                                 (when (and (not diff-buf?) storage (aget addrs idx))
                                   (storage/markFreed storage (aget addrs idx)))
                                 (aset addrs idx nil))
                               (when (and measure-ops (.-_measure this))
                                 (set! (.-_measure this)
                                       (replace-measure this storage measure-ops)))
                               (when diff-buf? (await (deposit-replace this storage idx old-key new-key anchor0 opts)))
                               (if last-child?
                                 (arrays/array this)  ; Last child, need to propagate
                                 :early-exit))        ; Not last child, early exit
                             ;; Persistent: clone ALL arrays — sharing would allow a
                             ;; later transient editable path to corrupt the original
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
                                 (await (carry-and-deposit-replace new-branch storage (.-_slots this) idx old-key new-key anchor0 opts)))
                               (arrays/array new-branch))))))))))))

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
                           (if (nil? sl)
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
                 (let [addrs  (.-addresses this)
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
                                   (let [nested (if (some? (:diff sl)) (:diff sl) (await (assemble-nested storage child opts)))]
                                     (recur (inc i) pass (conj buf {:i i :sz (:buf-entries sl) :nested nested}) wl))))))))
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
                               (sort-by :sz (:buf classified)))]
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
