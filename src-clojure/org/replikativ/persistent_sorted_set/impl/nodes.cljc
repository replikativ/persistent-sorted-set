(ns org.replikativ.persistent-sorted-set.impl.nodes
  "Format-AGNOSTIC core of PSS node/root serialization.

   Every wire format PSS supports — Fressian, boring (CBOR), transit — encodes
   exactly the same thing: a node projected to a plain Clojure map, and a root
   projected to a plain Clojure map. Only the tag plumbing differs. This
   namespace owns the projections and the reconstruction; the format modules
   own nothing but `write this map under this tag`.

   That split is deliberate. `fressian.cljc` set out to be \"one place that
   fixes the JVM/cljs drift\"; three copies of the same settings/slots logic
   would reintroduce exactly the drift it was written to prevent, and a codec
   that drifts between formats loses data silently. So this is the one place,
   and adding a fourth format must not add a fourth copy.

   WHAT TRAVELS vs WHAT IS RESOLVED (unchanged from the Fressian contract):

     SERIALIZED (in the blob):  keys / addresses / level / subtree-count /
                                measure-VALUE / slots, the node's own
                                `:branching-factor` + `:diff-buf-size`, and
                                (non-SOFT) `:ref-type`.
     RESOLVED AT READ (runtime): the IStorage (live), the comparator (fn), the
                                measure-OPS (fn), the leaf-processor (fn).

   A node is therefore SELF-DESCRIBING: its `Settings` are reconstructed per
   node from the blob plus the consumer's measure-ops. See `fressian.cljc`'s
   docstring for the full rationale — it is the reference explanation and is
   not repeated here."
  #?(:clj (:require [org.replikativ.persistent-sorted-set.impl.boundary :as bnd]))
  #?(:cljs (:require [org.replikativ.persistent-sorted-set.impl.node :as node]
                     [org.replikativ.persistent-sorted-set.impl.boundary :as bnd]
                     [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]]
                     [org.replikativ.persistent-sorted-set.branch :refer [Branch] :as branch]
                     [org.replikativ.persistent-sorted-set.btset :refer [BTSet]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set ANode Leaf Branch Settings Slot
                    IMeasure IBoundary RefType PersistentSortedSet]
                   [java.util List])))

;; Tag NAMES, shared across formats. Fressian and transit use them literally;
;; boring uses them as tag-27 type names. Keeping one set of names means a
;; consumer's mental model does not change with the format.
(def ^:const leaf-tag "pss/leaf")
(def ^:const branch-tag "pss/branch")
(def ^:const set-tag "pss/set")

;; ---------------------------------------------------------------------------
;; Settings (de)construction. A Settings splits into DATA and FUNCTIONS:
;;   DATA (serialized):    branching-factor, diff-buf-size, ref-type (an enum).
;;   FUNCTIONS (never):    measure-ops (IMeasure), leaf-processor, comparator.
;; ---------------------------------------------------------------------------

#?(:clj (defn ref-type->kw [^RefType rt] (when rt (keyword (.toLowerCase (.name rt))))))
#?(:clj (defn kw->ref-type ^RefType [kw]
          (case kw :strong RefType/STRONG :soft RefType/SOFT :weak RefType/WEAK nil)))

#?(:clj
   (defn settings-for ^Settings [bf dbs ^IMeasure measure ref-type bdesc]
     ;; ref-type nil ⇒ Settings normalizes to SOFT. `measure` is the IMeasure OPS the consumer
     ;; supplied (never serialized); leaf-processor stays nil — it's the operating root's, threaded.
     ;; bdesc (a serialized boundary descriptor, nil for count) is resolved INTERNALLY.
     (let [s (Settings. (int bf) (kw->ref-type ref-type) measure nil (int dbs))]
       (if bdesc (.withBoundary s ^IBoundary (bnd/resolve-boundary bdesc)) s)))
   :cljs
   (defn settings-for [bf dbs measure ref-type bdesc]
     ;; cljs has no soft/weak refs, so ref-type is inert here — but CARRY it so it round-trips
     ;; losslessly through a cljs relay (deserialize → re-serialize) back to a JVM reader.
     (cond-> {:branching-factor bf :diff-buf-size dbs :measure measure}
       ref-type (assoc :ref-type ref-type)
       bdesc    (assoc :boundary (bnd/resolve-boundary bdesc)))))

(defn node-config
  "A node's SERIALIZABLE settings — branching-factor + diff-buf-size + (non-default) ref-type. These
   ride in the blob (self-describing) but NOT in `node->map` (the content hash), so addresses are
   unchanged. ref-type is omitted when SOFT (the default), so common-case blobs are byte-unchanged."
  [node]
  #?(:clj  (let [^Settings s (.-_settings ^ANode node)
                 rt (.refType s)
                 bdesc (.descriptor (.boundary s))]   ; nil for count, {:type :mst :lzpl n} for MST
             (cond-> {:branching-factor (.branchingFactor s) :diff-buf-size (.diffBufSize s)}
               (and rt (not= rt RefType/SOFT)) (assoc :ref-type (ref-type->kw rt))
               bdesc (assoc :boundary bdesc)))
     :cljs (let [s (.-settings node)
                 bdesc (when-let [bd (:boundary s)] (bnd/-descriptor bd))]
             (cond-> {:branching-factor (:branching-factor s) :diff-buf-size (:diff-buf-size s)}
               (:ref-type s) (assoc :ref-type (:ref-type s))
               bdesc (assoc :boundary bdesc)))))

;; ---------------------------------------------------------------------------
;; node->map — the CONTENT projection (for content-addressing). Branching-factor/diff-buf are
;; deliberately absent here (config, not content); `node->blob` appends them.
;; ---------------------------------------------------------------------------

(defn node->map
  "Project a PSS node to its canonical CONTENT map — for content-addressing (hash this map) or
   storing the map directly. Leaf → {:keys …}; Branch → {:level :keys :addresses :subtree-count
   (:measure) (:slots)}. Comparator/storage/settings-free; element values stay raw. NOTE: this is
   the CONTENT projection — the serialized blob additionally carries :branching-factor/:diff-buf-size
   (see `node->blob`), which are NOT part of the content hash."
  [node]
  #?(:clj
     ;; `vec` the keys/addresses: the raw trimmed Java List isn't hash-coercible (hasch) and differs
     ;; from the cljs vector form — a Clojure vector is both, and every format reads either back as a vector.
     (if (instance? Branch node)
       (let [^Branch b node
             slots (.slotsForStorage b)]
         (cond-> {:level         (.level b)
                  :keys          (vec (.keys b))
                  :addresses     (vec (.addresses b))
                  :subtree-count (.subtreeCount b)}
           (some? (.-_measure b)) (assoc :measure (.-_measure b))
           slots                  (assoc :slots slots)))
       (cond-> {:keys (vec (.keys ^ANode node))}
         (some? (.-_measure ^ANode node)) (assoc :measure (.-_measure ^ANode node))))
     :cljs
     (if (instance? Branch node)
       (let [slots (branch/slots-for-storage node)]
         (cond-> {:level         (node/level node)
                  :keys          (vec (.-keys node))
                  :addresses     (vec (.-addresses node))
                  :subtree-count (.-subtree-count node)}
           (some? (.-_measure node)) (assoc :measure (.-_measure node))
           slots                     (assoc :slots slots)))
       (cond-> {:keys (vec (.-keys node))}
         (some? (.-_measure node)) (assoc :measure (.-_measure node))))))

(defn node->blob
  "What every format actually writes for a node: the content projection PLUS the node's own
   branching-factor/diff-buf-size/ref-type, so a read self-describes."
  [node]
  (merge (node->map node) (node-config node)))

(defn branch?
  "Does this node serialize as a branch? Format modules need this to pick a tag."
  [node]
  (instance? Branch node))

;; ---------------------------------------------------------------------------
;; Reconstruction: blob → Leaf / Branch / root.
;; ---------------------------------------------------------------------------

#?(:clj
   (defn- attach-slots!
     "Rebuild a restored Branch's slots from the stored {idx -> entry} map.
      anchor = addresses[idx] (re-derived, not stored — matches the reference codec).
      Installed via Branch.installSlots — ONE volatile publish of the {slots, entries}
      snapshot (entries BUF_LAZY: derived from the slots on first read)."
     [^Branch b ^List addresses slots]
     (let [arr (object-array (alength (.-_keys b)))]
       (doseq [[idx entry] slots]
         (aset arr (int idx)
               (Slot. (:diff entry) (long (:count entry)) (:measure entry)
                      (nth addresses (int idx)))))
       (.installSlots b arr Branch/BUF_LAZY)))
   :cljs
   (defn- attach-slots!
     [node addresses slots]
     (let [arr (make-array (count addresses))
           av  (vec addresses)]
       (doseq [[idx entry] slots]
         (aset arr (int idx) {:diff    (:diff entry)
                              :count   (:count entry)
                              :measure (:measure entry)
                              :anchor  (nth av (int idx))}))
       (set! (.-_slots node) arr))))

(defn reader-context
  "Bundle the read-side knobs once, so each format module builds it in one call and the
   per-node functions stay pure of option parsing.

   `:measure-ops` is the node-level IMeasure (nil for most consumers); `:default-bf` is the
   fallback branching-factor for pre-bf blobs; `:ref-type` overrides the blob's serialized
   ref-type. `:resolve-storage`/`:resolve-cmp`/`:resolve-measure` are the root's three
   non-serializable bits, each `(fn [meta] -> thing)`.

   `mk-settings` is memoized: nodes in one store share a handful of distinct settings, so this
   turns a per-node Settings allocation into a map lookup."
  [{:keys [measure-ops default-bf ref-type resolve-storage resolve-cmp resolve-measure]}]
  (let [default-bf (or default-bf 0)]
    {:measure-ops     measure-ops
     :default-bf      default-bf
     :ref-type        ref-type
     :resolve-storage (or resolve-storage (constantly nil))
     :resolve-cmp     (or resolve-cmp (constantly nil))
     :resolve-measure (or resolve-measure (constantly nil))
     :mk-settings     (memoize (fn [bf dbs rt bdesc]
                                 (settings-for bf dbs measure-ops rt bdesc)))}))

(defn- blob-settings
  [{:keys [mk-settings default-bf ref-type]} blob]
  (mk-settings (or (:branching-factor blob) default-bf)
               (or (:diff-buf-size blob) 0)
               (or ref-type (:ref-type blob))
               (:boundary blob)))

(defn blob->leaf
  "Reconstruct a Leaf from its blob. `ctx` comes from `reader-context`."
  [ctx blob]
  (let [{:keys [keys measure]} blob
        settings (blob-settings ctx blob)]
    #?(:clj (let [l (Leaf. ^List keys settings)]
              (when (some? measure) (set! (.-_measure ^ANode l) measure))
              l)
       :cljs (Leaf. (to-array keys) settings measure))))

(defn blob->branch
  "Reconstruct a Branch from its blob, re-attaching diff-buf slots. `ctx` from `reader-context`."
  [ctx blob]
  (let [{:keys [level keys addresses subtree-count measure slots]} blob
        settings (blob-settings ctx blob)]
    #?(:clj (let [b (Branch. (int level) ^List keys ^List addresses settings)]
              (set! (.-_subtreeCount b) (long (or subtree-count -1)))
              (when (some? measure) (set! (.-_measure ^ANode b) measure))
              (when slots (attach-slots! b addresses slots))
              b)
       :cljs (let [node (branch/from-map {:level         level
                                          :keys          (to-array keys)
                                          :addresses     (to-array addresses)
                                          :subtree-count subtree-count
                                          :measure       measure
                                          :settings      settings})]
               (when slots (attach-slots! node addresses slots))
               node))))

;; ---------------------------------------------------------------------------
;; Root projection.
;; ---------------------------------------------------------------------------

(defn root->blob
  "Project a flushed PSS root to `{:meta :address :count :branching-factor :diff-buf-size
   (:ref-type) (:boundary)}`. Throws if the root has not been flushed — an unflushed root has
   no address, so serializing it would silently produce an unreadable blob.

   `:count` is the set's CACHED count and may be -1 (unknown): a restore+mutate invalidates
   ancestor subtree counts, and recomputing here would need the set's storage to materialize lazy
   children — but serialization must work on a storage-DETACHED root. The read ctor treats -1 as
   compute-lazily-on-demand, so the count comes back right on the reader."
  [pset]
  #?(:clj
     (do
       (when (nil? (.-_address ^PersistentSortedSet pset))
         (throw (ex-info "PSS root must be flushed before serialization" {:type :must-be-flushed})))
       (let [^Settings s (.-_settings ^PersistentSortedSet pset)
             rt (.refType s)
             bdesc (.descriptor (.boundary s))]
         (cond-> {:meta             (meta pset)
                  :address          (.-_address ^PersistentSortedSet pset)
                  :count            (.-_count ^PersistentSortedSet pset)
                  :branching-factor (.branchingFactor s)
                  :diff-buf-size    (.diffBufSize s)}
           (and rt (not= rt RefType/SOFT)) (assoc :ref-type (ref-type->kw rt))
           bdesc (assoc :boundary bdesc))))
     :cljs
     (do
       (when (nil? (.-address pset))
         (throw (ex-info "PSS root must be flushed before serialization" {:type :must-be-flushed})))
       (let [s (.-settings pset)
             bdesc (when-let [bd (:boundary s)] (bnd/-descriptor bd))]
         (cond-> {:meta             (meta pset)
                  :address          (.-address pset)
                  :count            (.-cnt pset)
                  :branching-factor (:branching-factor s)
                  :diff-buf-size    (:diff-buf-size s)}
           (:ref-type s) (assoc :ref-type (:ref-type s))
           bdesc (assoc :boundary bdesc))))))

(defn blob->root
  "Reconstruct a lazy PSS root, resolving storage/comparator/measure per read from the root's
   `:meta` via the `ctx` resolvers. `ctx` from `reader-context`."
  [{:keys [resolve-storage resolve-cmp resolve-measure default-bf ref-type]} blob]
  (let [{:keys [meta address count branching-factor diff-buf-size]} blob
        settings (settings-for (or branching-factor default-bf)
                               (or diff-buf-size 0)
                               (resolve-measure meta)
                               (or ref-type (:ref-type blob))
                               (:boundary blob))]
    #?(:clj (PersistentSortedSet. meta (resolve-cmp meta) address (resolve-storage meta)
                                  nil (int count) settings 0)
       ;; BTSet deftype: [root cnt comparator meta _hash storage address settings]
       :cljs (BTSet. nil count (resolve-cmp meta) meta nil (resolve-storage meta) address settings))))

;; ---------------------------------------------------------------------------
;; The three runtime registries — the non-serializable bits, keyed by an id a root stamps in its
;; meta. A LEXICAL (one-store) serializer can ignore these and close over its own context; a
;; shared/WIRE serializer resolves by these via the `registry-*-resolver` helpers.
;;
;; Namespaced under `pss/` (matching the tags) so the codec never squats a BARE keyword in the
;; consumer's root-metadata namespace — these keys are library-owned.
;; ---------------------------------------------------------------------------

(def ^:const storage-id-key    :pss/storage-id)
(def ^:const comparator-id-key :pss/comparator-id)
(def ^:const measure-id-key    :pss/measure-id)

(defonce ^{:doc "storage-id → IStorage (live; per-connect lifecycle)."}      storage-registry    (atom {}))
(defonce ^{:doc "comparator-id → Comparator (static fn; ns-load)."}          comparator-registry (atom {}))
(defonce ^{:doc "measure-id → IMeasure (static fn; ns-load, usually empty)."} measure-registry   (atom {}))

(defn register-storage!      [id storage] (swap! storage-registry assoc id storage) storage)
(defn unregister-storage!    [id] (swap! storage-registry dissoc id) nil)
(defn registered-storage     [id] (get @storage-registry id))
(defn register-comparator!   [id cmp] (swap! comparator-registry assoc id cmp) cmp)
(defn unregister-comparator! [id] (swap! comparator-registry dissoc id) nil)
(defn registered-comparator  [id] (get @comparator-registry id))
(defn register-measure!      [id m] (swap! measure-registry assoc id m) m)
(defn unregister-measure!    [id] (swap! measure-registry dissoc id) nil)
(defn registered-measure     [id] (get @measure-registry id))

(defn registry-storage-resolver "Wire resolver: storage by (:pss/storage-id meta)."       [] (fn [meta] (registered-storage    (get meta storage-id-key))))
(defn registry-cmp-resolver     "Wire resolver: comparator by (:pss/comparator-id meta)." [] (fn [meta] (registered-comparator (get meta comparator-id-key))))
(defn registry-measure-resolver "Wire resolver: measure-ops by (:pss/measure-id meta)."   [] (fn [meta] (registered-measure    (get meta measure-id-key))))
