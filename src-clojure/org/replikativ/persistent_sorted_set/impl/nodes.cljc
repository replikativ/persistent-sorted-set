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
             ;; `(or … 0)`: the cljs settings map simply lacks the key when no diff-buf was
             ;; configured, so this emitted nil where the JVM -- whose Settings.diffBufSize is
             ;; an int -- emits 0. Same logical node, two different blobs, and a decode+encode
             ;; on cljs was not idempotent (nil in, 0 back out, because the reader defaults it).
             (cond-> {:branching-factor (:branching-factor s) :diff-buf-size (or (:diff-buf-size s) 0)}
               (:ref-type s) (assoc :ref-type (:ref-type s))
               bdesc (assoc :boundary bdesc)))))

;; ---------------------------------------------------------------------------
;; node->map — the CONTENT projection (for content-addressing). Branching-factor/diff-buf are
;; deliberately absent here (config, not content); `node->blob` appends them.
;; ---------------------------------------------------------------------------

(defn node->map
  "Project a PSS node to the map that gets STORED. Leaf → {:keys … (:measure)}; Branch →
   {:level :keys :addresses :subtree-count (:measure) (:slots)}. Comparator/storage/settings-free;
   element values stay raw. `node->blob` appends :branching-factor/:diff-buf-size/:ref-type so a
   read self-describes.

   DO NOT HASH THIS MAP FOR A CONTENT ADDRESS — use `node->identity` instead.

   An earlier version of this docstring said \"for content-addressing (hash this map)\", and that
   is wrong: two of these keys are CACHES, not content, so the same logical node projects
   differently depending on what happened to it earlier.

     :measure        null until something forces it, and `forceComputeMeasure` ASSIGNS — so a
                     read-only aggregate query changes what the node later serializes as. Two
                     structurally identical leaves, one warm and one cold, produce
                     {:keys [...] :measure ...} and {:keys [...]}.
     :subtree-count  written raw from `Branch.subtreeCount()`, which is -1 (\"unknown\") whenever
                     a child's count was unavailable. Measured: 2 of the branches on a second
                     commit over 200 elements at bf 8 wrote -1. Read-back normalises it, so it
                     is not corruption — but it is an address change.

   Hashing this map therefore gives an address that is not a function of the node's content:
   measured, the same set under the same operation produced 0 of 4 addresses in common between
   two stores that differed only in which caches happened to be populated. Dedup and cross-peer
   sharing are lost exactly where they are wanted.

   Both real consumers already avoid this, independently, which is the strongest evidence the
   old advice was the bug: datahike hashes `[addresses (canon slots)]` for a Branch and the
   element vector for a Leaf; stratum hashes the address vector and the leaf's chunk keys.
   `node->identity` is that subset, named."
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

(defn node->identity
  "The CONTENT of a node, for a content address: hash THIS, not `node->map`.

   Leaf → {:keys …}; Branch → {:level :keys :addresses (:slots)}.

   Deliberately excludes `:measure` and `:subtree-count`: both are caches that can be present or
   absent for the same logical node (see `node->map`), and both are recomputable — a leaf's
   measure from its own keys, a branch's count from its children. Including them makes the
   address depend on warmth rather than on content.

   Deliberately INCLUDES `:slots`. Buffered diffs are content, not cache: a buffered child is
   stored as `anchor + diff`, so two trees can share every child address and differ only in
   their diffs. A hash that omits slots collides on logically different trees, and a tampered
   diff would be invisible to a merkle audit. (datahike folds slots in for exactly this reason;
   stratum does not, which is safe only while it leaves diff-buf off.)

   Deliberately excludes :branching-factor/:diff-buf-size/:ref-type — configuration, not content
   — which `node->blob` carries separately so a read self-describes."
  [node]
  (let [m (select-keys (node->map node) [:level :keys :addresses :slots])]
    (cond-> m
      (:slots m)
      ;; STRIP the per-slot caches. `slotsForStorage` writes `:count` and `:measure` into
      ;; every slot entry, and that `:measure` is `child.measure()` captured at deposit time
      ;; — exactly the null-until-forced cache this projection excludes at the top level. An
      ;; earlier version kept the slot map whole and so re-admitted them one level down:
      ;; measured, two sets with identical content and identical operations, differing only
      ;; in whether a READ-ONLY `set/measure` query ran first, gave
      ;;     cold slot measures [[0 nil]]
      ;;     warm slot measures [[0 NumericStats{count=13, sum=665.0, ...}]]
      ;; and hashed to 1979634333 vs 1940705680.
      ;;
      ;; `:diff` and `:max-key` STAY — the buffered diff is content (a buffered child is
      ;; stored as anchor + diff, so omitting it collides logically different trees) and the
      ;; separator is what the diff is keyed against.
      (update :slots
              (fn [slots]
                (reduce-kv (fn [acc k entry]
                             (assoc acc k (select-keys entry [:diff :max-key])))
                           (empty slots) slots))))))

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
                  ;; `(or … 0)` for the same reason as in `node-config` — match the JVM,
                  ;; which writes an int here, so a root relayed through a cljs peer keeps
                  ;; the bytes it arrived with.
                  :diff-buf-size    (or (:diff-buf-size s) 0)}
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

(defn- resolve-or-throw
  "Look `id-key` up in `registry`, distinguishing the two nil cases.

   NO id in the root's meta means the writer deliberately had none — a lexically-scoped
   serializer, or a root with no storage — so nil is the answer.

   An id that IS present but unregistered is a MISCONFIGURATION, and returning nil for it
   used to defer the failure: `blob->root` accepted the nil, decode reported success, and the
   first traversal called a method on nil somewhere inside the tree walk. The result was an
   opaque NullPointerException at a point arbitrarily far from the cause, in a lazy structure
   where the cause (a store not registered, or unregistered early) is invisible. Failing here
   names the id and the registry."
  [registry meta id-key what]
  (if-let [id (get meta id-key)]
    (or (get @registry id)
        (throw (ex-info (str "PSS: no " what " registered for " id-key " " (pr-str id)
                             " — register it before deserializing this root")
                        {:type ::unregistered :id id :id-key id-key
                         :registered (vec (keys @registry))})))
    nil))

(defn registry-storage-resolver "Wire resolver: storage by (:pss/storage-id meta). Throws on an id that is present but unregistered."
  [] (fn [meta] (resolve-or-throw storage-registry meta storage-id-key "storage")))
(defn registry-cmp-resolver     "Wire resolver: comparator by (:pss/comparator-id meta). Throws on an id that is present but unregistered."
  [] (fn [meta] (resolve-or-throw comparator-registry meta comparator-id-key "comparator")))
(defn registry-measure-resolver "Wire resolver: measure-ops by (:pss/measure-id meta). Throws on an id that is present but unregistered."
  [] (fn [meta] (resolve-or-throw measure-registry meta measure-id-key "measure")))
