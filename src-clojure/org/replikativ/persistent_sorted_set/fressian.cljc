(ns org.replikativ.persistent-sorted-set.fressian
  "Canonical, OPTIONAL Fressian read/write handlers for PSS B-tree nodes AND roots — one wire
   form shared by every konserve/kabel-backed consumer (datahike, yggdrasil, proximum, stratum).
   On a single websocket there is exactly one write-handler per type, so a shared codec is forced
   anyway; centralizing it here gives one wire form (causal ordering on one socket) and one place
   that fixes the JVM/cljs drift.

   WHAT TRAVELS vs WHAT IS RESOLVED. A node/root blob can carry plain data but not live objects or
   functions, so the split is:

     SERIALIZED (in the blob):  keys / addresses / level / subtree-count / measure-VALUE / slots,
                                and the node's own `:branching-factor` + `:diff-buf-size`.
     RESOLVED AT READ (runtime): the IStorage (live), the comparator (fn), the measure-OPS (fn).

   Because `:branching-factor`/`:diff-buf-size` ride in the blob, a node is SELF-DESCRIBING for its
   structure — its `Settings` are reconstructed per node from the blob (+ a default ref-type + the
   consumer's measure-ops). This lifts the old one-`Settings`-per-store constraint: a store may hold
   nodes of different branching factors. (`branching-factor`/`diff-buf-size` are NOT in `node->map`,
   the content-hash projection, so content addresses are unchanged.)

   THE THREE NON-SERIALIZABLE BITS are resolved by the consumer via three resolvers passed to the
   handlers, each `(fn [meta] -> thing)`:
     resolve-storage  — the root's live IStorage.
     resolve-cmp      — the root's comparator (datahike: by `:index-type`; others constant/nil).
     resolve-measure  — the IMeasure ops (nil for measure-less consumers).
   Two ways to supply them:
     LEXICAL (a serializer that owns ONE store): close over that store's storage/cmp/measure — no
       ids needed, old roots (without ids) just read. The convenient default.
     REGISTRY (a shared/wire serializer over MANY stores): `(registry-storage-resolver)` etc. resolve
       by an id the root stamps in its meta (`:storage-id` / `:comparator-id` / `:measure-id`) from
       the registries below. The consumer `register-*!`s its storage (per-connect) and its
       comparator/measure (static, ns-load).

   ELEMENT-AGNOSTIC. `:keys`, the diff-buf `:slots`, and slot values are the consumer's domain
   elements (Datoms, ChunkEntry, …); they recurse through the consumer's OWN element handlers.

   diff-buf (opt-in, Settings.diffBufSize>0): a Branch additionally carries
   `:slots {idx -> {:count :measure :diff :max-key}}`; reconstructed into `_slots` after the node is
   built (`anchor = addresses[idx]`). At diffBufSize=0 `:slots` is absent.

   USAGE — a consumer assembles its handler maps via the bundle builders below, e.g. a local store:
     (canonical-read-handlers  {:resolve-storage (fn [_] my-storage) :resolve-cmp my-cmp-fn
                                :measure-ops nil :default-bf 512 :element-read-handlers {…}})
     (canonical-write-handlers {:element-write-handlers {…}})
   or a wire peer: pass `(registry-storage-resolver)`/`(registry-cmp-resolver)`/`(registry-measure-resolver)`."
  #?(:cljs (:require [fress.api :as fress]
                     [org.replikativ.persistent-sorted-set.impl.node :as node]
                     [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]]
                     [org.replikativ.persistent-sorted-set.branch :refer [Branch] :as branch]
                     [org.replikativ.persistent-sorted-set.btset :refer [BTSet]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set ANode Leaf Branch Settings Slot IMeasure
                    PersistentSortedSet]
                   [org.fressian.handlers WriteHandler ReadHandler]
                   [java.util List])))

(def ^:const leaf-tag "pss/leaf")
(def ^:const branch-tag "pss/branch")
(def ^:const set-tag "pss/set")

;; ---------------------------------------------------------------------------
;; Settings (de)construction — the serializable bits (bf, diff-buf) travel; ref-type is a
;; read-time default; measure-ops is supplied by the consumer (never serialized).
;; ---------------------------------------------------------------------------

#?(:clj
   (defn- settings-for ^Settings [bf dbs ^IMeasure measure]
     ;; ref-type left nil ⇒ Settings normalizes to SOFT (a per-process policy, not serialized).
     (Settings. (int bf) nil measure nil (int dbs)))
   :cljs
   (defn- settings-for [bf dbs measure]
     {:branching-factor bf :diff-buf-size dbs :measure measure}))

(defn- node-config
  "A node's SERIALIZABLE settings — branching-factor + diff-buf-size. These ride in the blob
   (self-describing) but NOT in `node->map` (the content hash), so addresses are unchanged."
  [node]
  #?(:clj  (let [^Settings s (.-_settings ^ANode node)]
             {:branching-factor (.branchingFactor s) :diff-buf-size (.diffBufSize s)})
     :cljs (let [s (.-settings node)]
             {:branching-factor (:branching-factor s) :diff-buf-size (:diff-buf-size s)})))

;; ---------------------------------------------------------------------------
;; node->map — the CONTENT projection (for content-addressing). Branching-factor/diff-buf are
;; deliberately absent here (config, not content); the write handler appends them to the blob.
;; ---------------------------------------------------------------------------

(defn node->map
  "Project a PSS node to its canonical CONTENT map — for content-addressing (hash this map) or
   storing the map directly. Leaf → {:keys …}; Branch → {:level :keys :addresses :subtree-count
   (:measure) (:slots)}. Comparator/storage/settings-free; element values stay raw. NOTE: this is
   the CONTENT projection — the serialized blob additionally carries :branching-factor/:diff-buf-size
   (see the write handlers), which are NOT part of the content hash."
  [node]
  #?(:clj
     ;; `vec` the keys/addresses: the raw trimmed Java List isn't hash-coercible (hasch) and differs
     ;; from the cljs vector form — a Clojure vector is both, and fressian reads either back as a vector.
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

(def write-handlers
  "Fressian write handlers for PSS nodes. JVM: {Class {tag WriteHandler}}; cljs: {Type fn}. Each
   emits `node->map` PLUS the node's own :branching-factor/:diff-buf-size (so reads self-describe)."
  #?(:clj
     {Leaf   {leaf-tag   (reify WriteHandler (write [_ w leaf] (.writeTag w leaf-tag 1)   (.writeObject w (merge (node->map leaf) (node-config leaf)))))}
      Branch {branch-tag (reify WriteHandler (write [_ w node] (.writeTag w branch-tag 1) (.writeObject w (merge (node->map node) (node-config node)))))}}
     :cljs
     {Leaf   (fn [w leaf] (fress/write-tag w leaf-tag 1)   (fress/write-object w (merge (node->map leaf) (node-config leaf))))
      Branch (fn [w node] (fress/write-tag w branch-tag 1) (fress/write-object w (merge (node->map node) (node-config node))))}))

;; ---------------------------------------------------------------------------
;; Read handlers — blob → Leaf/Branch, settings reconstructed PER NODE from the blob's
;; bf/diff-buf (+ the consumer's measure-ops + default ref-type); diff-buf `_slots` re-attached.
;; ---------------------------------------------------------------------------

#?(:clj
   (defn- attach-slots!
     "Rebuild a restored Branch's _slots from the stored {idx -> entry} map.
      anchor = addresses[idx] (re-derived, not stored — matches the reference codec)."
     [^Branch b ^List addresses slots]
     (let [arr (object-array (alength (.-_keys b)))]
       (doseq [[idx entry] slots]
         (aset arr (int idx)
               (Slot. (:diff entry) (long (:count entry)) (:measure entry)
                      (nth addresses (int idx)))))
       (set! (.-_slots b) arr)))
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

(defn read-handlers
  "Fressian read handlers for PSS nodes. Each node's `Settings` are reconstructed from its blob's
   `:branching-factor`/`:diff-buf-size` (falling back to `:default-bf`/0 for pre-bf blobs) + the
   consumer's `:measure-ops` (the non-serializable IMeasure, nil for measure-less consumers).
   Returns {tag handler}. Comparator-free (the comparator lives on the root)."
  [{:keys [measure-ops default-bf] :or {default-bf 0}}]
  (let [mk-settings (memoize (fn [bf dbs] (settings-for bf dbs measure-ops)))]
    #?(:clj
       {leaf-tag
        (reify ReadHandler
          (read [_ rdr _tag _n]
            (let [{:keys [keys measure branching-factor diff-buf-size]} (.readObject rdr)
                  l (Leaf. ^List keys (mk-settings (or branching-factor default-bf) (or diff-buf-size 0)))]
              (when (some? measure) (set! (.-_measure ^ANode l) measure))
              l)))
        branch-tag
        (reify ReadHandler
          (read [_ rdr _tag _n]
            (let [{:keys [level keys addresses subtree-count measure slots branching-factor diff-buf-size]} (.readObject rdr)
                  b (Branch. (int level) ^List keys ^List addresses (mk-settings (or branching-factor default-bf) (or diff-buf-size 0)))]
              (set! (.-_subtreeCount b) (long (or subtree-count -1)))
              (when (some? measure) (set! (.-_measure ^ANode b) measure))
              (when slots (attach-slots! b addresses slots))
              b)))}
       :cljs
       {leaf-tag
        (fn [rdr _tag _n]
          (let [{:keys [keys measure branching-factor diff-buf-size]} (fress/read-object rdr)]
            (Leaf. (to-array keys) (mk-settings (or branching-factor default-bf) (or diff-buf-size 0)) measure)))
        branch-tag
        (fn [rdr _tag _n]
          (let [{:keys [level keys addresses subtree-count measure slots branching-factor diff-buf-size]} (fress/read-object rdr)
                node (branch/from-map {:level         level
                                       :keys          (to-array keys)
                                       :addresses     (to-array addresses)
                                       :subtree-count subtree-count
                                       :measure       measure
                                       :settings      (mk-settings (or branching-factor default-bf) (or diff-buf-size 0))})]
            (when slots (attach-slots! node addresses slots))
            node))})))

;; ---------------------------------------------------------------------------
;; The three runtime registries — the non-serializable bits, keyed by an id a root stamps in its
;; meta. A LEXICAL (one-store) serializer can ignore these and close over its own context; a
;; shared/WIRE serializer resolves by these via the `registry-*-resolver` helpers.
;; ---------------------------------------------------------------------------

(def ^:const storage-id-key    :storage-id)
(def ^:const comparator-id-key :comparator-id)
(def ^:const measure-id-key    :measure-id)

(defonce ^{:doc "storage-id → IStorage (live; per-connect lifecycle)."}    storage-registry    (atom {}))
(defonce ^{:doc "comparator-id → Comparator (static fn; ns-load)."}        comparator-registry (atom {}))
(defonce ^{:doc "measure-id → IMeasure (static fn; ns-load, usually empty)."} measure-registry  (atom {}))

(defn register-storage!     [id storage] (swap! storage-registry assoc id storage) storage)
(defn unregister-storage!   [id] (swap! storage-registry dissoc id) nil)
(defn registered-storage    [id] (get @storage-registry id))
(defn register-comparator!  [id cmp] (swap! comparator-registry assoc id cmp) cmp)
(defn unregister-comparator! [id] (swap! comparator-registry dissoc id) nil)
(defn registered-comparator [id] (get @comparator-registry id))
(defn register-measure!     [id m] (swap! measure-registry assoc id m) m)
(defn unregister-measure!   [id] (swap! measure-registry dissoc id) nil)
(defn registered-measure    [id] (get @measure-registry id))

(defn registry-storage-resolver "Wire resolver: storage by (:storage-id meta)."    [] (fn [meta] (registered-storage    (get meta storage-id-key))))
(defn registry-cmp-resolver     "Wire resolver: comparator by (:comparator-id meta)." [] (fn [meta] (registered-comparator (get meta comparator-id-key))))
(defn registry-measure-resolver "Wire resolver: measure-ops by (:measure-id meta)."  [] (fn [meta] (registered-measure    (get meta measure-id-key))))

;; ---------------------------------------------------------------------------
;; ROOT handlers. The root blob = {:meta :address :count :branching-factor :diff-buf-size}; the
;; reconstruction context (storage/cmp/measure) is resolved per read via the consumer's resolvers.
;; ---------------------------------------------------------------------------

(defn root-write-handler
  "Canonical write handler for a PSS root → `{:meta :address :count :branching-factor :diff-buf-size}`
   under `pss/set`. `:meta` carries whatever ids the consumer stamped (`:storage-id` etc.). The set
   MUST be flushed (root address realized) first."
  []
  #?(:clj
     (reify WriteHandler
       (write [_ w pset]
         (when (nil? (.-_address ^PersistentSortedSet pset))
           (throw (ex-info "PSS root must be flushed before serialization" {:type :must-be-flushed})))
         (let [^Settings s (.-_settings ^PersistentSortedSet pset)]
           (.writeTag w set-tag 1)
           (.writeObject w {:meta             (meta pset)
                            :address          (.-_address ^PersistentSortedSet pset)
                            :count            (count pset)
                            :branching-factor (.branchingFactor s)
                            :diff-buf-size    (.diffBufSize s)}))))
     :cljs
     (fn [w pset]
       (when (nil? (.-address pset))
         (throw (ex-info "PSS root must be flushed before serialization" {:type :must-be-flushed})))
       (let [s (.-settings pset)]
         (fress/write-tag w set-tag 1)
         (fress/write-object w {:meta             (meta pset)
                                :address          (.-address pset)
                                :count            (count pset)
                                :branching-factor (:branching-factor s)
                                :diff-buf-size    (:diff-buf-size s)})))))

(def root-write-handlers
  "Pre-keyed root write handler, shaped like `write-handlers` so a cljc consumer merges it WITHOUT
   importing the root type. JVM: {PersistentSortedSet {tag WriteHandler}}; cljs: {BTSet fn}."
  #?(:clj  {PersistentSortedSet {set-tag (root-write-handler)}}
     :cljs {BTSet (root-write-handler)}))

(defn root-read-handler
  "Canonical read handler for a PSS root. Reconstructs a lazy root from
   `{:meta :address :count :branching-factor :diff-buf-size}`, resolving the three non-serializable
   bits per read from the root's `meta` via:
     :resolve-storage (fn [meta] -> IStorage)   — default `(constantly nil)`.
     :resolve-cmp     (fn [meta] -> Comparator) — default `(constantly nil)`.
     :resolve-measure (fn [meta] -> IMeasure)   — default `(constantly nil)`.
   `:default-bf` is the fallback branching-factor for pre-bf root blobs. Pass lexical closures
   (a one-store serializer) or the `registry-*-resolver`s (a shared/wire serializer)."
  ([] (root-read-handler {}))
  ([{:keys [resolve-storage resolve-cmp resolve-measure default-bf]
     :or {resolve-storage (constantly nil) resolve-cmp (constantly nil)
          resolve-measure (constantly nil) default-bf 0}}]
   #?(:clj
      (reify ReadHandler
        (read [_ rdr _tag _n]
          (let [{:keys [meta address count branching-factor diff-buf-size]} (.readObject rdr)
                settings (settings-for (or branching-factor default-bf) (or diff-buf-size 0) (resolve-measure meta))]
            (PersistentSortedSet. meta (resolve-cmp meta) address (resolve-storage meta)
                                  nil (int count) settings 0))))
      :cljs
      (fn [rdr _tag _n]
        (let [{:keys [meta address count branching-factor diff-buf-size]} (fress/read-object rdr)
              settings (settings-for (or branching-factor default-bf) (or diff-buf-size 0) (resolve-measure meta))]
          ;; BTSet deftype: [root cnt comparator meta _hash storage address settings]
          (BTSet. nil count (resolve-cmp meta) meta nil (resolve-storage meta) address settings))))))

;; ---------------------------------------------------------------------------
;; Bundle builders — assemble a consumer's full canonical handler maps in one call.
;; ---------------------------------------------------------------------------

(defn canonical-read-handlers
  "Full canonical READ handler map: node handlers (pss/leaf + pss/branch) + the root handler
   (pss/set) + the consumer's `:element-read-handlers`. `:measure-ops` is the node-level IMeasure
   (nil for most); `:resolve-storage`/`:resolve-cmp`/`:resolve-measure` resolve the root's
   non-serializable bits (lexical closures for a one-store serializer, `registry-*-resolver`s for a
   wire peer); `:default-bf` is the pre-bf fallback."
  [{:keys [resolve-storage resolve-cmp resolve-measure measure-ops default-bf element-read-handlers]
    :or {default-bf 0}}]
  (merge (read-handlers {:measure-ops measure-ops :default-bf default-bf})
         {set-tag (root-read-handler {:resolve-storage resolve-storage
                                      :resolve-cmp     resolve-cmp
                                      :resolve-measure resolve-measure
                                      :default-bf      default-bf})}
         element-read-handlers))

(defn canonical-write-handlers
  "Full canonical WRITE handler map: node + root writes + the consumer's `:element-write-handlers`.
   JVM shape `{Class {tag WH}}`; cljs `{Type fn}`."
  [{:keys [element-write-handlers]}]
  (merge write-handlers root-write-handlers element-write-handlers))
