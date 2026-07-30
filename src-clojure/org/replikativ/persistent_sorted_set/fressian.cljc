(ns org.replikativ.persistent-sorted-set.fressian
  "Canonical, OPTIONAL Fressian read/write handlers for PSS B-tree nodes AND roots — one wire
   form shared by every konserve/kabel-backed consumer (datahike, yggdrasil, proximum, stratum).
   On a single websocket there is exactly one write-handler per type, so a shared codec is forced
   anyway; centralizing it here gives one wire form (causal ordering on one socket) and one place
   that fixes the JVM/cljs drift.

   WHAT TRAVELS vs WHAT IS RESOLVED. A node is a self-describing VALUE: a blob carries plain DATA but
   not live objects or FUNCTIONS. So the split is:

     SERIALIZED (in the blob):  keys / addresses / level / subtree-count / measure-VALUE / slots,
                                the node's own `:branching-factor` + `:diff-buf-size`, and (non-SOFT)
                                `:ref-type` — the caching policy, an enum, hence data.
     RESOLVED AT READ (runtime): the IStorage (live), the comparator (fn), the measure-OPS (fn), and
                                the leaf-processor (fn). These are bound by the consumer at the
                                operating root / its per-store reader, never serialized — and a node
                                never reaches for a function (storage/comparator/leaf-processor are
                                the operating root's, threaded down; measure-ops comes from each
                                store's own reader). So one shared wire can carry many stores' nodes
                                without knowing any store's functions.

   Because `:branching-factor`/`:diff-buf-size`/`:ref-type` ride in the blob, a node is SELF-DESCRIBING:
   its `Settings` are reconstructed per node from the blob (+ the consumer's measure-ops). This lifts
   the one-`Settings`-per-store constraint (a store may hold nodes of different branching factors) and
   means `:ref-type` survives a rootless replication (konserve-sync) — a node loaded by a reader that
   knows nothing about it still caches with the writer's policy; a read-time `:ref-type` can override.
   (None of these are in `node->map`, the content-hash projection, so content addresses are unchanged;
   SOFT — the default — is omitted, so common blobs are byte-unchanged.)

   THE THREE NON-SERIALIZABLE BITS are resolved by the consumer via three resolvers passed to the
   handlers, each `(fn [meta] -> thing)`:
     resolve-storage  — the root's live IStorage.
     resolve-cmp      — the root's comparator (datahike: by `:index-type`; others constant/nil).
     resolve-measure  — the IMeasure ops (nil for measure-less consumers).
   Two ways to supply them:
     LEXICAL (a serializer that owns ONE store): close over that store's storage/cmp/measure — no
       ids needed, old roots (without ids) just read. The convenient default.
     REGISTRY (a shared/wire serializer over MANY stores): `(registry-storage-resolver)` etc. resolve
       by an id the root stamps in its meta (`:pss/storage-id` / `:pss/comparator-id` / `:pss/measure-id`) from
       the registries in `impl.nodes`. The consumer `register-*!`s its storage (per-connect) and its
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
   or a wire peer: pass `(registry-storage-resolver)`/`(registry-cmp-resolver)`/`(registry-measure-resolver)`.

   NOTE ON STRUCTURE. Everything above the tag plumbing — the projections, the settings
   reconstruction, the slots re-attachment, the registries — now lives in
   `org.replikativ.persistent-sorted-set.impl.nodes` and is SHARED with the boring (CBOR) and
   transit handler modules. Three copies of that logic would reintroduce precisely the drift this
   namespace was written to prevent. The names below are re-exported so existing consumers are
   unaffected."
  (:require [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
  #?(:cljs (:require [fress.api :as fress]
                     [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]]
                     [org.replikativ.persistent-sorted-set.branch :refer [Branch]]
                     [org.replikativ.persistent-sorted-set.btset :refer [BTSet]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set Leaf Branch PersistentSortedSet]
                   [org.fressian.handlers WriteHandler ReadHandler])))

;; ---------------------------------------------------------------------------
;; Re-exports. These were this namespace's public API before the shared core was
;; extracted; consumers (datahike) reference them by this name.
;; ---------------------------------------------------------------------------

(def ^:const leaf-tag   nodes/leaf-tag)
(def ^:const branch-tag nodes/branch-tag)
(def ^:const set-tag    nodes/set-tag)

(def ^:const storage-id-key    nodes/storage-id-key)
(def ^:const comparator-id-key nodes/comparator-id-key)
(def ^:const measure-id-key    nodes/measure-id-key)

(def node->map
  "See `impl.nodes/node->map` — the CONTENT projection used for content-addressing."
  nodes/node->map)

(def node-config
  "See `impl.nodes/node-config` — the node's SERIALIZABLE settings."
  nodes/node-config)

(def node->blob
  "See `impl.nodes/node->blob` — what a write handler actually emits."
  nodes/node->blob)

(def storage-registry    nodes/storage-registry)
(def comparator-registry nodes/comparator-registry)
(def measure-registry    nodes/measure-registry)

(def register-storage!      nodes/register-storage!)
(def unregister-storage!    nodes/unregister-storage!)
(def registered-storage     nodes/registered-storage)
(def register-comparator!   nodes/register-comparator!)
(def unregister-comparator! nodes/unregister-comparator!)
(def registered-comparator  nodes/registered-comparator)
(def register-measure!      nodes/register-measure!)
(def unregister-measure!    nodes/unregister-measure!)
(def registered-measure     nodes/registered-measure)

(def registry-storage-resolver nodes/registry-storage-resolver)
(def registry-cmp-resolver     nodes/registry-cmp-resolver)
(def registry-measure-resolver nodes/registry-measure-resolver)

;; ---------------------------------------------------------------------------
;; Write handlers — tag plumbing over `nodes/node->blob`.
;; ---------------------------------------------------------------------------

(def write-handlers
  "Fressian write handlers for PSS nodes. JVM: {Class {tag WriteHandler}}; cljs: {Type fn}. Each
   emits `node->blob` — the content projection PLUS the node's own settings, so reads self-describe."
  #?(:clj
     {Leaf   {nodes/leaf-tag   (reify WriteHandler (write [_ w leaf] (.writeTag w nodes/leaf-tag 1)   (.writeObject w (nodes/node->blob leaf))))}
      Branch {nodes/branch-tag (reify WriteHandler (write [_ w node] (.writeTag w nodes/branch-tag 1) (.writeObject w (nodes/node->blob node))))}}
     :cljs
     {Leaf   (fn [w leaf] (fress/write-tag w nodes/leaf-tag 1)   (fress/write-object w (nodes/node->blob leaf)))
      Branch (fn [w node] (fress/write-tag w nodes/branch-tag 1) (fress/write-object w (nodes/node->blob node)))}))

(defn read-handlers
  "Fressian read handlers for PSS nodes. Each node's `Settings` are reconstructed from its blob's
   `:branching-factor`/`:diff-buf-size` (falling back to `:default-bf`/0 for pre-bf blobs) + the
   consumer's `:measure-ops` (non-serializable IMeasure, nil for most). `:ref-type` overrides the
   blob's serialized ref-type (default: use the blob, falling back to SOFT).
   Returns {tag handler}. Comparator-free (the comparator lives on the root)."
  [opts]
  (let [ctx (nodes/reader-context opts)]
    #?(:clj
       {nodes/leaf-tag
        (reify ReadHandler (read [_ rdr _tag _n] (nodes/blob->leaf ctx (.readObject rdr))))
        nodes/branch-tag
        (reify ReadHandler (read [_ rdr _tag _n] (nodes/blob->branch ctx (.readObject rdr))))}
       :cljs
       {nodes/leaf-tag   (fn [rdr _tag _n] (nodes/blob->leaf ctx (fress/read-object rdr)))
        nodes/branch-tag (fn [rdr _tag _n] (nodes/blob->branch ctx (fress/read-object rdr)))})))

;; ---------------------------------------------------------------------------
;; ROOT handlers.
;; ---------------------------------------------------------------------------

(defn root-write-handler
  "Canonical write handler for a PSS root → `{:meta :address :count :branching-factor :diff-buf-size}`
   under `pss/set`. `:meta` carries whatever ids the consumer stamped (`:pss/storage-id` etc.). The set
   MUST be flushed (root address realized) first — `nodes/root->blob` throws otherwise."
  []
  #?(:clj
     (reify WriteHandler
       (write [_ w pset]
         (.writeTag w nodes/set-tag 1)
         (.writeObject w (nodes/root->blob pset))))
     :cljs
     (fn [w pset]
       (fress/write-tag w nodes/set-tag 1)
       (fress/write-object w (nodes/root->blob pset)))))

(def root-write-handlers
  "Pre-keyed root write handler, shaped like `write-handlers` so a cljc consumer merges it WITHOUT
   importing the root type. JVM: {PersistentSortedSet {tag WriteHandler}}; cljs: {BTSet fn}."
  #?(:clj  {PersistentSortedSet {nodes/set-tag (root-write-handler)}}
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
  ([opts]
   (let [ctx (nodes/reader-context opts)]
     #?(:clj  (reify ReadHandler (read [_ rdr _tag _n] (nodes/blob->root ctx (.readObject rdr))))
        :cljs (fn [rdr _tag _n] (nodes/blob->root ctx (fress/read-object rdr)))))))

;; ---------------------------------------------------------------------------
;; Bundle builders — assemble a consumer's full canonical handler maps in one call.
;; ---------------------------------------------------------------------------

(defn canonical-read-handlers
  "Full canonical READ handler map: node handlers (pss/leaf + pss/branch) + the root handler
   (pss/set) + the consumer's `:element-read-handlers`. `:measure-ops` is the node-level IMeasure
   (nil for most); `:resolve-storage`/`:resolve-cmp`/`:resolve-measure` resolve the root's
   non-serializable bits (lexical closures for a one-store serializer, `registry-*-resolver`s for a
   wire peer); `:default-bf` is the pre-bf fallback."
  [{:keys [element-read-handlers] :as opts}]
  (merge (read-handlers opts)
         {nodes/set-tag (root-read-handler opts)}
         element-read-handlers))

(defn canonical-write-handlers
  "Full canonical WRITE handler map: node + root writes + the consumer's `:element-write-handlers`.
   JVM shape `{Class {tag WH}}`; cljs `{Type fn}`."
  [{:keys [element-write-handlers]}]
  (merge write-handlers root-write-handlers element-write-handlers))
