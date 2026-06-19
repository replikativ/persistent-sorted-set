(ns org.replikativ.persistent-sorted-set.fressian
  "Canonical, OPTIONAL Fressian read/write handlers for PSS B-tree NODES — one wire
   form shared by every konserve/kabel-backed consumer (datahike, yggdrasil, proximum,
   stratum). On a single websocket there is exactly one write-handler per type, so a
   shared node codec is forced anyway; centralizing it here gives one wire form (causal
   ordering on one socket) and one place that fixes the JVM/cljs node-shape drift.

   SCOPE — NODES ONLY. A node carries neither a comparator nor storage nor the set's
   identity, so these handlers need none of those. The canonical store-map (the format
   `IStorage.store`/`restore` already prescribe, and the library's own reference storage
   uses) is:

     pss/leaf   → {:keys <elements>}
     pss/branch → {:level n :keys <separators> :addresses <child-addrs> :slots? <diff-buf>}

   The comparator lives on the ROOT and is re-stamped lazily on descent; storage is the
   root's IStorage; the root/record handler (datahike DB, yggdrasil CRDT, …) is the
   CONSUMER's. So the only parameter is `settings` (branching-factor + measure-ops),
   supplied at read time — `(read-handlers settings)`.

   ELEMENT-AGNOSTIC. `:keys`, the diff-buf `:slots`, and the slot `:max-key`/`:absent`/
   `:present` values are the consumer's domain elements (Datoms, ChunkEntry, …); they
   recurse through the writer using the consumer's OWN element handlers. This ns never
   looks at element types.

   diff-buf (opt-in, Settings.diffBufSize>0): a Branch additionally carries
   `:slots {idx -> {:count :measure :diff :max-key}}`, where a leaf child's `:diff` is the
   comparator-agnostic `{:absent [..] :present [..]}` form (`Slot/leafDiffForStorage`,
   produced by `slotsForStorage`). All plain data; reconstructed into `_slots` after the
   node is built (`anchor = addresses[idx]`), exactly as the reference storage does. At
   diffBufSize=0 `_slots` is nil, so `:slots` is absent and the form is baseline-identical.

   subtree-count and measure are NOT serialized — they recompute lazily on restore (the
   library's reference codec relies on the same), keeping the wire minimal and identical
   across platforms. cljs `keys`/`addresses` are JS arrays; normalized here once.

   USAGE (consumer merges these into its own fressian handler atoms, alongside its element
   + root/record handlers):

     ;; JVM (clojure.data.fressian): {Class {\"tag\" WriteHandler}} / {\"tag\" ReadHandler}
     (fress/create-writer out :handlers
       (-> (merge fress/clojure-write-handlers write-handlers my-element-write-handlers)
           fress/associative-lookup fress/inheritance-lookup))
     (fress/create-reader in :handlers
       (-> (merge fress/clojure-read-handlers (read-handlers settings) my-element-read-handlers)
           fress/associative-lookup))"
  #?(:cljs (:require [fress.api :as fress]
                     [org.replikativ.persistent-sorted-set.impl.node :as node]
                     [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]]
                     [org.replikativ.persistent-sorted-set.branch :refer [Branch] :as branch]
                     [org.replikativ.persistent-sorted-set.btset :refer [BTSet]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set ANode Leaf Branch Settings Slot
                    PersistentSortedSet]
                   [org.fressian.handlers WriteHandler ReadHandler]
                   [java.util List])))

(def ^:const leaf-tag "pss/leaf")
(def ^:const branch-tag "pss/branch")
(def ^:const set-tag "pss/set")

;; ---------------------------------------------------------------------------
;; Write handlers — Leaf/Branch → canonical store-map. Element values inside
;; :keys / :slots recurse through the consumer's own handlers.
;; ---------------------------------------------------------------------------

(defn node->map
  "Project a PSS node to its canonical, serializable store-map — exactly the value the
   write handler emits. PUBLIC so a consumer can CONTENT-ADDRESS a node (hash this map)
   without serializing it, or store the map directly. Leaf → {:keys …}; Branch →
   {:level :keys :addresses :subtree-count (:measure) (:slots)}. `:measure` /
   `:subtree-count` carried when present so a restored node needs no recompute.
   Comparator/storage-free; element values stay raw (they recurse / hash through the
   consumer's own handlers)."
  [node]
  #?(:clj
     ;; `vec` the keys/addresses: the raw trimmed Java List (Arrays$ArrayList) isn't
     ;; hash-coercible (hasch) and differs from the cljs vector form — a Clojure vector
     ;; is both, and fressian reads either back as a vector, so round-trip is identical.
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
  "Fressian write handlers for PSS nodes. JVM: {Class {tag WriteHandler}} (for
   clojure.data.fressian's associative+inheritance lookup). cljs: {Type fn} (for fress).
   Merge into the consumer's write-handler map next to its element handlers. Both
   leaf+branch emit `node->map` under their canonical tag."
  #?(:clj
     {Leaf   {leaf-tag   (reify WriteHandler (write [_ w leaf] (.writeTag w leaf-tag 1)   (.writeObject w (node->map leaf))))}
      Branch {branch-tag (reify WriteHandler (write [_ w node] (.writeTag w branch-tag 1) (.writeObject w (node->map node))))}}
     :cljs
     {Leaf   (fn [w leaf] (fress/write-tag w leaf-tag 1)   (fress/write-object w (node->map leaf)))
      Branch (fn [w node] (fress/write-tag w branch-tag 1) (fress/write-object w (node->map node)))}))

;; ---------------------------------------------------------------------------
;; Read handlers — canonical store-map → Leaf/Branch, with `settings` injected
;; and diff-buf `_slots` re-attached (anchor = the child's address).
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
  "Fressian read handlers for PSS nodes, closed over the consumer's `settings`
   (JVM: a `Settings`; cljs: a settings map like {:branching-factor n :measure …}).
   Returns {tag handler}; merge into the consumer's read-handler map. Comparator-free:
   the comparator is stamped on the root and projected lazily on descent."
  [settings]
  #?(:clj
     (let [^Settings settings settings]
       {leaf-tag
        (reify ReadHandler
          (read [_ rdr _tag _n]
            (let [{:keys [keys measure]} (.readObject rdr)
                  l (Leaf. ^List keys settings)]
              (when (some? measure) (set! (.-_measure ^ANode l) measure))
              l)))
        branch-tag
        (reify ReadHandler
          (read [_ rdr _tag _n]
            (let [{:keys [level keys addresses subtree-count measure slots]} (.readObject rdr)
                  b (Branch. (int level) ^List keys ^List addresses settings)]
              ;; restore the cached count (−1 ⇒ recompute lazily); _subtreeCount is public.
              (set! (.-_subtreeCount b) (long (or subtree-count -1)))
              (when (some? measure) (set! (.-_measure ^ANode b) measure))
              (when slots (attach-slots! b addresses slots))
              b)))})
     :cljs
     {leaf-tag
      (fn [rdr _tag _n]
        (let [{:keys [keys measure]} (fress/read-object rdr)]
          (Leaf. (to-array keys) settings measure)))
      branch-tag
      (fn [rdr _tag _n]
        (let [{:keys [level keys addresses subtree-count measure slots]} (fress/read-object rdr)
              node (branch/from-map {:level         level
                                     :keys          (to-array keys)
                                     :addresses     (to-array addresses)
                                     :subtree-count subtree-count   ; from-map: (or subtree-count -1)
                                     :measure       measure
                                     :settings      settings})]
          (when slots (attach-slots! node addresses slots))
          node))}))

;; ---------------------------------------------------------------------------
;; ROOT (PersistentSortedSet / BTSet) handlers + the reconstruction SCOPE.
;;
;; Like the nodes, the ROOT write handler keys on ONE type — so on a shared serializer
;; there can be exactly one. Hence it too is canonical: `root-write-handler` emits
;; `{:meta :address :count}` under `pss/set` for everyone.
;;
;; Unlike a node, reconstructing a root needs three things its bytes can't carry — a live
;; `IStorage` (for lazy child loads), a `Settings` (branching-factor / diff-buf / measure-ops),
;; and a comparator. These form the per-store SCOPE = `{:storage :settings :resolve-cmp}`.
;; Storage is a LIVE object, so a deserialized root can only find it by an id at runtime; that
;; same id resolves the WHOLE scope. So a root carries `:store-id` in its meta, and
;; `root-read-handler` resolves the scope from `scope-registry` by that id. (Settings is NOT
;; serialized into the root — that would let the root's settings diverge from its nodes'; one
;; id → one scope keeps them identical. The settings DATA lives in the consumer's config; the
;; FUNCTION parts — comparator via the meta, measure-ops in settings — are resolved by id, never
;; serialized.)
;;
;; A single LOCAL store can instead pass a fixed scope (and skip `:store-id`); a shared/wire peer
;; uses the registry. Roots resolve by id; nodes carry no id and never travel inside a value (they
;; lazy-load store-locally from the storage the root resolves to). On raw konserve-sync replication
;; a node DOES cross, but only transiently — re-stored settings-independently, re-read with the
;; destination store's real settings — so its wire settings never matters.
;; ---------------------------------------------------------------------------

(def ^:const store-id-key
  "Canonical key, in a root's meta, carrying the id of the store/scope it belongs to. A shared/wire
   serializer resolves a root's reconstruction scope by this id. Consumers stamp it at root creation."
  :store-id)

(defonce ^{:doc "store-id → scope `{:storage :settings :resolve-cmp}`. A root reader on a shared/wire
   serializer resolves a root's reconstruction context by `(:store-id meta)`. `register-scope!` each
   participating store before reading its roots; `unregister-scope!` on release."}
  scope-registry (atom {}))

(defn register-scope!
  "Register a store's reconstruction scope under `id`. scope = `{:storage :settings :resolve-cmp}`;
   `:resolve-cmp` is `(fn [meta] -> Comparator)` (defaults to `(constantly nil)` when absent)."
  [id scope] (swap! scope-registry assoc id scope) scope)
(defn unregister-scope! [id] (swap! scope-registry dissoc id) nil)
(defn registered-scope  [id] (get @scope-registry id))

(defn root-write-handler
  "Canonical write handler for a PSS root. One per the root type, so consumers on a
   shared serializer don't conflict. Emits `{:meta :address :count}` under `pss/set`;
   the set MUST be flushed (root address realized) first."
  []
  #?(:clj
     (reify WriteHandler
       (write [_ w pset]
         (when (nil? (.-_address ^PersistentSortedSet pset))
           (throw (ex-info "PSS root must be flushed before serialization" {:type :must-be-flushed})))
         (.writeTag w set-tag 1)
         (.writeObject w {:meta    (meta pset)
                          :address (.-_address ^PersistentSortedSet pset)
                          :count   (count pset)})))
     :cljs
     (fn [w pset]
       (when (nil? (.-address pset))
         (throw (ex-info "PSS root must be flushed before serialization" {:type :must-be-flushed})))
       (fress/write-tag w set-tag 1)
       (fress/write-object w {:meta    (meta pset)
                              :address (.-address pset)
                              :count   (count pset)}))))

(def root-write-handlers
  "Pre-keyed root write handler, shaped like `write-handlers` so a cljc consumer can
   `(merge write-handlers root-write-handlers …)` WITHOUT importing the root type itself.
   JVM: {PersistentSortedSet {tag WriteHandler}}; cljs: {BTSet fn}."
  #?(:clj  {PersistentSortedSet {set-tag (root-write-handler)}}
     :cljs {BTSet (root-write-handler)}))

(defn root-read-handler
  "Canonical read handler for a PSS root. Reconstructs a lazy root from `{:meta :address :count}`,
   resolving its SCOPE — `{:storage :settings :resolve-cmp}` — per read.

   Opts:
     :resolve-scope (fn [meta] -> scope). DEFAULT: `(registered-scope (get meta store-id-key))`
                    — a shared/wire serializer. A single local store may pass
                    `(constantly its-own-scope)`.
   Back-compat single-store form (builds a fixed scope from these keys instead of :resolve-scope):
     :settings  :resolve-storage  [:resolve-cmp]."
  ([] (root-read-handler {}))
  ([{:keys [resolve-scope settings resolve-storage resolve-cmp]
     :or {resolve-cmp (constantly nil)}}]
   (let [resolve-scope (cond
                         resolve-scope    resolve-scope
                         resolve-storage  (fn [_meta] {:storage     (resolve-storage _meta)
                                                       :settings    settings
                                                       :resolve-cmp resolve-cmp})
                         :else            (fn [meta] (registered-scope (get meta store-id-key))))]
     #?(:clj
        (reify ReadHandler
          (read [_ rdr _tag _n]
            (let [{:keys [meta address count]} (.readObject rdr)
                  {scope-storage :storage scope-settings :settings
                   scope-cmp :resolve-cmp :or {scope-cmp (constantly nil)}} (resolve-scope meta)]
              (PersistentSortedSet. meta (scope-cmp meta) address scope-storage
                                    nil (int count) scope-settings 0))))
        :cljs
        (fn [rdr _tag _n]
          (let [{:keys [meta address count]} (fress/read-object rdr)
                {scope-storage :storage scope-settings :settings
                 scope-cmp :resolve-cmp :or {scope-cmp (constantly nil)}} (resolve-scope meta)]
            ;; BTSet deftype: [root cnt comparator meta _hash storage address settings]
            (BTSet. nil count (scope-cmp meta) meta nil scope-storage address scope-settings)))))))

;; ---------------------------------------------------------------------------
;; Bundle builders — assemble a consumer's full canonical handler maps in one call.
;; A local STORE passes its real `settings` + a fixed `resolve-scope`; a shared kabel PEER
;; passes a default `settings` (node settings is transient on the wire) + omits `resolve-scope`
;; (⇒ registry by store-id) + the UNION of participants' element handlers (one per type).
;; ---------------------------------------------------------------------------

(defn canonical-read-handlers
  "Full canonical READ handler map: node handlers (pss/leaf + pss/branch, from `settings`) +
   the root handler (pss/set, via `resolve-scope`) + the consumer's `element-read-handlers`.
   Omit `:resolve-scope` to resolve roots from `scope-registry` by `(:store-id meta)`."
  [{:keys [settings resolve-scope element-read-handlers]}]
  (merge (read-handlers settings)
         {set-tag (root-read-handler (cond-> {} resolve-scope (assoc :resolve-scope resolve-scope)))}
         element-read-handlers))

(defn canonical-write-handlers
  "Full canonical WRITE handler map: node + root writes (settings-independent) + the consumer's
   `element-write-handlers`. JVM shape `{Class {tag WH}}`; cljs `{Type fn}`."
  [{:keys [element-write-handlers]}]
  (merge write-handlers root-write-handlers element-write-handlers))
