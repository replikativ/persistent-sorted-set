(ns org.replikativ.persistent-sorted-set.transit
  "Canonical, OPTIONAL transit read/write handlers for PSS B-tree nodes AND roots.

   The wire CONTENT is identical to the Fressian and boring modules' — all three write
   `impl.nodes/node->blob` and read it back through `impl.nodes/blob->leaf` / `blob->branch` /
   `blob->root`. Only the framing differs. See `org.replikativ.persistent-sorted-set.fressian`
   for the full contract (what travels vs what is resolved at read, the three registries,
   diff-buf slots); none of that is repeated here because none of it is format-specific.

   Transit uses STRING tags, so the tag names are the same `pss/leaf` / `pss/branch` / `pss/set`
   strings the Fressian module uses.

   ## Usage

       (require '[cognitect.transit :as transit]
                '[org.replikativ.persistent-sorted-set.transit :as pss-transit])

       (transit/writer out :json
         {:handlers (pss-transit/write-handlers)})

       (transit/reader in :json
         {:handlers (pss-transit/read-handlers
                      {:resolve-storage (fn [_] my-storage)
                       :resolve-cmp     my-cmp-fn
                       :default-bf      512})})

   Merge your own element handlers (Datoms and friends) into those maps; `:keys`, slot values
   and slot `:diff`s recurse through them exactly as under Fressian.

   ## A caution specific to transit

   Transit's `:json` mode stringifies map keys. A node blob's keys are all keywords and its
   values are the consumer's domain elements, so this is only a concern if your ELEMENT
   handlers rely on non-scalar map keys. `:msgpack` and the `:json-verbose` modes do not have
   this constraint. Nothing in the PSS blob itself is affected."
  (:require [cognitect.transit :as transit]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
  #?(:cljs (:require [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]]
                     [org.replikativ.persistent-sorted-set.branch :refer [Branch]]
                     [org.replikativ.persistent-sorted-set.btset :refer [BTSet]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set Leaf Branch PersistentSortedSet])))

(def ^:const leaf-tag   nodes/leaf-tag)
(def ^:const branch-tag nodes/branch-tag)
(def ^:const set-tag    nodes/set-tag)

;; ---------------------------------------------------------------------------
;; Write handlers
;; ---------------------------------------------------------------------------

(defn node-write-handlers
  "Transit WRITE handlers for PSS nodes, as `{Type handler}`. Each emits `node->blob` — the
   content projection PLUS the node's own settings, so reads self-describe."
  []
  {Leaf   (transit/write-handler (constantly leaf-tag)   nodes/node->blob)
   Branch (transit/write-handler (constantly branch-tag) nodes/node->blob)})

(defn root-write-handlers
  "Transit WRITE handler for a PSS root, as `{Type handler}`. The set MUST be flushed first —
   `nodes/root->blob` throws otherwise, rather than emitting a blob with a nil address that
   would read back broken."
  []
  {#?(:clj PersistentSortedSet :cljs BTSet)
   (transit/write-handler (constantly set-tag) nodes/root->blob)})

(defn write-handlers
  "Full canonical WRITE handler map: nodes + root. Merge your `:element-write-handlers` in."
  ([] (write-handlers {}))
  ([{:keys [element-write-handlers]}]
   (merge (node-write-handlers) (root-write-handlers) element-write-handlers)))

;; ---------------------------------------------------------------------------
;; Read handlers
;; ---------------------------------------------------------------------------

(defn node-read-handlers
  "Transit READ handlers for PSS nodes, as `{tag handler}`.

   `opts` are `impl.nodes/reader-context`'s: `:measure-ops`, `:default-bf`, `:ref-type`. The
   context is built ONCE here, so its memoized settings cache is shared across every node this
   reader decodes."
  [opts]
  (let [ctx (nodes/reader-context opts)]
    {leaf-tag   (transit/read-handler (fn [blob] (nodes/blob->leaf ctx blob)))
     branch-tag (transit/read-handler (fn [blob] (nodes/blob->branch ctx blob)))}))

(defn root-read-handlers
  "Transit READ handler for a PSS root, as `{tag handler}`.

   `opts` additionally take `:resolve-storage` / `:resolve-cmp` / `:resolve-measure`, each
   `(fn [meta] -> thing)` — lexical closures for a one-store serializer, or the
   `impl.nodes/registry-*-resolver`s for a wire peer."
  [opts]
  (let [ctx (nodes/reader-context opts)]
    {set-tag (transit/read-handler (fn [blob] (nodes/blob->root ctx blob)))}))

(defn read-handlers
  "Full canonical READ handler map: nodes + root + the consumer's `:element-read-handlers`."
  [{:keys [element-read-handlers] :as opts}]
  (merge (node-read-handlers opts)
         (root-read-handlers opts)
         element-read-handlers))
