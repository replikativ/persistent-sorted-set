(ns org.replikativ.persistent-sorted-set.boring
  "Canonical, OPTIONAL boring (CBOR) read/write handlers for PSS B-tree nodes AND roots.

   The wire CONTENT is identical to the Fressian module's — both write
   `impl.nodes/node->blob` and read it back through `impl.nodes/blob->leaf` /
   `blob->branch` / `blob->root`. Only the framing differs. See
   `org.replikativ.persistent-sorted-set.fressian` for the full contract
   (what travels vs what is resolved at read, the three registries, diff-buf
   slots); none of that is repeated here because none of it is format-specific.

   ## Framing: CBOR tag 27, not a private tag

   Nodes and roots ride **tag 27** — IANA's registered \"serialised
   language-independent object with type name and constructor arguments\",
   which is precisely what a PSS node is. The wire form is

       27([\"pss/leaf\", {:keys [...] :branching-factor 512 ...}])

   so the tag NAMES stay the same strings the Fressian module uses, and a
   consumer's mental model does not change with the format.

   This is deliberate and worth stating: the alternative was three private tag
   numbers from CBOR's First-Come-First-Served range. Those would each need an
   IANA registration to be safe from collision, and boring already carries one
   unregistered provisional tag (39649, shaped arrays). Riding a registered,
   standard tag costs nothing here and adds no new claim on the tag space.

   It also degrades well. A reader with no PSS handlers installed decodes a
   node to a `boring.data/UnknownRecord` carrying the same name and fields —
   inspectable, and re-encodable to identical bytes — rather than failing or,
   worse, silently misreading.

   ## Usage

   boring registries are immutable VALUES, so handlers are threaded in rather
   than merged from maps:

       (require '[boring.core :as boring]
                '[org.replikativ.persistent-sorted-set.boring :as pss-boring])

       (def registry
         (-> (boring/tag-registry)
             (pss-boring/install {:resolve-storage (fn [_] my-storage)
                                  :resolve-cmp     my-cmp-fn
                                  :default-bf      512})
             (boring/register-record \"my.ns.Datom\" map->Datom)))

       (boring/encode a-flushed-set {:registry registry})
       (boring/decode bytes         {:registry registry})

   Element handlers (Datoms and friends) are the consumer's own and are
   registered on the same registry — `:keys`, slot values and slot `:diff`s
   recurse through them exactly as under Fressian."
  (:require [boring.core :as boring]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
  #?(:cljs (:require [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]]
                     [org.replikativ.persistent-sorted-set.branch :refer [Branch]]
                     [org.replikativ.persistent-sorted-set.btset :refer [BTSet]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set Leaf Branch PersistentSortedSet])))

(def ^:const leaf-tag   nodes/leaf-tag)
(def ^:const branch-tag nodes/branch-tag)
(def ^:const set-tag    nodes/set-tag)

;; CBOR tag 27 — "serialised language-independent object with type name and
;; constructor arguments". Registering a WRITER for it emits the frame; the
;; READ side is boring's own built-in tag-27 path, which looks the type name up
;; among the registered records. Hence `nil` for every read-fn below: passing
;; one would replace that built-in dispatch wholesale.
(def ^:const generic-object-tag 27)

(defn- tagged
  "A tag-27 payload: [type-name field-map]."
  [tag-name blob]
  [tag-name blob])

;; ---------------------------------------------------------------------------
;; Write side
;; ---------------------------------------------------------------------------

(defn install-node-writers
  "Register Leaf/Branch WRITE handlers. Returns a NEW registry."
  [reg]
  (-> reg
      (boring/register-tag generic-object-tag
                           #?(:clj Leaf :cljs Leaf)
                           (fn [leaf] (tagged leaf-tag (nodes/node->blob leaf)))
                           nil)
      (boring/register-tag generic-object-tag
                           #?(:clj Branch :cljs Branch)
                           (fn [node] (tagged branch-tag (nodes/node->blob node)))
                           nil)))

(defn install-root-writer
  "Register the PSS root WRITE handler. Returns a NEW registry.

   The root must be flushed first — `nodes/root->blob` throws otherwise, rather
   than emitting a blob with a nil address that would read back broken.

   Note this relies on boring resolving an explicit registration BEFORE its
   structural encoding: a PSS root implements java.util.Set, and an earlier
   boring silently encoded it as a CBOR set of its elements instead of running
   this handler."
  [reg]
  (boring/register-tag reg generic-object-tag
                       #?(:clj PersistentSortedSet :cljs BTSet)
                       (fn [pset] (tagged set-tag (nodes/root->blob pset)))
                       nil))

;; ---------------------------------------------------------------------------
;; Read side — record constructors keyed by the tag names above.
;; ---------------------------------------------------------------------------

(defn install-node-readers
  "Register Leaf/Branch READ constructors. Returns a NEW registry.

   `opts` are `impl.nodes/reader-context`'s: `:measure-ops`, `:default-bf`,
   `:ref-type`. The context is built ONCE here, so its memoized settings cache
   is shared by every node this registry decodes."
  [reg opts]
  (let [ctx (nodes/reader-context opts)]
    (-> reg
        (boring/register-record leaf-tag   (fn [blob] (nodes/blob->leaf ctx blob)))
        (boring/register-record branch-tag (fn [blob] (nodes/blob->branch ctx blob))))))

(defn install-root-reader
  "Register the PSS root READ constructor. Returns a NEW registry.

   `opts` additionally take `:resolve-storage` / `:resolve-cmp` /
   `:resolve-measure`, each `(fn [meta] -> thing)` — lexical closures for a
   one-store serializer, or the `impl.nodes/registry-*-resolver`s for a wire
   peer."
  [reg opts]
  (let [ctx (nodes/reader-context opts)]
    (boring/register-record reg set-tag (fn [blob] (nodes/blob->root ctx blob)))))

;; ---------------------------------------------------------------------------
;; Bundle
;; ---------------------------------------------------------------------------

(defn install
  "Install the full canonical PSS handler set — node writers and readers plus
   the root writer and reader — into `reg`. Returns a NEW registry.

   Thread your own element handlers onto the result; they are orthogonal."
  [reg opts]
  (-> reg
      install-node-writers
      install-root-writer
      (install-node-readers opts)
      (install-root-reader opts)))

(defn registry
  "Convenience: `install` onto a fresh empty boring registry."
  [opts]
  (install (boring/tag-registry) opts))
