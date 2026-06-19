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
                     [org.replikativ.persistent-sorted-set.branch :refer [Branch] :as branch]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set ANode Leaf Branch Settings Slot]
                   [org.fressian.handlers WriteHandler ReadHandler]
                   [java.util List])))

(def ^:const leaf-tag "pss/leaf")
(def ^:const branch-tag "pss/branch")

;; ---------------------------------------------------------------------------
;; Write handlers — Leaf/Branch → canonical store-map. Element values inside
;; :keys / :slots recurse through the consumer's own handlers.
;; ---------------------------------------------------------------------------

(def write-handlers
  "Fressian write handlers for PSS nodes. JVM: {Class {tag WriteHandler}} (for
   clojure.data.fressian's associative+inheritance lookup). cljs: {Type fn} (for fress).
   Merge into the consumer's write-handler map next to its element handlers."
  #?(:clj
     {Leaf
      {leaf-tag
       (reify WriteHandler
         (write [_ w leaf]
           (.writeTag w leaf-tag 1)
           (.writeObject w {:keys (.keys ^ANode leaf)})))}
      Branch
      {branch-tag
       (reify WriteHandler
         (write [_ w node]
           (.writeTag w branch-tag 1)
           (let [^Branch b node
                 slots (.slotsForStorage b)]
             (.writeObject w (cond-> {:level     (.level b)
                                      :keys      (.keys b)
                                      :addresses (.addresses b)}
                               slots (assoc :slots slots))))))}}
     :cljs
     {Leaf
      (fn [w leaf]
        (fress/write-tag w leaf-tag 1)
        (fress/write-object w {:keys (vec (.-keys leaf))}))
      Branch
      (fn [w node]
        (fress/write-tag w branch-tag 1)
        (let [slots (branch/slots-for-storage node)]
          (fress/write-object w (cond-> {:level     (node/level node)
                                         :keys      (vec (.-keys node))
                                         :addresses (vec (.-addresses node))}
                                  slots (assoc :slots slots)))))}))

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
            (let [{:keys [keys]} (.readObject rdr)]
              (Leaf. ^List keys settings))))
        branch-tag
        (reify ReadHandler
          (read [_ rdr _tag _n]
            (let [{:keys [level keys addresses slots]} (.readObject rdr)
                  b (Branch. (int level) ^List keys ^List addresses settings)]
              (when slots (attach-slots! b addresses slots))
              b)))})
     :cljs
     {leaf-tag
      (fn [rdr _tag _n]
        (let [{:keys [keys]} (fress/read-object rdr)]
          (Leaf. (to-array keys) settings nil)))
      branch-tag
      (fn [rdr _tag _n]
        (let [{:keys [level keys addresses slots]} (fress/read-object rdr)
              node (branch/from-map {:level     level
                                     :keys      (to-array keys)
                                     :addresses (to-array addresses)
                                     :settings  settings})]
          (when slots (attach-slots! node addresses slots))
          node))}))
