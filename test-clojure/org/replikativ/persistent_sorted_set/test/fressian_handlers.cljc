(ns org.replikativ.persistent-sorted-set.test.fressian-handlers
  "Round-trips a storage-backed PSS through the canonical Fressian node handlers
   (org.replikativ.persistent-sorted-set.fressian) on BOTH platforms — a fressian-backed
   IStorage serializes every node via the shared handlers. Only the storage shim is
   platform-specific (data.fressian + Java IStorage on the JVM; fress.api + the cljs
   IStorage protocol on cljs); the roundtrip body is shared. Exercises baseline AND
   diff-buf branches (slots), and heterogeneous (vector) elements that recurse through
   the ordinary fressian collection handlers — proving the node codec is element-agnostic."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.fressian :as pss-fress]
            [org.replikativ.persistent-sorted-set.boundary :as bnd]
            [hasch.core :as hasch]
            #?(:clj  [clojure.data.fressian :as fress]
               :cljs [fress.api :as fress])
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.storage :refer [IStorage]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set IStorage ANode Leaf Branch Settings RefType
                    PersistentSortedSet NumericStats NumericStatsOps]
                   [org.fressian.handlers WriteHandler ReadHandler]
                   [java.io ByteArrayOutputStream ByteArrayInputStream])))

;; ---- per-platform fressian storage shim (the only platform-specific code) -----------------

#?(:clj
   (do
     (def ^:private write-lookup
       (-> (merge fress/clojure-write-handlers pss-fress/write-handlers)
           fress/associative-lookup fress/inheritance-lookup))
     (defn- read-lookup [^Settings settings]
       ;; bf now self-describes per node from the blob; the storage's settings only supplies the
       ;; consumer's measure-ops + a default-bf fallback for pre-bf blobs.
       (-> (merge fress/clojure-read-handlers
                  (pss-fress/read-handlers {:measure-ops (.-_measure settings)
                                            :default-bf   (.branchingFactor settings)
                                            :boundary-resolver bnd/boundary-from-descriptor}))
           fress/associative-lookup))
     (defn- ser ^bytes [node]
       (let [out (ByteArrayOutputStream.)]
         (.writeObject (fress/create-writer out :handlers write-lookup) node)
         (.toByteArray out)))
     (defn- deser [settings ^bytes bs]
       (.readObject (fress/create-reader (ByteArrayInputStream. bs) :handlers (read-lookup settings))))
     (defrecord FressStorage [*disk settings]
       IStorage
       (store    [_ node] (let [a (random-uuid)] (swap! *disk assoc a (ser node)) a))
       (accessed [_ _addr] nil)
       (restore  [_ addr] (deser settings (@*disk addr)))
       (markFreed [_ _addr] nil) (isFreed [_ _addr] false) (freedInfo [_ _addr] nil))
     (defn make-fress-storage [bf dbs]
       (->FressStorage (atom {}) (Settings. (int bf) nil nil nil (int dbs)))))

   :cljs
   (do
     (defn- ser [node] (fress/write node :handlers pss-fress/write-handlers))
     (defn- deser [settings bs]
       (fress/read bs :handlers (pss-fress/read-handlers {:measure-ops (:measure settings)
                                                          :default-bf  (:branching-factor settings)
                                                          :boundary-resolver bnd/boundary-from-descriptor})))
     (defrecord FressStorage [*disk settings]
       IStorage
       (store    [_ node _opts] (let [a (random-uuid)] (swap! *disk assoc a (ser node)) a))
       (accessed [_ _addr] nil)
       (delete   [_ _addrs] nil)
       (restore  [_ addr _opts] (deser settings (@*disk addr)))
       (markFreed [_ _addr] nil) (isFreed [_ _addr] false) (freedInfo [_ _addr] nil))
     (defn make-fress-storage [bf dbs]
       (->FressStorage (atom {}) {:branching-factor bf :diff-buf-size dbs}))))

;; ---- shared roundtrip (default comparator ⇒ set/conj+store+restore are portable) ----------

(defn- build [storage bf dbs elems]
  (reduce (fn [s e] (set/conj s e compare))
          (set/sorted-set* {:storage storage :branching-factor bf :diff-buf-size dbs})
          elems))

(defn roundtrip [bf dbs elems]
  (let [storage (make-fress-storage bf dbs)
        addr    (set/store (build storage bf dbs elems) storage)]
    (vec (set/restore addr storage))))

(deftest baseline-roundtrip
  (testing "baseline: nodes round-trip through pss/leaf + pss/branch"
    (let [elems (vec (range 2000))]                ; bf 8 ⇒ a real multi-level tree
      (is (= elems (roundtrip 8 0 elems))))))

(deftest heterogeneous-elements
  (testing "element-agnostic: vector keys recurse through the fressian collection handlers"
    (let [elems (vec (for [a (range 40) b (range 5)] [a b]))]
      (is (= elems (roundtrip 8 0 elems))))))

(deftest diff-buf-roundtrip
  (testing "diff-buf: branches carry :slots; the comparator-agnostic leaf-diff form
            round-trips and reconstructs into _slots (buffered deletes survive the wire)"
    (let [bf 8 dbs 64
          storage  (make-fress-storage bf dbs)
          elems    (vec (range 3000))
          a0       (set/store (build storage bf dbs elems) storage)
          s1       (set/restore a0 storage)
          s2       (reduce (fn [s e] (set/disj s e compare)) s1 (range 0 3000 7))
          a2       (set/store s2 storage)
          back     (vec (set/restore a2 storage))
          expected (vec (remove #(zero? (mod % 7)) elems))]
      (is (= expected back)))))

;; JVM-only: ref-type is policy DATA (an enum), so it rides in the blob and a node reconstructs with
;; its own caching policy even when the reader knows nothing about it (the konserve-sync rootless
;; case). SOFT (the default) is omitted so common blobs are byte-unchanged; a read-time `:ref-type`
;; overrides what was serialized ("reflects what each context needs").
#?(:clj
   (deftest ref-type-roundtrip
     (testing ":weak rides in the node blob and reconstructs; SOFT omitted; read override wins"
       (let [keys   (java.util.ArrayList. [1 2 3 4 5])
             weak   (Leaf. keys (Settings. (int 8) RefType/WEAK nil nil (int 0)))
             soft   (Leaf. keys (Settings. (int 8) RefType/SOFT nil nil (int 0)))
             rt-of  (fn [node] (.refType (.-_settings ^ANode node)))
             ;; serialize `node` via the canonical write handlers, then read it back fresh — exactly
             ;; the konserve-sync path (a stored node blob deserialized without knowing the root).
             deser* (fn [node opts]
                      (let [bs (ser node)]
                        (-> (fress/create-reader (java.io.ByteArrayInputStream. bs)
                                                 :handlers (-> (merge fress/clojure-read-handlers
                                                                      (pss-fress/read-handlers opts))
                                                               fress/associative-lookup))
                            .readObject)))]
         ;; write side — node-config carries :weak; SOFT (the default) is omitted
         (is (= :weak (:ref-type (#'pss-fress/node-config weak)))           ":weak is serialized")
         (is (not (contains? (#'pss-fress/node-config soft) :ref-type))     "SOFT default is omitted")
         ;; read side — the same :weak blob, three ways
         (is (= RefType/WEAK   (rt-of (deser* weak {})))                    "no reader config ⇒ :weak from the blob")
         (is (= RefType/STRONG (rt-of (deser* weak {:ref-type :strong})))   "read-time override beats the blob")
         (is (= RefType/SOFT   (rt-of (deser* weak {:ref-type :soft})))     "override can force SOFT too")))))

;; JVM-only: prove a non-nil node measure is CARRIED + restored (not silently
;; recomputed) by round-tripping a single measured Branch with the consumer's OWN
;; measure-value handler — the element-agnostic recursion that keeps the codec generic.
#?(:clj
   (deftest measure-roundtrip
     (testing "a Branch's _measure survives via a consumer measure handler"
       (let [ops      (NumericStatsOps/instance)
             settings (Settings. (int 8) nil ops)
             nstats-w {NumericStats
                       {"test/numeric-stats"
                        (reify WriteHandler
                          (write [_ w s]
                            (.writeTag w "test/numeric-stats" 1)
                            (let [^NumericStats s s]
                              (.writeObject w [(.-count s) (.-sum s) (.-sumSq s) (.-min s) (.-max s)]))))}}
             nstats-r {"test/numeric-stats"
                       (reify ReadHandler
                         (read [_ rdr _ _]
                           (let [[c s sq mn mx] (.readObject rdr)]
                             (NumericStats. (long c) (double s) (double sq) mn mx))))}
             wl (-> (merge fress/clojure-write-handlers pss-fress/write-handlers nstats-w)
                    fress/associative-lookup fress/inheritance-lookup)
             rl (-> (merge fress/clojure-read-handlers
                           (pss-fress/read-handlers {:measure-ops ops :default-bf 8}) nstats-r)
                    fress/associative-lookup)
             ser2   (fn [node] (let [o (ByteArrayOutputStream.)]
                                 (.writeObject (fress/create-writer o :handlers wl) node) (.toByteArray o)))
             deser2 (fn [bs] (.readObject (fress/create-reader (ByteArrayInputStream. bs) :handlers rl)))
             ;; a storage that flushes children, so the root is a Branch we can inspect
             *disk   (atom {})
             storage (reify IStorage
                       (store [_ node] (let [a (random-uuid)] (swap! *disk assoc a (ser2 node)) a))
                       (accessed [_ _] nil)
                       (restore [_ a] (deser2 (@*disk a)))
                       (markFreed [_ _] nil) (isFreed [_ _] false) (freedInfo [_ _] nil))
             s    (reduce (fn [s e] (set/conj s e compare))
                          (set/sorted-set* {:storage storage :measure ops :branching-factor 8})
                          (range 500))
             _    (set/measure s)                         ; force the aggregates to compute
             root (.root s)
             m0   ^NumericStats (.-_measure ^ANode root)
             root2 (deser2 (ser2 root))                   ; round-trip the root node alone
             m1   ^NumericStats (.-_measure ^ANode root2)]
         (is (instance? Branch root) "multi-level tree ⇒ a Branch root")
         (is (some? m0) "the live root carries a computed measure")
         (is (some? m1) "the restored root carries the measure (not nil ⇒ it was serialized, not recomputed)")
         (is (= [(.-count m0) (.-sum m0)] [(.-count m1) (.-sum m1)])
             "the restored Branch measure equals the original")))))

#?(:clj
   (deftest root-roundtrip
     (testing "a flushed PSS root serializes as a pointer (pss/set) and restores lazily, with storage
               resolved per-call (lexical here) and bf self-describing from the blob"
       (let [settings (Settings. (int 8) nil)
             storage  (->FressStorage (atom {}) settings)
             elems    (vec (range 500))
             s        (reduce (fn [s e] (set/conj s e compare))
                              (set/sorted-set* {:storage storage :branching-factor 8}) elems)
             _        (set/store s storage)            ; flush ⇒ root address realized
             root-wl  (-> (merge fress/clojure-write-handlers
                                 {PersistentSortedSet {pss-fress/set-tag (pss-fress/root-write-handler)}})
                          fress/associative-lookup fress/inheritance-lookup)
             out      (ByteArrayOutputStream.)
             _        (.writeObject (fress/create-writer out :handlers root-wl) s)
             root-rl  (-> (merge fress/clojure-read-handlers
                                 {pss-fress/set-tag
                                  (pss-fress/root-read-handler {:resolve-storage (constantly storage)
                                                                :default-bf 8})})
                          fress/associative-lookup)
             s2       (.readObject (fress/create-reader (ByteArrayInputStream. (.toByteArray out)) :handlers root-rl))]
         (is (instance? PersistentSortedSet s2) "the pss/set pointer restores a PersistentSortedSet")
         (is (= elems (vec s2)) "elements load lazily from the resolved storage")))))

;; ---- bf self-describes per node ⇒ many branching-factors in ONE store/serializer --------------

#?(:clj
   (deftest multi-bf-one-store-roundtrip
     (testing "two trees with DIFFERENT branching factors stored through ONE store + ONE serializer:
               each node self-describes its bf in the blob, so reconstruction honors the per-node bf
               (the serializer's default-bf 999 is never used) — the one-Settings-per-store constraint
               is lifted."
       (let [*disk   (atom {})
             wl      (-> (merge fress/clojure-write-handlers pss-fress/write-handlers)
                         fress/associative-lookup fress/inheritance-lookup)
             rl      (-> (merge fress/clojure-read-handlers (pss-fress/read-handlers {:default-bf 999}))
                         fress/associative-lookup)
             ser1    (fn [node] (let [o (ByteArrayOutputStream.)]
                                  (.writeObject (fress/create-writer o :handlers wl) node) (.toByteArray o)))
             storage (reify IStorage
                       (store [_ node] (let [a (random-uuid)] (swap! *disk assoc a (ser1 node)) a))
                       (accessed [_ _] nil)
                       (restore [_ a] (.readObject (fress/create-reader (ByteArrayInputStream. (@*disk a)) :handlers rl)))
                       (markFreed [_ _] nil) (isFreed [_ _] false) (freedInfo [_ _] nil))
             store-tree (fn [bf elems]
                          (set/store (reduce (fn [s e] (set/conj s e compare))
                                             (set/sorted-set* {:storage storage :branching-factor bf}) elems)
                                     storage))
             a4     (store-tree 4  (vec (range 400)))
             a32    (store-tree 32 (vec (range 400)))
             root4  (.restore storage a4)
             root32 (.restore storage a32)]
         (is (= 4  (.branchingFactor (.-_settings ^ANode root4)))  "bf-4 root node reconstructs at bf 4")
         (is (= 32 (.branchingFactor (.-_settings ^ANode root32))) "bf-32 root node reconstructs at bf 32 — same serializer")
         (is (= (vec (range 400)) (vec (set/restore a4 storage))))
         (is (= (vec (range 400)) (vec (set/restore a32 storage))))))))

;; ---- many stores / one wire serializer — resolve each root's storage by :pss/storage-id ------------

(defn- rooted
  "Build a storage-backed set, stamp its :pss/storage-id into meta, flush it (realize the root address),
   and return the flushed set — ready to serialize as a `pss/set` pointer."
  [storage bf storage-id elems]
  (let [s (-> (reduce (fn [s e] (set/conj s e compare))
                      (set/sorted-set* {:storage storage :branching-factor bf}) elems)
              (vary-meta assoc pss-fress/storage-id-key storage-id))]
    (set/store s storage)
    s))

#?(:clj
   (deftest two-store-wire-roundtrip
     (testing "two stores with DIFFERENT branching factors registered under distinct :pss/storage-ids; ONE
               composite value carrying a root from EACH round-trips through ONE wire serializer — each
               root resolves its OWN storage from storage-registry by (:pss/storage-id meta), bf
               self-describes from the blob, and lazy child loads go through each resolved storage."
       (let [stA    (make-fress-storage 8 0)
             stB    (make-fress-storage 16 0)
             elemsA (vec (range 300))
             elemsB (vec (range 1000 1500))
             sA     (rooted stA 8  :store/a elemsA)
             sB     (rooted stB 16 :store/b elemsB)
             _      (pss-fress/register-storage! :store/a stA)
             _      (pss-fress/register-storage! :store/b stB)
             wl     (-> (merge fress/clojure-write-handlers pss-fress/root-write-handlers)
                        fress/associative-lookup fress/inheritance-lookup)
             ;; wire reader: storage by :pss/storage-id from the registry; comparator constant here.
             rl     (-> (merge fress/clojure-read-handlers
                               {pss-fress/set-tag (pss-fress/root-read-handler
                                                   {:resolve-storage (pss-fress/registry-storage-resolver)
                                                    :resolve-cmp     (constantly compare)})})
                        fress/associative-lookup)
             out    (ByteArrayOutputStream.)
             _      (.writeObject (fress/create-writer out :handlers wl) {:a sA :b sB})
             back   (.readObject (fress/create-reader (ByteArrayInputStream. (.toByteArray out)) :handlers rl))]
         (try
           (is (instance? PersistentSortedSet (:a back)))
           (is (= elemsA (vec (:a back))) "root :a lazy-loads its tree from store A")
           (is (= elemsB (vec (:b back))) "root :b lazy-loads its tree from store B")
           (is (identical? stA (.-_storage ^PersistentSortedSet (:a back))) "root :a resolved store A's storage")
           (is (identical? stB (.-_storage ^PersistentSortedSet (:b back))) "root :b resolved store B's storage")
           (finally (pss-fress/unregister-storage! :store/a)
                    (pss-fress/unregister-storage! :store/b)))))))

;; A custom consumer record + its element handlers — models a CRDT/record (e.g. a yggdrasil CRDT, or
;; a datahike Datom) that must coexist with the canonical PSS handlers on ONE serializer.
#?(:clj (defrecord Tag [v]))
#?(:clj
   (def ^:private tag-wh
     {Tag {"test/tag" (reify WriteHandler (write [_ w t] (.writeTag w "test/tag" 1) (.writeObject w (:v ^Tag t))))}}))
#?(:clj
   (def ^:private tag-rh
     {"test/tag" (reify ReadHandler (read [_ rdr _ _] (->Tag (.readObject rdr))))}))

#?(:clj
   (deftest cross-system-shape-roundtrip
     (testing "a custom record handler (Tag) composes on the SAME serializer as the canonical root
               handler — one handler per type, no collision — modeling a CRDT/record sibling of a PSS
               root from another store (the yggdrasil-CRDT-carrying-a-datahike-ref shape)."
       (let [st   (make-fress-storage 8 0)
             s    (rooted st 8 :store/x (vec (range 200)))
             _    (pss-fress/register-storage! :store/x st)
             wl   (-> (merge fress/clojure-write-handlers pss-fress/root-write-handlers tag-wh)
                      fress/associative-lookup fress/inheritance-lookup)
             rl   (-> (merge fress/clojure-read-handlers
                             {pss-fress/set-tag (pss-fress/root-read-handler
                                                 {:resolve-storage (pss-fress/registry-storage-resolver)
                                                  :resolve-cmp     (constantly compare)})}
                             tag-rh)
                      fress/associative-lookup)
             out  (ByteArrayOutputStream.)
             _    (.writeObject (fress/create-writer out :handlers wl) {:root s :tag (->Tag 42)})
             back (.readObject (fress/create-reader (ByteArrayInputStream. (.toByteArray out)) :handlers rl))]
         (try
           (is (= (->Tag 42) (:tag back)) "the custom record round-trips alongside the PSS root")
           (is (instance? PersistentSortedSet (:root back)))
           (is (= (vec (range 200)) (vec (:root back))) "the PSS root resolves storage + lazy-loads")
           (finally (pss-fress/unregister-storage! :store/x)))))))

;; ---- MST (content-defined boundary) durable round-trip (JVM) ------------------------------
;; Content-addressed storage (address = hasch/uuid(node->map)) so equal trees share a root
;; address: proves history-independence survives the real codec, that restore reconstructs the
;; MST boundary from the self-describing blob, and that mutate-after-restore stays canonical.
#?(:clj
   (do
     (defn- ca-store []
       (let [*disk (atom {})
             settings (Settings. (int 64) nil nil nil (int 0))]   ; boundary comes from the blob
         (reify IStorage
           (store     [_ node] (let [a (str (hasch/uuid (pss-fress/node->map node)))]
                                 (swap! *disk assoc a (ser node)) a))
           (accessed  [_ _addr] nil)
           (restore   [_ addr] (deser settings (get @*disk addr)))
           (markFreed [_ _addr] nil) (isFreed [_ _addr] false) (freedInfo [_ _addr] nil))))

     (defn- mst-set [storage lzpl elems]
       (reduce (fn [s e] (set/conj s e compare))
               (set/sorted-set* {:storage storage :branching-factor 64 :boundary (bnd/mst-boundary lzpl)})
               elems))

     (deftest mst-codec-history-independent
       (testing "two insertion orders of the same set ⇒ identical content-addressed root through the codec"
         (let [els (vec (range 4000))
               ra  (set/store (mst-set (ca-store) 5 (shuffle els)) (ca-store))
               rb  (set/store (mst-set (ca-store) 5 (shuffle els)) (ca-store))
               ;; count build of the same elements is order-dependent ⇒ generally different roots
               rc1 (set/store (reduce #(set/conj %1 %2 compare)
                                      (set/sorted-set* {:storage (ca-store) :branching-factor 64}) (sort els)) (ca-store))
               rc2 (set/store (reduce #(set/conj %1 %2 compare)
                                      (set/sorted-set* {:storage (ca-store) :branching-factor 64}) (reverse (sort els))) (ca-store))]
           (is (= ra rb) "MST: same key set, different order ⇒ same content-addressed root")
           (is (not= rc1 rc2) "count: different order ⇒ different root (the gap MST closes)"))))

     (deftest mst-restore-preserves-boundary
       (testing "restore reconstructs the MST boundary ⇒ mutate-after-restore is canonical"
         (let [els    (vec (range 3000))
               st     (ca-store)
               root   (set/store (mst-set st 5 (shuffle els)) st)
               s'     (set/restore root st)                       ; restored — boundary self-restores
               ;; mutate the restored set: add 9000 (a fresh key) and remove 1500
               s''    (-> s' (set/conj 9000 compare) (set/disj 1500 compare))
               r''    (set/store s'' st)                          ; same store (lazy children live here)
               survivors (-> (into (sorted-set) els) (conj 9000) (disj 1500))
               rfresh (set/store (mst-set (ca-store) 5 (shuffle (vec survivors))) (ca-store))]
           (is (= (vec s') (sort els)) "restored elements correct")
           (is (= (vec s'') (vec survivors)) "mutated-after-restore elements correct")
           (is (= r'' rfresh)
               "mutate-after-restore yields the SAME tree as a fresh MST build (boundary preserved)"))))

     (deftest mst-self-describing-blob
       (testing "the boundary descriptor rides in the node AND root blobs (self-describing restore)"
         (let [s (reduce #(set/conj %1 %2 compare)
                         (set/sorted-set* {:branching-factor 64 :boundary (bnd/mst-boundary 5)})
                         (range 2000))]
           (is (= {:type :mst :lzpl 5} (:boundary (#'pss-fress/node-config (.root ^PersistentSortedSet s))))
               "node-config self-describes the MST boundary")
           ;; reconstruct the strategy from the descriptor alone, no consumer interaction
           (is (= true (.contentDefined (bnd/boundary-from-descriptor {:type :mst :lzpl 5})))
               "PSS resolves the descriptor internally"))))))
