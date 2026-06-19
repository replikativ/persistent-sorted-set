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
            #?(:clj  [clojure.data.fressian :as fress]
               :cljs [fress.api :as fress])
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.storage :refer [IStorage]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set IStorage ANode Branch Settings
                    PersistentSortedSet NumericStats NumericStatsOps]
                   [org.fressian.handlers WriteHandler ReadHandler]
                   [java.io ByteArrayOutputStream ByteArrayInputStream])))

;; ---- per-platform fressian storage shim (the only platform-specific code) -----------------

#?(:clj
   (do
     (def ^:private write-lookup
       (-> (merge fress/clojure-write-handlers pss-fress/write-handlers)
           fress/associative-lookup fress/inheritance-lookup))
     (defn- read-lookup [settings]
       (-> (merge fress/clojure-read-handlers (pss-fress/read-handlers settings))
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
     (defn- deser [settings bs] (fress/read bs :handlers (pss-fress/read-handlers settings)))
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
             rl (-> (merge fress/clojure-read-handlers (pss-fress/read-handlers settings) nstats-r)
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
     (testing "a flushed PSS root serializes as a pointer (pss/set) and restores lazily,
               with storage + comparator resolved per-call from the wire meta"
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
                                  (pss-fress/root-read-handler {:settings settings
                                                                :resolve-storage (constantly storage)})})
                          fress/associative-lookup)
             s2       (.readObject (fress/create-reader (ByteArrayInputStream. (.toByteArray out)) :handlers root-rl))]
         (is (instance? PersistentSortedSet s2) "the pss/set pointer restores a PersistentSortedSet")
         (is (= elems (vec s2)) "elements load lazily from the resolved storage")))))

;; ---- Scope registry: many stores / one serializer (the shared-wire model) -----------------

(defn- rooted
  "Build a storage-backed set, stamp its store-id into meta, flush it (realize the root
   address), and return the flushed set — ready to serialize as a `pss/set` pointer."
  [storage bf store-id elems]
  (let [s (-> (reduce (fn [s e] (set/conj s e compare))
                      (set/sorted-set* {:storage storage :branching-factor bf}) elems)
              (vary-meta assoc pss-fress/store-id-key store-id))]
    (set/store s storage)
    s))

#?(:clj
   (deftest two-scope-registry-roundtrip
     (testing "two stores with DIFFERENT settings, registered under distinct store-ids; ONE
               composite value carrying a root from EACH round-trips through ONE serializer — each
               root resolves its own storage+settings+cmp from scope-registry by (:store-id meta)
               and lazy-loads its tree from the RIGHT store (the serializer carries no node handlers;
               child loads go through each resolved storage's own deser)."
       (let [stA    (make-fress-storage 8 0)
             stB    (make-fress-storage 16 0)
             elemsA (vec (range 300))
             elemsB (vec (range 1000 1500))
             sA     (rooted stA 8  :store/a elemsA)
             sB     (rooted stB 16 :store/b elemsB)
             _      (pss-fress/register-scope! :store/a {:storage stA :settings (:settings stA) :resolve-cmp (constantly compare)})
             _      (pss-fress/register-scope! :store/b {:storage stB :settings (:settings stB) :resolve-cmp (constantly compare)})
             wl     (-> (merge fress/clojure-write-handlers pss-fress/root-write-handlers)
                        fress/associative-lookup fress/inheritance-lookup)
             ;; root reader uses the DEFAULT (registry by :store-id) — no closed-over storage/settings
             rl     (-> (merge fress/clojure-read-handlers {pss-fress/set-tag (pss-fress/root-read-handler)})
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
           (finally (pss-fress/unregister-scope! :store/a)
                    (pss-fress/unregister-scope! :store/b)))))))

;; A custom consumer record + its element handlers — models a CRDT/record (e.g. a yggdrasil
;; CRDT, or a datahike Datom) that must coexist with the canonical PSS handlers on ONE serializer.
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
             _    (pss-fress/register-scope! :store/x {:storage st :settings (:settings st) :resolve-cmp (constantly compare)})
             wl   (-> (merge fress/clojure-write-handlers pss-fress/root-write-handlers tag-wh)
                      fress/associative-lookup fress/inheritance-lookup)
             rl   (-> (merge fress/clojure-read-handlers {pss-fress/set-tag (pss-fress/root-read-handler)} tag-rh)
                      fress/associative-lookup)
             out  (ByteArrayOutputStream.)
             _    (.writeObject (fress/create-writer out :handlers wl) {:root s :tag (->Tag 42)})
             back (.readObject (fress/create-reader (ByteArrayInputStream. (.toByteArray out)) :handlers rl))]
         (try
           (is (= (->Tag 42) (:tag back)) "the custom record round-trips alongside the PSS root")
           (is (instance? PersistentSortedSet (:root back)))
           (is (= (vec (range 200)) (vec (:root back))) "the PSS root resolves its scope + lazy-loads")
           (finally (pss-fress/unregister-scope! :store/x)))))))
