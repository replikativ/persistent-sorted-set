(ns org.replikativ.persistent-sorted-set.test.transit-handlers
  "Round-trips a storage-backed PSS through the Transit node handlers
   (org.replikativ.persistent-sorted-set.transit) on BOTH platforms, mirroring
   `test.cbor-handlers` case for case.

   The transit module shipped with NO test and, on ClojureScript, with no way to
   even load: `transit.cljc` requires `cognitect.transit` from shared code, and
   the `:transit` alias supplied only `transit-clj`, whose jar carries just
   `cognitect/transit.clj`. The JVM half passed, nothing exercised the other
   half, and the gap was invisible. This namespace is deliberately `.cljc` and in
   `:node-tests` so that failure mode cannot recur silently.

   Only the storage shim is codec-specific -- a transit writer/reader over the
   handlers from `transit/write-handlers` and `transit/read-handlers` -- while the
   roundtrip bodies are the CBOR test's."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.transit :as pss-transit]
            [cognitect.transit :as transit]
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.storage :refer [IStorage]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set IStorage PersistentSortedSet]
                   [java.io ByteArrayOutputStream ByteArrayInputStream])))

;; ---- transit storage shim (the only codec-specific code) ------------------------------

;; `:json` rather than `:msgpack`: msgpack needs an extra JVM-only dependency, and the node
;; blob's own keys are all keywords, so json's key-stringification (documented in the module)
;; does not touch anything the codec owns.
#?(:clj
   (do
     (defn- ser ^bytes [node handlers]
       (let [out (ByteArrayOutputStream.)]
         (transit/write (transit/writer out :json {:handlers handlers}) node)
         (.toByteArray out)))
     (defn- deser [^bytes bs handlers]
       (transit/read (transit/reader (ByteArrayInputStream. bs) :json {:handlers handlers}))))

   :cljs
   (do
     (defn- ser [node handlers]
       (transit/write (transit/writer :json {:handlers handlers}) node))
     (defn- deser [s handlers]
       (transit/read (transit/reader :json {:handlers handlers}) s))))

(defn- node-read-handlers [bf]
  (pss-transit/node-read-handlers {:default-bf bf}))

(defrecord TransitStorage [*disk bf]
  IStorage
  #?@(:clj
      [(store    [_ node] (let [a (random-uuid)]
                            (swap! *disk assoc a (ser node (pss-transit/node-write-handlers)))
                            a))
       (accessed [_ _addr] nil)
       (restore  [_ addr] (deser (@*disk addr) (node-read-handlers bf)))]
      :cljs
      [(store    [_ node _opts] (let [a (random-uuid)]
                                  (swap! *disk assoc a (ser node (pss-transit/node-write-handlers)))
                                  a))
       (accessed [_ _addr] nil)
       (delete   [_ _addrs] nil)
       (restore  [_ addr _opts] (deser (@*disk addr) (node-read-handlers bf)))])
  (markFreed [_ _addr] nil) (isFreed [_ _addr] false) (freedInfo [_ _addr] nil))

(defn make-transit-storage [bf] (->TransitStorage (atom {}) bf))

(defn- build [storage bf dbs elems]
  (reduce (fn [s e] (set/conj s e compare))
          (set/sorted-set* {:storage storage :branching-factor bf :diff-buf-size dbs})
          elems))

(defn roundtrip [bf dbs elems]
  (let [storage (make-transit-storage bf)
        addr    (set/store (build storage bf dbs elems) storage)]
    (vec (set/restore addr storage))))

(deftest baseline-roundtrip
  (testing "nodes round-trip through the leaf and branch tags"
    (let [elems (vec (range 2000))]              ; bf 8 ⇒ a real multi-level tree
      (is (= elems (roundtrip 8 0 elems))))))

(deftest diff-buf-roundtrip
  (testing "the diff-buf (slots) branch survives transit"
    (let [elems (vec (range 2000))]
      (is (= elems (roundtrip 8 4 elems))))))

(deftest heterogeneous-elements
  (testing "elements recurse through transit's ordinary collection handling, so the
            node codec is element-agnostic"
    (let [elems (vec (for [i (range 300)] [i (str "v" i)]))]
      (is (= elems (mapv vec (roundtrip 8 0 elems)))))))

(deftest root-roundtrip
  (testing "a flushed root serializes as a POINTER and restores lazily -- the same
            case the CBOR module has, which transit lacked entirely"
    (let [bf      8
          storage (make-transit-storage bf)
          elems   (vec (range 500))
          s       (build storage bf 0 elems)
          _       (set/store s storage)          ; flush ⇒ address realized
          wh      (pss-transit/write-handlers)
          rh      (pss-transit/read-handlers {:default-bf bf
                                              :resolve-storage (constantly storage)
                                              :resolve-cmp (constantly compare)})
          bs      (ser s wh)
          s2      (deser bs rh)]
      (is (= elems (vec s2)) "elements load lazily from the resolved storage")
      (is (< (count bs) 512)
          "a pointer, not a copy: 500 elements do not fit in the blob"))))

(deftest unflushed-root-is-refused
  (testing "an unflushed root has no address, so emitting it would produce a blob
            that reads back broken -- it must throw instead"
    (let [storage (make-transit-storage 8)
          s       (build storage 8 0 (vec (range 10)))]   ; never stored
      ;; transit's WriterFactory wraps a handler's exception in a RuntimeException,
      ;; so the ExceptionInfo `root->blob` throws does not surface directly.
      (is (thrown-with-msg?
           #?(:clj RuntimeException :cljs js/Error) #"must be flushed"
           (ser s (pss-transit/write-handlers)))))))
