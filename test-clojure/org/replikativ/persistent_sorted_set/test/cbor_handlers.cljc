(ns org.replikativ.persistent-sorted-set.test.cbor-handlers
  "Round-trips a storage-backed PSS through the CBOR node handlers
   (org.replikativ.persistent-sorted-set.cbor) on BOTH platforms, mirroring
   `test.fressian-handlers` case for case.

   The fressian handlers had a test and the CBOR ones did not, which is the
   wrong way round for the newer codec: fressian's shape is settled and CBOR's
   is what this branch introduces. Only the storage shim is codec-specific —
   `boring/encode` and `boring/decode` against a registry from
   `cbor/registry` — while the roundtrip bodies are the fressian test's.

   Exercises baseline AND diff-buf branches (slots), heterogeneous (vector)
   elements that recurse through boring's ordinary collection encoding, the
   storage-aware MST remove, and the root-as-pointer form."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.cbor :as pss-cbor]
            [org.replikativ.persistent-sorted-set.boundary :as bnd]
            [boring.core :as boring]
            #?@(:cljs [[org.replikativ.persistent-sorted-set.impl.storage :refer [IStorage]]
                       ;; BTSet is NOT re-exported from the main namespace; the root
                       ;; test asserts on the concrete type, so require it from its own.
                       [org.replikativ.persistent-sorted-set.btset :refer [BTSet]]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set IStorage Settings PersistentSortedSet])))

;; ---- CBOR storage shim (the only codec-specific code) --------------------------------

(defn- node-registry
  "A registry with the node handlers only. `:resolve-storage` is absent on
   purpose: nodes are addressed, so restoring one never needs the store."
  [bf]
  (-> (boring/tag-registry)
      pss-cbor/install-node-writers
      (pss-cbor/install-node-readers {:default-bf bf})))

#?(:clj
   (defrecord CborStorage [*disk reg]
     IStorage
     (store    [_ node] (let [a (random-uuid)]
                          (swap! *disk assoc a (boring/encode node {:registry reg}))
                          a))
     (accessed [_ _addr] nil)
     (restore  [_ addr] (boring/decode (@*disk addr) {:registry reg}))
     (markFreed [_ _addr] nil) (isFreed [_ _addr] false) (freedInfo [_ _addr] nil))

   ;; The ClojureScript protocol is not the JVM interface: `store`/`restore`
   ;; take an extra opts argument and there is a `delete`. Matching
   ;; test.fressian-handlers rather than the Java signature.
   :cljs
   (defrecord CborStorage [*disk reg]
     IStorage
     (store    [_ node _opts] (let [a (random-uuid)]
                                (swap! *disk assoc a (boring/encode node {:registry reg}))
                                a))
     (accessed [_ _addr] nil)
     (delete   [_ _addrs] nil)
     (restore  [_ addr _opts] (boring/decode (@*disk addr) {:registry reg}))
     (markFreed [_ _addr] nil) (isFreed [_ _addr] false) (freedInfo [_ _addr] nil)))

(defn make-cbor-storage [bf]
  (->CborStorage (atom {}) (node-registry bf)))

;; ---- shared roundtrip (default comparator ⇒ conj/store/restore are portable) ----------

(defn- build [storage bf dbs elems]
  (reduce (fn [s e] (set/conj s e compare))
          (set/sorted-set* {:storage storage :branching-factor bf :diff-buf-size dbs})
          elems))

(defn roundtrip [bf dbs elems]
  (let [storage (make-cbor-storage bf)
        addr    (set/store (build storage bf dbs elems) storage)]
    (vec (set/restore addr storage))))

(deftest baseline-roundtrip
  (testing "baseline: nodes round-trip through the leaf and branch tags"
    (let [elems (vec (range 2000))]                ; bf 8 ⇒ a real multi-level tree
      (is (= elems (roundtrip 8 0 elems))))))

(deftest heterogeneous-elements
  (testing "element-agnostic: vector keys recurse through boring's own collection
            encoding, so the node codec never sees them"
    (let [elems (vec (for [a (range 40) b (range 5)] [a b]))]
      (is (= elems (roundtrip 8 0 elems))))))

(deftest keyword-and-string-elements
  (testing "elements that exercise boring's identifier and stringref paths inside a
            node blob — the case where a shared string table could leak across nodes"
    (let [elems (vec (sort (for [i (range 200)] (keyword "e" (str "k" i)))))]
      (is (= elems (roundtrip 8 0 elems))))))

(deftest diff-buf-roundtrip
  (testing "diff-buf: branches carry :slots; the comparator-agnostic leaf-diff form
            round-trips and reconstructs into _slots (buffered deletes survive)"
    (let [bf 8 dbs 64
          storage  (make-cbor-storage bf)
          elems    (vec (range 3000))
          a0       (set/store (build storage bf dbs elems) storage)
          s1       (set/restore a0 storage)
          s2       (reduce (fn [s e] (set/disj s e compare)) s1 (range 0 3000 7))
          a2       (set/store s2 storage)
          back     (vec (set/restore a2 storage))
          expected (vec (remove #(zero? (mod % 7)) elems))]
      (is (= expected back)))))

(deftest mst-durable-remove-roundtrip
  (testing "MST conj/store/restore/disj/store/restore over CBOR storage: the
            storage-aware remove materializes children through branch/child, and the
            boundary self-restores from the node's own blob"
    (let [storage  (make-cbor-storage 64)
          elems    (vec (range 3000))
          s0       (reduce (fn [s e] (set/conj s e compare))
                           (set/sorted-set* {:storage storage :branching-factor 64
                                             :boundary (bnd/mst-boundary 5)})
                           (shuffle elems))
          a0       (set/store s0 storage)
          s1       (set/restore a0 storage)                 ; boundary self-restores
          s2       (reduce (fn [s e] (set/disj s e compare)) s1 (range 0 3000 7))
          a2       (set/store s2 storage)
          back     (vec (set/restore a2 storage))
          expected (vec (remove #(zero? (mod % 7)) elems))]
      (is (= elems (vec s1)) "restored set correct before removes")
      (is (= expected back) "durable MST remove yields the right elements"))))

;; The root is a POINTER, not a copy: a flushed root serializes as its address and
;; restores lazily against a storage resolved at read time. Worth its own case
;; because a PSS root implements java.util.Set, and boring will happily encode it
;; structurally as a CBOR set of elements unless the explicit registration wins --
;; which it silently did not at one point.
;; Runs on BOTH platforms. It was JVM-only, which left the cljs side of the very
;; registration this comment warns about — root-as-pointer vs root-as-set — with no
;; coverage at all, on the platform where BTSet is a different type entirely.
(defn- root-registry [bf storage]
  (-> (boring/tag-registry)
      pss-cbor/install-node-writers
      pss-cbor/install-root-writer
      (pss-cbor/install-node-readers {:default-bf bf})
      (pss-cbor/install-root-reader
       {:default-bf bf :resolve-storage (constantly storage)
        :resolve-cmp (constantly compare)})))

(defn- blob-size [bs] #?(:clj (alength ^bytes bs) :cljs (.-length bs)))

(deftest root-roundtrip
  (testing "a flushed root serializes as a pointer and restores lazily, with storage
            resolved per-call and bf self-describing from the blob"
    (let [bf      8
          storage (make-cbor-storage bf)
          elems   (vec (range 500))
          s       (reduce (fn [s e] (set/conj s e compare))
                          (set/sorted-set* {:storage storage :branching-factor bf})
                          elems)
          _       (set/store s storage)                 ; flush ⇒ address realized
          reg     (root-registry bf storage)
          bs      (boring/encode s {:registry reg})
          s2      (boring/decode bs {:registry reg})]
      (is (instance? #?(:clj PersistentSortedSet :cljs BTSet) s2)
          "the root pointer restores a PSS root, not a CBOR set")
      (is (= elems (vec s2)) "elements load lazily from the resolved storage")
      (is (< (blob-size bs) 512)
          "a pointer, not a copy: 500 elements do not fit in the blob"))))

(deftest re-encoding-a-root-touches-no-storage
  (testing "decode a root pointer, then re-encode it WITHOUT forcing the tree.

            A root is a pointer plus settings, so relaying one — decode on a peer,
            re-encode to pass along — must not materialize a single node. If it
            did, every hop through a relay would fault the whole B-tree into
            memory and hit storage O(nodes) times, which at datahike scale is the
            difference between forwarding a message and loading a database.

            Asserted by making every `restore` throw: the previous test could not
            catch this because it re-encoded a root whose nodes were already
            materialized in the same process."
    (let [bf      8
          storage (make-cbor-storage bf)
          elems   (vec (range 500))
          s       (reduce (fn [s e] (set/conj s e compare))
                          (set/sorted-set* {:storage storage :branching-factor bf})
                          elems)
          _       (set/store s storage)
          reg     (root-registry bf storage)
          bs      (boring/encode s {:registry reg})
          ;; A storage that refuses to serve anything: any lazy load is now a failure.
          exploding (reify IStorage
                      #?@(:clj  [(store [_ _] (throw (ex-info "must not store" {})))
                                 (accessed [_ _] nil)
                                 (restore [_ _] (throw (ex-info "must not restore" {})))]
                          :cljs [(store [_ _ _] (throw (ex-info "must not store" {})))
                                 (accessed [_ _] nil)
                                 (delete [_ _] nil)
                                 (restore [_ _ _] (throw (ex-info "must not restore" {})))])
                      (markFreed [_ _] nil) (isFreed [_ _] false) (freedInfo [_ _] nil))
          lazy-reg (-> (boring/tag-registry)
                       pss-cbor/install-node-writers
                       pss-cbor/install-root-writer
                       (pss-cbor/install-node-readers {:default-bf bf})
                       (pss-cbor/install-root-reader
                        {:default-bf bf :resolve-storage (constantly exploding)
                         :resolve-cmp (constantly compare)}))
          relayed (boring/decode bs {:registry lazy-reg})]
      ;; `vec`, not `seq`: on cljs these are js/Uint8Array, and comparing two
      ;; seqs over typed arrays does not reliably yield value equality.
      (is (= (vec bs) (vec (boring/encode relayed {:registry lazy-reg})))
          "relaying a root reproduces the same pointer blob, touching no node"))))
