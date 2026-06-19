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
  #?(:clj (:import [org.replikativ.persistent_sorted_set IStorage Settings]
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
