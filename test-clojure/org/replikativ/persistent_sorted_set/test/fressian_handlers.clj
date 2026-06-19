(ns org.replikativ.persistent-sorted-set.test.fressian-handlers
  "Round-trips a storage-backed PSS through the canonical Fressian node handlers
   (org.replikativ.persistent-sorted-set.fressian) — a fressian-backed IStorage that
   serializes every node via the shared handlers. Exercises baseline AND diff-buf
   branches (slots), and heterogeneous domain elements (the keys recurse through the
   ordinary clojure write-handlers, proving the node codec is element-agnostic)."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.data.fressian :as fress]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.fressian :as pss-fress])
  (:import [org.replikativ.persistent_sorted_set IStorage PersistentSortedSet Settings]
           [java.io ByteArrayOutputStream ByteArrayInputStream]))

(def write-lookup
  (-> (merge fress/clojure-write-handlers pss-fress/write-handlers)
      fress/associative-lookup
      fress/inheritance-lookup))

(defn read-lookup [settings]
  (-> (merge fress/clojure-read-handlers (pss-fress/read-handlers settings))
      fress/associative-lookup))

(defn ser ^bytes [node]
  (let [out (ByteArrayOutputStream.)
        w   (fress/create-writer out :handlers write-lookup)]
    (.writeObject w node)
    (.toByteArray out)))

(defn deser [settings ^bytes bs]
  (let [r (fress/create-reader (ByteArrayInputStream. bs) :handlers (read-lookup settings))]
    (.readObject r)))

(defrecord FressStorage [*disk settings]
  IStorage
  (store [_ node]
    (let [addr (random-uuid)]
      (swap! *disk assoc addr (ser node))
      addr))
  (accessed [_ _addr] nil)
  (restore [_ addr]
    (deser settings (@*disk addr)))
  (markFreed [_ _addr] nil)
  (isFreed [_ _addr] false)
  (freedInfo [_ _addr] nil))

(defn- settings-for
  "The Settings a set built with `opts` would carry — so the fressian storage
   reconstructs nodes with matching branching/measure/diff-buf (no ctor guessing)."
  ^Settings [opts cmp]
  (.-_settings ^PersistentSortedSet (set/sorted-set* (assoc opts :comparator cmp))))

(defn- build [storage opts cmp elems]
  (reduce (fn [s e] (set/conj s e cmp))
          (set/sorted-set* (assoc opts :comparator cmp :storage storage))
          elems))

(defn roundtrip
  "Build a storage-backed set over `elems`, flush it through the fressian handlers,
   then restore from the root address and read everything back."
  [opts cmp elems]
  (let [storage (->FressStorage (atom {}) (settings-for opts cmp))
        address (set/store (build storage opts cmp elems) storage)]
    (vec (set/restore-by cmp address storage))))

(deftest baseline-roundtrip
  (testing "baseline: nodes round-trip through pss/leaf + pss/branch"
    (let [opts  {:branching-factor 8 :diff-buf-size 0}   ; small branching ⇒ multi-level tree
          elems (vec (range 2000))]
      (is (= elems (roundtrip opts compare elems))
          "every element survives store → fressian → restore"))))

(deftest heterogeneous-elements
  (testing "element-agnostic: vector keys recurse through clojure-write-handlers"
    (let [opts  {:branching-factor 8 :diff-buf-size 0}
          elems (vec (for [a (range 40) b (range 5)] [a b]))]
      (is (= elems (roundtrip opts compare elems))
          "vector elements survive the node codec unchanged"))))

(deftest diff-buf-roundtrip
  (testing "diff-buf: branches carry :slots; the comparator-agnostic leaf-diff form
            round-trips and reconstructs into _slots (deletes ride buffered leaf-diffs)"
    (let [opts     {:branching-factor 8 :diff-buf-size 64}
          settings (settings-for opts compare)
          storage  (->FressStorage (atom {}) settings)
          elems    (vec (range 3000))
          a0       (set/store (build storage opts compare elems) storage)
          s1       (set/restore-by compare a0 storage)
          ;; scatter deletes → live Present/Absent leaf-diffs accumulate in slots
          s2       (reduce (fn [s e] (set/disj s e compare)) s1 (range 0 3000 7))
          a2       (set/store s2 storage)
          back     (vec (set/restore-by compare a2 storage))
          expected (vec (remove #(zero? (mod % 7)) elems))]
      (is (= expected back)
          "diff-buf slots round-trip: buffered deletes survive the wire"))))
