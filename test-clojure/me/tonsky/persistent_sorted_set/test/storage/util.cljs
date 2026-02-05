(ns me.tonsky.persistent-sorted-set.test.storage.util
  (:require [cljs.test :as test :refer-macros [is are deftest testing]]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.impl.storage :refer [IStorage]]
            [me.tonsky.persistent-sorted-set.impl.node :as node]
            [me.tonsky.persistent-sorted-set.btset :refer [BTSet]]
            [me.tonsky.persistent-sorted-set.leaf :refer [Leaf] :as leaf]
            [me.tonsky.persistent-sorted-set.branch :refer [Branch] :as branch]))

(def ^:dynamic *debug* false)

(defn dbg [& args]
  (when *debug*
    (apply println args)))

(defn gen-addr [] (random-uuid))

(def *stats
  (atom
    {:reads 0
     :writes 0
     :accessed 0}))

(defn branch? [node] (instance? Branch node))
(defn leaf? [node] (instance? Leaf node))

(defn level [node] (node/level node))

(defrecord Storage [*memory *disk settings]
  IStorage
  (store [_ node opts]
    (assert (not (false? (:sync? opts))))
    (dbg "store<" (type node) ">")
    (swap! *stats update :writes inc)
    (let [address (gen-addr)]
      (swap! *disk assoc address
             (pr-str
              {:level     (node/level node)
               :keys      (.-keys node)
               :addresses (when (branch? node) (.-addresses node))
               :subtree-count (when (branch? node) (.-subtree-count node))
               :stats     (.-_stats node)}))
      address))
  (restore [_ address opts]
    (assert (not (false? (:sync? opts))))
    (or
     (@*memory address)
     (let [{:keys [keys addresses level stats] :as m} (edn/read-string (@*disk address))
           node (if addresses
                  (branch/from-map (assoc m :settings settings))
                  (Leaf. keys settings stats))]
       (dbg "restored<" (type node) ">")
       (swap! *stats update :reads inc)
       (swap! *memory assoc address node)
       node)))
  (accessed [_ address] (swap! *stats update :accessed inc) nil))

(defn storage
  ([] (storage (atom {}) (atom {})))
  ([*disk] (storage (atom {}) *disk))
  ([*memory *disk] (storage *memory *disk {}))
  ([*memory *disk opts]
   (->Storage *memory *disk (merge {:branching-factor 512} opts))))

#!------------------------------------------------------------------------------

(defrecord AsyncStorage [*memory *disk settings]
  IStorage
  (store [_ node opts]
    (assert (false? (:sync? opts)))
    (dbg "store<" (type node) ">")
    (swap! *stats update :writes inc)
    (let [address (gen-addr)]
      (swap! *disk assoc address
            (pr-str
             {:level     (node/level node)
              :keys      (.-keys node)
              :addresses (when (branch? node) (.-addresses node))
              :subtree-count (when (branch? node) (.-subtree-count node))
              :stats     (.-_stats node)}))
    (async address)))
  (restore [_ address opts]
    (assert (false? (:sync? opts)))
    (async
     (or
      (@*memory address)
      (let [{:keys [keys addresses level stats] :as m} (edn/read-string (@*disk address))
            node (if addresses
                   (branch/from-map (assoc m :settings settings))
                   (Leaf. keys settings stats))]
        (dbg "restored<" (type node) ">")
        (swap! *stats update :reads inc)
        (swap! *memory assoc address node)
        node))))
  (accessed [_ address] (swap! *stats update :accessed inc) nil))

(defn async-storage
  ([] (async-storage (atom {}) (atom {})))
  ([*disk] (async-storage (atom {}) *disk))
  ([*memory *disk] (async-storage *memory *disk {}))
  ([*memory *disk opts]
   (->AsyncStorage *memory *disk (merge {:branching-factor 512} opts))))