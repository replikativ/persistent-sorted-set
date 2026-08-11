(ns org.replikativ.persistent-sorted-set.test.storage.util
  (:require [cljs.test :as test :refer-macros [is are deftest testing]]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.impl.storage :refer [IStorage]]
            [org.replikativ.persistent-sorted-set.impl.node :as node]
            [org.replikativ.persistent-sorted-set.btset :refer [BTSet]]
            [org.replikativ.persistent-sorted-set.leaf :refer [Leaf] :as leaf]
            [org.replikativ.persistent-sorted-set.branch :refer [Branch] :as branch]
            [org.replikativ.persistent-sorted-set.impl.numeric-stats :as numeric-stats]))

;; `:measure` is `pr-str`ed into the blob, so reading it back needs a reader for the record.
;; This never mattered before: on ClojureScript the incremental path never gave any node a
;; measure, so `:measure` was always nil in these blobs and the reader was never exercised.
;; Once the measure bootstraps, an edn round-trip without this fails with
;; "No reader function for tag ...NumericStats". The JVM test storage sidesteps it by not
;; persisting `:measure` at all.
(def ^:private edn-readers
  {'org.replikativ.persistent-sorted-set.impl.numeric-stats.NumericStats
   numeric-stats/map->NumericStats})

(defn- read-blob [s] (edn/read-string {:readers edn-readers} s))

(defn dbg [& args]
  nil)

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
              (cond-> {:level     (node/level node)
                       :keys      (.-keys node)
                       :addresses (when (branch? node) (.-addresses node))
                       :subtree-count (when (branch? node) (.-subtree-count node))
                       :measure   (.-_measure node)}
                ;; diff-buf: persist per-child buffered diffs so diff-buf round-trips here.
                (branch? node) (assoc :slots (branch/slots-for-storage node)))))
      address))
  (restore [_ address opts]
    (assert (not (false? (:sync? opts))))
    (or
     (@*memory address)
     (let [{:keys [keys addresses level measure slots] :as m} (read-blob (@*disk address))
           node (if addresses
                  (branch/from-map (assoc m :settings settings))
                  (Leaf. keys settings measure))
           ;; diff-buf: reconstruct per-child buffered diffs (anchor = child address);
           ;; Branch.child projects them on descent. Absent ⇒ baseline.
           _    (when (and slots addresses)
                  (let [arr (make-array (count keys))]
                    (doseq [[idx entry] slots]
                      (aset arr (int idx) {:diff    (:diff entry)
                                           :count   (:count entry)
                                           :measure (:measure entry)
                                           :anchor  (nth (vec addresses) (int idx))}))
                    (set! (.-_slots node) arr)))]
       (dbg "restored<" (type node) ">")
       (swap! *stats update :reads inc)
       (swap! *memory assoc address node)
       node)))
  (accessed [_ address] (swap! *stats update :accessed inc) nil)
  (markFreed [_ address] nil)
  (isFreed [_ address] false)
  (freedInfo [_ address] nil))

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
              (cond-> {:level     (node/level node)
                       :keys      (.-keys node)
                       :addresses (when (branch? node) (.-addresses node))
                       :subtree-count (when (branch? node) (.-subtree-count node))
                       :measure   (.-_measure node)}
                ;; diff-buf: persist per-child buffered diffs so diff-buf round-trips here.
                (branch? node) (assoc :slots (branch/slots-for-storage node)))))
      (async address)))
  (restore [_ address opts]
    (assert (false? (:sync? opts)))
    (async
     (or
      (@*memory address)
      (let [{:keys [keys addresses level measure slots] :as m} (read-blob (@*disk address))
            node (if addresses
                   (branch/from-map (assoc m :settings settings))
                   (Leaf. keys settings measure))
            ;; diff-buf: reconstruct per-child buffered diffs, exactly as the SYNC
            ;; storage above does. This half was missing while `store` wrote
            ;; `:slots` faithfully — a storage that persists buffered diffs and
            ;; never reads them back, which is indistinguishable from a storage
            ;; that lost them. It stayed invisible because the cljs set-level
            ;; `:diff-buf-size` was being dropped by `select-keys`, so nothing
            ;; buffered in the first place; with that fixed it showed up
            ;; immediately as the async arm of the diff test reporting `:added []`
            ;; where the sync arm reported the two added keys.
            _    (when (and slots addresses)
                   (let [arr (make-array (count keys))]
                     (doseq [[idx entry] slots]
                       (aset arr (int idx) {:diff    (:diff entry)
                                            :count   (:count entry)
                                            :measure (:measure entry)
                                            :anchor  (nth (vec addresses) (int idx))}))
                     (set! (.-_slots node) arr)))]
        (dbg "restored<" (type node) ">")
        (swap! *stats update :reads inc)
        (swap! *memory assoc address node)
        node))))
  (accessed [_ address] (swap! *stats update :accessed inc) nil)
  (markFreed [_ address] nil)
  (isFreed [_ address] false)
  (freedInfo [_ address] nil))

(defn async-storage
  ([] (async-storage (atom {}) (atom {})))
  ([*disk] (async-storage (atom {}) *disk))
  ([*memory *disk] (async-storage *memory *disk {}))
  ([*memory *disk opts]
   (->AsyncStorage *memory *disk (merge {:branching-factor 512} opts))))