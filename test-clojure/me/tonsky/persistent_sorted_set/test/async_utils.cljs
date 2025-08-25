(ns me.tonsky.persistent-sorted-set.test.async-utils
  "Simple utilities for async testing"
  (:require [await-cps :refer [await smart-trampoline]
                       :refer-macros [async]]
            [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.btset :as btset]
            [me.tonsky.persistent-sorted-set.leaf :as leaf]
            [me.tonsky.persistent-sorted-set.node :as node]
            [me.tonsky.persistent-sorted-set.protocols :refer [IStorage]]))

(defn make-node-from-storage
  "Create a Node with addresses for lazy restoration"
  [keys addresses]
  (node/Node. keys nil (into-array addresses) nil))

(defn make-leaf-from-storage
  "Create a Leaf from stored data"
  [keys]
  (leaf/Leaf. keys nil))

(defrecord TestSyncStorage [*store]
  IStorage
  (restore [this address opts]
    (if-let [{:keys [type keys addresses]} (get @*store address)]
      (case type
        :node (make-node-from-storage keys (vec addresses))
        :leaf (make-leaf-from-storage keys))
      (throw (ex-info "Node not found" {:address address}))))
  (store [_ node opts]
    (let [addr (random-uuid)
          data (cond
                 (= (type node) node/Node)
                 {:type :node
                  :keys (.-keys node)
                  :addresses (when (.-addresses node) (vec (.-addresses node)))}
                 (= (type node) leaf/Leaf)
                 {:type :leaf
                  :keys (.-keys node)}
                 :else
                 (throw (ex-info "Unknown node type" {:node node})))]
      (swap! *store assoc addr data)
      addr))
  (accessed [_ address] nil)
  (delete [_ addresses] (swap! *store #(apply dissoc % addresses))))

(defrecord TestAsyncStorage [*store delay-ms]
  IStorage
  (restore [this address opts]
    (fn [resolve raise]
      (if (zero? delay-ms)
        ;; Fast path: no delay = immediate callback
        (try
          (if-let [{:keys [type keys addresses]} (get @*store address)]
            (let [node (case type
                         :node (make-node-from-storage keys (vec addresses))
                         :leaf (make-leaf-from-storage keys))]
              (resolve node))
            (raise (ex-info "Node not found" {:address address})))
          (catch :default e
            (raise e)))
        (js/setTimeout
         (fn []
           (smart-trampoline
            (fn []
              (try
                (if-let [{:keys [type keys addresses]} (get @*store address)]
                  (let [node (case type
                               :node
                               (make-node-from-storage keys (vec addresses))
                               :leaf
                               (make-leaf-from-storage keys))]
                    (resolve node))
                  (raise (ex-info "Node not found" {:address address})))
                (catch :default e
                  (raise e))))))
         delay-ms))))

  (store [_ node opts]
    (fn [resolve raise]
      (if (zero? delay-ms)
        ;; Fast path: no delay = immediate callback
        (try
          (let [addr (random-uuid)
                data (cond
                       (= (type node) node/Node)
                       {:type :node
                        :keys (.-keys node)
                        :addresses (when (.-addresses node) (vec (.-addresses node)))}
                       (= (type node) leaf/Leaf)
                       {:type :leaf
                        :keys (.-keys node)}
                       :else
                       (throw (ex-info "Unknown node type" {:node node})))]
            (swap! *store assoc addr data)
            (resolve addr))
          (catch :default e
            (raise e)))
        ;; Slow path: delay = async callback
        (js/setTimeout
         (fn []
           ;; Wrap callback execution in smart-trampoline to restart await-cps context
           (smart-trampoline
            (fn []
              (try
                (let [addr (random-uuid)
                      data (cond
                             (= (type node) node/Node)
                             {:type :node
                              :keys (.-keys node)
                              :addresses (when (.-addresses node) (vec (.-addresses node)))}
                             (= (type node) leaf/Leaf)
                             {:type :leaf
                              :keys (.-keys node)}
                             :else
                             (throw (ex-info "Unknown node type" {:node node})))]
                  (swap! *store assoc addr data)
                  (resolve addr))
                (catch :default e
                  (raise e))))))
         delay-ms))))
  (accessed [_ address] nil)
  (delete [_ addresses] (swap! *store #(apply dissoc % addresses))))

(defn make-sync-storage [] (->TestSyncStorage (atom {})))

(defn make-async-storage
  ([] (make-async-storage 10))
  ([delay-ms] (->TestAsyncStorage (atom {}) delay-ms)))

(defn async-build-set [n]
  (async
   (let [storage (make-async-storage 0)
         s0      (set/sorted-set* {:storage storage})]
     (await
      (reduce (fn [s-promise n]
                (async
                 (let [s (if (fn? s-promise) (await s-promise) s-promise)]
                   (await (set/conj s n compare {:sync? false})))))
              s0
              (range n))))))
