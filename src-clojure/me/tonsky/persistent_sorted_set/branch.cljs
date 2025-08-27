(ns me.tonsky.persistent-sorted-set.branch
  (:require-macros [me.tonsky.persistent-sorted-set.macros :refer [async+sync]])
  (:require [await-cps :refer [await] :refer-macros [async]]
            ;[is.simm.lean-cps.async :refer [await] :refer-macros [async]]
            [goog.array :as garr]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [me.tonsky.persistent-sorted-set.constants :refer [max-len]]
            [me.tonsky.persistent-sorted-set.protocols :refer [INode] :as impl]
            [me.tonsky.persistent-sorted-set.impl.node :as node]
            [me.tonsky.persistent-sorted-set.util
             :refer [rotate lookup-exact splice cut-n-splice binary-search-l
                     return-array merge-n-split check-n-splice]]))

(declare Branch)

(defn ensure-children [node]
  (when (nil? (.-children node))
    (set! (.-children node) (make-array (alength (.-keys node)))))
  (.-children node))

(defn $child
  [^Branch node storage idx {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (some? idx)
               (number? idx)))
  (assert (or (and (some? (.-children node))
                   (some? (aget (.-children node) idx)))
              (and (some? (.-addresses node))
                   (some? (aget (.-addresses node) idx)))))
  (async+sync sync?
    (async
      (let [*child (atom nil)]
        (when (some? (.-children node))
          (reset! *child (aget (.-children node) idx)))
        (if (nil? @*child)
          (let [address (aget (.-addresses node) idx)
                _ (assert (some? address) "expected address to restore child")
                _ (assert (some? storage) "expected storage")
                child (await (impl/restore storage address opts))]
            (reset! *child child)
            (aset (ensure-children node) idx child))
          (when (and (some? (.-addresses node)) (some? (aget (.-addresses node) idx)))
            (assert (some? storage) "expected storage")
            (impl/accessed storage (aget (.-addresses node) idx))))
        @*child))))

(defn child [node idx]
   (arrays/aget (.-children node) idx))

(defn- $count
  [^Branch node storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
    (async
      (let [*cnt (atom 0)]
        (dotimes [i (alength (.-keys node))]
          (let [child (await ($child node storage i opts))]
            (swap! *cnt + (await (node/$count child storage opts)))))
        @*cnt))))

(defn- $contains?
  [^Branch node storage key cmp {:keys [sync?] :or {sync? true} :as opts}]
  (let [idx (garr/binarySearch (.-keys node) key cmp)]
    (async+sync sync?
      (async
        (if (<= 0 idx)
          true
          (let [ins (dec (- idx))]
            (if (== ins (alength (.-keys node)))
              false
              (do
                (assert (and (<= 0 ins) (< ins (alength (.-keys node)))))
                (let [child (await ($child node storage ins opts))]
                  (await (node/$contains? child storage key cmp opts)))))))))))

(defn- lookup-range [cmp arr key]
  (let [arr-l (arrays/alength arr)
        idx   (binary-search-l cmp arr (dec arr-l) key)]
    (if (== idx arr-l)
      -1
      idx)))

(defn $add
  [this storage key cmp opts]
  (let [{:keys [sync?] :or {sync? true}} opts
          idx   (binary-search-l cmp keys (- (arrays/alength keys) 2) key)]
    (async+sync sync?
      (async
        (let [child-node (await ($child this storage idx opts))]
          (when-let [nodes (await (node/$add child-node storage key cmp opts))]
            (let [children     (ensure-children this)
                  new-keys     (check-n-splice cmp keys     idx (inc idx) (arrays/amap impl/node-lim-key nodes))
                  new-children (splice             children idx (inc idx) nodes)]
              (if (<= (arrays/alength new-children) max-len)
                ;; ok as is
                (arrays/array (Branch. new-keys new-children nil nil))
                ;; sptta split it up
                (let [middle (arrays/half (arrays/alength new-children))]
                  (arrays/array
                   (Branch. (.slice new-keys     0 middle) (.slice new-children 0 middle) nil nil)
                   (Branch. (.slice new-keys     middle)   (.slice new-children middle)   nil nil)))))))))))

(defn $remove
  [^Branch this storage key left right cmp opts]
  (let [{:keys [sync?] :or {sync? true}} opts
        root? (and (nil? left) (nil? right))]
    (async+sync sync?
      (async
        (let [keys (.-keys this)
              idx  (lookup-range cmp keys key)]
          (when-not (== -1 idx)
            (let [children    (ensure-children this)
                  left-child  (when (> idx 0)
                                (await ($child this storage (dec idx) opts)))
                  right-child (when (< idx (dec (arrays/alength keys)))
                                (await ($child this storage (inc idx) opts)))
                  child       (await ($child this storage idx opts))
                  disjoined   (await (node/$remove child storage key left-child right-child cmp opts))]
              (when disjoined
                (let [left-idx      (if left-child  (dec idx) idx)
                      right-idx     (if right-child (+ idx 2) (inc idx))
                      new-keys      (check-n-splice cmp keys left-idx right-idx
                                                    (arrays/amap impl/node-lim-key disjoined))
                      new-children  (splice children left-idx right-idx disjoined)
                      new-addresses (when (.-addresses this)          ;; clear addresses for replaced window
                                      (splice (.-addresses this)
                                              left-idx right-idx
                                              (arrays/make-array (arrays/alength disjoined))))]
                  (rotate (Branch. new-keys new-children new-addresses nil)
                          root?
                          left
                          right))))))))))

(deftype Branch [keys ^:mutable children ^:mutable addresses ^:mutable _hash]
  Object
  (toString [_] (pr-str* (vec keys)))
  node/INode
  (len [_] (arrays/alength keys))
  ($add [this storage key cmp opts]
    ($add this storage key cmp opts))
  ($contains? [this storage key cmp opts]
    ($contains? this storage key cmp opts))
  ($count [this storage opts]
    ($count this storage opts))
  ($remove [this storage key left right cmp opts]
    ($remove this storage key left right cmp opts))
  INode
  (node-lim-key [_] (arrays/alast keys))
  (node-merge [_ next]
    (Branch. (arrays/aconcat keys (.-keys next))
           (arrays/aconcat children (.-children next))
           nil nil))
  (node-merge-n-split [_ next]
    (let [ks (merge-n-split keys     (.-keys next))
          ps (merge-n-split children (.-children next))]
      (return-array
       (Branch. (arrays/aget ks 0) (arrays/aget ps 0) nil nil)
       (Branch. (arrays/aget ks 1) (arrays/aget ps 1) nil nil)))))
