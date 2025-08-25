(ns me.tonsky.persistent-sorted-set.node
  (:require-macros [me.tonsky.persistent-sorted-set.macros :refer [async+sync]])
  (:require [await-cps :refer [await] :refer-macros [async]]
            [goog.array :as garr]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [me.tonsky.persistent-sorted-set.constants :refer [max-len]]
            [me.tonsky.persistent-sorted-set.protocols :refer [INode] :as impl]
            [me.tonsky.persistent-sorted-set.util
             :refer [rotate lookup-exact splice cut-n-splice binary-search-l
                     return-array merge-n-split check-n-splice]]))

(declare Node)

(defn $child
  "Get child at index, with lazy restoration if needed.
   When storage is provided, supports lazy restoration."
  [node idx storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
    (async
     (when (instance? Node node)
       ;; Initialize children array if needed
       (when (nil? (.-children node))
         (set! (.-children node) (arrays/make-array (arrays/alength (.-addresses node)))))
       (if-let [child (arrays/aget (.-children node) idx)]
         child
         ;; Lazy restoration from storage
         (when-let [addresses (.-addresses node)]
           (when-let [addr (arrays/aget addresses idx)]
             (let [child (await (impl/restore storage addr opts))]
               (arrays/aset (.-children node) idx child)
               child))))))))

(defn ensure-children [node]
  (when (nil? (.-children node))
    (set! (.-children node) (make-array (alength (.-keys node)))))
  (.-children node))



(defn child [node idx]
   (arrays/aget (.-children node) idx))



(defn- $$child-storage
  [^Node node storage idx {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (<= 0 idx)
               (< idx (alength (.-keys node)))))
  (assert (or (and (some? (.-children node))
                   (some? (aget (.-children node) idx)))
              (and (some? (.-addresses node))
                   (some? (aget (.-addresses node) idx)))))
  (async+sync sync?
    (async
      (if-let [child (and (some? (.-children node))
                          (aget (.-children node) idx))]
        (do
          (when-some [addr (and (some? (.-addresses node))
                                (aget (.-addresses node) idx))]
            (impl/accessed storage addr))
          child)
        (let [addr (aget (.-addresses node) idx)
              _(assert (some? addr) "expected address to restore child")
              child (await (impl/restore storage addr opts))]
          (aset (ensure-children node) idx child)
          child)))))

(defn- $count
  [^Node node storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
    (async
      (let [*cnt (atom 0)]
        (dotimes [i (alength (.-keys node))]
          (let [child (await ($$child-storage node storage i opts))]
            (swap! *cnt + (await (impl/node-count child storage opts)))))
        @*cnt))))

(defn- $contains?
  [^Node node storage key cmp {:keys [sync?] :or {sync? true} :as opts}]
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
                (let [child (await ($$child-storage node storage ins opts))]
                  (await (impl/node-contains? child storage key cmp opts)))))))))))

(defn- lookup-range [cmp arr key]
  (let [arr-l (arrays/alength arr)
        idx   (binary-search-l cmp arr (dec arr-l) key)]
    (if (== idx arr-l)
      -1
      idx)))

(deftype Node [keys ^:mutable children ^:mutable addresses ^:mutable _hash]
  Object
  (toString [_] (pr-str* (vec keys)))

  INode
  (node-lim-key [_] (arrays/alast keys))

  (node-len [_] (arrays/alength keys))

  (node-merge [_ next]
    (Node. (arrays/aconcat keys (.-keys next))
           (arrays/aconcat children (.-children next))
           nil nil))

  (node-merge-n-split [_ next]
    (let [ks (merge-n-split keys     (.-keys next))
          ps (merge-n-split children (.-children next))]
      (return-array
       (Node. (arrays/aget ks 0) (arrays/aget ps 0) nil nil)
       (Node. (arrays/aget ks 1) (arrays/aget ps 1) nil nil))))

  (node-count [this storage opts] ($count this storage opts))

  (node-contains? [this storage key cmp opts] ($contains? this storage key cmp opts))

  (node-lookup [this cmp key storage opts]
    (let [{:keys [sync?] :or {sync? true}} opts
          idx (lookup-range cmp keys key)]
      (async+sync sync?
        (async
          (when-not (== -1 idx)
            (let [child-node (await ($child this idx storage opts))]
              (await (impl/node-lookup child-node cmp key storage opts))))))))

  (node-conj [this cmp key storage opts]
    (let [{:keys [sync?] :or {sync? true}} opts
          idx   (binary-search-l cmp keys (- (arrays/alength keys) 2) key)]
      (async+sync sync?
        (async
          (let [child-node (await ($child this idx storage opts))]
            (when-let [nodes (await (impl/node-conj child-node cmp key storage opts))]
              (let [new-keys     (check-n-splice cmp keys     idx (inc idx) (arrays/amap impl/node-lim-key nodes))
                    new-children (splice             children idx (inc idx) nodes)]
                (if (<= (arrays/alength new-children) max-len)
                  ;; ok as is
                  (arrays/array (Node. new-keys new-children nil nil))
                  ;; sptta split it up
                  (let [middle (arrays/half (arrays/alength new-children))]
                    (arrays/array
                     (Node. (.slice new-keys     0 middle) (.slice new-children 0 middle) nil nil)
                     (Node. (.slice new-keys     middle)   (.slice new-children middle)   nil nil)))))))))))

  (node-disj [this cmp key root? left right storage {:keys [sync?] :or {sync? true} :as opts}]
    (let [idx (lookup-range cmp keys key)]
      (async+sync sync?
        (async
          (when-not (== -1 idx) ;; short-circuit, key not here
            (let [child       (await ($child this idx storage opts))
                  left-child  (when (>= (dec idx) 0) (await ($child this (dec idx) storage opts)))
                  right-child (when (< (inc idx) (arrays/alength children)) (await ($child this (inc idx) storage opts)))
                  disjoined   (await (impl/node-disj child cmp key false left-child right-child storage opts))]
              (when disjoined     ;; short-circuit, key not here
                (let [left-idx     (if left-child  (dec idx) idx)
                      right-idx    (if right-child (+ 2 idx) (+ 1 idx))
                      new-keys     (check-n-splice cmp keys     left-idx right-idx (arrays/amap impl/node-lim-key disjoined))
                      new-children (splice             children left-idx right-idx disjoined)]
                  (rotate (Node. new-keys new-children nil nil) root? left right))))))))))
