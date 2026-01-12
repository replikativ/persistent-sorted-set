(ns me.tonsky.persistent-sorted-set.branch
  (:require-macros [me.tonsky.persistent-sorted-set.macros :refer [async+sync]])
  (:require [goog.array :as garr]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [me.tonsky.persistent-sorted-set.impl.node :as node :refer [INode]]
            [me.tonsky.persistent-sorted-set.impl.storage :as storage]
            [me.tonsky.persistent-sorted-set.util :as util]))

(declare Branch)

(defn ensure-children
  [^Branch node]
  (when (nil? (.-children node))
    (set! (.-children node) (make-array (alength (.-keys node)))))
  (.-children node))

(defn ensure-addresses
  [^Branch this]
  (when (nil? (.-addresses this))
    (set! (.-addresses this) (make-array (alength (.-keys this)))))
  (.-addresses this))

(defn $child
  [^Branch node storage idx {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (some? idx) (number? idx)))
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
                   (let [addr (aget (.-addresses node) idx)
                         _    (assert (some? addr) "expected address to restore child")
                         _    (assert (some? storage) "expected storage")
                         c    (await (storage/restore storage addr opts))]
                     (reset! *child c)
                     (aset (ensure-children node) idx c))
                   (when (and (some? (.-addresses node)) (some? (aget (.-addresses node) idx)))
                     (assert (some? storage) "expected storage")
                     (storage/accessed storage (aget (.-addresses node) idx))))
                 @*child))))

(defn address
  ([^Branch this idx]
   (assert (and (<= 0 idx) (< idx (alength (.-keys this)))))
   (when-some [addrs (.-addresses this)]
     (aget addrs idx)))
  ([^Branch this idx address]
   (assert (and (<= 0 idx) (< idx (alength (.-keys this)))))
   (when (or (some? (.-addresses this)) (some? address))
     (ensure-addresses this)
     (aset (.-addresses this) idx address))
   address))

(defn- $count
  [^Branch node storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [*cnt (atom 0)]
                 (dotimes [i (alength (.-keys node))]
                   (let [c (await ($child node storage i opts))]
                     (swap! *cnt + (await (node/$count c storage opts)))))
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
                         (let [c (await ($child node storage ins opts))]
                           (await (node/$contains? c storage key cmp opts)))))))))))

(defn- $lookup
  [^Branch node storage key cmp {:keys [sync?] :or {sync? true} :as opts}]
  (let [idx (garr/binarySearch (.-keys node) key cmp)]
    (async+sync sync?
                (async
                 (let [ins (if (<= 0 idx) idx (dec (- idx)))]
                   (when (< ins (alength (.-keys node)))
                     (let [c (await ($child node storage ins opts))]
                       (await (node/$lookup c storage key cmp opts)))))))))

(defn $add
  [^Branch this storage key cmp opts]
  (let [{:keys [sync?] :or {sync? true}} opts
        keys  (.-keys this)
        addrs (.-addresses this)
        idx   (util/binary-search-l cmp keys (- (arrays/alength keys) 2) key)]
    (async+sync sync?
                (async
                 (let [child-node (await ($child this storage idx opts))
                       nodes      (await (node/$add child-node storage key cmp opts))]
                   (when nodes
                     (let [branching-factor (:branching-factor (.-settings this))
                           children         (ensure-children this)
                           new-keys         (util/check-n-splice cmp keys idx (inc idx) (arrays/amap node/max-key nodes))
                           new-children     (util/splice children idx (inc idx) nodes)
                           nodes-len        (arrays/alength nodes)]
                       (if (<= (arrays/alength new-children) branching-factor)
                         (let [new-addrs
                               (when addrs
                                 (if (= nodes-len 1)
                                   (let [n0        (arrays/aget nodes 0)
                                         same-node (identical? n0 child-node)]
                                     (if same-node
                                       addrs
                                       (let [na (arrays/make-array (arrays/alength addrs))]
                                         (arrays/acopy addrs 0 (arrays/alength addrs) na 0)
                                         (aset na idx nil)
                                         na)))
                                   (util/splice addrs idx (inc idx) (arrays/array nil nil))))]
                           (arrays/array (Branch. (.-level this) new-keys new-children new-addrs (.-settings this))))
                         (let [middle      (arrays/half (arrays/alength new-children))
                               tmp-addrs   (when addrs
                                             (util/splice addrs idx (inc idx) (arrays/array nil nil)))
                               left-addrs  (when tmp-addrs (.slice tmp-addrs 0 middle))
                               right-addrs (when tmp-addrs (.slice tmp-addrs middle))]
                           (arrays/array
                            (Branch. (.-level this)
                                     (.slice new-keys 0 middle)
                                     (.slice new-children 0 middle)
                                     left-addrs
                                     (.-settings this))
                            (Branch. (.-level this)
                                     (.slice new-keys middle)
                                     (.slice new-children middle)
                                     right-addrs
                                     (.-settings this))))))))))))

(defn $remove
  [^Branch this storage key left right cmp {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [keys (.-keys this)
                     idx  (let [arr-l (arrays/alength keys)
                                i     (util/binary-search-l cmp keys (dec arr-l) key)]
                            (if (== i arr-l) -1 i))]
                 (when-not (== -1 idx)
                   (let [children    (ensure-children this)
                         addrs       (.-addresses this)
                         left-child  (when (> idx 0)
                                       (await ($child this storage (dec idx) opts)))
                         right-child (when (< idx (dec (arrays/alength keys)))
                                       (await ($child this storage (inc idx) opts)))
                         child       (await ($child this storage idx opts))
                         disjoined   (await (node/$remove child storage key left-child right-child cmp opts))]
                     (when disjoined
                       (let [left-idx  (if left-child  (dec idx) idx)
                             right-idx (if right-child (+ idx 2) (inc idx))
                             new-keys  (util/check-n-splice cmp keys left-idx right-idx
                                                            (arrays/amap node/max-key disjoined))
                             new-kids  (util/splice children left-idx right-idx disjoined)
                             new-addrs (when addrs
                                         (let [alen  (arrays/alength disjoined)
                                               repl  (arrays/make-array alen)
                                               laddr (when left-child  (arrays/aget addrs left-idx))
                                               raddr (when right-child (arrays/aget addrs (dec right-idx)))]
                                           (when (and left-child (> alen 1)
                                                      (identical? (arrays/aget disjoined 0) left-child))
                                             (aset repl 0 laddr))
                                           (when (and right-child (> alen 1)
                                                      (identical? (arrays/aget disjoined (dec alen)) right-child))
                                             (aset repl (dec alen) raddr))
                                           (util/splice addrs left-idx right-idx repl)))]
                         (util/rotate (Branch. (.-level this) new-keys new-kids new-addrs (.-settings this))
                                      (and (nil? left) (nil? right))
                                      left
                                      right
                                      (.-settings this))))))))))

(defn $replace
  [^Branch this storage old-key new-key cmp {:keys [sync?] :or {sync? true} :as opts}]
  (assert (== 0 (cmp old-key new-key)) "old-key and new-key must compare as equal (cmp must return 0)")
  (async+sync sync?
              (async
               (let [keys (.-keys this)
                     idx  (let [arr-l (arrays/alength keys)
                                i     (util/binary-search-l cmp keys (dec arr-l) old-key)]
                            (if (== i arr-l) -1 i))]
                 (when-not (== -1 idx)
                   (let [child  (await ($child this storage idx opts))
                         nodes  (await (node/$replace child storage old-key new-key cmp opts))]
                     (when nodes
                       (let [new-node      (arrays/aget nodes 0)
                             new-max-key   (node/max-key new-node)
                             children      (ensure-children this)
                             addrs         (.-addresses this)
                             max-key-changed (and (== idx (dec (arrays/alength keys)))
                                                  (not (== 0 (cmp new-max-key (arrays/aget keys idx)))))]
                         (if (== 0 (cmp new-max-key (arrays/aget keys idx)))
                  ;; maxKey unchanged - reuse keys array
                           (let [new-children (arrays/aclone children)
                                 new-addrs    (when addrs
                                                (let [na (arrays/aclone addrs)]
                                                  (aset na idx nil)
                                                  na))]
                             (aset new-children idx new-node)
                             (arrays/array (Branch. (.-level this) keys new-children new-addrs (.-settings this))))
                  ;; maxKey changed - create new keys array
                           (let [new-keys     (arrays/aclone keys)
                                 new-children (arrays/aclone children)
                                 new-addrs    (when addrs
                                                (let [na (arrays/aclone addrs)]
                                                  (aset na idx nil)
                                                  na))]
                             (aset new-keys idx new-max-key)
                             (aset new-children idx new-node)
                             (arrays/array (Branch. (.-level this) new-keys new-children new-addrs (.-settings this)))))))))))))

(defn $store
  [^Branch this storage {:keys [sync?] :or {sync? true} :as opts}]
  (ensure-addresses this)
  (async+sync sync?
              (async
               (let [keys-l (arrays/alength (.-keys this))]
                 (loop [i 0]
                   (when (< i keys-l)
                     (when (nil? (aget (.-addresses this) i))
                       (assert (some? (.-children this)))
                       (assert (some? (aget (.-children this) i)))
                       (assert (implements? node/INode (aget (.-children this) i)))
                       (let [child-address (await (node/$store (aget (.-children this) i) storage opts))]
                         (address this i child-address)))
                     (recur (inc i))))
                 (await (storage/store storage this opts))))))

(defn $walk-addresses
  [^Branch this storage on-address {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [keys-l (arrays/alength (.-keys this))]
                 (loop [i 0]
                   (when (< i keys-l)
                     (let [addr (when (.-addresses this)
                                  (arrays/aget (.-addresses this) i))]
                       (when (or (nil? addr) (on-address addr))
                         (let [child (await ($child this storage i opts))]
                           (when (instance? Branch child)
                             (await (node/$walk-addresses child storage on-address opts)))))
                       (recur (inc i)))))))))

(defn ^Branch from-map
  [{:keys [level keys addresses settings]}]
  (Branch. level keys nil addresses settings))

(deftype Branch [^number level keys ^:mutable children ^:mutable addresses settings]
  Object
  (toString [_] (pr-str* {:level level :keys (vec keys)}))
  INode
  (len [_] (arrays/alength keys))
  (level [_] level)
  (max-key [_] (arrays/alast keys))
  (merge [this next]
    (Branch. level
             (arrays/aconcat keys (.-keys next))
             (arrays/aconcat children (.-children next))
             nil
             settings))
  (merge-split [this next]
    (let [ks (util/merge-n-split keys     (.-keys next))
          ps (util/merge-n-split children (.-children next))]
      (util/return-array
       (Branch. level (arrays/aget ks 0) (arrays/aget ps 0) nil settings)
       (Branch. level (arrays/aget ks 1) (arrays/aget ps 1) nil settings))))
  ($add [this storage key cmp opts]
    ($add this storage key cmp opts))
  ($contains? [this storage key cmp opts]
    ($contains? this storage key cmp opts))
  ($count [this storage opts]
    ($count this storage opts))
  ($lookup [this storage key cmp opts]
    ($lookup this storage key cmp opts))
  ($remove [this storage key left right cmp opts]
    ($remove this storage key left right cmp opts))
  ($replace [this storage old-key new-key cmp opts]
    ($replace this storage old-key new-key cmp opts))
  ($store [this storage opts]
    ($store this storage opts))
  ($walk-addresses [this storage on-address opts]
    ($walk-addresses this storage on-address opts)))
