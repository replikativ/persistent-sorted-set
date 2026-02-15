(ns org.replikativ.persistent-sorted-set.branch
  (:require-macros [org.replikativ.persistent-sorted-set.macros :refer [async+sync]])
  (:require [goog.array :as garr]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [org.replikativ.persistent-sorted-set.arrays :as arrays]
            [org.replikativ.persistent-sorted-set.impl.node :as node :refer [INode]]
            [org.replikativ.persistent-sorted-set.impl.measure :as measure]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage]
            [org.replikativ.persistent-sorted-set.util :as util]))

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

(defn child
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
                   (let [c (await (child node storage i opts))]
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
                         (let [c (await (child node storage ins opts))]
                           (await (node/$contains? c storage key cmp opts)))))))))))

(defn- lookup
  [^Branch node storage key cmp {:keys [sync?] :or {sync? true} :as opts}]
  (let [idx (garr/binarySearch (.-keys node) key cmp)]
    (async+sync sync?
                (async
                 (let [ins (if (<= 0 idx) idx (dec (- idx)))]
                   (when (< ins (alength (.-keys node)))
                     (let [c (await (child node storage ins opts))]
                       (await (node/lookup c storage key cmp opts)))))))))

(defn- try-compute-subtree-count-from-children
  "Compute subtree count from in-memory children. Returns -1 if any child unavailable."
  [children len]
  (if (nil? children)
    -1
    (loop [i 0, cnt 0]
      (if (>= i len)
        cnt
        (let [child (aget children i)]
          (if (nil? child)
            -1
            (let [cc (node/subtree-count child)]
              (if (>= cc 0)
                (recur (inc i) (+ cnt cc))
                -1))))))))

(defn add
  [^Branch this storage key cmp opts]
  (let [{:keys [sync?] :or {sync? true}} opts
        keys  (.-keys this)
        addrs (.-addresses this)
        idx   (util/binary-search-l cmp keys (- (arrays/alength keys) 2) key)]
    (async+sync sync?
                (async
                 (let [child-node (await (child this storage idx opts))
                       nodes      (await (node/add child-node storage key cmp opts))]
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
                                   (let [na (arrays/make-array (arrays/alength addrs))]
                                     (arrays/acopy addrs 0 (arrays/alength addrs) na 0)
                                     ;; Mark old child address as freed before clearing
                                     (when (and storage (aget addrs idx))
                                       (storage/markFreed storage (aget addrs idx)))
                                     (aset na idx nil)
                                     na)
                                   (let [old-addr (aget addrs idx)]
                                     ;; Mark old child address as freed before clearing
                                     (when (and storage old-addr)
                                       (storage/markFreed storage old-addr))
                                     (util/splice addrs idx (inc idx) (arrays/array nil nil)))))
                               ;; After adding one element, increment count if known
                               old-sc (.-subtree-count this)
                               new-sc (if (>= old-sc 0) (inc old-sc) -1)
                               ;; Update measure incrementally only if already computed
                               measure-ops (:measure (.-settings this))
                               new-measure (when (and measure-ops (.-_measure this))
                                             (measure/merge-measure measure-ops (.-_measure this) (measure/extract measure-ops key)))]
                           (arrays/array (Branch. (.-level this) new-keys new-children new-addrs new-sc new-measure (.-settings this))))
                         (let [middle      (arrays/half (arrays/alength new-children))
                               tmp-addrs   (when addrs
                                             (let [old-addr (aget addrs idx)]
                                               ;; Mark old child address as freed before clearing
                                               (when (and storage old-addr)
                                                 (storage/markFreed storage old-addr))
                                               (util/splice addrs idx (inc idx) (arrays/array nil nil))))
                               left-addrs  (when tmp-addrs (.slice tmp-addrs 0 middle))
                               right-addrs (when tmp-addrs (.slice tmp-addrs middle))
                               left-children (.slice new-children 0 middle)
                               right-children (.slice new-children middle)
                               measure-ops (:measure (.-settings this))
                               ;; Compute measure for split branches from their children only if already computed
                               ;; Return nil if any child measure is nil (don't silently undercount)
                               left-measure (when (and measure-ops (.-_measure this))
                                              (reduce (fn [acc child]
                                                        (if (nil? acc)
                                                          (reduced nil)
                                                          (let [cs (node/measure child)]
                                                            (if cs
                                                              (measure/merge-measure measure-ops acc cs)
                                                              (reduced nil)))))
                                                      (measure/identity-measure measure-ops)
                                                      left-children))
                               right-measure (when (and measure-ops (.-_measure this))
                                               (reduce (fn [acc child]
                                                         (if (nil? acc)
                                                           (reduced nil)
                                                           (let [cs (node/measure child)]
                                                             (if cs
                                                               (measure/merge-measure measure-ops acc cs)
                                                               (reduced nil)))))
                                                       (measure/identity-measure measure-ops)
                                                       right-children))]
                           (arrays/array
                            (Branch. (.-level this)
                                     (.slice new-keys 0 middle)
                                     left-children
                                     left-addrs
                                     (try-compute-subtree-count-from-children left-children (arrays/alength left-children))
                                     left-measure
                                     (.-settings this))
                            (Branch. (.-level this)
                                     (.slice new-keys middle)
                                     right-children
                                     right-addrs
                                     (try-compute-subtree-count-from-children right-children (arrays/alength right-children))
                                     right-measure
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
                                       (await (child this storage (dec idx) opts)))
                         right-child (when (< idx (dec (arrays/alength keys)))
                                       (await (child this storage (inc idx) opts)))
                         child       (await (child this storage idx opts))
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
                                               raddr (when right-child (arrays/aget addrs (dec right-idx)))
                                               left-unchanged  (and left-child (> alen 1)
                                                                    (identical? (arrays/aget disjoined 0) left-child))
                                               right-unchanged (and right-child (> alen 1)
                                                                    (identical? (arrays/aget disjoined (dec alen)) right-child))]
                                           ;; Mark freed addresses before clearing
                                           (when storage
                                             (dotimes [i (- right-idx left-idx)]
                                               (let [addr-idx (+ left-idx i)
                                                     old-addr (arrays/aget addrs addr-idx)]
                                                 (when (and old-addr
                                                            (not (and (= addr-idx left-idx) left-unchanged))
                                                            (not (and (= addr-idx (dec right-idx)) right-unchanged)))
                                                   (storage/markFreed storage old-addr)))))
                                           (when left-unchanged
                                             (aset repl 0 laddr))
                                           (when right-unchanged
                                             (aset repl (dec alen) raddr))
                                           (util/splice addrs left-idx right-idx repl)))
                             ;; After removing one element, decrement count if known
                             old-sc (.-subtree-count this)
                             new-sc (if (>= old-sc 0) (dec old-sc) -1)
                             ;; Update measure only if already computed
                             measure-ops (:measure (.-settings this))
                             new-measure (when (and measure-ops (.-_measure this))
                                           (measure/remove-measure measure-ops (.-_measure this) key
                                                                   #(node/try-compute-measure
                                                                     (Branch. (.-level this) new-keys new-kids new-addrs new-sc nil (.-settings this))
                                                                     storage measure-ops {:sync? true})))]
                         (util/rotate (Branch. (.-level this) new-keys new-kids new-addrs new-sc new-measure (.-settings this))
                                      (and (nil? left) (nil? right))
                                      left
                                      right
                                      (.-settings this))))))))))

(defn- replace-measure
  "Try to recompute measure after replace (postpone if children unavailable)."
  [branch storage measure-ops]
  (when measure-ops
    (node/try-compute-measure branch storage measure-ops {:sync? true})))

(defn $replace
  [^Branch this storage old-key new-key cmp {:keys [sync?] :or {sync? true} :as opts}]
  (assert (== 0 (cmp old-key new-key)) "old-key and new-key must compare as equal (cmp must return 0)")
  (async+sync sync?
              (async
               (let [keys (.-keys this)
                     settings (.-settings this)
                     editable? (:edit settings)
                     measure-ops (:measure settings)
                     idx  (let [arr-l (arrays/alength keys)
                                i     (util/binary-search-l cmp keys (dec arr-l) old-key)]
                            (if (== i arr-l) -1 i))]
                 (when-not (== -1 idx)
                   (let [child  (await (child this storage idx opts))
                         nodes  (await (node/$replace child storage old-key new-key cmp opts))]
                     (cond
                       ;; Not found in child
                       (nil? nodes)
                       nil

                       ;; Early exit from child (transient, no maxKey change)
                       (= nodes :early-exit)
                       :early-exit

                       ;; Child returned updated node
                       :else
                       (let [new-node      (arrays/aget nodes 0)
                             new-max-key   (node/max-key new-node)
                             children      (ensure-children this)
                             addrs         (.-addresses this)
                             last-child?   (== idx (dec (arrays/alength keys)))
                             max-key-changed (and last-child? (not (== 0 (cmp new-max-key (arrays/aget keys idx)))))]
                         (if max-key-changed
                           ;; maxKey changed - update keys array
                           (if editable?
                             ;; Transient: mutate in place
                             (do
                               (aset keys idx new-max-key)
                               (aset children idx new-node)
                               (when addrs
                                 ;; Mark old child address as freed before clearing
                                 (when (and storage (aget addrs idx))
                                   (storage/markFreed storage (aget addrs idx)))
                                 (aset addrs idx nil))
                               (when (and measure-ops (.-_measure this))
                                 (set! (.-_measure this)
                                       (replace-measure this storage measure-ops)))
                               (arrays/array this))
                             ;; Persistent: clone arrays
                             (let [new-keys     (arrays/aclone keys)
                                   new-children (arrays/aclone children)
                                   new-addrs    (when addrs
                                                  (let [na (arrays/aclone addrs)]
                                                    ;; Mark old child address as freed before clearing
                                                    (when (and storage (aget addrs idx))
                                                      (storage/markFreed storage (aget addrs idx)))
                                                    (aset na idx nil)
                                                    na))
                                   _            (aset new-keys idx new-max-key)
                                   _            (aset new-children idx new-node)
                                   new-branch   (Branch. (.-level this) new-keys new-children new-addrs (.-subtree-count this) nil (.-settings this))
                                   new-measure    (when (and measure-ops (.-_measure this))
                                                    (replace-measure new-branch storage measure-ops))]
                               (set! (.-_measure new-branch) new-measure)
                               (arrays/array new-branch)))
                           ;; maxKey unchanged - reuse keys array
                           (if editable?
                             ;; Transient: mutate in place
                             (do
                               (aset children idx new-node)
                               (when addrs
                                 ;; Mark old child address as freed before clearing
                                 (when (and storage (aget addrs idx))
                                   (storage/markFreed storage (aget addrs idx)))
                                 (aset addrs idx nil))
                               (when (and measure-ops (.-_measure this))
                                 (set! (.-_measure this)
                                       (replace-measure this storage measure-ops)))
                               (if last-child?
                                 (arrays/array this)  ; Last child, need to propagate
                                 :early-exit))        ; Not last child, early exit
                             ;; Persistent: clone ALL arrays — sharing would allow a
                             ;; later transient editable path to corrupt the original
                             (let [new-keys     (arrays/aclone keys)
                                   new-children (arrays/aclone children)
                                   new-addrs    (when addrs
                                                  (let [na (arrays/aclone addrs)]
                                                    ;; Mark old child address as freed before clearing
                                                    (when (and storage (aget addrs idx))
                                                      (storage/markFreed storage (aget addrs idx)))
                                                    (aset na idx nil)
                                                    na))
                                   _            (aset new-children idx new-node)
                                   new-branch   (Branch. (.-level this) new-keys new-children new-addrs (.-subtree-count this) nil (.-settings this))
                                   new-measure    (when (and measure-ops (.-_measure this))
                                                    (replace-measure new-branch storage measure-ops))]
                               (set! (.-_measure new-branch) new-measure)
                               (arrays/array new-branch))))))))))))

(defn store
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
                       (let [child-address (await (node/store (aget (.-children this) i) storage opts))]
                         (address this i child-address)))
                     (recur (inc i))))
                 (await (storage/store storage this opts))))))

(defn walk-addresses
  [^Branch this storage on-address {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [keys-l (arrays/alength (.-keys this))]
                 (loop [i 0]
                   (when (< i keys-l)
                     (let [addr (when (.-addresses this)
                                  (arrays/aget (.-addresses this) i))]
                       (when (or (nil? addr) (on-address addr))
                         (let [child (await (child this storage i opts))]
                           (when (instance? Branch child)
                             (await (node/walk-addresses child storage on-address opts)))))
                       (recur (inc i)))))))))

(defn ^Branch from-map
  [{:keys [level keys addresses subtree-count measure settings]}]
  (Branch. level keys nil addresses (or subtree-count -1) measure settings))

(deftype Branch [^number level keys ^:mutable children ^:mutable addresses ^:mutable ^number subtree-count ^:mutable _measure settings]
  Object
  (toString [_] (pr-str* {:level level :keys (vec keys)}))
  INode
  (len [_] (arrays/alength keys))
  (level [_] level)
  (max-key [_] (arrays/alast keys))
  (subtree-count [_] subtree-count)
  (measure [_] _measure)
  (try-compute-measure [this storage measure-ops {:keys [sync?] :or {sync? true} :as opts}]
    ;; Try to compute measure from in-memory children only; postpone if any child not loaded
    (if sync?
      (when measure-ops
        (when (some? children)
          (let [result (loop [i 0
                              acc (measure/identity-measure measure-ops)]
                         (if (< i (arrays/alength keys))
                           (let [child (when (some? children) (aget children i))]
                             (if (nil? child)
                               nil ;; child not in memory, postpone
                               (let [child-measure (node/measure child)]
                                 (if child-measure
                                   (recur (inc i)
                                          (measure/merge-measure measure-ops acc child-measure))
                                   nil)))) ;; child measure unavailable, postpone
                           acc))]
            (when result
              (set! _measure result))
            result)))
      (async
       (when measure-ops
         (when (some? children)
           (let [result (loop [i 0
                               acc (measure/identity-measure measure-ops)]
                          (if (< i (arrays/alength keys))
                            (let [child (when (some? children) (aget children i))]
                              (if (nil? child)
                                nil
                                (let [child-measure (node/measure child)]
                                  (if child-measure
                                    (recur (inc i)
                                           (measure/merge-measure measure-ops acc child-measure))
                                    nil))))
                            acc))]
             (when result
               (set! _measure result))
             result))))))
  (force-compute-measure [this storage measure-ops {:keys [sync?] :or {sync? true} :as opts}]
    ;; Force compute measure, recursively descending if needed
    (async+sync sync?
                (async
                 (when measure-ops
                   (let [result (loop [i 0
                                       acc (measure/identity-measure measure-ops)]
                                  (if (< i (arrays/alength keys))
                                    (let [child (await (child this storage i opts))
                                          child-measure (or (node/measure child)
                                                            (await (node/force-compute-measure child storage measure-ops opts)))]
                                      (recur (inc i)
                                             (if child-measure
                                               (measure/merge-measure measure-ops acc child-measure)
                                               acc)))
                                    acc))]
                     (set! _measure result)
                     result)))))
  (merge [this next]
    (let [sc1 subtree-count
          sc2 (.-subtree-count next)
          new-sc (if (and (>= sc1 0) (>= sc2 0)) (+ sc1 sc2) -1)
          ;; Merge measure if both have them
          new-measure (when (and _measure (.-_measure next))
                        (when-let [measure-ops (:measure settings)]
                          (measure/merge-measure measure-ops _measure (.-_measure next))))
          ;; Ensure children arrays exist (may be arrays of nulls for lazy branches)
          c1 (ensure-children this)
          c2 (ensure-children next)
          ;; Merge addresses too if present
          new-addrs (when (or addresses (.-addresses next))
                      (arrays/aconcat (or (ensure-addresses this) (arrays/make-array (arrays/alength keys)))
                                      (or (ensure-addresses next) (arrays/make-array (arrays/alength (.-keys next))))))]
      (Branch. level
               (arrays/aconcat keys (.-keys next))
               (arrays/aconcat c1 c2)
               new-addrs
               new-sc
               new-measure
               settings)))
  (merge-split [this next]
    (let [;; Ensure children arrays exist
          c1 (ensure-children this)
          c2 (ensure-children next)
          ks (util/merge-n-split keys (.-keys next))
          ps (util/merge-n-split c1 c2)
          ;; Also merge-split addresses if present
          as (when (or addresses (.-addresses next))
               (util/merge-n-split (or (ensure-addresses this) (arrays/make-array (arrays/alength keys)))
                                   (or (ensure-addresses next) (arrays/make-array (arrays/alength (.-keys next))))))]
      (let [p0 (arrays/aget ps 0)
            p1 (arrays/aget ps 1)
            sc0 (try-compute-subtree-count-from-children p0 (arrays/alength p0))
            sc1 (try-compute-subtree-count-from-children p1 (arrays/alength p1))
            measure-ops (:measure settings)
            m0 (when (and measure-ops _measure (.-_measure next))
                 (reduce (fn [acc child]
                           (if (nil? acc)
                             (reduced nil)
                             (let [cs (node/measure child)]
                               (if cs
                                 (measure/merge-measure measure-ops acc cs)
                                 (reduced nil)))))
                         (measure/identity-measure measure-ops)
                         p0))
            m1 (when (and measure-ops _measure (.-_measure next))
                 (reduce (fn [acc child]
                           (if (nil? acc)
                             (reduced nil)
                             (let [cs (node/measure child)]
                               (if cs
                                 (measure/merge-measure measure-ops acc cs)
                                 (reduced nil)))))
                         (measure/identity-measure measure-ops)
                         p1))]
        (util/return-array
         (Branch. level (arrays/aget ks 0) p0 (when as (arrays/aget as 0)) sc0 m0 settings)
         (Branch. level (arrays/aget ks 1) p1 (when as (arrays/aget as 1)) sc1 m1 settings)))))
  (add [this storage key cmp opts]
    (add this storage key cmp opts))
  ($contains? [this storage key cmp opts]
    ($contains? this storage key cmp opts))
  ($count [this storage opts]
    ($count this storage opts))
  (lookup [this storage key cmp opts]
    (lookup this storage key cmp opts))
  ($remove [this storage key left right cmp opts]
    ($remove this storage key left right cmp opts))
  ($replace [this storage old-key new-key cmp opts]
    ($replace this storage old-key new-key cmp opts))
  (store [this storage opts]
    (store this storage opts))
  (walk-addresses [this storage on-address opts]
    (walk-addresses this storage on-address opts)))
