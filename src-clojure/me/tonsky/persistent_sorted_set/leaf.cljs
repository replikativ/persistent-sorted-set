(ns me.tonsky.persistent-sorted-set.leaf
  (:require-macros [me.tonsky.persistent-sorted-set.macros :refer [async+sync]])
  (:require [is.simm.lean-cps.async :refer [await] :refer-macros [async]]
            [goog.array :as garr]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [me.tonsky.persistent-sorted-set.constants :refer [MAX_LEN]]
            [me.tonsky.persistent-sorted-set.impl.node :as node :refer [INode]]
            [me.tonsky.persistent-sorted-set.impl.storage :as storage]
            [me.tonsky.persistent-sorted-set.util :as util]))

(deftype Leaf [keys]
  Object
  (toString [_] (pr-str* (vec keys)))
  INode
  (len [_] (arrays/alength keys))
  (level [_] 0)
  (max-key [_] (arrays/alast keys))
  (merge [_ next] (Leaf. (arrays/aconcat keys (.-keys next))))
  (merge-split [_ next]
    (let [ks (util/merge-n-split keys (.-keys next))]
      (util/return-array (Leaf. (arrays/aget ks 0))
                         (Leaf. (arrays/aget ks 1)))))
  ($add [this storage key cmp {:keys [sync?] :or {sync? true}}]
    (let [idx    (util/binary-search-l cmp keys (dec (arrays/alength keys)) key)
          keys-l (arrays/alength keys)
          result (cond
                   (and (< idx keys-l) (== 0 (cmp key (arrays/aget keys idx))))
                   nil

                   (== keys-l MAX_LEN)
                   (let [middle (arrays/half (inc keys-l))]
                     (if (> idx middle)
                       (arrays/array
                        (Leaf. (.slice keys 0 middle))
                        (Leaf. (util/cut-n-splice keys middle keys-l idx idx (arrays/array key))))
                       (arrays/array
                        (Leaf. (util/cut-n-splice keys 0 middle idx idx (arrays/array key)))
                        (Leaf. (.slice keys middle keys-l)))))

                   :else
                   (arrays/array (Leaf. (util/splice keys idx idx (arrays/array key)))))]
      (if sync?
        result
        (async result))))
  ($contains? [this storage key cmp {:keys [sync?] :or {sync? true}}]
    (async+sync sync?
      (async (<= 0 ^number (garr/binarySearch keys key cmp)))))
  ($count [this storage {:keys [sync?] :or {sync? true}}]
    (if sync?
      (alength keys)
      (async (alength keys))))
  ($remove [this storage key left right cmp {:keys [sync?] :or {sync? true}}]
    (let [root? (and (nil? left) (nil? right))
          idx   (garr/binarySearch keys key cmp)]
     (async+sync sync?
       (async
        (when (<= 0 idx)
          (let [new-keys (util/splice keys idx (inc idx) (arrays/array))]
            (util/rotate (Leaf. new-keys) root? left right)))))))
  ($store [this storage {:keys [sync?] :or {sync? true} :as opts}]
    (async+sync sync?
     (async
      (await (storage/store storage this opts)))))
  ($walk-addresses [this storage on-address {:keys [sync?] :or {sync? true}}]
    (when-not sync? (async))))