(ns me.tonsky.persistent-sorted-set.leaf
  (:require-macros [me.tonsky.persistent-sorted-set.macros :refer [async+sync]])
  (:require [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [goog.array :as garr]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [me.tonsky.persistent-sorted-set.impl.node :as node :refer [INode]]
            [me.tonsky.persistent-sorted-set.impl.storage :as storage]
            [me.tonsky.persistent-sorted-set.util :as util]))

(deftype Leaf [keys settings]
  Object
  (toString [_] (pr-str* (vec keys)))
  INode
  (len [_] (arrays/alength keys))
  (level [_] 0)
  (max-key [_] (arrays/alast keys))
  ($subtree-count [_] (arrays/alength keys))
  (merge [_ next] (Leaf. (arrays/aconcat keys (.-keys next)) settings))
  (merge-split [_ next]
    (let [ks (util/merge-n-split keys (.-keys next))]
      (util/return-array (Leaf. (arrays/aget ks 0) settings)
                         (Leaf. (arrays/aget ks 1) settings))))
  ($add [this storage key cmp {:keys [sync?] :or {sync? true}}]
    (let [branching-factor (:branching-factor settings)
          idx              (util/binary-search-l cmp keys (dec (arrays/alength keys)) key)
          keys-l           (arrays/alength keys)
          result           (cond
                             (and (< idx keys-l) (== 0 (cmp key (arrays/aget keys idx))))
                             nil

                             (== keys-l branching-factor)
                             (let [middle (arrays/half (inc keys-l))]
                               (if (> idx middle)
                                 (arrays/array
                                  (Leaf. (.slice keys 0 middle) settings)
                                  (Leaf. (util/cut-n-splice keys middle keys-l idx idx (arrays/array key)) settings))
                                 (arrays/array
                                  (Leaf. (util/cut-n-splice keys 0 middle idx idx (arrays/array key)) settings)
                                  (Leaf. (.slice keys middle keys-l) settings))))

                             :else
                             (arrays/array (Leaf. (util/splice keys idx idx (arrays/array key)) settings)))]
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
  ($lookup [this storage key cmp {:keys [sync?] :or {sync? true}}]
    (async+sync sync?
                (async
                 (let [idx (garr/binarySearch keys key cmp)]
                   (when (<= 0 idx)
                     (arrays/aget keys idx))))))
  ($remove [this storage key left right cmp {:keys [sync?] :or {sync? true}}]
    (let [root? (and (nil? left) (nil? right))
          idx   (garr/binarySearch keys key cmp)]
      (async+sync sync?
                  (async
                   (when (<= 0 idx)
                     (let [new-keys (util/splice keys idx (inc idx) (arrays/array))]
                       (util/rotate (Leaf. new-keys settings) root? left right settings)))))))
  ($replace [this storage old-key new-key cmp {:keys [sync?] :or {sync? true}}]
    (assert (== 0 (cmp old-key new-key)) "old-key and new-key must compare as equal (cmp must return 0)")
    (async+sync sync?
                (async
                 (let [idx (garr/binarySearch keys old-key cmp)]
                   (when (<= 0 idx)
                     ;; Always create new leaf (no transient support at node level for now)
                     (let [new-keys (arrays/aclone keys)]
                       (aset new-keys idx new-key)
                       (arrays/array (Leaf. new-keys settings))))))))
  ($store [this storage {:keys [sync?] :or {sync? true} :as opts}]
    (async+sync sync?
                (async
                 (await (storage/store storage this opts)))))
  ($walk-addresses [this storage on-address {:keys [sync?] :or {sync? true}}]
    (when-not sync? (async))))