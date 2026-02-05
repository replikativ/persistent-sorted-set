(ns me.tonsky.persistent-sorted-set.leaf
  (:require-macros [me.tonsky.persistent-sorted-set.macros :refer [async+sync]])
  (:require [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [goog.array :as garr]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [me.tonsky.persistent-sorted-set.impl.node :as node :refer [INode]]
            [me.tonsky.persistent-sorted-set.impl.stats :as stats]
            [me.tonsky.persistent-sorted-set.impl.storage :as storage]
            [me.tonsky.persistent-sorted-set.util :as util]))

(deftype Leaf [keys settings ^:mutable _stats]
  Object
  (toString [_] (pr-str* (vec keys)))
  INode
  (len [_] (arrays/alength keys))
  (level [_] 0)
  (max-key [_] (arrays/alast keys))
  ($subtree-count [_] (arrays/alength keys))
  ($stats [_] _stats)
  ($compute-stats [this storage stats-ops {:keys [sync?] :or {sync? true}}]
    (if sync?
      (when stats-ops
        (let [result (reduce (fn [acc key]
                               (stats/merge-stats stats-ops acc (stats/extract stats-ops key)))
                             (stats/identity-stats stats-ops)
                             keys)]
          (set! _stats result)
          result))
      (async
        (when stats-ops
          (let [result (reduce (fn [acc key]
                                 (stats/merge-stats stats-ops acc (stats/extract stats-ops key)))
                               (stats/identity-stats stats-ops)
                               keys)]
            (set! _stats result)
            result)))))
  (merge [_ next]
    (let [new-leaf (Leaf. (arrays/aconcat keys (.-keys next)) settings nil)]
      ;; Stats will be recomputed lazily if needed
      new-leaf))
  (merge-split [_ next]
    (let [ks (util/merge-n-split keys (.-keys next))]
      ;; Stats will be recomputed lazily for new leaves
      (util/return-array (Leaf. (arrays/aget ks 0) settings nil)
                         (Leaf. (arrays/aget ks 1) settings nil))))
  ($add [this storage key cmp {:keys [sync?] :or {sync? true}}]
    (let [branching-factor (:branching-factor settings)
          stats-ops (:stats settings)
          idx              (util/binary-search-l cmp keys (dec (arrays/alength keys)) key)
          keys-l           (arrays/alength keys)
          result           (cond
                             (and (< idx keys-l) (== 0 (cmp key (arrays/aget keys idx))))
                             nil

                             (== keys-l branching-factor)
                             (let [middle (arrays/half (inc keys-l))
                                   left-leaf (if (> idx middle)
                                               (Leaf. (.slice keys 0 middle) settings nil)
                                               (Leaf. (util/cut-n-splice keys 0 middle idx idx (arrays/array key)) settings nil))
                                   right-leaf (if (> idx middle)
                                                (Leaf. (util/cut-n-splice keys middle keys-l idx idx (arrays/array key)) settings nil)
                                                (Leaf. (.slice keys middle keys-l) settings nil))]
                               ;; Compute stats for split leaves
                               (when stats-ops
                                 (node/$compute-stats left-leaf nil stats-ops {:sync? true})
                                 (node/$compute-stats right-leaf nil stats-ops {:sync? true}))
                               (arrays/array left-leaf right-leaf))

                             :else
                             (let [new-keys (util/splice keys idx idx (arrays/array key))
                                   new-leaf (Leaf. new-keys settings nil)]
                               ;; Update stats incrementally
                               (when stats-ops
                                 (let [prev-stats (or _stats (stats/identity-stats stats-ops))]
                                   (set! (.-_stats new-leaf)
                                         (stats/merge-stats stats-ops prev-stats (stats/extract stats-ops key)))))
                               (arrays/array new-leaf)))]
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
          stats-ops (:stats settings)
          idx   (garr/binarySearch keys key cmp)]
     (async+sync sync?
       (async
        (when (<= 0 idx)
          (let [new-keys (util/splice keys idx (inc idx) (arrays/array))
                new-leaf (Leaf. new-keys settings nil)]
            ;; Update stats
            (when stats-ops
              (if _stats
                (set! (.-_stats new-leaf)
                      (stats/remove-stats stats-ops _stats key
                                          #(node/$compute-stats new-leaf storage stats-ops {:sync? true})))
                ;; Stats were never initialized, compute from scratch
                (node/$compute-stats new-leaf storage stats-ops {:sync? true})))
            (util/rotate new-leaf root? left right settings)))))))
  ($store [this storage {:keys [sync?] :or {sync? true} :as opts}]
    (async+sync sync?
     (async
      (await (storage/store storage this opts)))))
  ($walk-addresses [this storage on-address {:keys [sync?] :or {sync? true}}]
    (when-not sync? (async))))