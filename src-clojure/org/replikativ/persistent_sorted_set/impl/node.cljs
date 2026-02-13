(ns org.replikativ.persistent-sorted-set.impl.node
  (:refer-clojure :exclude [max-key merge]))

(defprotocol INode
  (len [this])
  (level [this])
  (max-key [this])
  (merge       [_ next])
  (merge-split [_ next])
  ($add            [this storage key cmp opts])
  ($contains?      [this storage key cmp opts])
  ($count          [this storage opts])
  ($lookup         [this storage key cmp opts])
  ($subtree-count  [this])
  ($measure        [this])
  (try-compute-measure  [this storage measure-ops opts])
  (force-compute-measure [this storage measure-ops opts])
  ($remove         [this storage key left right cmp opts])
  ($replace        [this storage old-key new-key cmp opts])
  ($store          [this storage opts])
  ($walk-addresses [this storage on-address opts]))

(defn new-len [len _settings]
  len)
