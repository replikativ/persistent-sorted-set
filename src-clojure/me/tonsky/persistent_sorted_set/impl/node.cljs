(ns me.tonsky.persistent-sorted-set.impl.node
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
  ($remove         [this storage key left right cmp opts])
  ($store          [this storage opts])
  ($walk-addresses [this storage on-address opts]))

