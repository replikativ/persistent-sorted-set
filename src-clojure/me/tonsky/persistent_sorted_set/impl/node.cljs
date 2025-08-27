(ns me.tonsky.persistent-sorted-set.impl.node)

(defprotocol INode
  (len [this])
  ($add       [this storage key cmp opts])
  ($contains? [this storage key cmp opts])
  ($count     [this storage opts])
  ($remove    [this storage key left right cmp opts]))

