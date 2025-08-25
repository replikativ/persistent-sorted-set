(ns me.tonsky.persistent-sorted-set.protocols)

(defprotocol INode
  (node-lim-key       [_])
  (node-len           [_])
  (node-merge         [_ next])
  (node-merge-n-split [_ next])
  (node-count         [_ storage opts])
  (node-contains?     [_ storage key cmp opts])
  (node-lookup        [_ cmp key storage opts])
  (node-conj          [_ cmp key storage opts])
  (node-disj          [_ cmp key root? left right storage opts]))

(defprotocol IAsyncSeq
  (-afirst [this] "Returns async expression yielding first element")
  (-arest [this] "Returns async expression yielding rest of sequence"))

(defprotocol IStorage
  (store [this node opts])
  (restore [this address opts])
  (accessed [this address])
  (delete [this addresses]))
