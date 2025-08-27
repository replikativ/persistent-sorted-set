(ns me.tonsky.persistent-sorted-set.protocols)

(defprotocol INode
  (node-lim-key       [_])
  (node-merge         [_ next])
  (node-merge-n-split [_ next]))

(defprotocol IAsyncSeq
  (-afirst [this] "Returns async expression yielding first element")
  (-arest [this] "Returns async expression yielding rest of sequence"))

(defprotocol IStorage
  (store [this node opts])
  (restore [this address opts])
  (accessed [this address])
  (delete [this addresses]))
