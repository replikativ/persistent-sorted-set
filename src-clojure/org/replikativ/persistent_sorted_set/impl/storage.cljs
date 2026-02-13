(ns org.replikativ.persistent-sorted-set.impl.storage)

(defprotocol IStorage
  (store [this node opts])
  (restore [this address opts])
  (accessed [this address])
  (delete [this addresses])
  (markFreed [this address])
  (isFreed [this address])
  (freedInfo [this address]))