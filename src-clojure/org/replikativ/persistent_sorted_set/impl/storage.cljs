(ns org.replikativ.persistent-sorted-set.impl.storage
  (:refer-clojure :exclude [comparator]))

(defprotocol IStorage
  (store [this node opts])
  (restore [this address opts])
  (accessed [this address])
  (delete [this addresses])
  (markFreed [this address])
  (isFreed [this address])
  (freedInfo [this address])
  ;; OP_BUF_V5: per-set/index comparator used to project buffered leaf-diffs on
  ;; restore (mirrors JVM IStorage.comparator()). nil ⇒ no per-index comparator.
  (comparator [this]))