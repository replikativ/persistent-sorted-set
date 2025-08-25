(ns me.tonsky.persistent-sorted-set.leaf
  (:require-macros [me.tonsky.persistent-sorted-set.macros :refer [async+sync]])
  (:require [await-cps :refer [await] :refer-macros [async]]
            ; [is.simm.lean-cps.async :refer [await] :refer-macros [async]]
            [goog.array :as garr]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [me.tonsky.persistent-sorted-set.constants :refer [max-len]]
            [me.tonsky.persistent-sorted-set.protocols :refer [INode]]
            [me.tonsky.persistent-sorted-set.util
             :refer [rotate lookup-exact splice cut-n-splice binary-search-l
                     return-array merge-n-split]]))

(deftype Leaf [keys ^:mutable _hash]
  Object
  (toString [_] (pr-str* (vec keys)))

  INode
  (node-lim-key [_] (arrays/alast keys))

  (node-len [_] (arrays/alength keys))

  (node-merge [_ next] (Leaf. (arrays/aconcat keys (.-keys next)) nil))

  (node-merge-n-split [_ next]
                      (let [ks (merge-n-split keys (.-keys next))]
                        (return-array (Leaf. (arrays/aget ks 0) nil)
                                      (Leaf. (arrays/aget ks 1) nil))))

  (node-contains? [this _ key cmp opts]
    (let [{:keys [sync?] :or {sync? true}} opts]
      (let [res (<= 0 ^number (garr/binarySearch keys key cmp))]
        (if sync?
          res
          (async res)))))

  (node-count [_ _storage opts]
    (let [{:keys [sync?] :or {sync? true}} opts]
      (if sync?
        (alength keys)
        (async (alength keys)))))

  (node-lookup [_ cmp key storage opts]
               (let [{:keys [sync?] :or {sync? true}} opts
                     idx (lookup-exact cmp keys key)
                     result (when-not (== -1 idx)
                              (arrays/aget keys idx))]
                 (if sync?
                   result
                   (async result))))

  (node-conj [_ cmp key storage opts]
             (let [{:keys [sync?] :or {sync? true}} opts
                   idx    (binary-search-l cmp keys (dec (arrays/alength keys)) key)
                   keys-l (arrays/alength keys)
                   result (cond
                            ;; element already here
                            (and (< idx keys-l)
                                 (== 0 (cmp key (arrays/aget keys idx))))
                            nil

                            ;; splitting
                            (== keys-l max-len)
                            (let [middle (arrays/half (inc keys-l))]
                              (if (> idx middle)
                                ;; new key spes to the second half
                                (arrays/array
                                 (Leaf. (.slice keys 0 middle) nil)
                                 (Leaf. (cut-n-splice keys middle keys-l idx idx (arrays/array key)) nil))
                                ;; new key spes to the first half
                                (arrays/array
                                 (Leaf. (cut-n-splice keys 0 middle idx idx (arrays/array key)) nil)
                                 (Leaf. (.slice keys middle keys-l) nil))))

                            ;; ok as is
                            :else
                            (arrays/array (Leaf. (splice keys idx idx (arrays/array key)) nil)))]
               (if sync?
                 result
                 (async result))))
  (node-disj [_ cmp key root? left right storage {:keys [sync?] :or {sync? true} :as opts}]
             (let [idx (lookup-exact cmp keys key)]
               (async+sync sync?
                           (async
                            (when-not (== -1 idx) ;; key is here
                              (let [new-keys (splice keys idx (inc idx) (arrays/array))]
                                (rotate (Leaf. new-keys nil) root? left right))))))))