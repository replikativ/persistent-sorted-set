(ns org.replikativ.persistent-sorted-set.impl.range
  (:require [org.replikativ.persistent-sorted-set.arrays :as arrays]
            [org.replikativ.persistent-sorted-set.branch :as branch]
            [org.replikativ.persistent-sorted-set.impl.node :as node]
            [org.replikativ.persistent-sorted-set.util :as util]))

(def ^:private sync-opts {:sync? true})

(defn- entries-from [n storage from cmp]
  (lazy-seq
   (let [keys (.-keys n)
         size (node/len n)
         i (if (nil? from) 0 (util/binary-search-l cmp keys (dec size) from))]
     (when (< i size)
       (if (zero? (node/level n))
         (map #(arrays/aget keys %) (range i size))
         (mapcat (fn [idx]
                   (entries-from (branch/child n storage idx sync-opts) storage
                                 (when (= idx i) from) cmp))
                 (range i size)))))))

(defn- entries-backward [n storage from cmp]
  (lazy-seq
   (let [keys (.-keys n)
         size (node/len n)
         end (if (nil? from) size (util/binary-search-r cmp keys (dec size) from))]
     (if (zero? (node/level n))
       (map #(arrays/aget keys %) (range (dec end) -1 -1))
       ;; Branch keys are child maxima. The first child whose maximum exceeds
       ;; from may still contain matching entries, including comparator ties.
       (letfn [(siblings [idx bound]
                 (lazy-seq
                  (when (>= idx 0)
                    (lazy-cat (entries-backward (branch/child n storage idx sync-opts)
                                                storage bound cmp)
                              (siblings (dec idx) nil)))))]
         (siblings (min end (dec size)) from))))))

(defn slice [root storage lo hi cmp]
  (let [xs (entries-from root storage lo cmp)]
    (seq (if (nil? hi) xs (take-while #(not (pos? (cmp % hi))) xs)))))

(defn rslice [root storage hi lo cmp]
  (let [xs (entries-backward root storage hi cmp)]
    (seq (if (nil? lo) xs (take-while #(not (neg? (cmp % lo))) xs)))))
