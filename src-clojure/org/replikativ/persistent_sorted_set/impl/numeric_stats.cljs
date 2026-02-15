(ns org.replikativ.persistent-sorted-set.impl.numeric-stats
  "Numeric statistics implementation for testing and demonstration.
   Computes count, sum, min, max over numeric keys."
  (:require [org.replikativ.persistent-sorted-set.impl.measure :as measure]))

(defrecord NumericStats [cnt sum min-val max-val])

(def numeric-stats-ops
  "Statistics operations for numeric keys.
   Computes count, sum, min, max."
  (reify measure/IMeasure
    (identity-measure [_]
      (->NumericStats 0 0 nil nil))

    (extract [_ key]
      (->NumericStats 1 key key key))

    (merge-measure [_ m1 m2]
      (cond
        (nil? m1) m2
        (nil? m2) m1
        (zero? (:cnt m1)) m2
        (zero? (:cnt m2)) m1
        :else
        (->NumericStats
         (+ (:cnt m1) (:cnt m2))
         (+ (:sum m1) (:sum m2))
         (min (:min-val m1) (:min-val m2))
         (max (:max-val m1) (:max-val m2)))))

    (remove-measure [this current key recompute-fn]
      (let [new-cnt (dec (:cnt current))
            new-sum (- (:sum current) key)]
        (if (or (= key (:min-val current))
                (= key (:max-val current)))
          ;; Min or max was removed, need to recompute
          (recompute-fn)
          ;; Can update incrementally
          (->NumericStats new-cnt new-sum (:min-val current) (:max-val current)))))

    (weight [_ measure]
      (:cnt measure))))
