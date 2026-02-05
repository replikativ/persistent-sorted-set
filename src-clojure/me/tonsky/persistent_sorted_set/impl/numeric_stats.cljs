(ns me.tonsky.persistent-sorted-set.impl.numeric-stats
  "Numeric statistics implementation for testing and demonstration.
   Computes count, sum, min, max over numeric keys."
  (:require [me.tonsky.persistent-sorted-set.impl.stats :as stats]))

(defrecord NumericStats [cnt sum min-val max-val])

(def numeric-stats-ops
  "Statistics operations for numeric keys.
   Computes count, sum, min, max."
  (reify stats/IStats
    (identity-stats [_]
      (->NumericStats 0 0 nil nil))

    (extract [_ key]
      (->NumericStats 1 key key key))

    (merge-stats [_ s1 s2]
      (cond
        (nil? s1) s2
        (nil? s2) s1
        (zero? (:cnt s1)) s2
        (zero? (:cnt s2)) s1
        :else
        (->NumericStats
          (+ (:cnt s1) (:cnt s2))
          (+ (:sum s1) (:sum s2))
          (min (:min-val s1) (:min-val s2))
          (max (:max-val s1) (:max-val s2)))))

    (remove-stats [this current key recompute-fn]
      (let [new-cnt (dec (:cnt current))
            new-sum (- (:sum current) key)]
        (if (or (= key (:min-val current))
                (= key (:max-val current)))
          ;; Min or max was removed, need to recompute
          (recompute-fn)
          ;; Can update incrementally
          (->NumericStats new-cnt new-sum (:min-val current) (:max-val current)))))))
