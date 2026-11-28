(ns org.replikativ.persistent-sorted-set.test.bulk-cases
  (:require [clojure.test :refer [is]]
            [org.replikativ.persistent-sorted-set :as pss]))

(defn check-builders [make-storage tree-shape]
  ;; Include both sides of the final-node cuts and inputs spanning several levels.
  (doseq [[bf xs] (concat (for [bf [4 8 16 32]
                                n [0 1 2 3 7 15 16 17 31 32 33 47 48 63 64 65 100 257 1000 5000]]
                            [bf (range n)])
                          (for [n [50 200 777]] [16 (map #(* 37 %) (range n))]))]
    (let [storage (make-storage)
          opts {:branching-factor bf :storage storage}
          arr (pss/from-sorted-array compare (to-array xs) (count xs) opts)
          streamed (pss/from-sorted-seq compare xs opts)
          context (str "bf=" bf " n=" (count xs))]
      (is (= (vec xs) (vec (seq arr)) (vec (seq streamed))) context)
      (when (seq xs)
        (is (= (tree-shape arr storage) (tree-shape streamed storage)) context))))
  (let [xs (sort (map #(str "k" %) (range 500)))
        s (pss/from-sorted-seq compare xs {:storage (make-storage) :branching-factor 16})]
    (is (= xs (seq s)))))
