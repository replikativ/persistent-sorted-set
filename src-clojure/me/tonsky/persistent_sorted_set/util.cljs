(ns me.tonsky.persistent-sorted-set.util
  (:require [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [me.tonsky.persistent-sorted-set.impl.node :as node]))

(defn- binary-search-l [cmp arr r k]
  (loop [l 0
         r (long r)]
    (if (<= l r)
      (let [m  (arrays/half (+ l r))
            mk (arrays/aget arr m)]
        (if (neg? (cmp mk k))
          (recur (inc m) r)
          (recur l (dec m))))
      l)))

(defn- binary-search-r [cmp arr r k]
  (loop [l 0
         r (long r)]
    (if (<= l r)
      (let [m  (arrays/half (+ l r))
            mk (arrays/aget arr m)]
        (if (pos? (cmp mk k))
          (recur l (dec m))
          (recur (inc m) r)))
      l)))

(defn cut-n-splice
  [arr cut-from cut-to splice-from splice-to xs]
  (let [xs-l (arrays/alength xs)
        l1   (- splice-from cut-from)
        l2   (- cut-to splice-to)
        l1xs (+ l1 xs-l)
        new-arr (arrays/make-array (+ l1 xs-l l2))]
    (arrays/acopy arr cut-from splice-from new-arr 0)
    (arrays/acopy xs 0 xs-l new-arr l1)
    (arrays/acopy arr splice-to cut-to new-arr l1xs)
    new-arr))

(defn splice [arr splice-from splice-to xs]
  (cut-n-splice arr 0 (arrays/alength arr) splice-from splice-to xs))

(defn- ^boolean eq-arr [cmp a1 a1-from a1-to a2 a2-from a2-to]
  (let [len (- a1-to a1-from)]
    (and
     (== len (- a2-to a2-from))
     (loop [i 0]
       (cond
         (== i len)
         true

         (not (== 0 (cmp
                     (arrays/aget a1 (+ i a1-from))
                     (arrays/aget a2 (+ i a2-from)))))
         false

         :else
         (recur (inc i)))))))

(defn check-n-splice [cmp arr from to new-arr]
  (if (eq-arr cmp arr from to new-arr 0 (arrays/alength new-arr))
    arr
    (splice arr from to new-arr)))

(defn return-array
  "Drop non-nil references and return array of arguments"
  ([a1]
   (arrays/array a1))
  ([a1 a2]
   (if a1
     (if a2
       (arrays/array a1 a2)
       (arrays/array a1))
     (arrays/array a2)))
  ([a1 a2 a3]
   (if a1
     (if a2
       (if a3
         (arrays/array a1 a2 a3)
         (arrays/array a1 a2))
       (if a3
         (arrays/array a1 a3)
         (arrays/array a1)))
     (if a2
       (if a3
         (arrays/array a2 a3)
         (arrays/array a2))
       (arrays/array a3)))))

(defn rotate [node root? left right settings]
  (let [min-len (/ (:branching-factor settings 32) 2)]
    (cond
    ;; root never merges
    root?
    (return-array node)

    ;; enough keys, nothing to merge
    (> (node/len node) min-len)
    (return-array left node right)

    ;; left and this can be merged to one
    (and left (<= (node/len left) min-len))
    (return-array (node/merge left node) right)

    ;; right and this can be merged to one
    (and right (<= (node/len right) min-len))
    (return-array left (node/merge node right))

    ;; left has fewer nodes, redestribute with it
    (and left (or (nil? right)
                  (< (node/len left) (node/len right))))
    (let [nodes (node/merge-split left node)]
      (return-array (arrays/aget nodes 0) (arrays/aget nodes 1) right))

    ;; right has fewer nodes, redestribute with it
    :else
    (let [nodes (node/merge-split node right)]
      (return-array left (arrays/aget nodes 0) (arrays/aget nodes 1))))))

(defn lookup-exact [cmp arr key]
  (let [arr-l (arrays/alength arr)
        idx   (binary-search-l cmp arr (dec arr-l) key)]
    (if (and (< idx arr-l)
             (== 0 (cmp (arrays/aget arr idx) key)))
      idx
      -1)))

(defn merge-n-split [a1 a2]
  (let [a1-l    (arrays/alength a1)
        a2-l    (arrays/alength a2)
        total-l (+ a1-l a2-l)
        r1-l    (arrays/half total-l)
        r2-l    (- total-l r1-l)
        r1      (arrays/make-array r1-l)
        r2      (arrays/make-array r2-l)]
    (if (<= a1-l r1-l)
      (do
        (arrays/acopy a1 0             a1-l          r1 0)
        (arrays/acopy a2 0             (- r1-l a1-l) r1 a1-l)
        (arrays/acopy a2 (- r1-l a1-l) a2-l          r2 0))
      (do
        (arrays/acopy a1 0    r1-l r1 0)
        (arrays/acopy a1 r1-l a1-l r2 0)
        (arrays/acopy a2 0    a2-l r2 (- a1-l r1-l))))
    (arrays/array r1 r2)))