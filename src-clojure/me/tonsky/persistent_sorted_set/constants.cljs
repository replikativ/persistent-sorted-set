(ns me.tonsky.persistent-sorted-set.constants
  (:require [me.tonsky.persistent-sorted-set.arrays :as arrays]))

(def ^:const max-safe-path
  "js limitation for bit ops"
  (js/Math.pow 2 31))

(def ^:const bits-per-level
  "tunable param"
  5)

(def ^:const max-len
  (js/Math.pow 2 bits-per-level)) ;; 32

(def ^:const min-len
  (/ max-len 2)) ;; 16

(def ^:private ^:const avg-len
  (arrays/half (+ max-len min-len))) ;; 24

(def ^:const max-safe-level
  (js/Math.floor (/ 31 bits-per-level))) ;; 6

(def ^:const bit-mask
  (- max-len 1)) ;; 0b011111 = 5 bit

(def ^:const uninitialized-hash nil)

(def ^:const empty-path 0)