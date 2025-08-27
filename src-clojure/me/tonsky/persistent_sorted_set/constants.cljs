(ns me.tonsky.persistent-sorted-set.constants
  (:require [me.tonsky.persistent-sorted-set.arrays :as arrays]))

(def ^:const MAX_SAFE_PATH
  "js limitation for bit ops"
  (js/Math.pow 2 31))

(def ^:const BITS_PER_LEVEL
  "tunable param"
  5)

(def ^:const MAX_LEN (js/Math.pow 2 BITS_PER_LEVEL)) ;; 32

(def ^:const MIN_LEN (/ MAX_LEN 2)) ;; 16

(def ^:private ^:const AVG_LEN
  (arrays/half (+ MAX_LEN MIN_LEN))) ;; 24

(def ^:const MAX_SAFE_LEVEL
  (js/Math.floor (/ 31 BITS_PER_LEVEL))) ;; 6

(def ^:const BIT_MASK (- MAX_LEN 1)) ;; 0b011111 = 5 bit

(def ^:const UNINITIALIZED_HASH nil)

(def ^:const EMPTY_PATH 0)
