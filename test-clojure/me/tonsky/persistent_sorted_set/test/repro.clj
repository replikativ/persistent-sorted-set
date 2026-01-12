(ns me.tonsky.persistent-sorted-set.test.repro
  (:require
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.test.storage32 :as storage32])
  (:import
   [java.util ArrayList Collections Random]
   [me.tonsky.persistent_sorted_set Settings PersistentSortedSet]))

(defn seeded-shuffle [coll ^java.util.Random rnd]
  (let [al (java.util.ArrayList. coll)]
    (java.util.Collections/shuffle al rnd)
    (vec al)))

(defn rand-nth-seeded [coll ^java.util.Random rnd]
  (nth coll (.nextInt rnd (count coll))))

;; adapted from storage/test-lazyness... if you switch branching factor to 32
;; and re-run that test enough times, you'll get this error:
;;  Execution error (ArrayIndexOutOfBoundsException) at me.tonsky.persistent_sorted_set.Stitch/copyAll (Stitch.java:15).
;;  arraycopy: last destination index 33 out of bounds for object array[32]

(defn oob! [seed]
  (let [rnd      (Random. seed)
        size     100000
        xs       (seeded-shuffle (range size) rnd)
        rm       (vec (repeatedly (quot size 5) #(rand-nth-seeded xs rnd)))
        original (-> (reduce disj (into (set/sorted-set* {:branching-factor 32}) xs) rm)
                     (disj (quot size 4) (quot size 2)))
        storage  (storage32/storage)
        address  (set/store original storage)
        loaded   ^PersistentSortedSet (set/restore address storage {:branching-factor 32})]
    ;; this will blow up
    (reduce conj! (transient loaded) (range -1000 0))))

(comment
  (oob! 1756343976668)
  (oob! 1756343977008)
  (oob! 1756343978053)
  (oob! 1756343974789))
