(ns me.tonsky.persistent-sorted-set.macros
  (:require [clojure.walk]))

(def async->sync '{async do, await do})

(defmacro async+sync [sync? async-code]
  (let [sync-code      (clojure.walk/postwalk
                        (fn [n] (async->sync n n))
                        async-code)]
    `(if ~sync?
       ~sync-code
       ~async-code)))
