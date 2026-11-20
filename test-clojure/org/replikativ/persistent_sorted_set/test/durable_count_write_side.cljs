(ns org.replikativ.persistent-sorted-set.test.durable-count-write-side
  "The ClojureScript twin of the JVM `durable-count-insert-side`: a write on a cold tree
   must not leave the ROOT without a count.

   A branch's own count is kept by delta on this side already (`(inc old-sc)` on every add
   arm). What was not kept is the count of a node that BECOMES the root: the wrap built when
   the root splits summed its two halves (unknown when their children are on disk), and the
   single child promoted when the root collapses is a join (unknown as soon as either joined
   node was). The set tracks its own element count, so both are known regardless."
  (:require [cljs.test :refer-macros [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.btset :as btset]
            [org.replikativ.persistent-sorted-set.branch :as branch]
            [org.replikativ.persistent-sorted-set.impl.node :as node]
            [org.replikativ.persistent-sorted-set.test.storage.util :as u]))

(defn- generations
  "`gens` rounds of (cold restore, ops, store). Returns [unknown-roots live final-cold-set]."
  [bf n gens ops-fn]
  ;; Baseline pinned: the point is the count arithmetic, and the suite's default buffers.
  (let [opts {:branching-factor bf :diff-buf-size 0}
        disk (atom {})
        st   #(u/storage (atom {}) disk opts)
        s0   (reduce s/conj (s/sorted-set* (assoc opts :storage (st))) (range n))]
    (loop [addr (s/store s0 (st)) gen 0 live (set (range n)) unknown 0]
      (let [cold (s/restore addr (st) opts)]
        (if (= gen gens)
          [unknown live cold]
          (let [ops   (ops-fn gen (vec (sort live)))
                s1    (reduce (fn [s [op k]] (if (= op :del) (s/disj s k) (s/conj s k))) cold ops)
                live' (reduce (fn [l [op k]] (if (= op :del) (disj l k) (conj l k))) live ops)
                root  (btset/root-node s1)]
            (recur (s/store s1 (st)) (inc gen) live'
                   (cond-> unknown
                     (and (instance? branch/Branch root) (neg? (node/subtree-count root))) inc))))))))

(def ^:private patterns
  {:append    (fn [gen _] (concat (map #(vector :add (+ 100000 (* gen 1000) %)) (range 400))
                                  ;; redundant inserts: must change nothing
                                  (map #(vector :add (+ 100000 (* gen 1000) %)) (range 0 400 7))))
   :prefix    (fn [_ sorted] (concat (map #(vector :del %) (take 400 sorted))
                                     ;; absent keys: must change nothing
                                     (map #(vector :del (- -1 %)) (range 20))))
   :scattered (fn [_ sorted] (map #(vector :del %) (take 400 (take-nth (max 1 (quot (count sorted) 400)) sorted))))
   :mixed     (fn [gen sorted] (interleave (map #(vector :del %) (take 200 (take-nth 5 sorted)))
                                           (map #(vector :add (+ 100000 (* gen 1000) %)) (range 200))))})

(deftest the-root-keeps-its-count-through-cold-writes
  (doseq [bf [8 16]
          [label ops-fn] patterns]
    (let [[unknown live cold] (generations bf 3000 6 ops-fn)
          root (btset/root-node cold)]
      (is (zero? unknown)
          (str label " bf=" bf ": " unknown " of 6 generations left the root without a count"))
      (when (instance? branch/Branch root)
        (is (= (count live) (node/subtree-count root))
            (str label " bf=" bf ": a cold reader's root carries the right count")))
      (is (= (count live) (count cold)) (str label " bf=" bf ": count is exact"))
      (is (= (sort live) (vec (s/seq cold))) (str label " bf=" bf ": elements are right")))))
