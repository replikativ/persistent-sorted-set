;; Run after clojure -T:build java. Use a fresh process for each implementation:
;; clojure -J-Xms384m -J-Xmx384m -M dev/portable_write_bench.clj portable
;; clojure -J-Xms384m -J-Xmx384m -M dev/portable_write_bench.clj java
(ns portable-write-bench
  (:require [org.replikativ.persistent-sorted-set :as pss]
            [org.replikativ.persistent-sorted-set.branch :as branch]
            [org.replikativ.persistent-sorted-set.impl.node :as node]
            [org.replikativ.persistent-sorted-set.leaf :as leaf]))

(def sync-opts {:sync? true})

(defn- portable-insert [root key]
  (if-let [^objects pieces (node/add root nil key compare sync-opts)]
    (if (= 1 (alength pieces))
      (aget pieces 0)
      (let [settings (if (zero? (node/level root))
                       (.-settings ^org.replikativ.persistent_sorted_set.leaf.Leaf root)
                       (.-settings ^org.replikativ.persistent_sorted_set.branch.Branch root))]
        (branch/promote-root pieces settings compare -1)))
    root))

(defn- portable-delete [root key]
  (if-let [^objects pieces (node/$remove root nil key nil nil compare sync-opts)]
    (let [r (aget pieces 0)]
      (if (and (pos? (node/level r)) (= 1 (node/len r)))
        (branch/child r nil 0 sync-opts)
        r))
    root))

(defn- by-id [a b] (compare (nth a 0) (nth b 0)))

(defn- measure [label run verify]
  (let [samples (mapv (fn [_]
                        (let [start (System/nanoTime)
                              result (run)
                              ms (/ (- (System/nanoTime) start) 1e6)]
                          (verify result)
                          ms))
                      (range 20))]
    (prn label (nth (sort (drop 11 samples)) 4))))

(let [[mode fanout] *command-line-args*
      settings {:branching-factor (parse-long (or fanout "32")) :diff-buf-size 0 :comparator compare}
      [empty-tree insert replace-key delete lookup size]
      (case mode
        "portable" [(leaf/empty-leaf settings) portable-insert
                    (fn [r k] (let [^objects pieces (node/$replace r nil k k by-id sync-opts)] (aget pieces 0)))
                    portable-delete #(node/lookup %1 nil %2 by-id sync-opts) node/subtree-count]
        "java" [(pss/sorted-set* settings) #(pss/conj %1 %2 compare) #(pss/replace %1 %2 %2 by-id)
                #(pss/disj %1 %2 compare) #(pss/lookup %1 %2 by-id) count])
      n 4096
      keys (mapv #(mod (* % 193) n) (range n))
      half (subvec keys 0 (quot n 2))
      tree (reduce insert empty-tree keys)
      old (reduce insert empty-tree (mapv #(vector % 0) keys))
      changes (mapv #(vector % 1) half)]
  (measure :insert #(reduce insert empty-tree keys) #(assert (= n (size %))))
  (measure :replace #(reduce replace-key old changes)
           (fn [r] (doseq [k changes]
                     (assert (= k (lookup r k)))
                     (assert (= [(first k) 0] (lookup old k))))))
  (measure :delete #(reduce delete tree half) #(assert (= (count half) (size %)))))
