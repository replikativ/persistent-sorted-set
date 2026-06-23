(ns pss-mst-bench
  "Stage A microbenchmark for the MST (content-defined) boundary mode. Quantifies the gains
   that motivate prolly mode for yggdrasil/spindel state sync, and the costs. Run:

     clj -Sdeps '{:deps {org.replikativ/hasch {:mvn/version \"0.4.98\"}}}' -M:dev -e \"(require 'pss-mst-bench)(pss-mst-bench/report)\"

   Node addresses mirror datahike/yggdrasil exactly: a node's address is hasch/uuid over its
   {:level :keys :addresses} map, computed Merkle-style bottom-up. So 'ship-set' = the node
   addresses one replica holds that the other lacks — the real konserve-sync transfer unit."
  (:require [org.replikativ.persistent-sorted-set :as pss]
            [org.replikativ.persistent-sorted-set.boundary :as bnd]
            [hasch.core :as h]
            [clojure.set :as set])
  (:import [org.replikativ.persistent_sorted_set Leaf Branch ANode PersistentSortedSet]))

;; ---------- node addressing (Merkle, bottom-up; == yggdrasil hasch/uuid(node->map)) ----------

(defn- keys-of [node]
  (let [k (.-_keys ^ANode node) n (.-_len ^ANode node)]
    (mapv #(aget ^objects k %) (range n))))

(defn- collect
  "Bottom-up walk. Returns {:addr <merkle hash> :addrs #{all node hashes} :leaves [leaf sizes]}."
  [node]
  (if (instance? Leaf node)
    (let [a (h/uuid {:level 0 :keys (keys-of node)})]
      {:addr a :addrs #{a} :leaves [(.-_len ^Leaf node)]})
    (let [n (.-_len ^Branch node)
          children (mapv #(aget ^objects (.-_children ^Branch node) %) (range n))
          sub (mapv collect children)
          a (h/uuid {:level (.-_level ^Branch node)
                     :keys (keys-of node)
                     :addresses (mapv :addr sub)})]
      {:addr a
       :addrs (conj (reduce set/union #{} (map :addrs sub)) a)
       :leaves (vec (mapcat :leaves sub))})))

(defn tree-info [^PersistentSortedSet s]
  (let [r (collect (.root s))]
    {:addrs (:addrs r) :nodes (count (:addrs r)) :leaves (:leaves r) :root (:addr r)}))

;; ---------- construction ----------

(def BF 64)                              ; matches yggdrasil durable.cljc:43
(defn- count-opts [] {:branching-factor BF})
(defn- mst-opts [lzpl] {:branching-factor BF :boundary (bnd/mst-boundary lzpl)})

(defn make-set [elements opts] (reduce conj (pss/sorted-set* opts) elements))

(defn elements [family n]
  (case family
    :int    (vec (range n))
    ;; ~RegistryEntry-sized: [sys branch tx tag]
    :record (mapv (fn [i] [(format "sys-%07d" i) "main" i :meta]) (range n))
    ;; ~OR-Map [hk uid k v] with two uuid-strings
    :ormap  (mapv (fn [i] [(str (h/uuid i)) (str (h/uuid (+ i 7777))) (keyword (str "k" i)) i])
                  (range n))))

;; ---------- metrics ----------

(defn dist-stats [leaves]
  (let [n (count leaves) s (reduce + leaves) mean (double (/ s n))
        var (double (/ (reduce + (map #(let [d (- % mean)] (* d d)) leaves)) n))
        sd (Math/sqrt var)]
    {:leaves n :mean (Math/round mean) :cv (/ sd mean)
     :min (apply min leaves) :max (apply max leaves)
     :singletons (count (filter #(= 1 %) leaves))}))

(defn dedup-scenario
  "Same element SET built in K different insertion orders. How many DISTINCT node addresses
   across all K trees, and the ship-set between two replicas that converged on the same set?"
  [base K opts]
  (let [infos (mapv #(tree-info (make-set % opts)) (repeatedly K #(shuffle base)))
        all   (reduce set/union #{} (map :addrs infos))
        per   (double (/ (reduce + (map :nodes infos)) K))]
    {:per-tree (Math/round per)
     :distinct (count all)
     :sharing  (format "%.2fx" (/ (* K per) (count all)))   ; 1x = no sharing, Kx = full sharing
     :roots-identical (apply = (map :root infos))
     :ship-between-converged-replicas (count (set/difference (:addrs (first infos))
                                                             (:addrs (second infos))))}))

(defn incremental-scenario
  "Two peers A,B independently hold the SAME base set (different histories). A then applies a
   small delta. Sync A->B: how many nodes must ship? Ideal = only the nodes the delta changed."
  [base delta opts]
  (let [A  (make-set (shuffle base) opts)
        B  (make-set (shuffle base) opts)
        A2 (reduce conj A delta)
        ia (tree-info A) ia2 (tree-info A2) ib (tree-info B)
        ideal (count (set/difference (:addrs ia2) (:addrs ia)))      ; nodes the delta touched
        ship  (count (set/difference (:addrs ia2) (:addrs ib)))]     ; nodes B must actually fetch
    {:delta (count delta) :ideal-new-nodes ideal :must-ship ship
     :overhead (format "%.0fx" (/ (double ship) (max 1 ideal)))}))

(defn op-cost-us [family n opts]
  (let [els (elements family n)]
    (make-set (take 200 els) opts)            ; warm
    (let [t0 (System/nanoTime) _ (make-set els opts) dt (- (System/nanoTime) t0)]
      (/ (double dt) n 1000.0))))             ; µs per conj

;; ---------- report ----------

(defn report []
  (println "\n=== Stage A: MST boundary microbenchmark (bf/fanout =" BF ", lzpl 6 = B 64) ===\n")
  (doseq [family [:int :record :ormap]]
    (println "########## element family:" family "##########")
    (let [n 20000
          base (elements family n)
          ;; delta = 5 fresh elements disjoint from base (indices >= n)
          delta (mapv (fn [i] (case family
                                :int (+ n i)
                                :record [(format "sys-%07d" (+ n i)) "main" (+ n i) :meta]
                                :ormap [(str (h/uuid (+ n i))) (str (h/uuid (+ n i 7777))) (keyword (str "k" (+ n i))) (+ n i)]))
                      (range 5))]
      (do
        (println "\n-- node-size distribution (n=" n ") --")
        (println "  count(bf64):" (dist-stats (:leaves (tree-info (make-set base (count-opts))))))
        (doseq [lzpl [4 6 8]]
          (println "  mst(lzpl" lzpl "B" (int (Math/pow 2 lzpl)) "):"
                   (dist-stats (:leaves (tree-info (make-set base (mst-opts lzpl)))))))

        (println "\n-- cross-replica dedup (same set, 4 orders) --")
        (println "  count(bf64):" (dedup-scenario base 4 (count-opts)))
        (println "  mst(lzpl6) :" (dedup-scenario base 4 (mst-opts 6)))

        (println "\n-- incremental sync between divergent-history replicas (+5 elems) --")
        (println "  count(bf64):" (incremental-scenario base delta (count-opts)))
        (println "  mst(lzpl6) :" (incremental-scenario base delta (mst-opts 6)))

        (println "\n-- per-conj cost (µs/op) --")
        (println "  count(bf64):" (format "%.2f" (op-cost-us family n (count-opts))))
        (println "  mst(lzpl6) :" (format "%.2f" (op-cost-us family n (mst-opts 6))))
        (println))))
  (println "=== done ==="))
