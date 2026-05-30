(ns op-buf-v5-flush-debug
  "Deterministic (seeded) reproduction of the OP_BUF_V5 flush-path bug, with durable
   tree dumping to localize where a buffered diff is lost under budget-overflow flushing."
  (:require [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore]
            [clojure.edn :as edn])
  (:import [org.replikativ.persistent_sorted_set Branch Leaf ANode Slot PersistentSortedSet IStorage Settings]
           [java.util Random]))

(def cmp (fn [a b] (compare (first a) (first b))))

(defn opbuf-settings ^Settings [bf b]
  (let [s (Settings. (int bf) nil nil nil (int b))]
    (set! (.-_comparator s) ^java.util.Comparator cmp)
    s))

(defn fresh-restore [addr disk bf b]
  (ss/restore-by cmp addr
                 (tstore/->Storage (atom {}) disk (opbuf-settings bf b))
                 {:branching-factor bf :op-buf-size b :comparator cmp}))

;; --- durable tree dump (reads disk objects; does NOT project) ------------------
(defn dump [disk addr indent]
  (let [{:keys [level keys addresses slots]} (edn/read-string (get @disk addr))]
    (if addresses
      (do
        (println indent "BRANCH L" level "addr" (subs (str addr) 0 8) "keys" (mapv first keys))
        (dotimes [i (count keys)]
          (let [a (nth addresses i)
                sl (get slots i)]
            (println indent "  child" i "→" (when a (subs (str a) 0 8))
                     (when sl (str "SLOT count=" (:count sl) " diff=" (pr-str (:diff sl)))))))
        ;; recurse only into children that have addresses (limit depth via indent length)
        (when (< (count indent) 8)
          (dotimes [i (count keys)]
            (when-let [a (nth addresses i)]
              (dump disk a (str indent "    "))))))
      (println indent "LEAF keys" (mapv first keys)))))

;; trace the path to cmp-key k through the durable tree, printing slots that mention k
(defn trace-key [disk addr k indent]
  (let [{:keys [level keys addresses slots]} (edn/read-string (get @disk addr))]
    (if addresses
      ;; find child index for k (high-key convention: first pivot >= k)
      (let [n (count keys)
            idx (loop [i 0] (cond (= i (dec n)) i
                                  (<= k (first (nth keys i))) i
                                  :else (recur (inc i))))
            a (nth addresses idx)
            sl (get slots idx)]
        (println indent "BRANCH L" level "addr" (subs (str addr) 0 8) "→ child" idx
                 (when sl (str " SLOT diff=" (pr-str (:diff sl)))))
        (when a (trace-key disk a k (str indent "  "))))
      (println indent "LEAF keys" (mapv first keys) " (k=" k "present?" (boolean (some #(= k (first %)) keys)) ")"))))

(defn run [seed bf b keyrange cycles ops]
  (let [rng (Random. seed)
        ri (fn [n] (.nextInt rng n))
        disk (atom {})
        mkst #(tstore/->Storage (atom {}) disk (opbuf-settings bf b))
        ref (atom (sorted-set-by cmp))
        s0 (reduce (fn [s i] (swap! ref conj [i 0]) (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor bf :op-buf-size b}) (range 0 keyrange 2))
        oplog (atom [])]
    (loop [c 0, addr (ss/store s0 (mkst)), prev-addr nil]
      (if (= c cycles)
        {:ok true}
        (let [loaded (fresh-restore addr disk bf b)
              got (vec (seq loaded)) exp (vec (seq @ref))]
          (if (not= got exp)
            (let [diff (first (remove (fn [[a bb]] (= a bb)) (map vector (concat got (repeat nil)) (concat exp (repeat nil)))))
                  bad-k (some (fn [[g e]] (when (not= g e) (first (or e g)))) (map vector (concat got (repeat nil)) (concat exp (repeat nil))))]
              {:ok false :cycle c :diff diff :bad-k bad-k :addr addr :prev-addr prev-addr :disk disk
               :oplog-key (filter #(= bad-k (second %)) @oplog)
               :oplog-prev-cycle (filter (fn [[_ kk cc]] (and (= cc (dec c)) (< kk 7))) @oplog)})
            (let [s2 (reduce (fn [s _]
                               (let [k (ri keyrange), op (ri 3)]
                                 (cond
                                   (= op 1) (do (swap! oplog conj [:disj k c]) (swap! ref disj [k 0]) (disj s [k 0]))
                                   (and (= op 2) (contains? @ref [k 0]))
                                   (do (swap! oplog conj [:repl k c]) (swap! ref #(-> % (disj [k 0]) (conj [k (inc c)]))) (ss/replace s [k 0] [k (inc c)]))
                                   :else (do (swap! oplog conj [:conj k c]) (swap! ref conj [k 0]) (conj s [k 0])))))
                             loaded (range ops))]
              (recur (inc c) (ss/store s2 (mkst)) addr))))))))

(defn scan [n bf b keyrange cycles ops]
  (let [fails (atom 0) bad (atom [])]
    (dotimes [seed n]
      (let [r (run seed bf b keyrange cycles ops)]
        (when-not (:ok r) (swap! fails inc) (swap! bad conj [seed (:cycle r) (:bad-k r)]))))
    (println "scan" n "seeds @ bf" bf "B" b "kr" keyrange "cyc" cycles "ops" ops "→"
             (- n @fails) "PASS," @fails "FAIL" (when (seq @bad) (str "(first few: " (vec (take 5 @bad)) ")")))
    @fails))

(defn find-and-dump []
  (loop [seed 0]
    (if (= seed 60)
      (println "no failure in 60 seeds")
      (let [r (run seed 4 8 40 25 20)]
        (if (:ok r)
          (recur (inc seed))
          (let [k (:bad-k r)]
            (println "=== FAIL seed" seed "cycle" (:cycle r) "bad-k" k "diff" (:diff r) "===")
            (println "op history for key" k ":" (:oplog-key r))
            (println "cycle" (dec (:cycle r)) "ops (keys<7) — the store that dropped k:" (:oplog-prev-cycle r))
            (println "--- PREV tree (had k), path to" k "---")
            (when (:prev-addr r) (trace-key (:disk r) (:prev-addr r) k ""))
            (println "--- FAILING tree (missing k), path to" k "---")
            (trace-key (:disk r) (:addr r) k "")
            (dissoc r :disk)))))))
