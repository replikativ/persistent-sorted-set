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
    (loop [c 0, addr (ss/store s0 (mkst)), prev-addr nil, produced-by s0]
      (if (= c cycles)
        {:ok true}
        (let [loaded (fresh-restore addr disk bf b)
              got (vec (seq loaded)) exp (vec (seq @ref))]
          (if (not= got exp)
            (let [diff (first (remove (fn [[a bb]] (= a bb)) (map vector (concat got (repeat nil)) (concat exp (repeat nil)))))
                  bad-k (some (fn [[g e]] (when (not= g e) (first (or e g)))) (map vector (concat got (repeat nil)) (concat exp (repeat nil))))
                  ;; produced-by = the LIVE in-memory set whose store produced `addr`.
                  live-has (boolean (some #(= bad-k (first %)) (seq produced-by)))]
              {:ok false :cycle c :diff diff :bad-k bad-k :addr addr :prev-addr prev-addr :disk disk
               :live-has-bad-k live-has
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
              (recur (inc c) (ss/store s2 (mkst)) addr s2))))))))

;; Replay the SAME rng stream, but after EVERY op compare the live op-buf set to ref.
;; Classifies the first divergence:
;;   - at a cycle's restore (loaded != ref before any op) -> STORE/RESTORE bug
;;   - mid-cycle after an op (op made live != ref)        -> IN-MEMORY MUTATION bug
;; For an in-memory failure, dumps the op, contains?-before/after, and the live seq window.
(defn isolate [seed bf b keyrange cycles ops]
  (let [rng (Random. seed)
        ri (fn [n] (.nextInt rng n))
        disk (atom {})
        mkst #(tstore/->Storage (atom {}) disk (opbuf-settings bf b))
        ref (atom (sorted-set-by cmp))
        s0 (reduce (fn [s i] (swap! ref conj [i 0]) (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor bf :op-buf-size b}) (range 0 keyrange 2))
        window (fn [s k] (vec (map first (subseq s >= [(- k 4) 0] <= [(+ k 4) 0]))))]
    (loop [c 0, addr (ss/store s0 (mkst))]
      (if (= c cycles)
        (do (println "no divergence in" cycles "cycles") {:ok true})
        (let [loaded (fresh-restore addr disk bf b)]
          (if (not= (vec (seq loaded)) (vec (seq @ref)))
            (do (println "=== STORE/RESTORE divergence at cycle" c "===")
                (println "restore produced a set != ref BEFORE any cycle-" c "op")
                {:store-restore-bug c})
            (let [bad (atom nil)
                  s2 (reduce (fn [s i]
                               (let [k (ri keyrange), op (ri 3)
                                     before-k (contains? s [k 0])
                                     refbefore (window @ref k)
                                     livebefore (window s k)
                                     [tag s'] (cond
                                                (= op 1) [[:disj k] (do (swap! ref disj [k 0]) (disj s [k 0]))]
                                                (and (= op 2) (contains? @ref [k 0]))
                                                [[:repl k] (do (swap! ref #(-> % (disj [k 0]) (conj [k (inc c)]))) (ss/replace s [k 0] [k (inc c)]))]
                                                :else [[:conj k] (do (swap! ref conj [k 0]) (conj s [k 0]))])]
                                 (when (and (nil? @bad) (not= (vec (seq s')) (vec (seq @ref))))
                                   (reset! bad {:cycle c :op-idx i :op tag :k k
                                                :contained-before before-k
                                                :ref-window-before refbefore :live-window-before livebefore
                                                :ref-window-after (window @ref k) :live-window-after (window s' k)}))
                                 s'))
                             loaded (range ops))]
              (if @bad
                (do (println "=== IN-MEMORY MUTATION divergence ===")
                    (doseq [[kk vv] @bad] (println " " kk vv))
                    @bad)
                (recur (inc c) (ss/store s2 (mkst)))))))))))

;; At the start of each cycle (fresh restore, NO mutation), check read consistency:
;; for every key, does contains? agree with membership in seq? Reports first mismatch.
(defn check-read-consistency [seed bf b keyrange cycles ops]
  (let [rng (Random. seed)
        ri (fn [n] (.nextInt rng n))
        disk (atom {})
        mkst #(tstore/->Storage (atom {}) disk (opbuf-settings bf b))
        ref (atom (sorted-set-by cmp))
        s0 (reduce (fn [s i] (swap! ref conj [i 0]) (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor bf :op-buf-size b}) (range 0 keyrange 2))]
    (loop [c 0, addr (ss/store s0 (mkst))]
      (if (= c cycles)
        (do (println "read-consistent across all" cycles "cycles") {:ok true})
        (let [loaded (fresh-restore addr disk bf b)
              present (set (map first (seq loaded)))
              mismatch (first (for [k (range keyrange)
                                    :let [c? (contains? loaded [k 0]) s? (contains? present k)]
                                    :when (not= c? s?)]
                                {:k k :contains c? :in-seq s?}))]
          (if mismatch
            (do (println "=== READ INCONSISTENCY at cycle" c "===")
                (println "  " mismatch "  (contains? disagrees with seq on a fresh restore)")
                (assoc mismatch :cycle c :addr addr :disk disk))
            (let [s2 (reduce (fn [s _]
                               (let [k (ri keyrange), op (ri 3)]
                                 (cond
                                   (= op 1) (do (swap! ref disj [k 0]) (disj s [k 0]))
                                   (and (= op 2) (contains? @ref [k 0]))
                                   (do (swap! ref #(-> % (disj [k 0]) (conj [k (inc c)]))) (ss/replace s [k 0] [k (inc c)]))
                                   :else (do (swap! ref conj [k 0]) (conj s [k 0])))))
                             loaded (range ops))]
              (recur (inc c) (ss/store s2 (mkst))))))))))

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
            (println "LIVE (pre-store) set had bad-k?" (:live-has-bad-k r)
                     "→" (if (:live-has-bad-k r) "STORE-SIDE drop (serialization)" "in-memory/mutation bug"))
            (println "op history for key" k ":" (:oplog-key r))
            (println "cycle" (dec (:cycle r)) "ops (keys<7) — the store that dropped k:" (:oplog-prev-cycle r))
            (println "--- PREV tree (had k), path to" k "---")
            (when (:prev-addr r) (trace-key (:disk r) (:prev-addr r) k ""))
            (println "--- FAILING tree (missing k), path to" k "---")
            (trace-key (:disk r) (:addr r) k "")
            (dissoc r :disk)))))))
