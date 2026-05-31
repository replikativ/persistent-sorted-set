(ns op-buf-v5-io-bench
  "Measure the IO savings of OP_BUF_V5 vs baseline PSS: object PUTs per commit
   (amortized over flushes), total durable objects/bytes, and the read-amplification
   cost of materializing a buffered tree. Each configuration is correctness-checked
   (fresh-restore == reference) so the numbers come from a verified-correct run."
  (:require [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore]
            [clojure.edn :as edn])
  (:import [org.replikativ.persistent_sorted_set Settings]
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

(defn disk-bytes [disk] (reduce + 0 (map (comp count str) (vals @disk))))

;; A "commit" = apply K random ops (insert-heavy mix: ~70% conj, ~15% replace, ~15% disj)
;; to the LIVE in-memory tree (kept across commits, like a datahike connection), then store.
;; b=0 ⇒ baseline (opBufSize off). Returns per-commit PUTs and end-state IO + a correctness flag.
;; Pure-replace (update-in-place) workload: every op replaces the value at an EXISTING
;; cmp-key, which never changes leaf sizes ⇒ always content-only ⇒ buffers to ~1 PUT/commit
;; until the accumulated diff exceeds B. Demonstrates the best-case ceiling.
(defn measure-replace [seed bf b M C K]
  (let [rng (Random. seed)
        ri (fn [n] (.nextInt rng n))
        disk (atom {})
        mkst #(tstore/->Storage (atom {}) disk (opbuf-settings bf b))
        s0 (reduce (fn [s i] (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor bf :op-buf-size b})
                   (range M))
        ref (atom (into (sorted-set-by cmp) (map #(vector % 0) (range M))))
        ckpt (ss/store s0 (mkst))]
    (loop [c 0, s s0, addr ckpt, wp (transient [])]
      (if (= c C)
        (let [loaded (fresh-restore addr disk bf b)]
          {:bf bf :b b :ok (= (vec (seq loaded)) (vec (seq @ref)))
           :puts-per-commit (double (/ (reduce + (persistent! wp)) C))
           :objects (count @disk) :bytes (disk-bytes disk)})
        (let [s2 (reduce (fn [s _]
                           (let [k (ri M)]
                             (swap! ref #(-> % (disj [k 0]) (conj [k (inc c)])))
                             (ss/replace s [k 0] [k (inc c)])))
                         s (range K))]
          (tstore/with-stats nil)
          (let [a2 (ss/store s2 (mkst))]
            (recur (inc c) s2 a2 (conj! wp (:writes @tstore/*stats)))))))))

;; Pure-insert split rate: only conj of FRESH keys (mode :seq = append at right edge,
;; :rand = scattered). Isolates how often an insert triggers a structural spine write,
;; with no delete/replace noise. Reports structural commits per 1000 inserts.
(defn measure-insert [seed bf b M C K mode]
  (let [rng (Random. seed)
        disk (atom {})
        mkst #(tstore/->Storage (atom {}) disk (opbuf-settings bf b))
        s0 (reduce (fn [s i] (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor bf :op-buf-size b})
                   (range M))
        nxt (atom M)                                   ; next sequential key
        ckpt (ss/store s0 (mkst))]
    (loop [c 0, s s0, wp (transient [])]
      (if (= c C)
        (let [wpv (persistent! wp), multi (filter #(> % 1) wpv)]
          {:bf bf :b b :mode mode
           :puts-per-commit (double (/ (reduce + wpv) C))
           :one-put-frac (double (/ (count (filter #(<= % 1) wpv)) C))
           :structural-commits (count multi)
           :struct-per-1k-inserts (double (/ (* 1000 (count multi)) (* C K)))
           :inserts-per-split (if (seq multi) (double (/ (* C K) (count multi))) ##Inf)
           :mean-structural-puts (if (seq multi) (double (/ (reduce + multi) (count multi))) 0.0)})
        (let [s2 (reduce (fn [s _]
                           (let [k (if (= mode :seq) (swap! nxt inc)
                                       (+ M (.nextInt rng (* 50 M))))]
                             (conj s [k 0])))
                         s (range K))]
          (tstore/with-stats nil)
          (ss/store s2 (mkst))
          (recur (inc c) s2 (conj! wp (:writes @tstore/*stats))))))))

(defn measure [seed bf b M C K keyrange]
  (let [rng (Random. seed)
        ri (fn [n] (.nextInt rng n))
        disk (atom {})
        mkst #(tstore/->Storage (atom {}) disk (opbuf-settings bf b))
        ref (atom (sorted-set-by cmp))
        s0 (reduce (fn [s i] (swap! ref conj [i 0]) (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor bf :op-buf-size b})
                   (range M))
        ckpt-addr (ss/store s0 (mkst))]            ; initial checkpoint (full write, not counted)
    (loop [c 0, s s0, addr ckpt-addr, writes-per (transient [])]
      (if (= c C)
        (let [;; correctness: fresh-restore the final durable tree, compare to ref
              loaded (fresh-restore addr disk bf b)
              ok (= (vec (seq loaded)) (vec (seq @ref)))
              ;; read-amplification: reads to FULLY materialize a fresh restore
              _ (tstore/with-stats nil)
              l2 (fresh-restore addr disk bf b)
              _ (dorun (seq l2))
              read-amp (:reads @tstore/*stats)
              wp (persistent! writes-per)]
          (let [one (count (filter #(<= % 1) wp))      ; content-only commits (≤1 PUT = buffered root)
                multi (filter #(> % 1) wp)]            ; split/merge-triggered spine writes
            {:bf bf :b b :ok ok
             :commits C :ops-per-commit K
             :puts-total (reduce + wp)
             :puts-per-commit (double (/ (reduce + wp) C))
             :min-puts (apply min wp) :max-puts (apply max wp)
             :one-put-frac (double (/ one C))           ; fraction of commits that cost ≤1 PUT
             :structural-commits (count multi)          ; commits that triggered a spine write
             :struct-per-1k-ops (double (/ (* 1000 (count multi)) (* C K)))
             :mean-structural-puts (if (seq multi) (double (/ (reduce + multi) (count multi))) 0.0)
             :objects (count @disk) :bytes (disk-bytes disk)
             :restore-reads read-amp}))
        (let [s2 (reduce (fn [s _]
                           (let [k (ri keyrange), r (ri 100)]
                             (cond
                               (< r 15) (do (swap! ref disj [k 0]) (disj s [k 0]))
                               (and (< r 30) (contains? @ref [k 0]))
                               (do (swap! ref #(-> % (disj [k 0]) (conj [k (inc c)])))
                                   (ss/replace s [k 0] [k (inc c)]))
                               :else (do (swap! ref conj [k 0]) (conj s [k 0])))))
                         s (range K))]
          (tstore/with-stats nil)
          (let [addr2 (ss/store s2 (mkst))
                w (:writes @tstore/*stats)]
            (recur (inc c) s2 addr2 (conj! writes-per w))))))))

(defn bench [seed bf M C K keyrange bs]
  (println (format "=== IO bench: bf=%d  init=%d  commits=%d  ops/commit=%d  keyrange=%d  seed=%d ==="
                    bf M C K keyrange seed))
  (println (format "%-10s %-5s %8s %8s %8s %9s %9s %10s %8s"
                   "config" "ok" "PUTs/c" "minP" "maxP" "objects" "bytes" "rd/restore" "totPUTs"))
  (let [base (measure seed bf 0 M C K keyrange)]
    (doseq [r (cons base (map #(measure seed bf % M C K keyrange) bs))]
      (println (format "%-10s %-5s %8.2f %8d %8d %9d %9d %10d %8d"
                       (if (zero? (:b r)) "baseline" (str "opbuf B=" (:b r)))
                       (str (:ok r)) (:puts-per-commit r) (:min-puts r) (:max-puts r)
                       (:objects r) (:bytes r) (:restore-reads r) (:puts-total r))))
    (let [bvals (map #(measure seed bf % M C K keyrange) bs)
          bpc (:puts-per-commit base)]
      (println "--- savings vs baseline (PUTs/commit) ---")
      (doseq [r bvals]
        (println (format "  B=%-4d  %.2f  →  %.1f×  fewer PUTs/commit"
                         (:b r) (:puts-per-commit r) (/ bpc (:puts-per-commit r))))))))
