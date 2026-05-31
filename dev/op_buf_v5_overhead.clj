(ns op-buf-v5-overhead
  "Isolate the CPU/byte OVERHEAD of OP_BUF_V5 vs baseline (opBufSize=0) using an
   in-process EDN storage, so the IO is ~free and only the buffer machinery
   (deposit / assembleNested / contentOnlyDiffSize / slotsForStorage / projection)
   and the serialized-size delta show up. Real backends (datahike :mem / S3) net the
   IO saving on top; here we want the cost the op-buffer adds."
  (:require [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set Settings IStorage Branch ANode]
           [java.util Random]))

(def cmp (fn [a b] (compare (first a) (first b))))
(defn opbuf-settings ^Settings [bf b]
  (let [s (Settings. (int bf) nil nil nil (int b))]
    (set! (.-_comparator s) ^java.util.Comparator cmp) s))
(defn fresh-restore [addr disk bf b]
  (ss/restore-by cmp addr (tstore/->Storage (atom {}) disk (opbuf-settings bf b))
                 {:branching-factor bf :op-buf-size b :comparator cmp}))
(defn disk-bytes [disk] (reduce + 0 (map (comp count str) (vals @disk))))
(defn ms [ns] (/ ns 1e6))

(defn measure [seed bf b M C K reps]
  (let [results
        (for [_ (range reps)]
          (let [rng (Random. seed)
                disk (atom {})
                mkst #(tstore/->Storage (atom {}) disk (opbuf-settings bf b))
                s0 (reduce (fn [s i] (conj s [i 0]))
                           (ss/sorted-set* {:comparator cmp :branching-factor bf :op-buf-size b}) (range M))
                addr0 (ss/store s0 (mkst))
                mut-ns (atom 0) store-ns (atom 0)
                _ (loop [c 0 s s0]
                    (when (< c C)
                      (let [t0 (System/nanoTime)
                            s2 (reduce (fn [s _]
                                         (let [k (.nextInt rng (* 4 M))]
                                           (conj s [k 0]))) s (range K))
                            t1 (System/nanoTime)
                            _  (ss/store s2 (mkst))
                            t2 (System/nanoTime)]
                        (swap! mut-ns + (- t1 t0)) (swap! store-ns + (- t2 t1))
                        (recur (inc c) s2))))
                ;; cold restore + full scan (projection cost)
                last-addr (ss/store (reduce (fn [s i] (conj s [(+ (* 9 M) i) 0])) (fresh-restore addr0 disk bf b) (range K)) (mkst))
                t3 (System/nanoTime)
                loaded (fresh-restore last-addr disk bf b)
                n (count (vec (seq loaded)))
                t4 (System/nanoTime)]
            {:mut-ns-per-op (/ (double @mut-ns) (* C K))
             :store-us-per-commit (/ (double @store-ns) C 1e3)
             :restore-scan-ms (ms (- t4 t3))
             :scanned n
             :bytes (disk-bytes disk)}))
        med (fn [k] (let [xs (sort (map k results))] (nth xs (quot (count xs) 2))))]
    {:bf bf :b b
     :mut-ns-per-op (med :mut-ns-per-op)
     :store-us-per-commit (med :store-us-per-commit)
     :restore-scan-ms (med :restore-scan-ms)
     :bytes (med :bytes)}))

;; No-serialization object store: store keeps the live node under a fresh uuid (still
;; calls .slotsForStorage on a Branch to incur that map-building cost), restore returns
;; it. store/restore are O(1) ⇒ the timing reflects ONLY the buffer MACHINERY
;; (deposit + assembleNested + contentOnlyDiffSize + slotsForStorage), not serialization.
(defn fast-store [^java.util.concurrent.atomic.AtomicLong ctr cmp*]
  (let [m (atom {})]
    (reify IStorage
      (comparator [_] cmp*)
      (restore [_ addr] (get @m addr))
      (store [_ node]
        (let [a (.incrementAndGet ctr)]
          (when (instance? Branch node) (.slotsForStorage ^Branch node))
          (swap! m assoc a node) a))
      (accessed [_ _])
      (markFreed [_ _])
      (isFreed [_ _] false))))

(defn measure-machinery [seed bf b M C K reps]
  (let [results
        (for [_ (range reps)]
          (let [rng (Random. seed)
                ctr (java.util.concurrent.atomic.AtomicLong.)
                mkst #(fast-store ctr cmp)
                s0 (reduce (fn [s i] (conj s [i 0]))
                           (ss/sorted-set* {:comparator cmp :branching-factor bf :op-buf-size b :storage (mkst)}) (range M))
                _ (ss/store s0 (mkst))
                store-ns (atom 0) calls0 (.get ctr)
                _ (loop [c 0 s s0]
                    (when (< c C)
                      (let [s2 (reduce (fn [s _] (conj s [(.nextInt rng (* 4 M)) 0])) s (range K))
                            t1 (System/nanoTime) _ (ss/store s2 (mkst)) t2 (System/nanoTime)]
                        (swap! store-ns + (- t2 t1)) (recur (inc c) s2))))]
            {:store-us-per-commit (/ (double @store-ns) C 1e3)
             :store-calls-per-commit (/ (double (- (.get ctr) calls0)) C)}))
        med (fn [k] (let [xs (sort (map k results))] (nth xs (quot (count xs) 2))))]
    {:b b :store-us-per-commit (med :store-us-per-commit) :store-calls-per-commit (med :store-calls-per-commit)}))

(defn run-machinery []
  (let [bf 512 M 50000 C 50 K 10 reps 7]
    (println (format "MACHINERY-ONLY (no serialization) bf=%d init=%d commits=%d ops/commit=%d reps=%d" bf M C K reps))
    (println (format "%-10s %18s %20s" "config" "store us/commit" "store() calls/commit"))
    (doseq [b [0 256 1024 4096]]
      (let [r (measure-machinery 1 bf b M C K reps)]
        (println (format "%-10s %18.1f %20.2f" (if (zero? b) "baseline" (str "B=" b))
                         (:store-us-per-commit r) (:store-calls-per-commit r)))))))

(defn run []
  (let [bf 512 M 50000 C 50 K 10 reps 5]
    (println (format "bf=%d init=%d commits=%d ops/commit=%d reps=%d (median)" bf M C K reps))
    (println (format "%-10s %14s %18s %16s %12s" "config" "mut ns/op" "store us/commit" "restore+scan ms" "bytes"))
    (doseq [b [0 256 1024 4096]]
      (let [r (measure 1 bf b M C K reps)]
        (println (format "%-10s %14.0f %18.1f %16.1f %12d"
                         (if (zero? b) "baseline" (str "B=" b))
                         (:mut-ns-per-op r) (:store-us-per-commit r) (:restore-scan-ms r) (:bytes r)))))))
