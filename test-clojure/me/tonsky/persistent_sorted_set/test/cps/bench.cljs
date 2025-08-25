(ns me.tonsky.persistent-sorted-set.test.cps.bench
  "Benchmarks comparing synchronous vs asynchronous performance"
  (:require
   [cljs.test :refer-macros [deftest testing is] :as test]
   [clojure.string :as string]
   [is.simm.lean-cps.async :refer [await] :refer-macros [async]]
   [is.simm.lean-cps.runtime :refer [run]]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.btset :as btset]
   [me.tonsky.persistent-sorted-set.leaf :as leaf]
   [me.tonsky.persistent-sorted-set.node :as node]
   [me.tonsky.persistent-sorted-set.test.cps.utils :as utils]))

(defn- now [] (js/performance.now))

(defn- mean [nums]
  (/ (reduce + nums) (count nums)))

(defn- median [nums]
  (let [sorted (sort nums)
        n (count sorted)
        mid (quot n 2)]
    (if (even? n)
      (mean [(nth sorted (dec mid)) (nth sorted mid)])
      (nth sorted mid))))

(defn- std-dev [nums]
  (let [m (mean nums)
        squared-diffs (map #(js/Math.pow (- % m) 2) nums)]
    (js/Math.sqrt (mean squared-diffs))))

(defn- percentile [nums p]
  (let [sorted (sort nums)
        idx (js/Math.floor (* (/ p 100) (dec (count sorted))))]
    (nth sorted idx)))

(defn- run-benchmark
  "Run a benchmark function multiple times and collect timing statistics"
  [name f warmup-runs test-runs]
  ;; Warmup
  (dotimes [_ warmup-runs]
    (f))

  ;; Actual measurements
  (let [timings (atom [])]
    (dotimes [_ test-runs]
      (let [start (now)]
        (f)
        (let [end (now)]
          (swap! timings conj (- end start)))))

    (let [times @timings]
      {:name name
       :mean (mean times)
       :median (median times)
       :std-dev (std-dev times)
       :min (apply min times)
       :max (apply max times)
       :p95 (percentile times 95)
       :p99 (percentile times 99)
       :samples test-runs})))

(defn- run-async-benchmark
  "Run an async benchmark function multiple times"
  [name f warmup-runs test-runs]
  ;; Return async result directly using new await-cps API
  (async
    ;; Warmup
    (dotimes [_ warmup-runs]
      (await (f)))

    ;; Actual measurements
    (let [timings (atom [])]
      (dotimes [_ test-runs]
        (let [start (now)]
          (await (f))
          (let [end (now)]
            (swap! timings conj (- end start)))))

      (let [times @timings]
        {:name name
         :mean (mean times)
         :median (median times)
         :std-dev (std-dev times)
         :min (apply min times)
         :max (apply max times)
         :p95 (percentile times 95)
         :p99 (percentile times 99)
         :samples test-runs}))))

(defn- format-comparison [sync-stats async-stats]
  (let [overhead-factor (/ (:mean async-stats) (:mean sync-stats))
        overhead-pct (* (- overhead-factor 1) 100)]
    {:sync sync-stats
     :async async-stats
     :overhead-factor overhead-factor
     :overhead-percent overhead-pct}))

(defn- print-comparison [comparison]
  (println "\n===" (:name (:sync comparison)) "===")
  (println (str "Sync:  mean=" (.toFixed (:mean (:sync comparison)) 3) "ms"
                " median=" (.toFixed (:median (:sync comparison)) 3) "ms"
                " p95=" (.toFixed (:p95 (:sync comparison)) 3) "ms"))
  (println (str "Async: mean=" (.toFixed (:mean (:async comparison)) 3) "ms"
                " median=" (.toFixed (:median (:async comparison)) 3) "ms"
                " p95=" (.toFixed (:p95 (:async comparison)) 3) "ms"))
  (println (str "Overhead: " (.toFixed (:overhead-factor comparison) 2) "x"
                " (" (if (pos? (:overhead-percent comparison)) "+" "")
                (.toFixed (:overhead-percent comparison) 1) "%)")))

#!------------------------------------------------------------------------------

(defn do-single-ops-bench []
  (async
   (println "\n### Single Element Operations ###")
   (let [sync-storage (utils/make-sync-storage)
         async-storage (utils/make-async-storage 0) ; Zero delay for pure overhead measurement
         sync-set (reduce set/conj
                          (set/sorted-set* {:storage sync-storage})
                          (range 1000))
         async-set (await (utils/async-build-set 1000))
         warmup-runs 10000
         test-runs 5000]
     (assert (instance? btset/BTSet sync-set))
     (assert (instance? btset/BTSet async-set))
     (testing "Benchmark: conj single element"
       (let [sync-conj (run-benchmark
                        "conj"
                        (fn [] (set/conj sync-set 1001))
                        warmup-runs test-runs)
             async-conj (await (run-async-benchmark
                                "conj"
                                (fn [] (set/conj async-set 1001 compare {:sync? false}))
                                warmup-runs test-runs))]
         (print-comparison (format-comparison sync-conj async-conj))))
     (testing "Benchmark: lookup existing element"
       (let [sync-lookup (run-benchmark
                          "Sync lookup"
                          (fn [] (get sync-set 50))
                          warmup-runs test-runs)
             async-lookup (await (run-async-benchmark
                                  "lookup"
                                  (fn [] (async (await (set/lookup-async async-set 50))))
                                  warmup-runs test-runs))]
         (print-comparison (format-comparison sync-lookup async-lookup))))
     (testing "Benchmark: contains? check"
       (let [sync-contains (run-benchmark
                            "contains?"
                            (fn [] (contains? sync-set 50))
                            warmup-runs test-runs)
             async-contains (await (run-async-benchmark
                                    "contains?"
                                    (fn [] (async (await (set/contains? async-set 50 {:sync? false}))))
                                    warmup-runs test-runs))]
         (print-comparison (format-comparison sync-contains async-contains))))
     (testing "Benchmark: disj single element"
       (let [sync-disj (run-benchmark
                        "disj"
                        (fn [] (set/disj sync-set 50))
                        warmup-runs test-runs)
             async-disj (await (run-async-benchmark
                                "disj"
                                (fn [] (set/disj async-set 50 compare {:sync? false}))
                                warmup-runs test-runs))]
         (print-comparison (format-comparison sync-disj async-disj)))))))

(deftest bench-single-operations
  (test/async done
    (run (do-single-ops-bench)
      (fn [ok]
        (js/console.info "bench-single-operations success" ok)
        (done))
      (fn [err]
        (js/console.warn "bench-single-operations failed")
        (is (nil? err))
        (js/console.warn err)
        (done)))))

(defn do-bulk-ops-bench []
  (async
   (println "\n### Bulk Operations ###")
   (let [sync-storage (utils/make-sync-storage)
         async-storage (utils/make-async-storage 0)]
     (testing "Benchmark: Building sets of different sizes"
       (doseq [n [100 1000 10000 100000]]
         (let [nums (shuffle (range n))
               sync-build (run-benchmark
                           (str "Sync conj " n " elements")
                           #(reduce set/conj
                                    (set/sorted-set* {:storage sync-storage})
                                    nums)
                           2 10)
               async-build (await (run-async-benchmark
                                   (str "Async conj " n " elements")
                                   (fn []
                                     (reduce (fn [s-ch num]
                                               (async (let [s (await s-ch)]
                                                        (await (set/conj s num compare {:sync? false})))))
                                             (async (set/sorted-set* {:storage async-storage}))
                                             nums))
                                   2 10))]
           (print-comparison (format-comparison sync-build async-build))))))))

(deftest bench-bulk-operations
  (test/async done
    (run (do-single-ops-bench)
      (fn [ok]
        (js/console.info "bench-bulk-operations success" ok)
        (done))
      (fn [err]
        (js/console.warn "bench-bulk-operations failed")
        (is (nil? err))
        (js/console.warn err)
        (done)))))

(defn do-iteration-bench []
  (async
   (println "\n### Iteration Performance ###")
   (let [sync-storage (utils/make-sync-storage)
         async-storage (utils/make-async-storage 0)
         sync-set (reduce set/conj
                          (set/sorted-set* {:storage sync-storage})
                          (range 1000))
         async-set (await (utils/async-build-set 1000))
         warmup-runs 500
         iteration-runs 2000]
     (assert (instance? btset/BTSet sync-set))
     (assert (instance? btset/BTSet async-set))
     (testing "Benchmark: Full iteration"
       (let [sync-iter (run-benchmark
                        "Sync full iteration"
                        (fn [] (doall (seq sync-set)))
                        warmup-runs iteration-runs)
             async-iter (await (run-async-benchmark
                                "Async full iteration"
                                (fn [] (async
                                        (let [async-seq (await (set/async-slice async-set nil nil))]
                                          (loop [s async-seq
                                                 count 0]
                                            (if s
                                              (let [v (await (set/afirst s))]
                                                (if v
                                                  (recur (await (set/arest s)) (inc count))
                                                  count))
                                              count)))))
                                warmup-runs iteration-runs))]
         (print-comparison (format-comparison sync-iter async-iter))))
     (testing "Benchmark: Slice iteration (100 elements)"
       (let [sync-slice (run-benchmark
                         "Sync slice (100 elements)"
                         (fn [] (doall (set/slice sync-set 400 499)))
                         5 20)
             async-slice (await (run-async-benchmark
                                 "Async slice (100 elements)"
                                 (fn [] (async
                                         (let [async-seq (await (set/async-slice async-set 400 499))]
                                           (loop [s async-seq
                                                  count 0]
                                             (if s
                                               (let [v (await (set/afirst s))]
                                                 (if v
                                                   (recur (await (set/arest s)) (inc count))
                                                   count))
                                               count)))))
                                 warmup-runs iteration-runs))]
         (print-comparison (format-comparison sync-slice async-slice)))))))

(deftest bench-iteration
  (test/async done
    (run (do-iteration-bench)
      (fn [ok]
        (js/console.info "bench-iteration success" ok)
        (done))
      (fn [err]
        (js/console.warn "bench-iteration failed")
        (is (nil? err))
        (js/console.warn err)
        (done)))))

;;; NOTE restore do nothing but set address respectively
;;; this bench does not actually measure anything
;;; FIX --> all store ops happen lazily on reads, both sync & async
;;;  + (sync store & async store)
;;;  + (sync restored enumeration & async restored enumeration) x (ordered-acces vs random-access)

; (defn do-bench-storage-delays []
;   (async
;    (println "\n### Storage Operations with Delays ###")
;    (doseq [delay-ms [0 1 5]]
;      (println (str "\n--- Storage delay: " delay-ms "ms ---"))
;      (let [sync-storage (utils/make-sync-storage)
;            async-storage (utils/make-async-storage delay-ms)
;            sync-set (reduce set/conj
;                             (set/sorted-set* {:storage sync-storage})
;                             (range 500))
;            async-set (await (reduce (fn [acc-ch v]
;                                       (async (let [acc (await acc-ch)]
;                                                (await (set/conj acc v compare {:sync? false})))))
;                                     (async (set/sorted-set* {:storage async-storage}))
;                                     (range 500)))]
;        (testing "Store and restore benchmark"
;          (let [sync-store-restore (run-benchmark
;                                    "Sync store+restore"
;                                    #(let [store-info (set/store sync-set)]
;                                       (set/restore store-info sync-storage))
;                                    2 10)
;
;                async-store-restore (await (run-async-benchmark
;                                            "Async store+restore"
;                                            #(async
;                                              (let [store-info (await (set/store async-set {:sync? false}))]
;                                                (set/restore store-info async-storage)))
;                                            2 10))]
;            (print-comparison (format-comparison sync-store-restore async-store-restore))))))))
;
; (deftest bench-storage-delays
;   (test/async done
;     (run-async (do-bench-storage-delays)
;       (fn [ok]
;         (js/console.info "bench-storage-delays success" ok)
;         (done))
;       (fn [err]
;        (js/console.warn "bench-storage-delays failed")
;        (is (nil? err))
;         (js/console.warn err)
;         (done)))))

(defn do-bench-lazy-loading []
  (async
   (println "\n### Lazy Loading Overhead ###")
   (let [sync-storage (utils/make-sync-storage)
         async-storage (utils/make-async-storage 1) ; Small delay to simulate I/O
         large-set (reduce set/conj
                           (set/sorted-set* {:storage sync-storage})
                           (range 1000))
         async-large-set (reduce set/conj
                           (set/sorted-set* {:storage async-storage})
                           (range 1000))
         sync-addr  (set/store large-set)
         sync-lazy  (set/restore sync-addr sync-storage)
         async-addr (await (set/store async-large-set {:sync? false}))
         async-lazy (set/restore async-addr async-storage {:sync? false})]
     (testing "Benchmark: First access (cold)"
       (let [sync-first (run-benchmark
                         "Sync first access (cold)"
                         (fn [] (get sync-lazy 500))
                         2 10)
             async-first (await (run-async-benchmark
                                 "Async first access (cold)"
                                 (fn [] (set/lookup-async async-lazy 500))
                                 2 10))]
         (print-comparison (format-comparison sync-first async-first))))
     (testing "Benchmark: Subsequent access (warm)"
       (let [sync-warm (run-benchmark
                        "Sync access (warm cache)"
                        (fn [] (get sync-lazy 500))
                        5 20)
             async-warm (await (run-async-benchmark
                                "Async access (warm cache)"
                                (fn [] (async (await (set/lookup-async async-lazy 500))))
                                5 20))]
         (print-comparison (format-comparison sync-warm async-warm)))))))

(deftest bench-lazy-loading
  (test/async done
    (run (do-bench-lazy-loading)
      (fn [ok]
        (js/console.info "bench-lazy-loading success" ok)
        (done))
      (fn [err]
        (js/console.warn "bench-lazy-loading failed")
        (is (nil? err))
        (js/console.warn err)
        (done)))))

