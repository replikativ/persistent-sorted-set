(ns hash-bench
  "Hashing-only microbenchmark (no PSS). The MST per-op cost is dominated by re-hashing every
   key in a leaf on each insert (the generic overflows/splitLengths scan). This measures whether
   (a) caching each key's hash/level once, and (b) hasch's xor aggregate for the node address,
   remove that cost — vs the unit cost of a single hash. Run:

     clj -Sdeps '{:deps {org.replikativ/hasch {:mvn/version \"0.4.98\"}}}' -M:dev -e \"(require 'hash-bench)(hash-bench/report)\""
  (:require [hasch.fast :as hf]
            [hasch.core :as hc]
            [hasch.benc :as benc])
  (:import [java.util HashMap]))

(defn- bench* [iters f]
  (dotimes [_ (quot iters 10)] (f))                 ; warm
  (let [t0 (System/nanoTime)]
    (dotimes [_ iters] (f))
    (/ (double (- (System/nanoTime) t0)) iters 1000.0))) ; µs/call

(defn key-level-fast ^long [k ^long lzpl]
  (let [^bytes h (hf/edn-hash k)
        x (bit-or (bit-shift-left (bit-and (aget h 0) 0xff) 24)
                  (bit-shift-left (bit-and (aget h 1) 0xff) 16)
                  (bit-shift-left (bit-and (aget h 2) 0xff) 8)
                  (bit-and (aget h 3) 0xff))]
    (quot (Integer/numberOfLeadingZeros (unchecked-int x)) lzpl)))

(def families
  {:int    (fn [i] i)
   :record (fn [i] [(format "sys-%07d" i) "main" i :meta])
   :ormap  (fn [i] [(str (hc/uuid i)) (str (hc/uuid (+ i 7777))) (keyword (str "k" i)) i])})

(defn report []
  (println "\n=== hashing-only microbenchmark (µs/op) ===\n")
  (let [N 50000]
    (doseq [[fam gen] families]
      (println "---- family" fam "----")
      (let [els (mapv gen (range 1000))
            pick (fn [] (nth els (rand-int 1000)))]
        ;; 1. unit hash cost
        (println " hasch.fast/edn-hash :" (format "%.3f" (bench* N #(hf/edn-hash (pick)))))
        (println " hasch.core/edn-hash :" (format "%.3f" (bench* (quot N 5) #(hc/edn-hash (pick)))))
        (println " key-level (fast+lz) :" (format "%.3f" (bench* N #(key-level-fast (pick) 6))))

        ;; 2. cache: hash each key once, then lookups
        (let [^HashMap cache (HashMap.)
              _ (doseq [e els] (.put cache e (key-level-fast e 6)))]
          (println " cached level lookup :" (format "%.4f" (bench* N #(.get cache (pick))))))

        ;; 3. leaf scan: naive (B hashes) vs cached (B lookups), B=64
        (let [leaf (vec (take 64 els))
              ^HashMap cache (HashMap.)
              _ (doseq [e leaf] (.put cache e (key-level-fast e 6)))]
          (println " leaf scan B=64 naive:" (format "%.2f" (bench* (quot N 50) #(reduce (fn [a e] (+ a (key-level-fast e 6))) 0 leaf))))
          (println " leaf scan B=64 cached:" (format "%.3f" (bench* (quot N 10) #(reduce (fn [a e] (+ a ^long (.get cache e))) 0 leaf)))))

        ;; 4. node address via xor-aggregate: from-scratch vs incremental (one key XORed in)
        (let [leaf (vec (take 64 els))
              khashes (mapv #(hf/edn-hash %) leaf)]   ; precomputed per-key hashes
          (println " node xor scratch B64:" (format "%.2f" (bench* (quot N 50) #(benc/xor-hashes (mapv hf/edn-hash leaf)))))
          (println " node xor from-cached:" (format "%.3f" (bench* (quot N 10) #(benc/xor-hashes khashes))))
          (let [base (benc/xor-hashes khashes)
                newh (hf/edn-hash (gen 999999))]
            (println " node xor incremental:" (format "%.4f"
                      (bench* N #(let [^bytes acc (aclone ^bytes base)]
                                   (dotimes [i (min 32 (alength acc))]
                                     (aset acc i (byte (bit-xor (aget acc i) (aget ^bytes newh i)))))
                                   acc))))))
        (println))))
  (println "=== done ==="))
