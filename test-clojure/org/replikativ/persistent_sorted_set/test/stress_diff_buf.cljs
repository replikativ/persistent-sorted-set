(ns org.replikativ.persistent-sorted-set.test.stress-diff-buf
  "cljs parity of the JVM diff-buf stress harness (stress_diff_buf.clj). Seeded random
   conj/disj/replace interleaved with store + COLD restore (fresh memory, shared disk —
   forces real deserialize + projection through the edn-serializing test storage), swept
   over a param grid (bf/B/keyrange) × seeds. Deterministic via a seeded xorshift32 (JS has
   no seedable Random), so any failure reproduces from (seed, params).

   Oracles are lazy-safe: full seq == model, count == model, random lookup sample (seek path).
   Model = a Clojure sorted-set-by the same comparator; replace = disj+conj (value-change at
   the same cmp-key). Runs in the :node-stress build (ns matches ^…test.stress)."
  (:require [cljs.test :as t :refer-macros [is deftest]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.test.storage.util :as util]))

(def cmp (fn [a b] (compare (first a) (first b))))

;; Seeded xorshift32 — cljs bit-ops are 32-bit, so this stays deterministic in JS.
(defn- make-rng [seed]
  (let [s (atom (if (zero? seed) 0x9e3779b9 (bit-or seed 0)))]
    (fn [n]
      (let [x  @s
            x  (bit-xor x (bit-shift-left x 13))
            x  (bit-xor x (unsigned-bit-shift-right x 17))
            x  (bit-xor x (bit-shift-left x 5))
            x  (bit-or x 0)]
        (reset! s x)
        (mod (js/Math.abs x) n)))))

(defn run-trial [seed {:keys [bf b keyrange init cycles ops] :as params}]
  (let [rng  (make-rng (inc seed))
        disk (atom {})
        mkst (fn [] (util/storage (atom {}) disk {:branching-factor bf :diff-buf-size b}))
        ropts {:comparator cmp :branching-factor bf :diff-buf-size b}
        ref  (atom (sorted-set-by cmp))
        s0   (reduce (fn [s _] (let [k (rng keyrange)] (swap! ref conj [k 0]) (set/conj s [k 0])))
                     (set/sorted-set* {:comparator cmp :branching-factor bf :diff-buf-size b})
                     (range init))
        check (fn [s tag]
                (cond
                  (not= (vec (seq @ref)) (vec (seq s))) {:fail :seq :tag tag :seed seed :params params}
                  (not= (count @ref) (count s))         {:fail :count :tag tag :seed seed :params params
                                                         :got (count s) :exp (count @ref)}
                  :else (loop [i 0]
                          (if (= i 10) nil
                              (let [k (rng keyrange)]
                                (if (not= (contains? @ref [k 0]) (contains? s [k 0]))
                                  {:fail :lookup :tag tag :seed seed :params params :k k}
                                  (recur (inc i))))))))]
    (or (check s0 :initial)
        (loop [c 0, addr (set/store s0 (mkst) {:sync? true})]
          (let [s (set/restore addr (mkst) ropts)
                bad (check s (str "restore-" c))]
            (if bad bad
                (let [s (reduce (fn [s _]
                                  (let [k (rng keyrange) r (rng 3)]
                                    (cond
                                      (= r 0) (do (swap! ref conj [k 0]) (set/conj s [k 0]))
                                      (= r 1) (do (swap! ref disj [k 0]) (set/disj s [k 0]))
                                      (contains? @ref [k 0])
                                      (let [v (inc (rng 1000000))]
                                        (swap! ref #(-> % (disj [k 0]) (conj [k v])))
                                        (set/replace s [k 0] [k v]))
                                      :else (do (swap! ref conj [k 0]) (set/conj s [k 0])))))
                                s (range ops))
                      bad (check s (str "ops-" c))]
                  (cond bad bad
                        (= c cycles) {:ok true :seed seed}
                        :else (recur (inc c) (set/store s (mkst) {:sync? true})))))))
        {:ok true :seed seed})))

(defn sweep [grid seeds]
  (let [results (for [params grid seed (range seeds)] (run-trial seed params))]
    (remove :ok results)))

(deftest stress-diff-buf-cljs
  (let [grid (for [bf [4 16] b [0 32 256] kr [80 800]]
               {:bf bf :b b :keyrange kr :init (min kr 300) :cycles 8 :ops 25})
        fails (sweep grid 6)]
    (is (empty? fails) (str "cljs diff-buf stress: " (count fails) " failure(s): "
                            (pr-str (vec (take 8 fails)))))))
