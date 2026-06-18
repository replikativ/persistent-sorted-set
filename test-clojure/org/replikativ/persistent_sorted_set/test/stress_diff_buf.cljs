(ns org.replikativ.persistent-sorted-set.test.stress-diff-buf
  "cljs parity of the JVM diff-buf stress harness (stress_diff_buf.clj). Seeded random
   conj/disj/replace (persistent AND transient) interleaved with store + COLD restore (fresh
   memory, shared disk — forces real deserialize + projection through the edn-serializing test
   storage), swept over a param grid (bf/B/keyrange) × seeds. Deterministic via a seeded
   xorshift32 (JS has no seedable Random), so any failure reproduces from (seed, params).

   Oracles (lazy-safe — validate-tree false-positives on lazily-restored trees, so we don't use
   it; in-memory structural invariants live in invariants.cljc):
     (1) full content seq == model (catches missing/stale/dup/misorder),
     (2) count == model (forces realization),
     (3) random lookup sample (seek/separators path a seq scan misses),
     (4) measure oracle: the set's aggregate ĝ.measure (sum of keys) == the model's,
     (5) GC oracle: no reachable node is markFreed (over-free), and under diff-buf every
         superseded blob IS freed (no leak).
   Model = a Clojure sorted-set-by the same comparator; replace = value-change at the same
   cmp-key. Runs in the :node-stress build (ns matches ^…test.stress)."
  (:require [cljs.test :as t :refer-macros [is deftest]]
            [clojure.set]
            [clojure.edn :as edn]
            [hasch.core :as hasch]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.impl.measure :as measure]
            [org.replikativ.persistent-sorted-set.impl.node :as node]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage :refer [IStorage]]
            [org.replikativ.persistent-sorted-set.branch :refer [Branch] :as branch]
            [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]]
            [org.replikativ.persistent-sorted-set.test.storage.util :as util]))

(def cmp (fn [a b] (compare (first a) (first b))))

;; Sum-of-keys monoid measure (cljs analogue of the JVM sum-measure): extract on (first key),
;; so a content-only replace [k 0]->[k v] leaves the measure unchanged while add/remove move it.
(def sum-measure
  (reify measure/IMeasure
    (identity-measure [_] 0)
    (extract [_ k] (first k))
    (merge-measure [_ a b] (+ a b))
    (remove-measure [_ cur k _recompute] (- cur (first k)))))

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

;; markFreed-recording wrapper over the test storage (the test storage's own markFreed is a
;; no-op). Mirrors the JVM `recording` storage so the GC over-free/leak oracle works here too.
(defn- recording [inner freed]
  (reify IStorage
    (store    [_ n opts] (storage/store inner n opts))
    (restore  [_ a opts] (storage/restore inner a opts))
    (accessed [_ a] (storage/accessed inner a))
    (markFreed [_ a] (swap! freed conj a) nil)
    (isFreed  [_ a] (contains? @freed a))
    (freedInfo [_ a] nil)))

(defn run-trial [seed {:keys [bf b keyrange init cycles ops restore-prob transient-prob measure? gc?] :as params}]
  (let [rng    (make-rng (inc seed))
        rd     (fn [] (/ (rng 1000000) 1000000.0))
        disk   (atom {})
        meas   (when measure? sum-measure)
        freed  (when gc? (atom #{}))
        sopts  (cond-> {:comparator cmp :branching-factor bf :diff-buf-size b} meas (assoc :measure meas))
        ropts  sopts
        mk     (fn [] (let [s (util/storage (atom {}) disk sopts)] (if freed (recording s freed) s)))
        cov    (atom {:stores 0 :restores 0 :adds 0 :removes 0 :replaces 0 :ops 0 :measure-checks 0})
        ref    (atom (sorted-set-by cmp))
        s0     (reduce (fn [s _] (let [k (rng keyrange)] (swap! ref conj [k 0]) (set/conj s [k 0])))
                       (set/sorted-set* sopts) (range init))
        check  (fn [s tag]
                 (cond
                   (not= (vec (seq @ref)) (vec (seq s))) {:fail :seq :tag tag :seed seed :params params}
                   (not= (count @ref) (count s))         {:fail :count :tag tag :seed seed :params params
                                                          :got (count s) :exp (count @ref)}
                   :else
                   (or (loop [i 0]
                         (if (= i 12) nil
                             (let [k (rng keyrange)]
                               (if (not= (contains? @ref [k 0]) (contains? s [k 0]))
                                 {:fail :lookup :tag tag :seed seed :params params :k k}
                                 (recur (inc i))))))
                       ;; measure oracle: the set's aggregate (sum of keys) must equal the model's.
                       (when meas
                         (swap! cov update :measure-checks inc)
                         (let [true-m (reduce + 0 (map first @ref))
                               got    (set/measure s)]
                           (when (and (some? got) (not= true-m got))
                             {:fail :measure :tag tag :seed seed :params params :exp true-m :got got}))))))
        do-ops (fn [s]
                 (if (< (rd) (or transient-prob 0.3))
                   (persistent!
                    (reduce (fn [t _]
                              (let [k (rng keyrange) r (rng 3)]
                                (swap! cov update :ops inc)
                                (cond
                                  (= r 0) (do (swap! cov update :adds inc)
                                              (swap! ref conj [k 0]) (conj! t [k 0]))
                                  (= r 1) (do (swap! cov update :removes inc)
                                              (swap! ref disj [k 0]) (disj! t [k 0]))
                                  (contains? @ref [k 0])
                                  (let [v (inc (rng 1000000))]
                                    (swap! cov update :replaces inc)
                                    (swap! ref #(-> % (disj [k 0]) (conj [k v])))
                                    (-> t (disj! [k 0]) (conj! [k v])))
                                  :else (do (swap! cov update :adds inc)
                                            (swap! ref conj [k 0]) (conj! t [k 0])))))
                            (transient s) (range ops)))
                   (reduce (fn [s _]
                             (let [k (rng keyrange) r (rng 3)]
                               (swap! cov update :ops inc)
                               (cond
                                 (= r 0) (do (swap! cov update :adds inc)
                                             (swap! ref conj [k 0]) (set/conj s [k 0]))
                                 (= r 1) (do (swap! cov update :removes inc)
                                             (swap! ref disj [k 0]) (set/disj s [k 0]))
                                 (contains? @ref [k 0])
                                 (let [v (inc (rng 1000000))]
                                   (swap! cov update :replaces inc)
                                   (swap! ref #(-> % (disj [k 0]) (conj [k v])))
                                   (set/replace s [k 0] [k v]))
                                 :else (do (swap! cov update :adds inc)
                                           (swap! ref conj [k 0]) (set/conj s [k 0])))))
                           s (range ops))))
        store! (fn [s] (swap! cov update :stores inc) (set/store s (mk) {:sync? true}))
        gc-check (fn [final-addr final]
                   (when freed
                     (let [reach   (let [r (atom #{})]   ; live walk (matches the JVM oracle exactly)
                                     (set/walk-addresses final (fn [a] (swap! r conj a) true))
                                     (conj @r final-addr))
                           written (set (keys @disk))
                           over    (clojure.set/intersection reach @freed)
                           leaked  (clojure.set/difference written reach @freed)]
                       (cond
                         (seq over)            {:fail :gc-over-free :tag :final :seed seed :params params :over (count over)}
                         (and (pos? b) (seq leaked)) {:fail :gc-leak :tag :final :seed seed :params params :leaked (count leaked)}
                         :else nil))))]
    (or (check s0 :initial)
        (loop [c 0, s s0, addr (store! s0)]
          (if (= c cycles)
            ;; final: cold-restore the last stored root, then content + GC oracles.
            (let [final (set/restore addr (mk) ropts)]
              (or (check final :final-restore)
                  (gc-check addr final)
                  {:ok true :seed seed :cov @cov}))
            ;; cold restore with prob (fresh memory, shared disk), else keep mutating the live set.
            (let [s   (if (< (rd) (or restore-prob 0.5))
                        (do (swap! cov update :restores inc) (set/restore addr (mk) ropts))
                        s)
                  bad (check s (str "restore-" c))]
              (if bad bad
                  (let [s2  (do-ops s)
                        bad (check s2 (str "ops-" c))]
                    (if bad bad
                        (recur (inc c) s2 (store! s2)))))))))))

(defn sweep [grid seeds]
  (let [results (for [params grid seed (range seeds)] (run-trial seed params))
        fails   (remove :ok results)
        cov     (reduce (fn [m c] (merge-with + m (dissoc c :ok :seed)))
                        {} (map :cov (filter :ok results)))]
    {:fails (vec fails) :cov cov}))

(deftest stress-diff-buf-cljs
  (let [grid (for [bf [4 16 64] b [0 32 256] kr [80 800]]
               {:bf bf :b b :keyrange kr :init (min kr 300) :cycles 8 :ops 25
                :restore-prob 0.5 :transient-prob 0.3 :measure? true :gc? true})
        {:keys [fails cov]} (sweep grid 5)]
    (is (empty? fails) (str "cljs diff-buf stress: " (count fails) " failure(s): "
                            (pr-str (vec (take 8 fails)))))
    ;; sanity: the grid must actually exercise each path it claims to test
    (is (pos? (:replaces cov)) "grid exercised replaces")
    (is (pos? (:restores cov)) "grid exercised cold restores")
    (is (pos? (:measure-checks cov)) "grid exercised the measure oracle")))

;; ---- content-address determinism / dedup under diff-buf (cljs parity of the JVM test) --------
;; A content-addressed storage: a node's address is the hasch UUID (SHA-512 content hash, the
;; same hashing konserve uses) of its serialized form INCLUDING diff-buf :slots. If that
;; serialization is deterministic, the same logical tree (built+mutated+stored identically)
;; yields IDENTICAL addresses ⇒ dedup works and merkle hashes are stable. This guards against
;; non-deterministic cljs map/slot serialization, which would silently break a content-addressed
;; backend. Mirrors the test storage's serialization exactly, but content-addressed.
(defn- branch? [n] (instance? Branch n))

(defn- ch-storage [disk bf b]
  (let [settings {:comparator cmp :branching-factor bf :diff-buf-size b}]
    (reify IStorage
      (store [_ n _opts]
        (let [s (pr-str (cond-> {:level     (node/level n)
                                 :keys      (.-keys n)
                                 :addresses (when (branch? n) (.-addresses n))
                                 :subtree-count (when (branch? n) (.-subtree-count n))
                                 :measure   (.-_measure n)}
                          (branch? n) (assoc :slots (branch/slots-for-storage n))))
              a (hasch/uuid s)]                       ; content address = hasch hash of the serialized form
          (swap! disk assoc a s)
          a))
      (restore [_ a _opts]
        (let [{:keys [keys addresses measure slots] :as m} (edn/read-string (get @disk a))
              node (if addresses (branch/from-map (assoc m :settings settings)) (Leaf. keys settings measure))]
          (when (and slots addresses)
            (let [arr (make-array (count keys))]
              (doseq [[idx entry] slots]
                (aset arr (int idx) {:diff (:diff entry) :count (:count entry) :measure (:measure entry)
                                     :anchor (nth (vec addresses) (int idx))}))
              (set! (.-_slots node) arr)))
          node))
      (accessed [_ _] nil) (markFreed [_ _] nil) (isFreed [_ _] false) (freedInfo [_ _] nil))))

(defn- ch-final-addr
  "Deterministic build+mutate+store under content-addressed storage; returns
   [root-address #{all node addresses}]. Same (seed,params) ⇒ identical iff serialization
   (incl. slots) is deterministic."
  [seed {:keys [bf b keyrange init cycles ops]}]
  (let [rng  (make-rng (inc seed))
        disk (atom {})
        st   (fn [] (ch-storage disk bf b))
        ropts {:comparator cmp :branching-factor bf :diff-buf-size b}
        s0   (reduce (fn [s _] (set/conj s [(rng keyrange) 0]))
                     (set/sorted-set* ropts) (range init))]
    (loop [c 0, addr (set/store s0 (st) {:sync? true})]
      (if (= c cycles)
        [addr (set (keys @disk))]
        (let [s  (set/restore addr (st) ropts)
              s2 (reduce (fn [s _] (let [k (rng keyrange) r (rng 3)]
                                     (cond (= r 0) (set/conj s [k 0])
                                           (= r 1) (set/disj s [k 0])
                                           (contains? s [k 0]) (set/replace s [k 0] [k (inc (rng 9))])
                                           :else (set/conj s [k 0]))))
                         s (range ops))]
          (recur (inc c) (set/store s2 (st) {:sync? true})))))))

(deftest address-determinism
  (let [bad (for [bf [4 16] b [0 64 256] kr [80 1500] seed (range 3)
                  :let [params {:bf bf :b b :keyrange kr :init (min kr 300) :cycles 6 :ops 25}
                        [r1 ks1] (ch-final-addr seed params)
                        [r2 ks2] (ch-final-addr seed params)]
                  :when (or (not= r1 r2) (not= ks1 ks2))]
              {:params params :seed seed :root= (= r1 r2) :nodes= (= ks1 ks2)})]
    (is (empty? bad) (str "non-deterministic content-addressed serialization: " (pr-str (vec (take 6 bad)))))))
