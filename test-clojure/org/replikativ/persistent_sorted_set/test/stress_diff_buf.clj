(ns org.replikativ.persistent-sorted-set.test.stress-diff-buf
  "Seeded random stress for diff-buf, with serialization in the loop. NOT a defspec —
   each trial is driven by a deterministic (Random. seed), so any failure reproduces from
   (seed, params) alone (no shrinking; replay the seed and bisect by hand if needed). The
   driver sweeps a wide param grid × many seeds and across two size tiers, and reports
   COVERAGE telemetry (flushes, merges, max depth, restores, replaces…) so we can SEE that
   the grid actually exercised the interesting paths — not just that N trivial trials passed.

   Model/oracle: a Clojure sorted-set-by the SAME comparator. Elements are [k v] ordered by
   k only (coarse cmp, datahike-like): add = new k, remove = disj [k _], replace = value-change
   at the same k (modeled as disj+conj). The test storage round-trips slots through pr-str/edn,
   so a stale/duplicate element introduced by buffering/projection is caught at the next restore.

   JVM-only — drive from a REPL: (run-broad) / (run-large) / (sweep opts). A small bounded
   slice is wired as a deftest for the regular suite."
  (:require [clojure.test :as t :refer [deftest is]]
            [clojure.set]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set Branch ANode PersistentSortedSet IStorage Settings]
           [java.util Random]))

(def cmp (fn [a b] (compare (first a) (first b))))

(defn- opbuf-settings ^Settings [bf b] (Settings. (int bf) nil nil nil (int b)))
(defn- mkst [disk bf b] (tstore/->Storage (atom {}) disk (opbuf-settings bf b)))

;; depth via public Branch.child (restores+projects lazily on descent).
(defn- tree-depth [^PersistentSortedSet s storage]
  (loop [n (.root s), d 1]
    (if (instance? Branch n)
      (let [^Branch b n
            c (.child b storage 0)]
        (recur c (inc d)))
      d)))

(defn- writes [] (:writes @tstore/*stats))

(defn run-trial
  "Run one deterministic trial. Returns {:ok? bool :seed :params :cov {...} (:detail on failure)}.
   Never throws — wraps failures so the sweep collects every failing seed."
  [seed {:keys [bf b keyrange init cycles ops restore-prob transient-prob] :as params}]
  (try
    (let [rng   (Random. seed)
          rint  (fn [n] (.nextInt rng (int n)))
          rd    (fn [] (.nextDouble rng))
          disk  (atom {})
          cov   (atom {:stores 0 :buffered 0 :flushed 0 :max-depth 0 :restores 0
                       :adds 0 :removes 0 :replaces 0 :ops 0})
          ref   (atom (sorted-set-by cmp))
          ;; initial bulk load
          s0    (reduce (fn [s _]
                          (let [k (rint keyrange)] (swap! ref conj [k 0]) (conj s [k 0])))
                        (ss/sorted-set* {:comparator cmp :branching-factor bf :diff-buf-size b})
                        (range init))
          store! (fn [s] (tstore/with-stats (let [a (ss/store s (mkst disk bf b))]
                                              (swap! cov update :stores inc)
                                              (if (> (writes) 1)
                                                (swap! cov update :flushed inc)
                                                (swap! cov update :buffered inc))
                                              a)))
          ;; Lazy-safe oracles (validate-tree's count invariant false-positives on lazily-restored
          ;; trees — unmaterialized children read count -1; tasks #33/#36 — so we don't use it here;
          ;; in-memory structural invariants are covered by invariants.cljc). Instead:
          ;;  (1) full content seq == model (catches missing/stale/dup/misorder),
          ;;  (2) count == model (forces realization; catches count bugs),
          ;;  (3) random lookup sample (exercises seek/separators, which a seq scan doesn't).
          check  (fn [s tag]
                   (when-not (= (vec (seq @ref)) (vec (seq s)))
                     (throw (ex-info (str "content mismatch @ " tag)
                                     {:tag tag :expected-count (count @ref) :got-count (count s)})))
                   (when-not (= (count @ref) (count s))
                     (throw (ex-info (str "count mismatch @ " tag)
                                     {:tag tag :ref (count @ref) :got (count s)})))
                   (dotimes [_ 12]
                     (let [k (rint keyrange) want (contains? @ref [k 0]) got (contains? s [k 0])]
                       (when (not= want got)
                         (throw (ex-info (str "lookup mismatch @ " tag) {:tag tag :k k :want want :got got}))))))
          ;; persistent branch uses ss/replace (exercises the buffered Absent+Present deposit);
          ;; transient branch models replace as disj!+conj! (ss/replace is persistent-only).
          do-ops (fn [s]
                   (if (< (rd) (double (or transient-prob 0.3)))
                     (persistent!
                      (reduce (fn [t _]
                                (let [k (rint keyrange) r (rd)]
                                  (swap! cov update :ops inc)
                                  (cond
                                    (< r 0.34) (do (swap! cov update :adds inc)
                                                   (swap! ref conj [k 0]) (conj! t [k 0]))
                                    (< r 0.67) (do (swap! cov update :removes inc)
                                                   (swap! ref disj [k 0]) (disj! t [k 0]))
                                    (contains? @ref [k 0])
                                    (let [v (inc (rint 1000000))]
                                      (swap! cov update :replaces inc)
                                      (swap! ref #(-> % (disj [k 0]) (conj [k v])))
                                      (-> t (disj! [k 0]) (conj! [k v])))
                                    :else (do (swap! cov update :adds inc)
                                              (swap! ref conj [k 0]) (conj! t [k 0])))))
                              (transient s) (range ops)))
                     (reduce (fn [s _]
                               (let [k (rint keyrange) r (rd)]
                                 (swap! cov update :ops inc)
                                 (cond
                                   (< r 0.34) (do (swap! cov update :adds inc)
                                                  (swap! ref conj [k 0]) (conj s [k 0]))
                                   (< r 0.67) (do (swap! cov update :removes inc)
                                                  (swap! ref disj [k 0]) (disj s [k 0]))
                                   (contains? @ref [k 0])
                                   (let [v (inc (rint 1000000))]
                                     (swap! cov update :replaces inc)
                                     (swap! ref #(-> % (disj [k 0]) (conj [k v])))
                                     (ss/replace s [k 0] [k v]))
                                   :else (do (swap! cov update :adds inc)
                                             (swap! ref conj [k 0]) (conj s [k 0])))))
                             s (range ops))))]
      (check s0 :initial)
      (loop [c 0, s s0, addr (store! s0)]
        (if (= c cycles)
          (let [st (mkst disk bf b)
                loaded (ss/restore-by cmp addr st {:branching-factor bf :diff-buf-size b :comparator cmp})]
            (check loaded :final-restore)
            (swap! cov update :max-depth max (tree-depth loaded st))
            {:ok? true :seed seed :params params :cov @cov})
          (let [;; cold restore with prob, else keep live set
                s (if (< (rd) (double (or restore-prob 0.5)))
                    (let [st (mkst disk bf b)
                          loaded (ss/restore-by cmp addr st {:branching-factor bf :diff-buf-size b :comparator cmp})]
                      (swap! cov update :restores inc)
                      (check loaded (str "restore-cycle-" c))
                      (swap! cov update :max-depth max (tree-depth loaded st))
                      loaded)
                    s)
                s (do-ops s)
                _ (check s (str "ops-cycle-" c))]
            (recur (inc c) s (store! s))))))
    (catch Throwable e
      {:ok? false :seed seed :params params
       :detail (str (.getMessage e) " " (ex-data e))})))

(defn- merge-cov [cs]
  (reduce (fn [m c] (merge-with + m (select-keys c [:stores :buffered :flushed :restores :adds :removes :replaces :ops])))
          {} cs))

(defn sweep
  "Run grid × seeds. opts: {:grid [param-maps] :seeds n :base-seed k}.
   Prints failures + aggregate coverage; returns {:trials :failures [..] :cov}."
  [{:keys [grid seeds base-seed label] :or {seeds 25 base-seed 0 label "sweep"}}]
  (let [results (doall (for [params grid
                             seed   (range base-seed (+ base-seed seeds))]
                         (run-trial seed params)))
        fails   (remove :ok? results)
        cov     (merge-cov (map :cov (filter :ok? results)))
        max-d   (apply max 0 (map (comp :max-depth :cov) (filter :ok? results)))]
    (println (format "\n=== %s: %d trials (%d grid × %d seeds) ===" label (count results) (count grid) seeds))
    (println (format "PASS %d   FAIL %d" (count (filter :ok? results)) (count fails)))
    (println "coverage:" (assoc cov :max-depth max-d))
    (when (seq fails)
      (println "FAILURES (reproduce via (run-trial seed params)):")
      (doseq [f (take 20 fails)] (println "  " (:seed f) (:params f) "->" (:detail f))))
    {:trials (count results) :failures (vec fails) :cov (assoc cov :max-depth max-d)}))

(def broad-grid
  (for [bf [4 16 64 512] b [0 4 32 256 4096] keyrange [50 500 5000]]
    {:bf bf :b b :keyrange keyrange :init (min keyrange 2000)
     :cycles 20 :ops 40 :restore-prob 0.5 :transient-prob 0.3}))

(def large-grid
  (for [bf [64 512] b [0 256] keyrange [100000]]
    {:bf bf :b b :keyrange keyrange :init 100000
     :cycles 8 :ops 5000 :restore-prob 0.3 :transient-prob 0.4}))

(defn run-broad [] (sweep {:grid broad-grid :seeds 25 :label "broad"}))
(defn run-large [] (sweep {:grid large-grid :seeds 5  :label "large"}))

;; Bounded slice for the regular suite (fast): a few seeds, modest sizes, B on and off.
(deftest stress-bounded
  (let [grid (for [bf [4 32] b [0 64] kr [80 2000]]
               {:bf bf :b b :keyrange kr :init (min kr 1000)
                :cycles 8 :ops 30 :restore-prob 0.5 :transient-prob 0.3})
        {:keys [failures cov]} (sweep {:grid grid :seeds 5 :label "stress-bounded(suite)"})]
    (is (empty? failures) (str (count failures) " stress trial(s) failed"))
    ;; sanity: the bounded grid must actually exercise flushes + replaces + restores
    (is (pos? (:flushed cov)) "bounded grid exercised at least one flush")
    (is (pos? (:replaces cov)) "bounded grid exercised replaces")
    (is (pos? (:restores cov)) "bounded grid exercised cold restores")))
