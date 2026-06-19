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
            [clojure.edn :as edn]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore]
            [hasch.core :as hasch])
  (:import [org.replikativ.persistent_sorted_set Branch ANode Leaf Slot PersistentSortedSet IStorage IMeasure Settings]
           [java.util Random]))

(def cmp (fn [a b] (compare (first a) (first b))))

;; A simple monoid measure for the stress oracle: SUM of element keys (first of each [k v]).
;; extract keys on (first), so a content-only replace [k 0]->[k v] leaves the measure
;; unchanged (k constant) — exercising that projection/maintenance preserves measure across
;; a buffered replace, while add/remove move it. This is the ĝ.measure sibling of ĝ.count.
(def ^IMeasure sum-measure
  (reify IMeasure
    (identity [_] (long 0))
    (extract  [_ k] (long (first k)))
    (merge    [_ a b] (+ (long a) (long b)))
    (remove   [_ cur k _recompute] (- (long cur) (long (first k))))))

(defn- opbuf-settings ^Settings [bf b measure] (Settings. (int bf) nil ^IMeasure measure nil (int b)))
(defn- mkst [disk bf b measure] (tstore/->Storage (atom {}) disk (opbuf-settings bf b measure)))

;; Wrap a storage to record every markFreed into `freed` (for the GC over-free/leak checks).
(defn- recording ^IStorage [^IStorage inner freed]
  (reify IStorage
    (restore   [_ a] (.restore inner a))
    (store     [_ n] (.store inner n))
    (accessed  [_ a] (.accessed inner a))
    (markFreed [_ a] (swap! freed conj a))
    (isFreed   [_ a] (contains? @freed a))
    (freedInfo [_ a] nil)))

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
  [seed {:keys [bf b keyrange init cycles ops restore-prob transient-prob measure? gc?] :as params}]
  (try
    (let [rng   (Random. seed)
          rint  (fn [n] (.nextInt rng (int n)))
          rd    (fn [] (.nextDouble rng))
          disk  (atom {})
          meas  (when measure? sum-measure)
          freed (when gc? (atom #{}))
          mk    (fn [] (let [s (mkst disk bf b meas)] (if freed (recording s freed) s)))
          ropts (cond-> {:branching-factor bf :diff-buf-size b :comparator cmp}
                  meas (assoc :measure meas))
          cov   (atom {:stores 0 :buffered 0 :flushed 0 :max-depth 0 :restores 0
                       :adds 0 :removes 0 :replaces 0 :ops 0 :measure-checks 0})
          ref   (atom (sorted-set-by cmp))
          ;; initial bulk load
          s0    (reduce (fn [s _]
                          (let [k (rint keyrange)] (swap! ref conj [k 0]) (conj s [k 0])))
                        (ss/sorted-set* (cond-> {:comparator cmp :branching-factor bf :diff-buf-size b}
                                          meas (assoc :measure meas)))
                        (range init))
          store! (fn [s] (tstore/with-stats (let [a (ss/store s (mk))]
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
                         (throw (ex-info (str "lookup mismatch @ " tag) {:tag tag :k k :want want :got got})))))
                   ;; measure oracle (ĝ.measure sibling of the count check): the set's aggregate
                   ;; measure (sum of keys) must equal the model's — both the cached/incremental
                   ;; value (the count-bug analogue: present-but-wrong) and a forced recompute
                   ;; from PROJECTED children (catches projection/ĝ.measure bugs).
                   (when meas
                     (swap! cov update :measure-checks inc)
                     (let [true-m (reduce + 0 (map (comp long first) @ref))
                           ^ANode root (.root ^PersistentSortedSet s)
                           cached (.-_measure root)]
                       (when (and (some? cached) (not= true-m (long cached)))
                         (throw (ex-info (str "cached measure mismatch @ " tag)
                                         {:tag tag :true true-m :cached cached})))
                       (let [forced (.forceComputeMeasure root (mk))]
                         (when (and (some? forced) (not= true-m (long forced)))
                           (throw (ex-info (str "forced measure mismatch @ " tag)
                                           {:tag tag :true true-m :forced forced})))))))
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
          (let [st (mk)
                loaded (ss/restore-by cmp addr st ropts)]
            (check loaded :final-restore)
            (swap! cov update :max-depth max (tree-depth loaded st))
            ;; GC checks (when gc?): no reachable node was ever markFreed (over-free), and
            ;; under diff-buf (b>0) every superseded blob WAS freed (no leak). Mirrors the
            ;; markfreed-never-frees-live unit test, but over the full sweep.
            (when freed
              (let [reachable (atom #{})]
                (ss/walk-addresses loaded (fn [a] (swap! reachable conj a)))
                (let [over   (clojure.set/intersection @reachable @freed)
                      leaked (clojure.set/difference (set (keys @disk)) @reachable @freed)]
                  (when (seq over)
                    (throw (ex-info (str "GC over-free: " (count over) " reachable node(s) markFreed")
                                    {:over (count over)})))
                  (when (and (pos? b) (seq leaked))
                    (throw (ex-info (str "GC leak: " (count leaked) " superseded blob(s) never freed")
                                    {:leaked (count leaked)}))))))
            {:ok? true :seed seed :params params :cov @cov})
          (let [;; cold restore with prob, else keep live set
                s (if (< (rd) (double (or restore-prob 0.5)))
                    (let [st (mk)
                          loaded (ss/restore-by cmp addr st ropts)]
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
  (reduce (fn [m c] (merge-with + m (select-keys c [:stores :buffered :flushed :restores :adds :removes :replaces :ops :measure-checks])))
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

;; broad-grid exercises ALL oracles per trial (content/count/lookup + measure + GC over-free/leak).
(def broad-grid
  (for [bf [4 16 64 512] b [0 4 32 256 4096] keyrange [50 500 5000]]
    {:bf bf :b b :keyrange keyrange :init (min keyrange 2000)
     :cycles 20 :ops 40 :restore-prob 0.5 :transient-prob 0.3 :measure? true :gc? true}))

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
                :cycles 8 :ops 30 :restore-prob 0.5 :transient-prob 0.3 :measure? true :gc? true})
        {:keys [failures cov]} (sweep {:grid grid :seeds 5 :label "stress-bounded(suite)"})]
    (is (empty? failures) (str (count failures) " stress trial(s) failed"))
    ;; sanity: the bounded grid must actually exercise each path it claims to test
    (is (pos? (:flushed cov)) "bounded grid exercised at least one flush")
    (is (pos? (:replaces cov)) "bounded grid exercised replaces")
    (is (pos? (:restores cov)) "bounded grid exercised cold restores")
    (is (pos? (:measure-checks cov)) "bounded grid exercised the measure oracle")))

;; ---- #4: content-address determinism / dedup under diff-buf -----------------
;; A content-addressed storage: a node's address is the SHA-256 of its serialized form
;; (incl. diff-buf :slots). If that serialization is deterministic, the same logical
;; tree (built+mutated+stored identically) yields IDENTICAL addresses — i.e. dedup works
;; and merkle hashes are stable. Mirrors the test storage's serialization exactly, but
;; with a content hash instead of a random UUID.

(deftype CHStorage [*disk ^Settings settings]
  IStorage
  (store [_ node]
    (let [slots (when (instance? Branch node) (.slotsForStorage ^Branch node))
          m {:level     (.level ^ANode node)
             :keys      (vec (.keys ^ANode node))
             :addresses (when (instance? Branch node) (vec (.addresses ^Branch node)))}
          m (if slots (assoc m :slots slots) m)
          a (str (hasch/uuid m))]    ; content address = hasch hash (SHA-512, konserve's hashing)
      (swap! *disk assoc a (pr-str m))
      a))
  (accessed [_ _addr] nil)
  (restore [_ address]
    (let [{:keys [level ^java.util.List keys ^java.util.List addresses slots]} (edn/read-string (@*disk address))
          node (if addresses (Branch. (int level) keys addresses settings) (Leaf. keys settings))]
      (when (and slots (instance? Branch node))
        (let [^Branch b node arr (object-array (alength (.-_keys b)))]
          (doseq [[idx entry] slots]
            (aset arr (int idx) (Slot. (:diff entry) (long (:count entry)) (:measure entry) (nth addresses (int idx)))))
          (set! (.-_slots b) arr)))
      node))
  (markFreed [_ _a] nil)
  (isFreed [_ _a] false)
  (freedInfo [_ _a] nil))

(defn- ch-final-addr
  "Run a deterministic build+mutate+store sequence under content-addressed storage;
   return [root-address, #{all node addresses}]. Same (seed,params) ⇒ identical result iff
   the serialized form (incl. slots) is deterministic."
  [seed {:keys [bf b keyrange init cycles ops]}]
  (let [rng  (Random. seed)
        rint (fn [n] (.nextInt rng (int n)))
        disk (atom {})
        st   (fn [] (->CHStorage disk (opbuf-settings bf b nil)))
        s0   (reduce (fn [s _] (conj s [(rint keyrange) 0]))
                     (ss/sorted-set* {:comparator cmp :branching-factor bf :diff-buf-size b})
                     (range init))]
    (loop [c 0, addr (ss/store s0 (st))]
      (if (= c cycles)
        [addr (set (keys @disk))]
        (let [s (ss/restore-by cmp addr (st) {:branching-factor bf :diff-buf-size b :comparator cmp})
              s (reduce (fn [s _] (let [k (rint keyrange) r (rint 3)]
                                    (cond (= r 0) (conj s [k 0])
                                          (= r 1) (disj s [k 0])
                                          (contains? s [k 0]) (ss/replace s [k 0] [k (inc (rint 9))])
                                          :else (conj s [k 0]))))
                        s (range ops))]
          (recur (inc c) (ss/store s (st))))))))

(deftest address-determinism
  ;; same seed+params, two independent content-addressed runs ⇒ identical root address AND
  ;; identical full address set (every node hashes identically) ⇒ deterministic serialization
  ;; / content-addressing / dedup, with diff-buf slots in the hash.
  (doseq [params (for [bf [4 16] b [0 64 256] kr [80 1500]]
                   {:bf bf :b b :keyrange kr :init (min kr 300) :cycles 6 :ops 25})
          seed (range 3)]
    (let [[r1 ks1] (ch-final-addr seed params)
          [r2 ks2] (ch-final-addr seed params)]
      (is (= r1 r2) (str "non-deterministic root address for " params " seed " seed))
      (is (= ks1 ks2) (str "non-deterministic node address set for " params " seed " seed)))))
