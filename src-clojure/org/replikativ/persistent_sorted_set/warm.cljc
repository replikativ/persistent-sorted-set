(ns org.replikativ.persistent-sorted-set.warm
  "Budget-bounded breadth-first warming — pull a stored tree's upper levels into
   the storage's cache in WAVES instead of discovering them one blocking round
   trip at a time.

   ## Why this lives in persistent-sorted-set

   A cold reader's wall time is `misses x RTT`, with nothing overlapping: a scan
   asks for a node, blocks on the restore, and only then learns the next
   address. It does not have to be that way, and the fix needs no prediction —
   a `Branch` holds EVERY child address the moment it is materialized, so the
   addresses of a whole level are known one level in advance. This walks
   breadth-first and restores each level concurrently. Measured (in datahike,
   where this walk was first written): 16.4x faster than serial at +20 ms
   injected latency, width 64.

   The walk speaks only this library's vocabulary — `Branch`, levels, child
   addresses, `IStorage.restore` — so it belongs here, where every consumer
   reaches it: datahike's primary indices, and any other tree built on this set
   (a secondary index keeps one tree per attribute and warms them the same
   way). The consumer keeps what is genuinely its own: how to build `:from`/
   `:to` bounds in the tree's key order, and any cache-size policy.

   ## The two bounds, and why there are two

   `:depth` bounds the SHAPE, `:budget` bounds the COST, and whichever binds
   first wins.

     :depth :interior     expand while (>= level 2) — children are branches, so
                          this stops exactly at the leaf boundary. Exact, not a
                          heuristic: leaves are level 0.
     :depth :with-leaves  expand while (>= level 1) — everything.
     :depth <integer>     at most that many levels below the root.

   Having both is what keeps this free of latency cliffs: a small tree with a
   large budget runs out of frontier and has fetched itself entirely; a large
   one hits the budget and stops. Same code, continuous in tree size.

   ## Selective warming

   `:from`/`:to` scope the walk to a key range: at every level only the child
   indices covering the range are expanded, so cost is proportional to the
   RANGE rather than to the tree. Bounds are keys in THIS tree's own order and
   comparator — a bound built in the wrong order warms a valid-but-different
   subtree with no error, so a consumer with multiple key orders (datahike's
   index permutations) must derive bounds with its own machinery and never let
   callers hand them in raw.

   ## Multiple trees, one budget

   `warm-trees!` interleaves several trees round-robin at the level of
   individual restores, so no tree can spend the budget before another gets
   any. Which tree the next read needs is not knowable at warm time; this is
   the connect-time shape for a database's indices and for a secondary index's
   per-attribute trees alike.

   ## Platforms

   JVM: synchronous, restores fan out on a per-call thread pool (`:width` in
   flight). `:sync? false` is refused rather than emulated — a JVM caller that
   wants the walk off-thread wraps it in its own thread, which composes better
   than this namespace owning one.

   ClojureScript: `(async+sync sync? ...)`, the same idiom as the rest of this
   library. `:sync? false` returns a partial-cps expression; restores are
   started `:width` at a time in a sliding window — JS is single-threaded, but
   the IO underneath is not, and starting a wave before awaiting it is what
   pipelines the round trips. `:width` defaults differ (64 JVM, 6 cljs: a
   browser gives ~6 connections per origin on HTTP/1.1, more merely queues).

   Concurrent restore of the same address is safe on both platforms: nodes are
   immutable and a racing restore duplicates a fetch, never corrupts one.

   ## Report

   {:fetched :by-level :rounds :height :by-index :budget-left
    :budget-exhausted? :budget-clamped? :ms}

   `:by-level` and `:budget-exhausted?` are the point: they make a decaying
   warm visible as a metric before it is visible in p99. `:by-index` says where
   a shared budget actually went, keyed by each entry's `:key`."
  #?(:cljs
     (:require [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
               [org.replikativ.persistent-sorted-set.btset :as btset]
               [org.replikativ.persistent-sorted-set.branch :refer [Branch]]
               [org.replikativ.persistent-sorted-set.impl.node :as node]
               [org.replikativ.persistent-sorted-set.impl.storage :as storage]
               [org.replikativ.persistent-sorted-set.util :as util]
               [org.replikativ.persistent-sorted-set.arrays :as arrays]))
  #?(:cljs
     (:require-macros [org.replikativ.persistent-sorted-set.macros :refer [async+sync]]))
  #?(:clj
     (:import [org.replikativ.persistent_sorted_set PersistentSortedSet ANode Branch IStorage]
              [java.util Comparator]
              [java.util.concurrent Executors ExecutorService Callable Future TimeUnit])))

(def default-width
  "Concurrent in-flight restores. 64 measured optimal against local MinIO in
   datahike's rig (128 regressed there); 6 on ClojureScript for the browser's
   per-origin connection budget. A starting point to measure from."
  #?(:clj 64 :cljs 6))

(def default-budget
  "Nodes to restore at most. In a B-tree the interior is a geometric series, so
   `interior/total` is a constant fraction of the tree — but size it from the
   MEASURED fill: nodes run about half full, so the interior is ~`2/bf` of the
   tree, twice the naive estimate."
  2000)

;; ---------------------------------------------------------------------------
;; Platform primitives — everything that touches a node or the storage.

(defn- branch? [n]
  (instance? Branch n))

(defn- node-level [n]
  #?(:clj  (.level ^ANode n)
     :cljs (node/level n)))

(defn- child-bounds
  "Inclusive child-index bounds of `node`, intersected with [from to] (nil =
   unbounded). A branch's `keys[i]` is the MAX key of child i, so the leftmost
   index whose key is >= the probe names the child that could contain it, for
   both ends of the range."
  [node cmp from to]
  #?(:clj
     (let [^ANode node node
           ^Comparator cmp cmp
           lst (dec (.len node))]
       [(if from (min (max 0 (.searchFirst node from cmp)) lst) 0)
        (if to   (min (max 0 (.searchFirst node to cmp)) lst) lst)])
     :cljs
     (let [ks  (.-keys node)
           lst (dec (arrays/alength ks))]
       [(if from (min (max 0 (util/binary-search-l cmp ks lst from)) lst) 0)
        (if to   (min (max 0 (util/binary-search-l cmp ks lst to)) lst) lst)])))

(defn- child-address [node i]
  #?(:clj  (.address ^Branch node (int i))
     :cljs (when-let [addrs (.-addresses node)]
             (aget addrs i))))

#?(:clj
   (defn- fetch-wave!
     "Restore `reqs` ({:storage :addr ...}) with at most `width` in flight.
      A per-call pool rather than a shared one: nothing to own, nothing to shut
      down on a failure path, and a warm is not hot enough for churn to matter."
     [reqs width _opts]
     (let [^ExecutorService pool (Executors/newFixedThreadPool (int (max 1 width)))]
       (try
         (->> reqs
              (mapv (fn [{:keys [^IStorage storage addr]}]
                      (reify Callable (call [_] (.restore storage addr)))))
              (.invokeAll pool)
              (mapv (fn [^Future f] (.get f))))
         (finally
           (.shutdown pool)
           (.awaitTermination pool 120 TimeUnit/SECONDS))))))

#?(:cljs
   (defn- run-window!
     "Start cps `exprs` with at most `width` in flight; resolve with the vector
      of results in order once every one has resolved, raise on the first
      failure. Sliding, not chunked: each completion starts the next expr, so a
      slow restore does not hold a whole chunk's worth of idle connections.

      EVERY continuation is entered from a FRESH microtask stack, and that is
      correctness, not style. A cps expression invoked from inside an
      already-running trampoline hands its continuation back as a Thunk it
      expects the caller's trampoline to force; a fan-out like this one sits
      BETWEEN trampolines, and a thunk dropped anywhere along the callback
      chain is a computation suspended forever — measured as a two-restore
      wave that started both, resolved both, and then never delivered the
      wave, hanging the walk with no error and an empty event loop. Entering
      from a clean stack means `*in-trampoline*` is false, so the machinery
      runs its own trampoline loop and no Thunk ever escapes to us. The cost
      is one microtask per restore, which is nothing next to the IO the
      restore performs."
     [exprs width resolve raise]
     (let [n (count exprs)]
       (if (zero? n)
         (js/queueMicrotask #(resolve []))
         (let [results (make-array n)
               state   (atom {:started 0 :done 0 :failed? false})]
           (letfn [(start-next! []
                     (let [{:keys [started]} @state]
                       (when (< started n)
                         (swap! state update :started inc)
                         (let [i started]
                           (js/queueMicrotask
                            (fn []
                              ((nth exprs i)
                               (fn [v]
                                 (aset results i v)
                                 (let [{:keys [done failed?]} (swap! state update :done inc)]
                                   (when-not failed?
                                     (if (= done n)
                                       (js/queueMicrotask #(resolve (vec results)))
                                       (start-next!)))))
                               (fn [err]
                                 (let [{:keys [failed?]} @state]
                                   (when-not failed?
                                     (swap! state assoc :failed? true)
                                     (js/queueMicrotask #(raise err))))))))))))]
             (dotimes [_ (min width n)]
               (start-next!))))))))

#?(:cljs
   (defn- fetch-wave!
     "One level's restores. `{:sync? true}` restores sequentially — synchronous
      JS has no concurrency to exploit — and returns the nodes; `{:sync? false}`
      returns a cps expression resolving to them, restores started `width` at a
      time in a sliding window."
     [reqs width opts]
     (if (:sync? opts)
       (mapv (fn [{:keys [storage addr]}] (storage/restore storage addr opts)) reqs)
       (fn [resolve raise]
         (run-window! (mapv (fn [{:keys [storage addr]}]
                              (storage/restore storage addr opts))
                            reqs)
                      width resolve raise)))))

(defn- now-ns []
  #?(:clj (System/nanoTime) :cljs (* 1e6 (js/Date.now))))

(defn- elapsed-ms [t0]
  (/ (- (now-ns) t0) 1e6))

;; ---------------------------------------------------------------------------
;; The walk

(defn- expand?
  "Should this node's children be fetched? A leaf never has children, so it
   always terminates the walk regardless of policy — which is what makes
   `:interior` fall out of the loop rather than needing to be enforced."
  [node depth round]
  (let [lvl (node-level node)]
    (cond
      (< lvl 1)              false
      (= :interior depth)    (>= lvl 2)
      (= :with-leaves depth) true
      (integer? depth)       (< round depth)
      :else                  false)))

(defn- round-robin
  "Fair interleave of unequal-length colls. Used to share one budget across
   trees: without it, whichever tree is enumerated first eats the budget and a
   read against a later one gets nothing warmed."
  [colls]
  (lazy-seq
   (let [colls (remove empty? colls)]
     (when (seq colls)
       (concat (map first colls) (round-robin (map next colls)))))))

(defn- clamp-budget
  "Budget capped to 0.8x `cache-size` when given. Warming past the cache
   fetches nodes only to evict them, so a budget above it is not a bigger warm
   — it is the same warm plus wasted round trips. The 0.8 leaves room for the
   read that follows to bring in its own leaves without evicting the spine.
   `:budget-clamped?` in the report says it happened; whether that deserves a
   warning is the consumer's call, so none is logged here."
  [budget cache-size]
  (if (and cache-size (> budget (* 0.8 cache-size)))
    (long (* 0.8 cache-size))
    budget))

(defn- level-requests
  "The next level's restore requests for the expandable part of `frontier`,
   round-robin across trees. Skips nil addresses — an in-memory child never
   stored cannot happen on a freshly-restored cold tree, and is skipped rather
   than trusted."
  [frontier depth round]
  (let [groups (->> frontier
                    (filter #(and (branch? (:node %))
                                  (expand? (:node %) depth round)))
                    (group-by :key))]
    (vec (round-robin
          (for [[_ es] groups]
            (for [{:keys [node from to cmp] :as e} es
                  :let  [[lo hi] (child-bounds node cmp from to)]
                  i     (range lo (inc hi))
                  :let  [a (child-address node i)]
                  :when (some? a)]
              (assoc e :addr a :node nil)))))))

(defn- final-report [fetched by-level rounds per-key height left exhausted? clamped? t0]
  {:fetched fetched :by-level by-level :rounds rounds :by-index per-key
   :height height :budget-left left :budget-exhausted? exhausted?
   :budget-clamped? clamped? :ms (elapsed-ms t0)})

#?(:clj
   (defn- root-entry
     "A walk entry for one tree, or nil when there is nothing to walk.
      `.root` restores from the stored address if it is not already in hand."
     [k ^PersistentSortedSet pset from to]
     (let [storage (.-_storage pset)
           root    (.root pset)]
       (when (and root storage)
         {:key k :node root :storage storage :cmp (.comparator pset)
          :from from :to to}))))

#?(:clj
   (defn- warm-loop!
     [entries {:keys [depth budget width cache-size]
               :or   {depth :interior budget default-budget width default-width}}]
     (let [capped   (clamp-budget budget cache-size)
           clamped? (< capped budget)
           t0       (now-ns)
           height   (reduce max 0 (map (comp node-level :node) entries))]
       (loop [frontier (vec entries)
              round    0
              left     (long capped)
              fetched  0
              by-level []
              per-key  {}]
         (let [reqs (when (pos? left) (level-requests frontier depth round))]
           (if (empty? reqs)
             (final-report fetched by-level round per-key height left false clamped? t0)
             (let [take-n  (min (count reqs) left)
                   batch   (subvec reqs 0 take-n)
                   nodes   (fetch-wave! batch width nil)
                   next-f  (mapv (fn [n r] (assoc r :node n)) nodes batch)
                   left'   (- left take-n)
                   per-key (reduce (fn [m {:keys [key]}] (update m key (fnil inc 0)))
                                   per-key batch)]
               (if (zero? left')
                 (final-report (+ fetched take-n) (conj by-level take-n) (inc round)
                               per-key height 0 true clamped? t0)
                 (recur next-f (inc round) left' (+ fetched take-n)
                        (conj by-level take-n) per-key)))))))))

#?(:cljs
   (defn- warm-loop!
     "The same walk under `async+sync`: with `:sync? false` this is a cps
      expression, and each wave's restores run through the sliding window."
     [entries {:keys [depth budget width cache-size sync?]
               :or   {depth :interior budget default-budget width default-width
                      sync? true}
               :as   opts}]
     (let [capped   (clamp-budget budget cache-size)
           clamped? (< capped budget)
           t0       (now-ns)
           height   (reduce max 0 (map (comp node-level :node) entries))]
       (async+sync sync?
                   (async
                    (loop [frontier (vec entries)
                           round    0
                           left     capped
                           fetched  0
                           by-level []
                           per-key  {}]
                      (let [reqs (when (pos? left) (level-requests frontier depth round))]
                        (if (empty? reqs)
                          (final-report fetched by-level round per-key height left false clamped? t0)
                          (let [take-n  (min (count reqs) left)
                                batch   (subvec reqs 0 take-n)
                                nodes   (await (fetch-wave! batch width opts))
                                next-f  (mapv (fn [n r] (assoc r :node n)) nodes batch)
                                left'   (- left take-n)
                                per-key (reduce (fn [m {:keys [key]}] (update m key (fnil inc 0)))
                                                per-key batch)]
                            (if (zero? left')
                              (final-report (+ fetched take-n) (conj by-level take-n) (inc round)
                                            per-key height 0 true clamped? t0)
                              (recur next-f (inc round) left' (+ fetched take-n)
                                     (conj by-level take-n) per-key)))))))))))

;; ---------------------------------------------------------------------------
;; Entry points

(defn warm-trees!
  "One breadth-first walk across SEVERAL trees, sharing one budget round-robin.

   `trees` is a seq of {:key label :set pss-set :from key :to key} — `:from`/
   `:to` optional, in each tree's own key order. Options: `:depth`, `:budget`,
   `:width`, `:cache-size`, and on ClojureScript `:sync?`.

   Returns the report (see the ns docstring); on ClojureScript with
   `:sync? false`, a partial-cps expression resolving to it."
  [trees {:keys [sync?] :or {sync? true} :as opts}]
  #?(:clj
     (do
       (when (false? sync?)
         (throw (IllegalArgumentException.
                 "warm-trees!: :sync? false is not supported on the JVM; run the (synchronous) walk in your own thread.")))
       (let [entries (keep (fn [{:keys [key set from to]}]
                             (root-entry key set from to))
                           trees)]
         (if (seq entries)
           (warm-loop! entries opts)
           (final-report 0 [] 0 {} 0 (:budget opts default-budget) false false (now-ns)))))
     :cljs
     ;; Roots first, then the walk. Root materialization is one restore per tree
     ;; (zero under fused roots, where the consumer hands sets whose root is in
     ;; hand) — a handful, sequential is fine; the waves are where width matters.
     (async+sync sync?
                 (async
                  (loop [acc [] ts (seq trees)]
                    (if-not ts
                      (if (seq acc)
                        (await (warm-loop! acc opts))
                        (final-report 0 [] 0 {} 0 (:budget opts default-budget) false false (now-ns)))
                      (let [{:keys [key set from to]} (first ts)
                            root (await (btset/$root set opts))
                            st   (.-storage set)]
                        (recur (cond-> acc
                                 (and root st)
                                 (conj {:key key :node root :storage st
                                        :cmp (.-comparator set)
                                        :from from :to to}))
                               (next ts)))))))))

(defn warm!
  "Breadth-first warm of ONE tree. `(warm! set opts)` =
   `(warm-trees! [{:key :tree :set set :from (:from opts) :to (:to opts)}] opts)`
   — see `warm-trees!` for options and the report."
  ([set] (warm! set {}))
  ([set {:keys [from to key] :or {key :tree} :as opts}]
   (warm-trees! [{:key key :set set :from from :to to}] opts)))
