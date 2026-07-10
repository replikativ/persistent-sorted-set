(ns org.replikativ.persistent-sorted-set.test.concurrent-store
  "Race harness for the diff-buf torn-pair publication bug (Branch.BufState).

   Scenario (mirrors the datahike pipelining writer): thread B calls `store` on set s —
   settling s's SHARED dirty spine nodes (store() nulls flushed children's slots and resets
   the node's running buffered-entry total) — while thread A concurrently derives the next
   tree value from the same s (transient replace-cycles mimicking datom upserts: the
   persistent-path node copies carry the source's {slots, entries} pair onto the successor).

   Before the fix the pair lived in two plain fields (_slots, _bufEntries) and A could
   observe it TORN — e.g. flushed slots already nulled but the total still counting them —
   baking a phantom total into its copy that delta maintenance preserved forever. The
   observed signature: `_bufEntries X != recomputed Y` where X - Y is exactly the size of
   the concurrently-flushed slots, with every individual slot internally consistent.

   Oracle: the -ea `assertBufEntries` cross-check at the top of Branch.store() (the :test
   alias runs with -ea), which recomputes the total by a fresh subtree walk — a torn carry
   in round r fires it when round r+1 stores A's result. Content is additionally checked
   against a model map at the end. With the single-volatile-publish BufState fix the run
   must be deterministically green; before it, it drifts (it is a race — the harness runs
   many rounds so a torn read has many chances to strike).

   JVM-only by nature (the cljs implementation is single-threaded)."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as ss])
  (:import [org.replikativ.persistent_sorted_set ANode Branch IStorage PersistentSortedSet]
           [java.util UUID]))

;; Elements are [k v t] triples (datom-shaped). The SET's comparator orders by k, then v,
;; then t; the REPLACE comparator keys on k only, so (replace [k _ _] [k v' t']) is a
;; content-only upsert of k's triple — exactly datahike's value-changing datom upsert
;; (deposited as Absent(old)+Present(new) into the leaf-parent's diff-buf slot).
(defn- full-cmp [[k1 v1 t1] [k2 v2 t2]]
  (let [c (compare k1 k2)]
    (if-not (zero? c) c
            (let [c (compare v1 v2)]
              (if-not (zero? c) c (compare t1 t2))))))

(defn- by-k [a b] (compare (nth a 0) (nth b 0)))

(defn- mem-storage
  "In-memory IStorage: fresh address per store, no reads (the whole tree stays resident —
   the set is built with :ref-type :strong so restore is never needed). `accessed` must be
   a real no-op: Branch.child LRU-touches storage on every descent through a child that
   HAS an address — and thread B's settle assigns addresses on shared nodes mid-race, so
   thread A's descents call it."
  []
  (reify IStorage
    (store [_ _node] (str (UUID/randomUUID)))
    (accessed [_ _address])
    (markFreed [_ _address])
    (isFreed [_ _address] false)
    (freedInfo [_ _address] nil)
    (restore [_ address]
      (throw (ex-info "unexpected restore — tree should be fully resident" {:address address})))))

(def ^:private ^:const n-init    10000)
(def ^:private ^:const bf        512)
(def ^:private ^:const diff-buf  256)
(def ^:private ^:const rounds    200)
(def ^:private ^:const replaces  32)

(defn- replace-cycle
  "One transient cycle from s: `replaces` k-only upserts on sequential ks + one rightmost
   conj (mimics a datahike tx: mostly upserts, tail growth, occasional structural split)."
  ^PersistentSortedSet [^PersistentSortedSet s ks round next-k]
  (let [t (reduce (fn [^PersistentSortedSet t k]
                    (.replace t [k 0 0] [k (inc round) round] by-k))
                  (.asTransient s) ks)]
    (.persistent ^PersistentSortedSet (.conj ^PersistentSortedSet t [next-k 0 0]))))

(deftest concurrent-store-vs-pipelined-writer
  (testing "store() settling shared nodes races a pipelining writer deriving the next tree:
            the diff-buf {slots, entries} pair must never be observed torn (-ea oracle in
            Branch.store), and the final content must equal the model"
    (let [storage (mem-storage)
          ;; the set must CARRY the storage: post-settle descents through addressed
          ;; children LRU-touch it (a null here NPEs the moment B's settle lands)
          s0      (into (ss/sorted-set* {:comparator       full-cmp
                                         :storage          storage
                                         :branching-factor bf
                                         :diff-buf-size    diff-buf
                                         :ref-type         :strong})
                        (map (fn [k] [k 0 0]) (range n-init)))
          ;; Run until `rounds` rounds pass or the store-time -ea assert fires. Each round:
          ;; thread B stores the CURRENT set s (settling its shared dirty spine) while
          ;; thread A concurrently derives the next tree from the same s; join; repeat with
          ;; A's result (whose next store re-runs the oracle over everything A carried).
          result
          (try
            (loop [round 0, s s0, model (into {} (map (fn [k] [k [k 0 0]])) (range n-init))]
              (if (== round rounds)
                (do (ss/store s storage)             ; final store: oracle over the last carry
                    {:ok true :s s :model model})
                (let [ks     (mapv #(+ (* round replaces) %) (range replaces))
                      next-k (+ n-init round)
                      fut-a  (future (replace-cycle s ks round next-k))
                      fut-b  (future (ss/store s storage))]
                  @fut-b                              ; join B (assert oracle runs inside)
                  (let [s' @fut-a                     ; join A
                        model' (-> (reduce (fn [m k] (assoc m k [k (inc round) round])) model ks)
                                   (assoc next-k [next-k 0 0]))]
                    (recur (inc round) s' model')))))
            (catch java.util.concurrent.ExecutionException e
              {:ok false :error (.getCause e)})
            (catch Throwable e
              {:ok false :error e}))]
      (is (:ok result)
          (str "diff-buf state observed torn (or other failure): " (:error result)))
      (when (:ok result)
        (let [{:keys [s model]} result]
          (is (= (+ n-init rounds) (count s)) "final count matches model")
          (is (= (sort-by first (vals model)) (seq s))
              "final content equals the model map"))))))

;; ---------------------------------------------------------------------------
;; Pair-tearing oracle: the BASELINE (diffBufSize=0) settle two-step
;; ---------------------------------------------------------------------------

;; Reflection-based access to a Branch's per-child {addresses, children} pair so this
;; test runs unchanged against BOTH the pre-fix layout (plain `_addresses`/`_children`
;; fields) and the post-fix layout (one volatile `_state` NodeState snapshot).
(def ^:private state-field
  (try (doto (.getDeclaredField Branch "_state") (.setAccessible true))
       (catch NoSuchFieldException _ nil)))

(def ^:private legacy-addresses-field
  (when-not state-field (doto (.getDeclaredField Branch "_addresses") (.setAccessible true))))

(def ^:private legacy-children-field
  (when-not state-field (doto (.getDeclaredField Branch "_children") (.setAccessible true))))

(def ^:private nodestate-addresses-field
  (when state-field
    (doto (.getDeclaredField (.getType ^java.lang.reflect.Field state-field) "addresses")
      (.setAccessible true))))

(def ^:private nodestate-children-field
  (when state-field
    (doto (.getDeclaredField (.getType ^java.lang.reflect.Field state-field) "children")
      (.setAccessible true))))

(defn- node-pair
  "[addresses children] of b — post-fix from ONE NodeState snapshot; pre-fix the two
   plain arrays (whose mutual consistency is exactly what this test challenges)."
  [^Branch b]
  (if state-field
    (let [st (.get ^java.lang.reflect.Field state-field b)]
      [(.get ^java.lang.reflect.Field nodestate-addresses-field st)
       (.get ^java.lang.reflect.Field nodestate-children-field st)])
    [(.get ^java.lang.reflect.Field legacy-addresses-field b)
     (.get ^java.lang.reflect.Field legacy-children-field b)]))

(defn- validate-pairs!
  "Resident walk of the tree under `node`: for every Branch slot assert the
   {address, child} pair is coherent — addresses[i] == null (dirty) ⇒ children[i] is a
   bare ANode or null. A dirty slot holding a Soft/WeakReference wrapper is the torn
   state: the copy mixed a pre-settle address with a post-settle (wrapped) child. Only
   descends into resident children (no IO)."
  [node path]
  (when (instance? Branch node)
    (let [^Branch b node
          [^objects addrs ^objects kids] (node-pair b)
          len (.-_len b)]
      (dotimes [i len]
        (let [addr (when addrs (aget addrs i))
              kid  (when kids (aget kids i))]
          (when (and (nil? addr)
                     (not (or (nil? kid) (instance? ANode kid))))
            (throw (ex-info "TORN PAIR: dirty child slot holds a Reference wrapper"
                            {:path (conj path i)
                             :level (.level b)
                             :child (class kid)})))
          ;; recurse into resident branches (bare or still-referenced wrapper)
          (let [child (if (instance? java.lang.ref.Reference kid)
                        (.get ^java.lang.ref.Reference kid)
                        kid)]
            (when (instance? Branch child)
              (validate-pairs! child (conj path i)))))))))

(defn- resident-storage
  "In-memory IStorage holding stored NODES strongly (so :soft wrappers never clear
   mid-test and restore is exact). Fresh address per store — like the real backends."
  []
  (let [disk (atom {})]
    (reify IStorage
      (store [_ node] (let [a (str (UUID/randomUUID))] (swap! disk assoc a node) a))
      (accessed [_ _address])
      (markFreed [_ _address])
      (isFreed [_ _address] false)
      (freedInfo [_ _address] nil)
      (restore [_ address] (or (@disk address)
                               (throw (ex-info "missing node" {:address address})))))))

(def ^:private ^:const pt-n-init   100000)
(def ^:private ^:const pt-bf       128)
(def ^:private ^:const pt-rounds   400)
(def ^:private ^:const pt-replaces 64)
(def ^:private ^:const pt-min-derives 8)

(defn- pt-replace-cycle
  "One transient derive from s: `pt-replaces` k-only upserts on random ks. Each FIRST
   touch of a shared branch goes through the persistent copy path (copyOfRange of the
   source's addresses, then of its children — the two reads whose coherence is under
   test) while thread B's settle rewrites that same shared node."
  ^PersistentSortedSet [^PersistentSortedSet s ^java.util.Random rnd round]
  (let [t (reduce (fn [^PersistentSortedSet t _]
                    (let [k (.nextInt rnd (int pt-n-init))]
                      (.replace t [k 0 0] [k (inc round) round] by-k)))
                  (.asTransient s) (range pt-replaces))]
    (.persistent ^PersistentSortedSet t)))

(deftest baseline-settle-pair-tearing
  (testing "store()'s settle of a SHARED node (write addresses[i], then wrap children[i])
            races an apply-thread copy (copyOfRange of addresses, then of children):
            every tree the apply thread derives must satisfy, per slot, the pair
            invariant addresses[i] == null => children[i] is a bare ANode or null.

            Pre-fix failure recorded on 2026-07-10 (commit 2b7db29, 16-core linux),
            5/5 consecutive runs, e.g.:
              pair invariant violated (or other failure): clojure.lang.ExceptionInfo:
              TORN PAIR: dirty child slot holds a Reference wrapper
              {:path [11 6], :level 1, :child java.lang.ref.SoftReference}
            (other runs: paths [1 63] [6 10] [0 18] [3 1] — always a level-1
            leaf-parent copied while the settle rewrote it). The derived copy mixed a
            pre-settle (null) address with the settle's post-wrap SoftReference child,
            the exact #17-class state that crashed baseline commits under heap
            pressure."
    (let [storage (resident-storage)
          s0 (into (ss/sorted-set* {:comparator       full-cmp
                                    :storage          storage
                                    :branching-factor pt-bf
                                    :diff-buf-size    0      ; BASELINE settle (the two-step)
                                    :ref-type         :soft}) ; wrapping must actually happen
                   (map (fn [k] [k 0 0]) (range pt-n-init)))
          rnd-a (java.util.Random. 42)
          rnd-d (java.util.Random. 7)
          result
          (try
            (loop [round 0, s s0]
              (if (== round pt-rounds)
                {:ok true :s s}
                ;; s1: freshly-derived tree with a dirty spine (what B settles);
                ;; A keeps deriving from s1 while B stores it, validating every result.
                (let [s1    (pt-replace-cycle s rnd-d round)
                      fut-b (future (ss/store s1 storage))
                      fut-a (future
                              (loop [j 0, last s1]
                                (if (and (>= j pt-min-derives) (future-done? fut-b))
                                  last
                                  (let [t (pt-replace-cycle s1 rnd-a round)]
                                    (validate-pairs! (.root ^PersistentSortedSet t) [])
                                    (recur (inc j) t)))))]
                  @fut-b
                  (let [s' @fut-a]
                    (validate-pairs! (.root ^PersistentSortedSet s') [])
                    (recur (inc round) s')))))
            (catch java.util.concurrent.ExecutionException e
              {:ok false :error (.getCause e)})
            (catch Throwable e
              {:ok false :error e}))]
      (is (:ok result)
          (str "pair invariant violated (or other failure): " (:error result)
               (when-let [^Throwable e (:error result)]
                 (str " " (ex-data e))))))))
