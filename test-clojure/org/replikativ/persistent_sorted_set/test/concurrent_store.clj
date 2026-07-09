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
  (:import [org.replikativ.persistent_sorted_set IStorage PersistentSortedSet]
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
