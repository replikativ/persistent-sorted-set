(ns org.replikativ.persistent-sorted-set.test.concurrent-restore
  "The two CAS paths on `Branch._state` that had no concurrent test at all.

   `concurrent_store.clj` is threaded, and covers the settle. It cannot reach
   either of these, by construction:

     * `concurrent-store-vs-pipelined-writer` builds with `:ref-type :strong`
       over a storage whose `restore` THROWS — the tree is fully resident, so
       `child()`'s restore branch never executes and neither does its
       cache-fill CAS.
     * `baseline-settle-pair-tearing` pins `:diff-buf-size 0`, so no slot is
       ever installed and `bufEntries()` is never LAZY — its resolve CAS never
       executes either.

   Both are commented as deliberate choices for what those tests are about, and
   both are correct for that. The consequence is that the two comments in
   `Branch.java` describing what these CASes protect against —

       \"a concurrent settle (or another fill) advancing _state makes the CAS
        fail and we re-read — the fill must never clobber a settle\"
       \"a plain write would CLOBBER the settled {addresses, children, slots,
        entries} with this pre-settle snapshot (re-buffering already-flushed
        children: double projection on read, phantom totals; resurrecting
        pre-settle addresses)\"

   — were, until this namespace, entirely unexercised claims.

   The shape here is the one that reaches them: a COLD set restored from an
   address (every child slot an address with no resident child, every branch's
   `entries` BUF_LAZY) read by several threads at once, while a writer derives
   the next tree from that same set and stores it. The readers' descents fill;
   the writer's store settles; they contend on the same shared branches.

   The storage CACHES restored nodes and shares one instance across every
   parent and thread that asks for an address — deliberately, because that is
   what datahike's `CachedStorage` does, and sharing a restored node is what
   made in-place projection (#19 on the JVM, D-F2 on the cljs side) a data-loss
   bug rather than a private inefficiency."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set Branch PersistentSortedSet Settings]))

(set! *warn-on-reflection* true)

;; [k v t], ordered by all three; the REPLACE comparator keys on k alone, so
;; `(replace [k _ _] [k v' t'])` is a content-only upsert — the datahike datom
;; upsert that deposits Absent(old)+Present(new) into the leaf-parent's slot,
;; which is what puts a diff in a slot for the readers to project.
(defn- full-cmp [[k1 v1 t1] [k2 v2 t2]]
  (let [c (compare k1 k2)]
    (if-not (zero? c) c
            (let [c (compare v1 v2)]
              (if-not (zero? c) c (compare t1 t2))))))

(defn- by-k [a b] (compare (nth a 0) (nth b 0)))

(def ^:private ^:const n-init  8000)
;; TWO levels, not three. A deeper tree trips a SEPARATE, pre-existing defect in
;; the NESTED diff-buf path — `assembleNested` casting a Leaf to a Branch — which
;; is not what this namespace is about and which fires single-threaded too. (An earlier
;; version of this note pointed at `nested_diff_buf_repro.clj`; no such file exists. The
;; nearest live coverage of the nested-diff restore path is `diff_buf_restore_cycle.clj`.)
;; At this branching factor the tree is root -> leaves, so the slots, the
;; projection, the cache-fill CAS and the LAZY resolve all still run; only the
;; nested-diff assembly is out of the picture.
(def ^:private ^:const bf      512)
(def ^:private ^:const dbs     128)
(def ^:private ^:const rounds  25)
(def ^:private ^:const readers 6)
(def ^:private ^:const upserts 48)

(defn- settings-of ^Settings [^PersistentSortedSet s]
  (let [f (.getDeclaredField PersistentSortedSet "_settings")]
    (.setAccessible f true)
    (.get f s)))

(defn- upsert-cycle
  "One transient derive: `upserts` k-only upserts spread across the key space, so
   the diffs land in many different leaf-parents rather than one."
  ^PersistentSortedSet [^PersistentSortedSet s round ^java.util.Random rnd]
  (let [t (reduce (fn [^PersistentSortedSet t _]
                    (let [k (.nextInt rnd (int n-init))]
                      (.replace t [k 0 0] [k (inc round) round] by-k)))
                  (.asTransient s) (range upserts))]
    (.persistent ^PersistentSortedSet t)))

(defn- slot-bearing-addresses
  "Addresses whose stored record carries `:slots` — i.e. branches that really do
   have a buffered diff to project and a LAZY entries total to resolve."
  [disk]
  (into #{} (keep (fn [[a s]] (when (re-find #":slots" s) a))) @disk))

(deftest a-cold-restore-fills-and-resolves-while-a-settle-races-it
  (testing "several readers materialising a cold restored set while a writer
            settles the same shared branches: no reader may observe a torn or
            clobbered state, and every reader must see the whole set"
    (let [disk (atom {})
          mem  (atom {})
          ;; EXPLICIT Settings, never the bare `(ts/storage ...)` arities: those build a bare
          ;; `(Settings.)`, whose diff-buf budget comes from the `pss.diffBufSize` system
          ;; property -- 256 under the `:test` alias, 0 otherwise. This namespace declares
          ;; `dbs` for the SET, so the bare arity silently ran set-128-over-nodes-256, a mixed
          ;; configuration it never intended and never stated. Measured with the property at 0,
          ;; the bare arity threw `diff-buf: a node reconstructed with diffBufSize=0 was handed
          ;; buffered slots` and failed two assertions. See marker_slot_staleness.clj's note.
          st   (ts/->Storage mem disk (Settings. (int bf) nil nil nil (int dbs)))
          opts {:comparator full-cmp :branching-factor bf
                :diff-buf-size dbs :ref-type :strong}
          s0   (into (ss/sorted-set* (assoc opts :storage st))
                     (map (fn [k] [k 0 0]) (range n-init)))
          _    (ss/store s0 st)
          ;; A second generation, so the stored branches carry buffered diffs:
          ;; restoring generation 1 alone would give slot-less branches and the
          ;; projection / LAZY-resolve paths would not run at all.
          rnd0 (java.util.Random. 11)
          s1   (upsert-cycle s0 0 rnd0)
          addr (ss/store s1 st)
          model (reduce (fn [m [k v t]] (assoc m k [k v t])) {} (seq s1))
          expected (vec (sort-by first (vals model)))]

      (is (pos? (count (slot-bearing-addresses disk)))
          "precondition: some stored branch actually carries slots — otherwise
           neither the projection nor the LAZY resolve is reachable and this
           whole namespace tests nothing")

      (let [reads-before (:reads @ts/*stats)
            result
            (try
              ;; Each round restores what the previous round STORED, so the race
              ;; runs on top of the full store/restore/modify cycle rather than
              ;; repeatedly against one frozen address. That cycle used to
              ;; corrupt the set on its own (test.diff-buf-restore-cycle,
              ;; defects A and B); with those fixed it belongs here, because a
              ;; fill racing a settle on a tree that has been through the cycle
              ;; is the shape a real consumer actually produces.
              (loop [round 0, prev-addr addr, expected expected]
                (if (== round rounds)
                  {:ok true}
                  (let [cold (ss/restore-by full-cmp prev-addr st
                                            (assoc opts :storage st))
                        rnd  (java.util.Random. (+ 100 round))
                        ;; readers: full traversals, forcing child() restore +
                        ;; cache-fill CAS, diff-buf projection, and count, which
                        ;; drives bufEntries()' LAZY resolve
                        rs (mapv (fn [_]
                                   (future
                                     {:seq (vec (seq cold))
                                      :count (count cold)}))
                                 (range readers))
                        ;; writer: derive from the SAME cold set and store it,
                        ;; settling the very branches the readers are filling
                        w  (future (let [s' (upsert-cycle cold round rnd)]
                                     {:addr (ss/store s' st) :set s'}))
                        rvs (mapv deref rs)
                        wv @w]
                    (doseq [rv rvs]
                      (when (not= expected (:seq rv))
                        (throw (ex-info "a reader saw a different set"
                                        {:round round
                                         :expected-count (count expected)
                                         :actual-count (count (:seq rv))})))
                      (when (not= (count expected) (:count rv))
                        (throw (ex-info "a reader's count disagreed with its seq"
                                        {:round round :count (:count rv)
                                         :seq-count (count (:seq rv))}))))
                    (recur (inc round) (:addr wv) (vec (seq (:set wv)))))))
              (catch java.util.concurrent.ExecutionException e
                {:ok false :error (.getCause e)})
              (catch Throwable e
                {:ok false :error e}))]

        (is (:ok result)
            (str "concurrent restore-fill vs settle failed: " (:error result)
                 " " (some-> ^Throwable (:error result) ex-data pr-str)))
        (is (> (:reads @ts/*stats) reads-before)
            "precondition: nodes were actually RESTORED — the gap this namespace
             exists to close is that the existing concurrent test uses a storage
             whose restore throws, so a version of this test that never read
             would reproduce exactly that blind spot")))))

(deftest the-restored-set-still-carries-its-diff-buf-settings
  (testing "guards the precondition of the test above from the other side: a
            restore that silently dropped `:diff-buf-size` would leave the
            projection and LAZY-resolve paths dead while everything still
            passed. This is D-F1's shape, pinned here for the concurrent
            harness specifically."
    (let [disk (atom {})
          st   (ts/->Storage (atom {}) disk (Settings. (int bf) nil nil nil (int dbs)))  ; explicit budget -- see above
          opts {:comparator full-cmp :branching-factor bf
                :diff-buf-size dbs :ref-type :strong}
          s0   (into (ss/sorted-set* (assoc opts :storage st))
                     (map (fn [k] [k 0 0]) (range 2000)))
          _    (ss/store s0 st)
          s1   (upsert-cycle s0 0 (java.util.Random. 3))
          addr (ss/store s1 st)
          cold (ss/restore-by full-cmp addr st (assoc opts :storage st))]
      (is (= dbs (.diffBufSize (settings-of cold)))
          "the restored set buffers at the size it was built with")
      (is (= (vec (seq s1)) (vec (seq cold)))
          "and reads back exactly what was stored"))))
