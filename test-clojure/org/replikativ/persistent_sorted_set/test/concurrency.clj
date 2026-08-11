(ns org.replikativ.persistent-sorted-set.test.concurrency
  "Everything in this namespace needs real threads, so all of it is JVM-only.

   Four independent races, one section each:

     * DIFF under concurrent mutation.
     * RESTORE racing a settle -- the cache-fill CAS and the LAZY resolve.
     * SETTLE seen by a second thread mid-commit (`BlockingStorage` parks the writer).
     * ROOT PUBLICATION -- `_root` is the publication point for `_settings` too.

   TWO STORAGE CONVENTIONS COEXIST HERE ON PURPOSE, and merging them would be a silent
   behaviour change rather than a tidy-up:

     * the RESTORE section passes an EXPLICIT `Settings`, because its budget (`dbs`) is the
       thing under test and a bare `(Settings.)` takes the budget from the `pss.diffBufSize`
       property -- 256 under `:test` -- which silently gives set-128-over-nodes-256;
     * the DIFF and ROOT-PUBLICATION sections use the bare `ts/storage` arities and so run at
       whatever the alias sets. That is how they were written and it is preserved here
       UNCHANGED. It is not obviously deliberate, and it is worth deciding on its own merits
       rather than by merge accident -- but a reorganisation is the wrong place to change it.

   `full-cmp`, `by-k`, `n-init`, `bf` and `dbs` were defined identically in the diff and
   restore files (verified byte-for-byte) and appear once below."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set
            PersistentSortedSet Branch ANode Leaf Settings IStorage]
           [java.lang.reflect Modifier]
           [java.util.concurrent CountDownLatch TimeUnit]))

(set! *warn-on-reflection* true)

;; ===========================================================================
;; shared fixture: the comparators and sizes used by the diff and restore sections
;; ===========================================================================

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

;; ===========================================================================
;; 1. diff under concurrent mutation
;; ===========================================================================

;; [k v t] ordered by all three; the upsert comparator keys on k alone, so

(defn- upsert
  ^PersistentSortedSet [^PersistentSortedSet s ks v]
  (let [t (reduce (fn [^PersistentSortedSet t k] (.replace t [k 0 0] [k v v] by-k))
                  (.asTransient s) ks)]
    (.persistent ^PersistentSortedSet t)))

(defn- mk []
  (let [st   (ts/storage (atom {}) (atom {}))
        opts {:comparator full-cmp :branching-factor bf
              :diff-buf-size dbs :ref-type :strong}
        s0   (into (ss/sorted-set* (assoc opts :storage st))
                   (map (fn [k] [k 0 0]) (range n-init)))]
    (ss/store s0 st)
    {:st st :s0 s0}))

(defn- pairs
  "Per-child [address slot] for a branch, from ONE snapshot."
  [^Branch b]
  (let [p     (.addressesAndSlots b)
        addrs (aget ^objects p 0)
        slots (aget ^objects p 1)]
    (mapv (fn [i] [(when addrs (aget ^objects addrs i))
                   (when slots (aget ^objects slots i))])
          (range (.len b)))))

;; ---------------------------------------------------------------------------

(deftest the-address-slot-pair-comes-from-one-snapshot
  (testing "`addressesAndSlots` must return exactly what the two separate
            accessors return, or the pairing it exists to make coherent is
            coherent about the wrong thing"
    (let [{:keys [st s0]} (mk)
          s1 (upsert s0 [1 2] 1)
          ^Branch root (.root ^PersistentSortedSet s1)
          p (.addressesAndSlots root)]
      (is (identical? (.addressArray root) (aget ^objects p 0)) "same address array")
      (is (identical? (.slots root) (aget ^objects p 1)) "same slot array")
      (is (some? (aget ^objects p 1))
          "precondition: the root really carries slots, else this compares two nils")
      (ss/store s1 st))))

(deftest a-settle-never-repoints-a-child-that-already-had-an-address
  (testing "THE COUPLING that makes diff's pruning safe, and which nothing
            asserted before this test.

            store()'s settle skips every child whose pre-settle address is
            non-null — it is a clean passthrough, keeping both its address and
            its slot. That is what makes it harmless for `child-refs` to pair an
            address with a slot: a child that could be re-pointed has a NULL
            address going in, and `prune-shared` never prunes on a null address.

            If this ever fails, a settle has started re-pointing or re-slotting a
            child that already had an address, and diff's frontier can then pair
            a stale address with a cleared slot and silently drop that subtree's
            delta from the answer."
    (let [{:keys [st s0]} (mk)]
      (loop [gen 1, s s0]
        (when (<= gen 6)
          (let [;; a mix each generation: an old buffered child, a far child, and
                ;; a structural change, so passthrough children coexist with
                ;; settled ones rather than the tree being uniformly dirty
                s' (-> s
                       (upsert [1 2] gen)
                       (upsert (range (* gen 700) (+ (* gen 700) 40)) gen)
                       (as-> x (reduce (fn [acc k] (ss/conj acc [k 9 9] full-cmp))
                                       x (range (+ 20000 (* gen 1000))
                                                (+ 20400 (* gen 1000))))))
                ^Branch r (.root ^PersistentSortedSet s')
                before (pairs r)]
            (ss/store s' st)
            (let [after (pairs r)]
              (is (= (count before) (count after))
                  (str "gen " gen ": the settle changed the child count"))
              (doseq [i (range (min (count before) (count after)))]
                (let [[a-pre s-pre] (nth before i)
                      [a-post s-post] (nth after i)]
                  (when (some? a-pre)
                    (is (= a-pre a-post)
                        (str "gen " gen " child " i
                             ": a child with a pre-settle address was RE-POINTED"))
                    (is (identical? s-pre s-post)
                        (str "gen " gen " child " i
                             ": a child with a pre-settle address had its SLOT changed")))))
              (recur (inc gen) s'))))))))

(deftest diff-answers-correctly-while-a-settle-races-it
  (testing "SMOKE TEST — see the ns docstring. It does not reproduce the
            two-read defect (it passed against that version every time); a
            window of two adjacent volatile reads is not addressable from
            Clojure. It is here because nothing else runs diff and store
            concurrently over shared branch objects."
    (let [{:keys [st s0]} (mk)
          result
          (try
            (loop [round 0, prev s0]
              (if (== round 30)
                {:ok true}
                (let [next (upsert prev (range (* round 97) (+ (* round 97) 24)) (inc round))
                      ;; the model comes from the two immutable set VALUES, so it is
                      ;; exact however the threads interleaved
                      expected {:added   (vec (sort full-cmp (remove (set (seq prev)) (seq next))))
                                :removed (vec (sort full-cmp (remove (set (seq next)) (seq prev))))}
                      ds (mapv (fn [_] (future (ss/diff prev next st))) (range 4))
                      w  (future (ss/store next st))
                      answers (mapv deref ds)]
                  @w
                  (doseq [a answers]
                    (when (not= expected a)
                      (throw (ex-info "a differ lost or invented a delta"
                                      {:round round
                                       :expected-added (count (:added expected))
                                       :actual-added (count (:added a))}))))
                  (recur (inc round) next))))
            (catch java.util.concurrent.ExecutionException e {:ok false :error (.getCause e)})
            (catch Throwable e {:ok false :error e}))]
      (is (:ok result)
          (str "concurrent diff vs settle: " (:error result) " "
               (some-> ^Throwable (:error result) ex-data pr-str))))))

;; ===========================================================================
;; 2. restore racing a settle
;; ===========================================================================

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

;; ===========================================================================
;; 3. a settle observed mid-commit
;; ===========================================================================

(defn- settings ^Settings [bf] (Settings. (int bf) nil nil nil (int 0)))

;; A storage that parks the FIRST thread to store a branch, holding it inside `store()` until
;; released. That keeps thread A's settle window open while thread B enters the same node.
(defrecord BlockingStorage [*disk ^Settings settings ^CountDownLatch entered ^CountDownLatch release *blocked?]
  IStorage
  (store [_ node]
    (let [^ANode node node]
      (when (and (instance? Branch node) (compare-and-set! *blocked? false true))
        (.countDown entered)
        (.await release 5 TimeUnit/SECONDS))
      (let [addr (str (java.util.UUID/randomUUID))]
        (swap! *disk assoc addr {:level (.level node) :keys (vec (.keys node))})
        addr)))
  (accessed [_ _] nil)
  (restore [_ _] (throw (ex-info "restore not used in this test" {}))))

(defn- build [bf n]
  (reduce #(ss/conj %1 %2 compare)
          (ss/sorted-set* {:comparator compare :branching-factor bf}) (range n)))

(deftest two-threads-settling-the-same-node-are-detected
  (testing "the contract is per LINEAGE, not per tree — two versions can share dirty nodes,
            and nothing serialises store(). Detected under -ea rather than enforced."
    (let [bf 8
          s   (build bf 200)
          disk (atom {})
          entered (CountDownLatch. 1)
          release (CountDownLatch. 1)
          st  (->BlockingStorage disk (settings bf) entered release (atom false))
          err (atom nil)
          ;; both threads settle THE SAME set, hence the same node objects
          t1  (Thread. #(try (ss/store s st) (catch Throwable t (reset! err t))))
          t2  (Thread. #(try (ss/store s st) (catch Throwable t (reset! err t))))]
      (.start t1)
      ;; wait until t1 is parked INSIDE store() with its settle window open
      (is (.await entered 5 TimeUnit/SECONDS)
          "PRECONDITION: the storage must have parked a thread inside store(); without that
           the two windows need not overlap and this test would prove nothing")
      (.start t2)
      (.join t2 5000)
      (.countDown release)
      (.join t1 5000)
      (let [e @err]
        (is (instance? AssertionError e)
            (str "expected the concurrent settle to be detected, got: " (pr-str e)))
        (when (instance? AssertionError e)
          (is (re-find #"concurrent settle" (str (.getMessage ^AssertionError e)))
              "and the message must name the contract"))))))

(deftest settling-disjoint-trees-concurrently-is-allowed
  (testing "the detector must not fire for two threads storing DIFFERENT trees to one
            storage — that is legal and common. A detector that fired here would be worse
            than none, and a test that only checked the positive case would not notice."
    (let [bf 8
          a (build bf 200)
          b (build bf 200)              ; independent tree, no shared nodes
          disk (atom {})
          st (->BlockingStorage disk (settings bf) (CountDownLatch. 1) (CountDownLatch. 0)
                                (atom true))   ; pre-tripped: never block
          err (atom nil)
          t1 (Thread. #(try (ss/store a st) (catch Throwable t (reset! err t))))
          t2 (Thread. #(try (ss/store b st) (catch Throwable t (reset! err t))))]
      (.start t1) (.start t2)
      (.join t1 10000) (.join t2 10000)
      (is (nil? @err)
          (str "storing disjoint trees concurrently must not trip the detector, got: "
               (pr-str @err))))))

(deftest the-detector-is-reentrant-for-one-thread
  (testing "store() recurses into children on the same thread; that must not look like a
            race. Ordinary single-threaded storing is the overwhelmingly common path."
    (let [bf 8 disk (atom {})
          st (->BlockingStorage disk (settings bf) (CountDownLatch. 1) (CountDownLatch. 0)
                                (atom true))]
      (is (some? (ss/store (build bf 500) st)) "a plain store must succeed")
      (is (pos? (count @disk)) "and must actually have written nodes"))))

;; ===========================================================================
;; 4. root publication
;; ===========================================================================

(deftest the-publication-field-is-volatile
  (testing "`_root` carries the happens-before edge for the `_settings` adoptions above it,
            so its modifier is load-bearing rather than decorative"
    (let [f (.getDeclaredField PersistentSortedSet "_root")]
      (is (Modifier/isVolatile (.getModifiers f))
          "_root must be volatile: root() writes _settings then _root, and only a volatile
           write makes the former visible to a thread that acquires the latter"))))

(deftest a-restored-set-materializes-lazily
  (testing "the precondition every claim above rests on — if restore materialized eagerly
            there would be no publication to get wrong"
    (let [st   (ts/storage)
          addr (set/store (into (set/sorted-set) (range 2000)) st)
          ^PersistentSortedSet s (set/restore addr st)]
      (is (nil? (.-_root s)) "restore leaves _root unmaterialized")
      (is (some? (.-_address s)) "and holds an address instead")
      (is (some? (.root s)) "root() materializes it")
      (is (some? (.-_root s)) "and publishes it")
      (is (= (range 2000) (vec s)) "contents survive the round trip"))))

(deftest concurrent-entry-agrees-on-the-settings-and-the-contents
  (testing "many threads entering root() on the same unmaterialized set must agree on the
            settings adopted from it, and on what the tree contains.

            NOT asserted, because it is not true and not promised: that they get the same
            NODE OBJECT. Measured here, 8 threads over 25 rounds: 24 rounds produced 2-4
            DISTINCT root objects. `root()` is a racy cache — losers of the race restore
            their own copy, use it, and whichever finishes last wins `_root`. That is
            benign (the copies are equal, no thread mutates a shared root, and a caching
            IStorage such as datahike's CachedStorage returns one object and collapses the
            race entirely) and costs only duplicated IO bounded by the thread count. An
            earlier version of this test asserted object identity and failed against
            correct code; recording the real behaviour instead of asserting a guarantee the
            implementation never made."
    (dotimes [_ 25]
      (let [st    (ts/storage)
            addr  (set/store (into (set/sorted-set) (range 500)) st)
            ^PersistentSortedSet s (set/restore addr st)
            n     8
            go    (CountDownLatch. 1)
            done  (CountDownLatch. n)
            seen  (atom [])
            ts*   (doall
                   (for [_ (range n)]
                     (doto (Thread.
                            (fn []
                              (.await go)
                              (let [r  (.root s)
                                    bf (.branchingFactor (.-_settings s))]
                                ;; maxKey, not identity: the copies are equal but need not
                                ;; be the same object (see the docstring above).
                                (swap! seen conj [(.maxKey r) bf]))
                              (.countDown done)))
                       (.start))))]
        (.countDown go)
        (is (.await done 10 TimeUnit/SECONDS) "all readers returned")
        (doseq [t ts*] (.join t 1000))
        (is (= n (count @seen)) "every thread reported")
        (is (= [499] (distinct (map first @seen)))
            "every thread's root must describe the same tree")
        (is (= 1 (count (distinct (map second @seen))))
            "and every thread must agree on the branching factor adopted from it")
        (is (= (range 500) (vec s)) "and the set still reads correctly afterwards")))))
