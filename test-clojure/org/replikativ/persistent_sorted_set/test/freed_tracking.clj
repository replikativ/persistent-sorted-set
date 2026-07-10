(ns org.replikativ.persistent-sorted-set.test.freed-tracking
  "Freed-address (markFreed) completeness oracle.

   Invariant (diffBufSize == 0): whenever a parent replaces a durable child pointer
   (old _addresses[i] != null) with a different/new child — persistent copy-unwind AND
   editable in-place, across add/replace/remove at EVERY level — the old address is
   markFreed exactly once. The old-root frees in PersistentSortedSet.{cons,disjoin,replace}
   handle the top; Branch.{add,remove,replace} handle the per-level child supersession.

   Primary oracle — the address-accounting identity, stronger than a % coverage number:

       set(stored) == set(freed) ⊎ reachable(final durable tree)

   It catches both LEAKS (stored but neither freed nor reachable) and PREMATURE FREES
   (freed AND reachable — would corrupt consumers running online GC). Plus exactly-once
   (no double frees) and content correctness against a model. Reachability is computed
   by walking a FRESH RESTORE of the final root (the durable form — same oracle as
   stress_diff_buf's GC check and gc_leak.cljs): under diff-buf a live post-store walk
   under-counts anchors referenced through nested-buffered blobs' stored addresses.

   Measured coverage BEFORE the per-level hooks (this test failing), at diffBufSize=0,
   20 flush cycles of 32-op batches over a 10k-element bf-32 tree (writes vs frees,
   post-initial-store; leaked = stored − freed − reachable):

       persistent-replace           writes   100  frees    20  coverage   20%  (leaked 80)
       transient-replace            writes   100  frees    40  coverage   40%  (leaked 60)
       transient-conj-growth        writes   123  frees    20  coverage   16%  (leaked 60)
       transient-disj               writes    81  frees    40  coverage   49%  (leaked 84)

   Root cause: only the old ROOT (PersistentSortedSet) and a few scattered editable /
   structural-drop sites fired; the superseded INTERIOR SPINE addresses — every level's
   old durable child pointer replaced on the root→leaf unwind — were never freed.

   diffBufSize > 0 keeps its deferral semantics (content-only supersessions freed at
   store() when flushed anchors are written; structural drops freed immediately via
   freeDroppedChild). The dbs-256 scenario asserts the same identity — it holds with
   the deferral design unchanged."
  (:require [clojure.set :as cset]
            [clojure.test :refer [deftest is]]
            [org.replikativ.persistent-sorted-set :as pset]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set IStorage PersistentSortedSet Settings]))

;; Elements are [k v] pairs. The SET's comparator orders by k then v; the replace
;; comparator keys on k only, so (replace [k _] [k v']) is a value-changing upsert
;; (datahike's path).
(def cmp-full
  (fn [[k1 v1] [k2 v2]]
    (let [c (compare k1 k2)]
      (if (zero? c) (compare v1 v2) c))))

(def cmp-k
  (fn [[k1 _] [k2 _]] (compare k1 k2)))

(defn counting-storage
  "Serializing storage (test.storage's pr-str/edn Storage — nodes are reconstructed
   fresh on restore, like a real backend) that records every address ever returned by
   store (stored, a vector) and every markFreed call (freed, a vector — duplicates
   visible). mk-fresh builds a storage with an empty node cache over the same disk,
   for cold restores (the reachability walk)."
  [dbs]
  (let [stored   (atom [])
        freed    (atom [])
        disk     (atom {})
        settings (Settings. (int 32) nil nil nil (int dbs))
        wrap     (fn [^IStorage inner]
                   (reify IStorage
                     (store [_ node]
                       (let [a (.store inner node)]
                         (swap! stored conj a)
                         a))
                     (accessed [_ a] (.accessed inner a))
                     (restore [_ a] (.restore inner a))
                     (markFreed [_ a] (swap! freed conj a))
                     (isFreed [_ _] false)
                     (freedInfo [_ _] nil)))
        mk-fresh #(wrap (tstore/->Storage (atom {}) disk settings))]
    {:stored stored :freed freed :disk disk
     :storage (mk-fresh) :mk-fresh mk-fresh}))

(defn reachable
  "All addresses reachable from the final DURABLE tree: cold-restore the root address
   (fresh node cache) and walk it, root included. The restore-side walk visits each
   blob's stored child addresses (with diff-buf slots re-pointing at anchors), i.e.
   exactly what a GC consumer must retain."
  [root-addr mk-fresh dbs]
  (let [s   (pset/restore-by cmp-full root-addr (mk-fresh)
                             {:branching-factor 32 :diff-buf-size dbs})
        acc (atom #{})]
    (pset/walk-addresses s (fn [a] (swap! acc conj a) true))
    @acc))

(defn run-scenario
  "10k-element bf-32 set with diff-buf-size dbs, stored; then 20 rounds of
   (mutate-batch set round) + store. Returns the accounting data."
  [dbs mutate-batch]
  (let [{:keys [stored freed storage mk-fresh]} (counting-storage dbs)
        s0 (reduce (fn [s k] (pset/conj s [k 0] cmp-full))
                   (pset/sorted-set* {:comparator cmp-full :storage storage
                                      :branching-factor 32 :diff-buf-size dbs})
                   (range 10000))
        _  (pset/store s0 storage)
        s-final (reduce (fn [s round]
                          (let [s' (mutate-batch s round)]
                            (pset/store s' storage)
                            s'))
                        s0 (range 20))
        root-addr (pset/store s-final storage)]
    {:s-final s-final :stored @stored :freed @freed
     :reachable (reachable root-addr mk-fresh dbs)}))

(defn assert-safety
  "Safety properties that must hold at ANY diff-buf size:
   exactly-once (no double frees) and no freed address still reachable."
  [{:keys [freed reachable]} label]
  (is (= (count freed) (count (set freed)))
      (str label ": no double frees (" (- (count freed) (count (set freed))) " duplicate frees)"))
  (is (empty? (cset/intersection (set freed) reachable))
      (str label ": no freed address is reachable from the final tree (premature frees: "
           (count (cset/intersection (set freed) reachable)) ")")))

(defn assert-accounting
  "The accounting identity: every stored address is either freed or reachable, and
   the two partitions are disjoint. Implies 100% coverage."
  [{:keys [stored freed reachable] :as r} label]
  (assert-safety r label)
  (let [leaked (cset/difference (set stored) (set freed) reachable)]
    (is (= (set stored) (into (set freed) reachable))
        (str label ": accounting identity stored = freed ⊎ reachable; leaked "
             (count leaked) " of " (count (set stored)) " stored"))))

(defn assert-content [{:keys [s-final]} model label]
  (is (= model (vec s-final)) (str label ": final contents match model")))

;; ---- dbs = 0 scenarios ----

(deftest persistent-replace-frees-complete
  (let [r (run-scenario 0
            (fn [s round]
              (reduce (fn [s i]
                        (let [k (+ (* round 32) i)]
                          (.replace ^PersistentSortedSet s [k 0] [k (inc round)] ^java.util.Comparator cmp-k)))
                      s (range 32))))]
    (assert-accounting r "persistent-replace")
    (assert-content r
                    (vec (for [k (range 10000)]
                           (if (< k 640) [k (inc (quot k 32))] [k 0])))
                    "persistent-replace")))

(deftest transient-replace-frees-complete
  (let [r (run-scenario 0
            (fn [s round]
              (let [t (reduce (fn [t i]
                                (let [k (+ (* round 32) i)]
                                  (.replace ^PersistentSortedSet t [k 0] [k (inc round)] ^java.util.Comparator cmp-k)))
                              (.asTransient ^PersistentSortedSet s)
                              (range 32))]
                (.persistent ^PersistentSortedSet t))))]
    (assert-accounting r "transient-replace")
    (assert-content r
                    (vec (for [k (range 10000)]
                           (if (< k 640) [k (inc (quot k 32))] [k 0])))
                    "transient-replace")))

(deftest transient-conj-growth-frees-complete
  (let [r (run-scenario 0
            (fn [s round]
              (let [t (reduce (fn [t i]
                                (pset/conj t [(+ 100000 (* round 32) i) 0] cmp-full))
                              (.asTransient ^PersistentSortedSet s)
                              (range 32))]
                (.persistent ^PersistentSortedSet t))))]
    (assert-accounting r "transient-conj-growth")
    (assert-content r
                    (vec (concat (for [k (range 10000)] [k 0])
                                 (for [j (range 640)] [(+ 100000 j) 0])))
                    "transient-conj-growth")))

(deftest transient-disj-frees-complete
  (let [r (run-scenario 0
            (fn [s round]
              (let [t (reduce (fn [t i]
                                (pset/disj t [(+ (* round 32) i) 0] cmp-full))
                              (.asTransient ^PersistentSortedSet s)
                              (range 32))]
                (.persistent ^PersistentSortedSet t))))]
    (assert-accounting r "transient-disj")
    (assert-content r
                    (vec (for [k (range 640 10000)] [k 0]))
                    "transient-disj")))

;; ---- dbs = 256 scenario (mixed ops) ----
;;
;; Deferral semantics unchanged: content-only supersessions are freed at store()
;; (flushed anchors), structural drops immediately (freeDroppedChild). The deferral
;; design achieves the full identity — a buffered anchor stays REACHABLE (re-pointed
;; _addresses[i] / nested-buffered blob addresses) until flushed, at which point
;; store() frees it. Measured leaked = 0.

(deftest diff-buf-256-mixed-frees
  (let [r (run-scenario 256
            (fn [s round]
              (let [t (.asTransient ^PersistentSortedSet s)
                    t (reduce (fn [t i]
                                (let [k (+ (* round 32) i)]
                                  (.replace ^PersistentSortedSet t [k 0] [k (inc round)] ^java.util.Comparator cmp-k)))
                              t (range 10))
                    t (reduce (fn [t i]
                                (pset/conj t [(+ 100000 (* round 32) i) 0] cmp-full))
                              t (range 11))
                    t (reduce (fn [t i]
                                (pset/disj t [(+ 5000 (* round 32) i) 0] cmp-full))
                              t (range 11))]
                (.persistent ^PersistentSortedSet t))))]
    ;; safety: unconditional at any diff-buf size
    (assert-safety r "diff-buf-256-mixed")
    ;; coverage: full accounting identity, achieved by the existing deferral
    (assert-accounting r "diff-buf-256-mixed")
    (assert-content r
                    (vec (concat
                          (for [k (range 10000)
                                :when (not (and (<= 5000 k) (< k 5640)
                                                (< (mod (- k 5000) 32) 11)))]
                            (if (and (< k 640) (< (mod k 32) 10))
                              [k (inc (quot k 32))]
                              [k 0]))
                          (for [round (range 20) i (range 11)]
                            [(+ 100000 (* round 32) i) 0])))
                    "diff-buf-256-mixed")))
