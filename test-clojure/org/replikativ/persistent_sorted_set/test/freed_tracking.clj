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

;; ---- checkpointed live transient: the EARLY_EXIT arms ----------------------
;;
;; Every scenario above calls `.persistent` BEFORE storing, so the store always sees
;; a settled tree. That is the whole reason this file's oracle — which is the right
;; oracle — reported 0 leaked while three sites leaked: it never reached them.
;;
;; The EARLY_EXIT arms of Branch.{add,remove,replace} run only when a child mutates
;; IN PLACE and returns no new node (an editable leaf with room, above min, or a
;; same-shape replace). They then clear the child's stale address, because a durable
;; address no longer describes the mutated child. Three of them cleared it WITHOUT
;; markFreed — so the old blob became unreachable and was never reported freed. It is
;; reachable from the public API by exactly this shape: store a live transient, mutate
;; it further, store again — datahike's checkpointing import.
;;
;; Measured before the fix, bf 8 / dbs 0 / 40 checkpoint rounds:
;;
;;     disk blobs 242 · reachable 82 · freed-reported 99 · ORPHANS 61
;;     orphan levels {2 -> 37, 1 -> 24} · content ok true
;;
;; Content was never wrong. This is unbounded storage growth for any consumer that
;; treats the freed stream as its GC candidate list, which datahike does.
;;
;; Only at dbs 0, and the fix is gated to match: under diff-buf the old address is
;; re-pointed as the buffered anchor at store, so freeing it would free a LIVE node.
;; The dbs-256 case below pins that gate — it must stay green without the free.

(defn run-checkpointed-transient
  "One transient, stored MID-EDIT each round and mutated further — never persistent!
   until the end. `mutate!` edits the transient in place."
  [dbs mutate!]
  (let [{:keys [stored freed storage mk-fresh]} (counting-storage dbs)
        s0 (reduce (fn [s k] (pset/conj s [k 0] cmp-full))
                   (pset/sorted-set* {:comparator cmp-full :storage storage
                                      :branching-factor 32 :diff-buf-size dbs})
                   (range 10000))
        _  (pset/store s0 storage)
        t  (.asTransient ^PersistentSortedSet s0)
        _  (dotimes [round 20]
             (mutate! t round)
             (pset/store t storage))
        root-addr (pset/store t storage)
        s-final   (.persistent ^PersistentSortedSet t)]
    {:s-final s-final :stored @stored :freed @freed
     :reachable (reachable root-addr mk-fresh dbs)}))

(deftest checkpointed-transient-conj-frees-complete
  (let [r (run-checkpointed-transient
           0 (fn [t round]
               ;; [k 1] lands strictly between [k 0] and [k+1 0]: an INTERIOR insert
               ;; into a leaf with room, which is what returns EARLY_EXIT.
               (dotimes [i 32] (pset/conj t [(+ (* round 32) i) 1] cmp-full))))]
    (assert-accounting r "checkpointed-transient-conj")
    (assert-content r
                    (vec (concat (mapcat (fn [k] [[k 0] [k 1]]) (range 640))
                                 (for [k (range 640 10000)] [k 0])))
                    "checkpointed-transient-conj")))

(deftest checkpointed-transient-disj-frees-complete
  (let [r (run-checkpointed-transient
           0 (fn [t round]
               (dotimes [i 32] (pset/disj t [(+ (* round 32) i) 0] cmp-full))))]
    (assert-accounting r "checkpointed-transient-disj")
    (assert-content r (vec (for [k (range 640 10000)] [k 0]))
                    "checkpointed-transient-disj")))

(deftest checkpointed-transient-replace-frees-complete
  (let [r (run-checkpointed-transient
           0 (fn [^PersistentSortedSet t round]
               (dotimes [i 32]
                 (let [k (+ (* round 32) i)]
                   (.replace t [k 0] [k (inc round)] ^java.util.Comparator cmp-k)))))]
    (assert-accounting r "checkpointed-transient-replace")
    (assert-content r
                    (vec (for [k (range 10000)]
                           (if (< k 640) [k (inc (quot k 32))] [k 0])))
                    "checkpointed-transient-replace")))

(deftest checkpointed-transient-diff-buf-frees-complete
  ;; The gate. Under diff-buf the EARLY_EXIT address must NOT be freed — store
  ;; re-points it as the buffered anchor, so freeing it would free a live node.
  ;; This asserts the same identity holds with the free suppressed.
  (let [r (run-checkpointed-transient
           256 (fn [t round]
                 (dotimes [i 32] (pset/conj t [(+ (* round 32) i) 1] cmp-full))))]
    (assert-accounting r "checkpointed-transient-diff-buf")
    (assert-content r
                    (vec (concat (mapcat (fn [k] [[k 0] [k 1]]) (range 640))
                                 (for [k (range 640 10000)] [k 0])))
                    "checkpointed-transient-diff-buf")))
