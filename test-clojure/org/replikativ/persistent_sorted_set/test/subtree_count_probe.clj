(ns org.replikativ.persistent-sorted-set.test.subtree-count-probe
  "One `disj` must not pull in the whole tree.

   `Branch.remove` asks `tryComputeSubtreeCountFromChildren` for the successor's count.
   That probe is documented as IN-MEMORY — \"Returns -1 if any child is not available in
   memory or has unknown count\" — and the ClojureScript twin implements it literally.
   The JVM enforced only the residency half: for a child with an unknown count it called
   `computeSubtreeCount`, which descends through `child(storage, i)` and RESTORES
   everything below it.

   The probe bails at the first non-resident child, scanning left to right, so it only
   ran to completion when a resident prefix reached the end of the array — which `remove`
   arranges by materialising `idx-1`, `idx` and `idx+1`. At root fanout 2 those ARE all
   the children, so a single delete on a cold tree walked the entire index. That is why
   the symptom correlates with root fanout rather than with size:

     n=80000, a storage that does not persist counts, one disj
       bf  16   root fanout   2    11426 of 11426 blobs restored   (100%)
       bf  64   root fanout   2     2580 of  2580 blobs restored   (100%)
       bf  32   root fanout  19      547 for a LEFT-edge delete, 10 for a middle one
       bf 512   root fanout 312        3-4

   It is a one-time warmup rather than an ongoing cost — the walk caches counts into the
   children it visits, so across 20 successive deletes only the first paid (11425, then
   0 x19). What it costs is a latency spike on the first write after a restore and, more
   seriously, a resident set of the WHOLE tree — precisely the bound `:ref-type` exists
   to enforce. Correctness was never affected.

   Persisting `:subtree-count` does not make it moot: `Branch.remove` writes -1 into its
   successors, so unknown-count branches exist in a count-persisting store too. Measured
   with counts persisted, left-edge delete, before -> after this fix: bf 16 70 -> 11,
   bf 32 67 -> 7, bf 128 131 -> 5.

   The bound below is deliberately generous (a quarter of the tree). The defect restores
   100% where the fix restores well under 2%, so anything in between still fails loudly
   without making the test brittle about the exact descent cost."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [org.replikativ.persistent-sorted-set :as ss])
  (:import [org.replikativ.persistent_sorted_set Settings IStorage PersistentSortedSet
            Branch ANode Leaf]))

(set! *warn-on-reflection* true)

(def ^:private *restores (atom 0))

;; A storage that does NOT persist :subtree-count — the case the probe is about, and the
;; shape of any store written before counts existed. Restores are counted, and there is no
;; memory cache, so every descent past a resident node shows up.
(defrecord CountingStorage [*disk ^Settings settings]
  IStorage
  (store [_ node]
    (let [^ANode node node
          addr (str (java.util.UUID/randomUUID))
          m {:level     (.level node)
             :keys      (.keys node)
             :addresses (when (instance? Branch node) (.addresses ^Branch node))}]
      (swap! *disk assoc addr (pr-str m))
      addr))
  (accessed [_ _] nil)
  (restore [_ address]
    (swap! *restores inc)
    (let [{:keys [level ^java.util.List keys ^java.util.List addresses]}
          (edn/read-string (@*disk address))]
      (if addresses
        (Branch. (int level) keys addresses settings)
        (Leaf. keys settings)))))

(defn- storage [disk bf]
  (->CountingStorage disk (Settings. (int bf) nil nil nil (int 0))))

;; [branching-factor n] — each MEASURED to give a root of fanout 2, the shape that made the
;; resident prefix reach the end of the array. Kept small so the test stays quick.
(def ^:private cases [[8 2500] [8 10000] [16 1200] [16 10000] [32 10000]])

(deftest one-disj-does-not-materialise-the-whole-tree
  (testing "a delete on a cold tree touches its path, not the index"
    (doseq [[bf n] cases]
      (let [disk  (atom {})
            opts  {:comparator compare :branching-factor bf}
            s0    (reduce #(ss/conj %1 %2 compare) (ss/sorted-set* opts) (range n))
            addr  (ss/store s0 (storage disk bf))
            blobs (count @disk)
            cold  (ss/restore-by compare addr (storage disk bf) opts)
            fan   (.len ^Branch (.root ^PersistentSortedSet cold))
            _     (reset! *restores 0)
            after (ss/disj cold 1 compare)
            reads @*restores
            lbl   (str "bf=" bf " n=" n " blobs=" blobs " root-fanout=" fan)]
        (is (= 2 fan)
            (str lbl ": PRECONDITION — this case exists to exercise a fanout-2 root. If the
                 shape has changed the test is no longer testing anything, so fail loudly
                 rather than pass vacuously."))
        (is (< reads (quot blobs 4))
            (str lbl ": one disj restored " reads " blobs. It must touch its path, not the
                 whole tree."))
        ;; the count must still be right — the fix defers the computation, it does not skip it
        (is (= (dec n) (count after)) (str lbl ": count after the delete"))
        (is (= 1 (- n (count (seq after)))) (str lbl ": exactly one element left"))))))

(deftest the-deferred-count-is-still-exact
  (testing "returning -1 from the in-memory probe defers the work to `count`, which
            computes and caches on demand — it must not lose or corrupt the value"
    (let [bf 16 n 10000
          disk (atom {})
          opts {:comparator compare :branching-factor bf}
          s0   (reduce #(ss/conj %1 %2 compare) (ss/sorted-set* opts) (range n))
          addr (ss/store s0 (storage disk bf))
          cold (ss/restore-by compare addr (storage disk bf) opts)
          ks   [1 2 3 500 5000 9999]
          after (reduce #(ss/disj %1 %2 compare) cold ks)]
      (is (= (- n (count ks)) (count after)) "count after a batch of deletes")
      (is (= (- n (count ks)) (count (seq after))) "and it agrees with the seq")
      (is (= (remove (set ks) (range n)) (seq after)) "contents exact"))))
