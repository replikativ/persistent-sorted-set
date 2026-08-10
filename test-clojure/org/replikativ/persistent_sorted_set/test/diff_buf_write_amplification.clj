(ns org.replikativ.persistent-sorted-set.test.diff-buf-write-amplification
  "diff-buf's REASON TO EXIST, asserted: buffering a content-only child diff into its parent
   must write materially fewer durable objects than rewriting the child, for identical content.

   Nothing in the suite measured this. Every other diff-buf test asks whether the tree is
   still CORRECT under buffering — none asks whether buffering is happening at all, let alone
   whether it pays. A change that silently stopped producing slots would leave the whole
   diff-buf suite green while removing the entire feature. That is not hypothetical: an
   investigation probe in this area ran a full grid at budgets 1..256 and measured zero
   failures for a while before anyone noticed it had also produced ZERO buffered slots, so it
   had been grading an unbuffered tree the whole time.

   Counts, not wall-clock, on purpose. The quantity diff-buf exists to reduce is the number of
   stored objects per commit — which is what a PUT-priced object store bills for — and a count
   is exact, deterministic and free of JVM noise. A timing assertion here would be both weaker
   and flakier.

   MEASURED at HEAD, 40 generations x 2 conj over 50000 elements, bf 32:

       budget    objects written    bytes written    mean blob
            0                181           127386          703
            8                 84            67721          806
           32                 60            52762          879
          256                 60            52762          879

   Two things that table shows and this test pins:
     * ~3x fewer objects and ~2.4x fewer bytes. Blobs get ~25% BIGGER (the parent now carries
       its children's diffs) and there are a third as many, so the byte win is smaller than
       the object win. Both are wins; neither is 25x on this shape.
     * The budget is a CEILING, not a reservation: 256 is byte-identical to 32 here, because
       only what is actually buffered is embedded. Over-provisioning the budget costs nothing
       on a workload whose diffs fit well under it.

   ## What it costs, so the trade is on the record

   Wall-clock, `:bench` at -Dpss.diffBufSize=0 vs 256, five alternating rounds in fresh JVMs,
   medians (round 1 discarded -- other JVMs were running):

       disj-transient-10K   3.1 -> 4.5  (1.45x)   spreads disjoint, the clearest signal
       conj-10K             6.2 -> 7.9  (1.27x)
       disj-10K             6.1 -> 7.5  (1.23x)
       conj-transient-10K   5.5 -> 6.3  (1.15x)   spreads overlap
       contains / reduce / doseq / reduce-lazy    0.98-1.05, i.e. READS ARE UNAFFECTED

   That cost is paid IN MEMORY, with no storage in the picture: the mutation paths take the
   diff-buf branches whether or not anything is ever stored. So the trade is roughly 3x fewer
   stored objects for ~45% more CPU on transient disj and ~25% on persistent conj/disj --
   clearly right when writes are priced per object, and worth knowing when they are not.

   NOTE FOR WHOEVER TOUCHES THE BENCHMARK SUITE: none of those benchmarks can show the WIN.
   `store-50K` stores a FRESH tree, where every child is dirty and nothing is bufferable by
   construction; the benefit only exists across restore -> modify -> store cycles, which no
   benchmark performs. Measured through the suite alone, diff-buf looks like a pure
   regression. That is why the win is asserted here, as counts."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [clojure.edn :as edn])
  (:import [org.replikativ.persistent_sorted_set Settings IStorage ANode Branch Leaf Slot]))

(set! *warn-on-reflection* true)

(def ^:private cmp (fn [a b] (compare (first a) (first b))))

;; Persists AND restores :slots. Without the restore half a buffered blob comes back with no
;; diffs and the next write rewrites the child wholesale, which would make this test measure
;; the storage rather than the feature.
(defrecord CountingStorage [*disk *writes ^Settings settings]
  IStorage
  (store [_ node]
    (swap! *writes inc)
    (let [^ANode node node
          addr  (str (java.util.UUID/randomUUID))
          slots (when (instance? Branch node) (.slotsForStorage ^Branch node))
          m     {:level     (.level node)
                 :keys      (vec (.keys node))
                 :addresses (when (instance? Branch node) (vec (.addresses ^Branch node)))}]
      (swap! *disk assoc addr (pr-str (if slots (assoc m :slots slots) m)))
      addr))
  (accessed [_ _] nil)
  (restore [_ address]
    (let [{:keys [level ^java.util.List keys ^java.util.List addresses slots]}
          (edn/read-string (@*disk address))
          node (if addresses
                 (Branch. (int level) keys addresses settings)
                 (Leaf. keys settings))]
      (when (and slots (instance? Branch node))
        (let [^Branch b node
              arr (object-array (alength (.-_keys b)))]
          (doseq [[idx entry] slots]
            (aset arr (int idx)
                  (Slot. (:diff entry) (long (:count entry)) (:measure entry)
                         (nth addresses (int idx)))))
          (.installSlots b arr Branch/BUF_LAZY)))
      node))
  (markFreed [_ _] nil)
  (isFreed [_ _] false)
  (freedInfo [_ _] nil))

(defn- churn
  "n elements, then `gens` rounds of (cold restore, `ops` conj, store). Returns what the
   generations wrote, plus the final contents and how many slotted blobs exist."
  [bf dbs n gens ops seed]
  (let [o     {:comparator cmp :branching-factor bf :diff-buf-size dbs}
        disk  (atom {}) writes (atom 0)
        mk    #(->CountingStorage disk writes (Settings. (int bf) nil nil nil (int dbs)))
        rng   (java.util.Random. seed)
        s0    (reduce (fn [s i] (set/conj s [i 0] cmp)) (set/sorted-set* o) (range n))
        a0    (set/store s0 (mk))
        seen  (set (keys @disk))]
    (reset! writes 0)
    (let [final (loop [g 0, addr a0]
                  (if (= g gens)
                    addr
                    (let [cold (set/restore-by cmp addr (mk) o)
                          s    (reduce (fn [acc _] (set/conj acc [(+ n (.nextInt rng 1000000)) g] cmp))
                                       cold (range ops))]
                      (recur (inc g) (set/store s (mk))))))
          fresh (remove (fn [[a _]] (seen a)) @disk)]
      {:writes   @writes
       :bytes    (reduce + 0 (map (fn [[_ v]] (count v)) fresh))
       :slotted  (count (filter (fn [[_ v]] (:slots (edn/read-string v))) @disk))
       ;; A DIGEST, not the element vector. Comparing 50000-element vectors with `=` makes a
       ;; failure dump megabytes of elements into the test log -- measured at 1.3MB when the
       ;; red check tripped it -- which buries the one line saying what broke.
       :n        (count (seq (set/restore-by cmp final (mk) o)))
       :digest   (hash (vec (seq (set/restore-by cmp final (mk) o))))})))

(deftest buffering-writes-fewer-objects-than-rewriting
  (testing "same workload, same content, budget 0 vs 256"
    (doseq [[bf n gens ops] [[32 50000 40 2] [64 20000 20 25]]]
      (let [base (churn bf 0   n gens ops 42)
            buf  (churn bf 256 n gens ops 42)
            lbl  (str "bf=" bf " n=" n " " gens "x" ops)]
        ;; NOT VACUOUS, part 1: buffering must actually have happened. Without this the whole
        ;; test passes trivially the day slots stop being produced -- it would just be
        ;; comparing baseline against baseline.
        (is (pos? (:slotted buf))
            (str lbl ": precondition — the budget-256 run must produce buffered slots"))
        (is (zero? (:slotted base))
            (str lbl ": precondition — the budget-0 run must produce none"))
        ;; NOT VACUOUS, part 2: both runs must hold exactly the same set, or a "win" could be
        ;; nothing more than one of them having written less data.
        (is (= [(:n base) (:digest base)] [(:n buf) (:digest buf)])
            (str lbl ": both budgets must produce identical contents — "
                 "baseline " (:n base) " elements/hash " (:digest base)
                 " vs buffered " (:n buf) "/" (:digest buf)))
        ;; The property itself. Measured ~3x; asserted at 1.5x so ordinary tuning does not
        ;; make this a change detector, while a regression that disabled buffering (ratio 1.0)
        ;; still fails.
        (is (< (* 1.5 (:writes buf)) (:writes base))
            (str lbl ": buffering must write substantially fewer objects — "
                 (:writes buf) " vs " (:writes base)))
        (is (< (:bytes buf) (:bytes base))
            (str lbl ": and fewer bytes, despite each blob being larger — "
                 (:bytes buf) " vs " (:bytes base)))))))

(deftest the-budget-is-a-ceiling-not-a-reservation
  (testing "a budget larger than the working set of diffs costs nothing.

            Pins the reasoning behind picking a budget: raising it cannot make blobs bigger on
            a workload whose diffs already fit, because only what is actually buffered is
            embedded. Measured at bf 32 / 50000 / 40x2, budgets 32 and 256 are byte-identical."
    (let [a (churn 32 32  50000 40 2 42)
          b (churn 32 256 50000 40 2 42)]
      (is (pos? (:slotted a)) "precondition — budget 32 buffers")
      (is (= (:writes a) (:writes b))
          (str "objects: " (:writes a) " vs " (:writes b)))
      (is (= (:bytes a) (:bytes b))
          (str "bytes: " (:bytes a) " vs " (:bytes b))))))
