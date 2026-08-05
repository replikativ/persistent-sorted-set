(ns org.replikativ.persistent-sorted-set.test.diff
  "`diff` — what changed between two versions of a set that share structure.

   The claim is not just \"the right elements\": `clojure.set/difference` gets that
   too. The claim is that the COST is proportional to the change rather than to
   the set, because two versions share every node they have in common and a
   subtree present in both cannot contain a difference.

   That is what the read-counting tests below assert, and it is the only reason
   the function exists — an incremental consumer (replication, an audit trail,
   catching a migration target up) needs delta-shaped cost or it may as well
   enumerate."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.set :as set]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.test.storage :as ts]))

;; `ts/storage` SERIALIZES: `store` writes an EDN projection and `restore`
;; rebuilds the node. That is load-bearing for every read count below. A
;; storage that keeps node OBJECTS and hands the same instance back on restore
;; leaves the tree fully resident, so descending it costs nothing and the
;; read-count tests pass no matter what the walk does — which is exactly how an
;; earlier version of this file certified a diff that read all 392 nodes of a
;; 100 000-element set to report a two-element delta.
(defn- mk-storage []
  (let [disk (atom {})]
    {:disk disk :storage (ts/storage disk)}))

(defn- cold
  "A second handle on the same disk with an EMPTY node cache, so a restore
   really goes to storage. Measure against this, not the writing handle."
  [{:keys [disk]}]
  (ts/storage disk))

(defn- reads [] (:reads @ts/*stats))
(defn- reset-reads! [] (swap! ts/*stats assoc :reads 0))

(defn- build [storage xs]
  (let [st (reduce #(s/conj %1 %2 compare) (s/sorted-set-by compare) xs)]
    (s/store st storage)
    st))

(defn- naive
  "What the answer must be, computed the expensive way."
  [a b]
  {:added (vec (sort (set/difference (set (seq b)) (set (seq a)))))
   :removed (vec (sort (set/difference (set (seq a)) (set (seq b)))))})

;; ---------------------------------------------------------------------------

(deftest diff-agrees-with-set-difference
  (testing "against the obvious implementation, over shapes that exercise leaf
            splits, merges and multi-level rebalancing"
    (doseq [n [0 1 2 511 512 513 5000]]
      (let [{:keys [storage]} (mk-storage)
            a (build storage (range n))
            b (-> (reduce #(s/conj %1 %2 compare) a (range n (+ n 7)))
                  (as-> x (reduce #(s/disj %1 %2 compare) x (take 3 (range n))))
                  (doto (s/store storage)))]
        (is (= (naive a b) (s/diff a b storage))
            (str "n=" n))))))

(deftest diff-of-a-set-with-itself-is-empty
  (let [{:keys [storage]} (mk-storage)
        a (build storage (range 1000))]
    (is (= {:added [] :removed []} (s/diff a a storage)))))

(deftest diff-handles-empty-on-either-side
  (let [{:keys [storage]} (mk-storage)
        e (build storage [])
        a (build storage (range 100))]
    (is (= (range 100) (:added (s/diff e a storage))))
    (is (= (range 100) (:removed (s/diff a e storage))))
    (is (= {:added [] :removed []} (s/diff e e storage)))))

(defn- delta-scenario
  "Two stored sets `delta` elements apart, handed back on a COLD storage
   handle, plus how many nodes the whole thing occupies on disk."
  [n delta]
  (let [{:keys [disk] :as st} (mk-storage)
        w  (:storage st)
        a0 (reduce #(s/conj %1 %2 compare) (s/sorted-set-by compare) (range n))
        ra (s/store a0 w)
        b0 (reduce #(s/conj %1 %2 compare) a0 (range (inc n) (+ n 1 delta)))
        rb (s/store b0 w)
        c  (cold st)]
    {:a (s/restore-by compare ra c) :b (s/restore-by compare rb c)
     :storage c :on-disk (count @disk)}))

(deftest diff-reads-only-the-nodes-that-changed
  (testing "THE point of the function. A two-element delta must not read the tree.

            Counted against a SERIALIZING storage with a cold node cache: a
            storage that returns resident node objects makes every descent free
            and this assertion vacuous."
    (doseq [n [1000 10000 100000]]
      (let [{:keys [a b storage on-disk]} (delta-scenario n 2)]
        (reset-reads!)
        (let [d (s/diff a b storage)
              r (reads)]
          (is (= [(+ n 1) (+ n 2)] (:added d)) (str "n=" n))
          (is (= [] (:removed d)))
          ;; the changed leaf and the spine above it, on both sides — a small
          ;; multiple of the DEPTH, and nothing to do with the node count
          (is (<= r 8)
              (str "n=" n ": read " r " nodes of " on-disk
                   " for a 2-element delta")))))))

(deftest diff-cost-does-not-grow-with-set-size
  (testing "the same delta against sets two orders of magnitude apart reads the
            same number of nodes, while the trees themselves differ 80-fold."
    (let [run   (fn [n] (let [{:keys [a b storage on-disk]} (delta-scenario n 1)]
                          (reset-reads!)
                          (s/diff a b storage)
                          [(reads) on-disk]))
          [small small-nodes] (run 1000)
          [big big-nodes]     (run 100000)]
      (is (< (* 8 small-nodes) big-nodes)
          "the two trees must actually differ in size for this to prove anything")
      (is (<= big (+ small 2))
          (str "1k set read " small " nodes, 100k set read " big
               " — cost is tracking set size, not delta")))))

(deftest diff-of-identical-roots-reads-nothing
  (testing "the same stored root on both sides is answered without touching
            storage at all"
    (let [st (mk-storage)
          a  (build (:storage st) (range 10000))
          ra (s/store a (:storage st))
          c  (cold st)]
      (reset-reads!)
      (is (= {:added [] :removed []}
             (s/diff (s/restore-by compare ra c) (s/restore-by compare ra c) c)))
      (is (zero? (reads))))))

(deftest diff-of-unrelated-sets-is-correct-if-not-cheap
  (testing "no shared structure means nothing prunes. The answer must still be
            right — a caller who diffs unrelated sets gets a slow correct result,
            not a wrong fast one."
    (let [{:keys [storage]} (mk-storage)
          a (build storage (range 0 200 2))
          b (build storage (range 1 201 2))]
      (is (= (naive a b) (s/diff a b storage))))))
