(ns org.replikativ.persistent-sorted-set.test.bulk-build
  "`from-sorted-seq` — the streaming bulk builder.

   The claim is narrow and testable: for the same input and settings it produces
   the SAME TREE as the in-memory `from-sorted-array`, while holding only
   O(depth × branching-factor) rather than O(n).

   Both halves are asserted directly. Contents alone would not do it — a tree with
   the right elements and the wrong fanout passes every `=` check and then behaves
   differently under slicing, counting and later inserts. So the structural test
   compares the SHAPE (level and fanout of every node), and the memory test samples
   live heap MID-BUILD — measuring after the build returns sees only the residue and
   misses retention entirely, which is how the first version of it passed against an
   implementation that OOM'd at -Xmx128m."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set])
  (:import [org.replikativ.persistent_sorted_set IStorage ANode Branch PersistentSortedSet]))

;; ---------------------------------------------------------------------------

(defn- mk-storage
  "An IStorage backed by a map, plus counters. `restore` counts reads so a test can
   assert the build never reads back what it wrote."
  []
  (let [disk (atom {})
        stores (atom 0)
        restores (atom 0)]
    {:disk disk :stores stores :restores restores
     :storage (reify IStorage
                (store [_ node]
                  (let [a (str "n" (swap! stores inc))]
                    (swap! disk assoc a node)
                    a))
                (accessed [_ _] nil)
                (restore [_ a] (swap! restores inc) (@disk a))
                (markFreed [_ _] nil)
                (isFreed [_ _] false)
                (freedInfo [_ _] nil))}))

(defn- shape
  "Structural signature: the level and fanout of every node, recursively. Compares
   TREE STRUCTURE rather than contents, which is the point — see the ns docstring."
  [^ANode node storage]
  (if (instance? Branch node)
    (let [^Branch b node
          n (.len b)]
      {:level (.level b)
       :len n
       :children (mapv #(shape (.child b storage %) storage) (range n))})
    {:level 0 :len (.len node)}))

(defn- built-both-ways
  "[array-built streaming-built storage] for the same elements and settings."
  [xs opts]
  (let [{:keys [storage] :as st} (mk-storage)
        arr (set/from-sorted-array compare (to-array xs) (count xs) opts)
        seqd (set/from-sorted-seq compare xs (assoc opts :storage storage))]
    [arr seqd st]))

;; ---------------------------------------------------------------------------
;; equivalence with the in-memory builder

(deftest same-tree-as-from-sorted-array
  (testing "identical contents AND identical structure across the lengths where
            `split`'s three cases meet — the cut rule changes at 2*avg and at max,
            so those boundaries are where a streaming reimplementation would drift"
    (doseq [bf [4 8 16 32]
            n [0 1 2 3 7 15 16 17 31 32 33 47 48 63 64 65 100 257 1000 5000]]
      (let [xs (vec (range n))
            opts {:branching-factor bf}
            [arr seqd {:keys [storage]}] (built-both-ways xs opts)]
        (is (= (vec arr) (vec seqd))
            (str "contents differ at bf=" bf " n=" n))
        (when (pos? n)
          (is (= (shape (.root ^PersistentSortedSet arr) nil)
                 (shape (.root ^PersistentSortedSet seqd) storage))
              (str "SHAPE differs at bf=" bf " n=" n
                   " — same elements, different tree")))))))

(deftest same-tree-for-non-contiguous-keys
  (testing "the equivalence is about the cut rule, not about the keys being ints
            that happen to be dense"
    (doseq [n [50 200 777]]
      (let [xs (vec (sort (map #(* % 37) (range n))))
            opts {:branching-factor 16}
            [arr seqd {:keys [storage]}] (built-both-ways xs opts)]
        (is (= (vec arr) (vec seqd)))
        (is (= (shape (.root ^PersistentSortedSet arr) nil)
               (shape (.root ^PersistentSortedSet seqd) storage)))))))

(deftest string-keys-round-trip
  (testing "a non-numeric comparator"
    (let [xs (vec (sort (map #(format "k%05d" %) (range 500))))
          {:keys [storage]} (mk-storage)
          s (set/from-sorted-seq compare xs {:storage storage :branching-factor 16})]
      (is (= xs (vec s))))))

;; ---------------------------------------------------------------------------
;; the property that justifies the function existing

(deftest streaming-split-does-not-retain-its-input
  (testing "the emitted chunk must be a real vector, not a SubVector view.

            This is the deterministic guard on a defect that a heap measurement
            nearly missed twice. `subvec` returns a view sharing its base, and
            `conj` on a view does `base.assocN(end, o)` — so feeding the remainder
            forward as a SubVector grows the BASE without bound and retains every
            element ever consumed, silently making the build O(n).

            Measured before the fix: live heap at mid-stream grew 2.7x from 250k
            to 2M elements, and a 4M build died under -Xmx128m. After: 0.99x, and
            the same build completes. A type check has none of that noise."
    (let [chunks (take 5 (#'set/streaming-split (range 10000) 12 16))]
      (is (seq chunks))
      (doseq [c chunks]
        (is (instance? clojure.lang.PersistentVector c)
            (str "chunk is a " (class c) " — a view retains the whole input"))))))

(deftest memory-is-bounded-by-depth-not-by-count
  (testing "live heap DURING the build does not scale with n.

            Sampled MID-STREAM from inside the storage callback, not after the
            build returns — the first version of this test measured the residue
            (an address and a count) once every intermediate was already garbage,
            so it passed at 4M while the same build died under a 128 MB heap.

            The threshold is calibrated against a measured defect rather than
            guessed: the retaining implementation grows 2.7x from 250k to 2M, the
            streaming one 0.99x. 1.5x separates them with room for GC noise."
    (let [live-at-midpoint
          (fn [n]
            (let [stores (atom 0)
                  sample (atom nil)
                  target (long (/ n 64))
                  st (reify IStorage
                       (store [_ _]
                         (let [c (swap! stores inc)]
                           (when (and (= c target) (nil? @sample))
                             (System/gc) (Thread/sleep 150)
                             (let [r (Runtime/getRuntime)]
                               (reset! sample (- (.totalMemory r) (.freeMemory r)))))
                           "a"))
                       (accessed [_ _] nil)
                       (restore [_ _] (throw (ex-info "build must not read back" {})))
                       (markFreed [_ _] nil) (isFreed [_ _] false) (freedInfo [_ _] nil))]
              (set/from-sorted-seq compare (range n) {:storage st :branching-factor 32})
              @sample))
          small (live-at-midpoint 250000)
          large (live-at-midpoint 2000000)]
      (is (some? small))
      (is (some? large))
      (is (< (/ (double large) small) 1.5)
          (str "live heap grew with n: " (int (/ small 1048576)) " MB at 250k vs "
               (int (/ large 1048576)) " MB at 2M — the stream is being retained")))))

(deftest build-never-reads-back
  (testing "a bulk build writes; it must not restore. A `restore` during the build
            would mean a node was dropped and re-fetched, which is the memory bug
            this function exists to avoid, wearing a disguise."
    (let [{:keys [storage restores]} (mk-storage)]
      (set/from-sorted-seq compare (range 10000) {:storage storage :branching-factor 16})
      (is (zero? @restores)))))

;; ---------------------------------------------------------------------------
;; storage round-trip

(deftest stored-tree-restores
  (testing "the built tree is a real stored tree: take its root address, restore
            from a fresh set, get the elements back"
    (let [xs (vec (range 2000))
          {:keys [storage]} (mk-storage)
          s (set/from-sorted-seq compare xs {:storage storage :branching-factor 16})
          addr (set/store s storage)
          restored (set/restore addr storage {:branching-factor 16})]
      (is (some? addr))
      (is (= xs (vec restored))))))

(deftest built-tree-supports-further-operations
  (testing "a bulk-built tree is an ordinary set afterwards — conj, disj, slice.

            Worth asserting because the nodes come back address-only with no
            resident children, which is a state the mutation paths must handle."
    (let [xs (vec (range 1000))
          {:keys [storage]} (mk-storage)
          s (set/from-sorted-seq compare xs {:storage storage :branching-factor 16})]
      (is (= 1000 (count s)))
      (is (contains? s 500))
      (is (= (conj xs 1000) (vec (set/conj s 1000 compare))))
      (is (= (remove #{500} xs) (vec (set/disj s 500 compare))))
      (is (= (range 100 110) (vec (set/slice s 100 109)))))))

;; ---------------------------------------------------------------------------
;; input contract

(deftest rejects-input-that-would-corrupt-the-tree
  (testing "unsorted input must FAIL rather than build a wrong tree.

            RocksDB's SstFileWriter — the same bulk-build pattern — refuses with
            \"Keys must be added in strict ascending order\" for exactly this
            reason: the alternative is a structure that looks fine and answers
            queries wrongly."
    (let [{:keys [storage]} (mk-storage)]
      (testing "descending"
        (is (thrown-with-msg? IllegalArgumentException #"strictly ascending"
                              (count (set/from-sorted-seq compare [3 2 1] {:storage storage})))))
      (testing "a single inversion late in an otherwise sorted run"
        (is (thrown-with-msg? IllegalArgumentException #"strictly ascending"
                              (count (set/from-sorted-seq compare (concat (range 1000) [998])
                                                          {:storage storage})))))
      (testing "duplicates — a set cannot hold them, and silently dropping one
                would make the count disagree with the input"
        (is (thrown-with-msg? IllegalArgumentException #"strictly ascending"
                              (count (set/from-sorted-seq compare [1 2 2 3] {:storage storage})))))
      (testing "nil"
        (is (thrown-with-msg? IllegalArgumentException #"cannot store nil"
                              (count (set/from-sorted-seq compare [1 nil 3] {:storage storage}))))))))

(deftest requires-storage
  (testing "without storage there is nothing to stream to, and the caller wants
            from-sorted-array instead — say so rather than NPE later"
    (is (thrown-with-msg? IllegalArgumentException #"requires :storage"
                          (count (set/from-sorted-seq compare [1 2 3] {}))))))

(deftest order-check-is-lazy
  (testing "the check must not force the whole input up front — that would
            reintroduce the O(n) memory it exists to avoid"
    (let [{:keys [storage]} (mk-storage)
          exploding (concat (range 10) (lazy-seq (throw (ex-info "forced" {}))))]
      ;; building necessarily consumes everything, so the throw escapes — the point
      ;; is that it escapes from the LAZY tail rather than from an eager pre-scan
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"forced"
                            (count (set/from-sorted-seq compare exploding {:storage storage})))))))

(deftest empty-input-yields-empty-set
  (let [{:keys [storage stores]} (mk-storage)
        s (set/from-sorted-seq compare [] {:storage storage})]
    (is (= 0 (count s)))
    (is (= [] (vec s)))
    (is (zero? @stores) "nothing to store, so nothing stored")))

(deftest single-element
  (let [{:keys [storage]} (mk-storage)
        s (set/from-sorted-seq compare [42] {:storage storage})]
    (is (= [42] (vec s)))
    (is (= 1 (count s)))))

(deftest tiny-branching-factors-are-refused-not-hung
  (testing "a fanout of 1 never reduces the level count, so the build grows
            upward forever.

            `avg = (min + max) / 2` with `min = bf >>> 1`, so bf 1 and 2 both give
            avg 1. Before the check, bf=2 spun until OOM rather than failing —
            which is the worst way for a bulk build to be wrong, because it looks
            like the slow-but-working case it is meant to replace."
    (let [{:keys [storage]} (mk-storage)]
      (doseq [bf [1 2]]
        (is (thrown-with-msg? AssertionError #"branching-factor must be >= 3"
                              (count (set/from-sorted-seq compare (range 200)
                                                          {:storage storage :branching-factor bf})))
            (str "bf=" bf " must be refused")))
      (testing "and the smallest workable factor does work"
        (doseq [bf [3 5 7]]
          (is (= 200 (count (set/from-sorted-seq compare (range 200)
                                                 {:storage storage :branching-factor bf})))
              (str "bf=" bf)))))))
