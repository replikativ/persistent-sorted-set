(ns org.replikativ.persistent-sorted-set.test.stored-measures
  "Every STORED branch records the measure of each of its children.

   ## What that buys

   A branch's own measure is the fold of that record, so a write below one child needs only
   that child's new measure -- and the child that comes back up from a write always has it.
   Nothing is asked of the measure but `extract` and `merge`: no subtraction, so a min/max or
   an order-sensitive measure is kept exactly like a sum. And a reader can pick a child by
   position, or fold the children a range covers, without restoring them.

   ## What it replaces

   Each write arm used to recompute the branch's measure from whichever children were in
   memory and store nil when one was not. On a tree that is mostly on disk that is the normal
   case. Measured on a tree of 100 000 elements, bf 64, after 200 scattered `replace`: 100 of
   101 stored branches without a measure, and the next cold `measure` read 3173 blobs, 3072
   of them leaves. `get-nth` restored every child to the left of its target at every level.

   ## What is asserted

   - the ORACLE: after every kind of write on a cold tree, stored and restored, each branch
     holds a record, each entry equals a fold of that child's own elements, and each cached
     measure equals a fold of the node's elements. Both diff-buf modes, persistent and
     transient, count and content-defined boundaries, and two measures that cannot be
     maintained by arithmetic (min/max without `remove`, and an order-sensitive one);
   - the BILL: what a cold write, `get-nth`, `measure` and `measure-slice` read;
   - OLDER TREES: blobs without the record answer correctly, cost more (which is also the
     negative control for the bill), recover as they are written, and `remeasure` repairs
     them in one pass."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.boundary :as boundary]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
  (:import [java.util Random]
           [java.util.function Supplier]
           [org.replikativ.persistent_sorted_set
            IMeasure IStorage ANode Branch Leaf PersistentSortedSet]))

(set! *warn-on-reflection* true)

;; Elements are [k payload], ordered by k alone, so `replace` can change what is measured.
(defn- cmp [a b] (compare (nth a 0) (nth b 0)))

;; count / sum / min / max of the payload, in longs so that a disagreement is a defect and
;; never rounding. `remove` cannot subtract: it only knows how to ask for a recomputation,
;; and answers nil (unknown) when nobody offers one.
(deftype StatOps []
  IMeasure
  (identity [_] {:n 0 :sum 0 :min Long/MAX_VALUE :max Long/MIN_VALUE})
  (extract  [_ e] (let [p (long (nth e 1))] {:n 1 :sum p :min p :max p}))
  (merge    [_ a b] {:n   (+ (long (:n a)) (long (:n b)))
                     :sum (+ (long (:sum a)) (long (:sum b)))
                     :min (min (long (:min a)) (long (:min b)))
                     :max (max (long (:max a)) (long (:max b)))})
  (remove   [_ _cur _k recompute] (when recompute (.get ^Supplier recompute)))
  (weight   [_ m] (long (:n m))))

;; The elements themselves, in order: `merge` is associative but not commutative, so any fold
;; that visits the children out of order, or folds a stale entry, shows.
(deftype ConcatOps []
  IMeasure
  (identity [_] [])
  (extract  [_ e] [e])
  (merge    [_ a b] (into a b))
  (remove   [_ _cur _k recompute] (when recompute (.get ^Supplier recompute)))
  (commutativeMerge [_] false)
  (weight   [_ m] (long (count m))))

(def ^:private stat-ops (StatOps.))
(def ^:private concat-ops (ConcatOps.))

(defn- truth [^IMeasure ops elements]
  (reduce (fn [acc e] (.merge ops acc (.extract ops e))) (.identity ops) elements))

(defrecord MStorage [*disk *reads *leaf-reads ctx]
  IStorage
  (store [_ node]
    (let [addr (str (java.util.UUID/randomUUID))]
      (swap! *disk assoc addr (nodes/node->blob node))
      addr))
  (accessed [_ _] nil)
  (restore [_ addr]
    (swap! *reads inc)
    (let [blob (get @*disk addr)]
      (if (:addresses blob)
        (nodes/blob->branch ctx blob)
        (do (swap! *leaf-reads inc) (nodes/blob->leaf ctx blob)))))
  (markFreed [_ _] nil)
  (isFreed [_ _] false)
  (freedInfo [_ _] nil))

(defn- storage [ops bf]
  (->MStorage (atom {}) (atom 0) (atom 0) (nodes/reader-context {:measure-ops ops :default-bf bf})))

(defn- opts [ops bf dbs extra]
  ;; :diff-buf-size is always given: the suite runs with a non-zero default, and 0 here has
  ;; to mean a tree without buffered edits.
  (merge {:comparator cmp :branching-factor bf :measure ops :ref-type :weak :diff-buf-size dbs}
         extra))

(defn- cold
  "The set at `addr`, with nothing in memory."
  ^PersistentSortedSet [o ^MStorage st addr]
  (set/restore-by cmp addr st (assoc o :storage st)))

(defn- reads [^MStorage st] @(:*reads st))

(defn- elems [n] (map (fn [k] [(* 2 (long k)) (mod (* (long k) 7919) 1000)]) (range n)))

(defn- built
  "`n` elements stored on a fresh storage. Returns {:st :o :addr}."
  [ops bf dbs extra n]
  (let [o  (opts ops bf dbs extra)
        st (storage ops bf)
        s  (reduce conj (set/sorted-set* (assoc o :storage st)) (elems n))]
    {:st st :o o :addr (set/store s st)}))

(defn- audit
  "Walk the whole tree. For each node: its cached measure, where it has one, against a fold
   of its own elements; for each branch, its record against a fold of each child.
   {:branches n :no-record n :wrong [..]}. Materialises the tree: read counters first."
  [^PersistentSortedSet s ^IMeasure ops]
  (let [storage (.-_storage s)
        acc     (volatile! {:branches 0 :no-record 0 :wrong []})]
    (letfn [(walk [^ANode node]
              (let [es (if (instance? Leaf node)
                         (vec (take (.-_len node) (.-_keys node)))
                         (let [^Branch b node
                               cm (.-_childMeasures b)
                               parts (mapv #(walk (.child b ^IStorage storage (int %)))
                                           (range (.-_len b)))]
                           (vswap! acc update :branches inc)
                           (if (nil? cm)
                             (vswap! acc update :no-record inc)
                             (do
                               (when (not= (alength cm) (.-_len b))
                                 (vswap! acc update :wrong conj [:length (.level b) (alength cm) (.-_len b)]))
                               (dotimes [i (min (alength cm) (.-_len b))]
                                 (when (not= (aget cm i) (truth ops (nth parts i)))
                                   (vswap! acc update :wrong conj [:entry (.level b) i])))))
                           (into [] cat parts)))]
                (when (and (some? (.-_measure node)) (not= (.-_measure node) (truth ops es)))
                  (vswap! acc update :wrong conj [:measure (.level node)]))
                es))]
      (walk (.root s)))
    @acc))

(defn- height [^PersistentSortedSet s] (inc (.level ^ANode (.root s))))

;; ---------------------------------------------------------------------------
;; The oracle
;; ---------------------------------------------------------------------------

(def ^:private batch
  "Each kind of write, as (fn [set ks]) over keys that exist (even) or do not (odd)."
  {:conj    (fn [s ks] (reduce (fn [s k] (conj s [(inc (long k)) 7])) s ks))
   :disj    (fn [s ks] (reduce (fn [s k] (disj s [k nil])) s ks))
   :replace (fn [s ks] (reduce (fn [s k] (set/replace s [k nil] [k 1234] cmp)) s ks))
   :mixed   (fn [s ks] (reduce (fn [s k]
                                 (case (int (mod (quot (long k) 2) 3))
                                   0 (conj s [(inc (long k)) 7])
                                   1 (disj s [k nil])
                                   2 (set/replace s [k nil] [k 1234] cmp)))
                               s ks))})

(def ^:private batch!
  {:conj    (fn [s ks] (reduce (fn [s k] (conj! s [(inc (long k)) 7])) s ks))
   :disj    (fn [s ks] (reduce (fn [s k] (disj! s [k nil])) s ks))
   :replace (fn [s ks] (reduce (fn [s k] (set/replace s [k nil] [k 1234] cmp)) s ks))
   :mixed   (fn [s ks] (reduce (fn [s k]
                                 (case (int (mod (quot (long k) 2) 3))
                                   0 (conj! s [(inc (long k)) 7])
                                   1 (disj! s [k nil])
                                   2 (set/replace s [k nil] [k 1234] cmp)))
                               s ks))})

(defn- scattered [n cnt]
  (let [step (max 1 (quot (long n) (long cnt)))]
    (map #(* 2 (* step (long %))) (range 1 cnt))))

(deftest every-kind-of-cold-write-keeps-the-record
  (doseq [[ops-name ops] [[:stat stat-ops] [:concat concat-ops]]
          bf   [4 8 64]
          dbs  [0 16]
          transient? [false true]
          kind [:conj :disj :replace :mixed]
          ;; an order-sensitive measure runs unbuffered: the pairing is refused (below)
          :when (not (and (= ops-name :concat) (pos? (long dbs))))
          :let [n   (if (= bf 64) 3000 400)
                lbl (str ops-name " bf=" bf " diff-buf=" dbs (when transient? " transient") " " kind)]]
    (let [{:keys [st o addr]} (built ops bf dbs nil n)
          s0    (cold o st addr)
          ks    (scattered n (if (= bf 64) 120 60))
          s1    (if transient?
                  (persistent! ((batch! kind) (transient s0) ks))
                  ((batch kind) s0 ks))
          live  (audit s1 ops)
          addr1 (set/store s1 st)
          c     (cold o st addr1)
          a     (audit c ops)]
      (is (empty? (:wrong live)) (str lbl ": in memory, first " (first (:wrong live))))
      (is (empty? (:wrong a)) (str lbl ": stored, first " (first (:wrong a))))
      (is (zero? (long (:no-record a)))
          (str lbl ": " (:no-record a) " of " (:branches a) " stored branches have no record"))
      (is (= (truth ops (seq c)) (set/measure (cold o st addr1))) (str lbl ": measure"))
      (is (= {:branches (:branches a) :unknown 0} (set/measure-census (cold o st addr1)))
          (str lbl ": census")))))

(deftest a-content-defined-tree-keeps-the-record
  (doseq [bf [8 16], kind [:conj :disj]
          :let [lbl (str "mst bf=" bf " " kind)]]
    (let [extra {:boundary (boundary/mst-boundary 3)}
          {:keys [st o addr]} (built stat-ops bf 0 extra 600)
          s1    ((batch kind) (cold o st addr) (scattered 600 80))
          addr1 (set/store s1 st)
          a     (audit (cold o st addr1) stat-ops)]
      (is (empty? (:wrong a)) (str lbl ": first " (first (:wrong a))))
      (is (zero? (long (:no-record a))) (str lbl ": " (:no-record a) " of " (:branches a)))
      (is (= (truth stat-ops (seq s1)) (set/measure (cold o st addr1))) lbl))))

(deftest a-long-sequence-of-writes-and-restarts
  (doseq [[ops-name ops] [[:stat stat-ops] [:concat concat-ops]]
          bf [4 8], dbs [0 16], seed [1 2 3]
          :when (not (and (= ops-name :concat) (pos? (long dbs))))
          :let [lbl (str ops-name " bf=" bf " diff-buf=" dbs " seed=" seed)]]
    (let [{:keys [st o addr]} (built ops bf dbs nil 150)
          rnd (Random. (long seed))]
      (loop [step 0, s (cold o st addr), model (into (sorted-map) (elems 150))]
        (if (= step 400)
          (is (= (seq (map (fn [[k p]] [k p]) model)) (seq s)) (str lbl ": contents at the end"))
          (let [k  (long (.nextInt rnd 400))
                op (.nextInt rnd 3)
                [s model] (case op
                            0 [(conj s [k (get model k step)]) (if (contains? model k) model (assoc model k step))]
                            1 [(disj s [k nil]) (dissoc model k)]
                            2 (if (contains? model k)
                                [(set/replace s [k nil] [k (- step)] cmp) (assoc model k (- step))]
                                [s model]))]
            (if (zero? (mod (inc step) 25))
              (let [a1 (set/store s st)
                    c  (cold o st a1)
                    a  (audit c ops)]
                (is (empty? (:wrong a)) (str lbl " step " step ": first " (first (:wrong a))))
                (is (zero? (long (:no-record a))) (str lbl " step " step ": branches without a record"))
                (is (= (truth ops (map (fn [[k p]] [k p]) model)) (set/measure (cold o st a1)))
                    (str lbl " step " step ": measure"))
                (recur (inc step) (cold o st a1) model))
              (recur (inc step) s model))))))))

;; ---------------------------------------------------------------------------
;; The bill
;; ---------------------------------------------------------------------------

(defn- reading
  "[result reads] of (f) against `st`."
  [^MStorage st f]
  (let [r0 (reads st), v (f)] [v (- (long (reads st)) (long r0))]))

(deftest what-a-cold-reader-pays
  (let [n 20000, bf 16
        {:keys [st o addr]} (built stat-ops bf 0 nil n)
        h    (height (cold o st addr))
        all  (vec (elems n))]
    (is (>= h 4) "precondition: a tree deep enough for a per-level bill to mean something")
    (testing "`measure` is the root's own"
      (let [c (cold o st addr)
            [m r] (reading st #(set/measure c))]
        (is (= (truth stat-ops all) m))
        (is (<= (long r) 1) (str r " reads"))))
    (testing "`get-nth` reads one node per level, wherever the position is"
      (doseq [i [0 1 777 (quot n 3) (quot n 2) (- n 2) (dec n)]]
        (let [c (cold o st addr)
              [v r] (reading st #(set/get-nth c i))]
          (is (= [(nth all i) 0] v) (str "position " i))
          (is (<= (long r) (long h)) (str "position " i ": " r " reads, height " h)))))
    (testing "`measure-slice` reads the two boundary paths"
      (let [c (cold o st addr)
            from (nth all 1234), to (nth all 15678)
            [m r] (reading st #(set/measure-slice c from to))]
        (is (= (truth stat-ops (subvec all 1234 15679)) m))
        (is (<= (long r) (* 2 (long h))) (str r " reads, height " h))))))

;; The WRITE bounds below hold on the old code too: it kept writes cheap by storing the
;; measure as unknown. What tells the two apart is the oracle above (no stored branch
;; without a record) and the reader bounds (`get-nth`, `measure-slice`), with the older-tree
;; test as the control that those can fail.
(deftest what-a-cold-writer-pays
  (let [n 20000, bf 16
        {:keys [st o addr]} (built stat-ops bf 0 nil n)
        h (height (cold o st addr))]
    (testing "a replace reads its path"
      (let [c (cold o st addr)
            [_ r] (reading st #(set/replace c [8642 nil] [8642 5] cmp))]
        (is (<= (long r) (long h)) (str r " reads, height " h))))
    (testing "an insert reads its path"
      (let [c (cold o st addr)
            [_ r] (reading st #(conj c [8643 5]))]
        (is (<= (long r) (long h)) (str r " reads, height " h))))
    (testing "a delete reads its path and the two neighbours it weighs at each level"
      (let [c (cold o st addr)
            [_ r] (reading st #(disj c [8642 nil]))]
        (is (<= (long r) (* 3 (long h))) (str r " reads, height " h))))
    (testing "lookup-then-replace, the way a positional column is updated: each round reads
              two paths, and the tree is never pulled in to rebuild a measure"
      (let [c (cold o st addr)
            rounds 100
            [s r] (reading st (fn []
                                (reduce (fn [s i]
                                          (let [[e _] (set/get-nth s (* 197 (long i)))]
                                            (set/replace s e [(nth e 0) -1] cmp)))
                                        c (range rounds))))]
        (is (<= (long r) (* 2 rounds (long h))) (str r " reads over " rounds " rounds, height " h))
        (is (empty? (:wrong (audit s stat-ops))))))))

;; ---------------------------------------------------------------------------
;; Older trees
;; ---------------------------------------------------------------------------

(defn- age!
  "Rewrite every stored branch the way an earlier version wrote it: no record of the
   children's measures, and with `erode?` no measure of its own either."
  [^MStorage st erode?]
  (swap! (:*disk st) update-vals
         (fn [blob]
           (if (:addresses blob)
             (cond-> (dissoc blob :child-measures) erode? (dissoc :measure))
             blob))))

(deftest an-older-tree-answers-and-recovers
  (doseq [erode? [false true]
          :let [lbl (if erode? "no records, no branch measures" "no records")]]
    (let [n 20000, bf 16
          {:keys [st o addr]} (built stat-ops bf 0 nil n)
          _   (age! st erode?)
          all (vec (elems n))
          h   (height (cold o st addr))
          census (set/measure-census (cold o st addr))]
      (testing lbl
        (is (pos? (long (:branches census))))
        (is (= (:branches census) (:unknown census)) "every branch is reported")
        (testing "answers"
          (is (= (truth stat-ops all) (set/measure (cold o st addr))))
          (doseq [i [0 777 (quot n 2) (dec n)]]
            (is (= [(nth all i) 0] (set/get-nth (cold o st addr) i)) (str "position " i)))
          (is (= (truth stat-ops (subvec all 1234 15679))
                 (set/measure-slice (cold o st addr) (nth all 1234) (nth all 15678)))))
        (testing "and costs more than a tree with records -- which is also what shows the
                  budgets above can fail"
          (let [c (cold o st addr)
                [_ r] (reading st #(set/get-nth c (dec n)))]
            (is (> (long r) (long h)) (str r " reads, height " h))))
        (testing "a read does not make a branch look repaired"
          (let [c (cold o st addr)]
            (set/get-nth c (quot n 2))
            (is (= census (set/measure-census c)))))
        (testing "a write through it stores the record for the branches it rewrites"
          (let [s1 (set/replace (cold o st addr) [8642 nil] [8642 5] cmp)
                a1 (set/store s1 st)
                c1 (set/measure-census (cold o st a1))
                a  (audit (cold o st a1) stat-ops)]
            (is (= (- (long (:unknown census)) (long h) -1) (:unknown c1))
                "the path's branches, and only those")
            (is (empty? (:wrong a)))))
        (testing "`remeasure` repairs it in one pass"
          (let [s1 (set/remeasure (cold o st addr))
                _  (is (= {:branches (:branches census) :unknown 0} (set/measure-census s1))
                       "before it is stored: what the store will write")
                a1 (set/store s1 st)
                c  (cold o st a1)
                a  (audit c stat-ops)]
            (is (= (seq all) (seq c)) "contents unchanged")
            (is (empty? (:wrong a)))
            (is (zero? (long (:no-record a))))
            (is (= {:branches (:branches census) :unknown 0} (set/measure-census (cold o st a1))))
            (is (identical? c (set/remeasure c)) "nothing left to do")
            (let [c2 (cold o st a1)
                  [_ r] (reading st #(set/get-nth c2 (dec n)))]
              (is (<= (long r) (long h)) (str "get-nth afterwards: " r " reads, height " h)))))))))

(deftest an-order-sensitive-measure-is-not-buffered
  ;; A buffered leaf is rebuilt in the order of the comparator that reads it, so an
  ;; aggregate that depends on element order can be stale where two sets share nodes under
  ;; comparators that order ties differently. See `Settings.diffBufForMeasure`.
  (testing "asking for both is refused"
    (is (thrown-with-msg? IllegalArgumentException #"not commutative"
                          (set/sorted-set* {:comparator cmp :measure concat-ops :diff-buf-size 16}))))
  (testing "a budget that is only inherited (the suite runs with one) is left off"
    (let [s (set/sorted-set* {:comparator cmp :measure concat-ops})]
      (is (zero? (.diffBufSize (.-_settings ^PersistentSortedSet s))))))
  (testing "a commutative measure buffers as before"
    (let [s (set/sorted-set* {:comparator cmp :measure stat-ops :diff-buf-size 16})]
      (is (= 16 (.diffBufSize (.-_settings ^PersistentSortedSet s)))))))

(deftest remeasure-declines-what-it-cannot-do
  (let [{:keys [st o addr]} (built stat-ops 8 0 nil 300)]
    (is (thrown? IllegalStateException (set/remeasure (transient (cold o st addr))))))
  (testing "a set without a measure has nothing to repair"
    (let [s (into (set/sorted-set) (range 500))]
      (is (identical? s (set/remeasure s)))
      (is (= {:branches 0 :unknown 0} (set/measure-census s))))))

(deftest a-record-of-the-wrong-length-is-not-trusted
  (let [{:keys [st o addr]} (built stat-ops 8 0 nil 600)
        all (vec (elems 600))]
    (swap! (:*disk st) update-vals
           (fn [blob] (if (:child-measures blob) (update blob :child-measures pop) blob)))
    (let [c (cold o st addr)]
      (is (= (:branches (set/measure-census c)) (:unknown (set/measure-census c))))
      (is (= (truth stat-ops all) (set/measure (cold o st addr))))
      (is (= [(nth all 599) 0] (set/get-nth (cold o st addr) 599)))
      (let [s1 (set/replace (cold o st addr) [100 nil] [100 5] cmp)]
        (is (empty? (:wrong (audit s1 stat-ops))))
        (is (some? (.-_childMeasures ^Branch (.root ^PersistentSortedSet s1)))
            "and the write replaced it with a record of its own, rather than leaving none")))))

(deftest a-buffered-write-does-not-make-an-older-branch-look-repaired
  ;; With a diff buffer a write need not rewrite the branch it passes: the parent can keep
  ;; the edit in a slot against the child's OLD blob. The child in memory then holds a
  ;; record its blob does not, and a census that believed the memory would report it
  ;; repaired -- and `remeasure` would skip it for good.
  (let [n 3000, bf 8
        {:keys [st o addr]} (built stat-ops bf 64 nil n)
        _      (age! st false)
        census (set/measure-census (cold o st addr))
        s1     (set/replace (cold o st addr) [2468 nil] [2468 5] cmp)
        a1     (set/store s1 st)
        stored (set/measure-census (cold o st a1))]
    (is (pos? (long (:unknown stored))) "precondition: the write left older branches behind")
    (is (= stored (set/measure-census s1)) "the set that was written says what a restore finds")
    (let [a2 (set/store (set/remeasure s1) st)
          c  (cold o st a2)]
      (is (= {:branches (:branches census) :unknown 0} (set/measure-census (cold o st a2))))
      (is (empty? (:wrong (audit c stat-ops))))
      (is (zero? (long (:no-record (audit (cold o st a2) stat-ops))))))))
