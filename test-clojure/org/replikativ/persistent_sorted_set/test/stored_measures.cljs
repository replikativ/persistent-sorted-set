(ns org.replikativ.persistent-sorted-set.test.stored-measures
  "The ClojureScript twin of the JVM `stored-measures`.

   Every STORED branch records the measure of each of its children. The branch's own measure
   is the fold of that record, so a write below one child needs only that child's new
   measure, and nothing is asked of the measure but `extract` and `merge`. A reader picks a
   child by position, or folds the children a range covers, without restoring them.

   Asserted here, as on the JVM:

   - the ORACLE: after every kind of write on a cold tree, stored and restored, each branch
     holds a record, each entry equals a fold of that child's own elements, and each cached
     measure equals a fold of the node's elements. Both diff-buf modes, a content-defined
     tree, and two measures that cannot be maintained by arithmetic;
   - the BILL: what a cold write, `get-nth`, `measure` and `measure-slice` read;
   - OLDER TREES: blobs without the record answer correctly, cost more (the negative
     control for the bill), recover as they are written, and `remeasure` repairs them.

   The storage here writes `nodes/node->blob`, the projection every real format writes, so
   what round-trips is what a consumer's codec carries."
  (:require [cljs.test :as test :refer-macros [deftest testing is async]]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async] :rename {async cps}]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.boundary :as boundary]
            [org.replikativ.persistent-sorted-set.branch :as branch :refer [Branch]]
            [org.replikativ.persistent-sorted-set.btset :as btset]
            [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]]
            [org.replikativ.persistent-sorted-set.impl.measure :as measure]
            [org.replikativ.persistent-sorted-set.impl.node :as node]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes]
            [org.replikativ.persistent-sorted-set.impl.storage :refer [IStorage]]))

;; Elements are [k payload], ordered by k alone, so `replace` can change what is measured.
(defn- cmp [a b] (compare (nth a 0) (nth b 0)))

;; count / sum / min / max of the payload. `remove-measure` cannot subtract: it only knows
;; how to ask for a recomputation.
(def ^:private stat-ops
  (reify measure/IMeasure
    (identity-measure [_] {:n 0 :sum 0 :min js/Number.MAX_SAFE_INTEGER :max js/Number.MIN_SAFE_INTEGER})
    (extract [_ e] (let [p (nth e 1)] {:n 1 :sum p :min p :max p}))
    (merge-measure [_ a b] {:n   (+ (:n a) (:n b))
                            :sum (+ (:sum a) (:sum b))
                            :min (min (:min a) (:min b))
                            :max (max (:max a) (:max b))})
    (remove-measure [_ _cur _k recompute-fn] (when recompute-fn (recompute-fn)))
    (weight [_ m] (:n m))))

;; The elements themselves, in order: associative, not commutative, so a fold that visits
;; the children out of order, or folds a stale entry, shows.
(def ^:private concat-ops
  (reify measure/IMeasure
    (identity-measure [_] [])
    (extract [_ e] [e])
    (merge-measure [_ a b] (into a b))
    (remove-measure [_ _cur _k recompute-fn] (when recompute-fn (recompute-fn)))
    (weight [_ m] (count m))))

(defn- truth [ops elements]
  (reduce (fn [acc e] (measure/merge-measure ops acc (measure/extract ops e)))
          (measure/identity-measure ops) elements))

(defn- restore-blob [ctx blob]
  (if (:addresses blob) (nodes/blob->branch ctx blob) (nodes/blob->leaf ctx blob)))

(defrecord MStorage [*disk *reads ctx]
  IStorage
  (store [_ node opts]
    (let [addr (random-uuid)]
      (swap! *disk assoc addr (nodes/node->blob node))
      (if (false? (:sync? opts)) (cps addr) addr)))
  (restore [_ addr opts]
    (swap! *reads inc)
    (let [n (restore-blob ctx (get @*disk addr))]
      (if (false? (:sync? opts)) (cps n) n)))
  (accessed [_ _] nil)
  (delete [_ _] nil)
  (markFreed [_ _] nil)
  (isFreed [_ _] false)
  (freedInfo [_ _] nil))

(defn- storage [ops bf]
  (->MStorage (atom {}) (atom 0) (nodes/reader-context {:measure-ops ops :default-bf bf})))

(defn- opts [ops bf dbs extra]
  (merge {:comparator cmp :branching-factor bf :measure ops :diff-buf-size dbs} extra))

(defn- cold [o st addr] (s/restore addr st (assoc o :storage st)))

(defn- reads [st] @(:*reads st))

(defn- elems [n] (map (fn [k] [(* 2 k) (mod (* k 7919) 1000)]) (range n)))

(defn- built [ops bf dbs extra n]
  (let [o  (opts ops bf dbs extra)
        st (storage ops bf)
        s0 (reduce s/conj (s/sorted-set* (assoc o :storage st)) (elems n))]
    {:st st :o o :addr (s/store s0 st)}))

(defn- root [set] (btset/-root set {:sync? true}))

(defn- audit
  "Walk the whole tree. {:branches n :no-record n :wrong [..]}. Materialises the tree."
  [set ops]
  (let [st  (.-storage set)
        acc (volatile! {:branches 0 :no-record 0 :wrong []})]
    (letfn [(walk [n]
              (let [es (if (instance? Leaf n)
                         (vec (.-keys n))
                         (let [len   (alength (.-keys n))
                               cm    (.-_childMeasures n)
                               parts (mapv #(walk (branch/child n st % {:sync? true})) (range len))]
                           (vswap! acc update :branches inc)
                           (if (nil? cm)
                             (vswap! acc update :no-record inc)
                             (do
                               (when (not= (alength cm) len)
                                 (vswap! acc update :wrong conj [:length (node/level n) (alength cm) len]))
                               (dotimes [i (min (alength cm) len)]
                                 (when (not= (aget cm i) (truth ops (nth parts i)))
                                   (vswap! acc update :wrong conj [:entry (node/level n) i])))))
                           (into [] cat parts)))]
                (when (and (some? (node/measure n)) (not= (node/measure n) (truth ops es)))
                  (vswap! acc update :wrong conj [:measure (node/level n)]))
                es))]
      (walk (root set)))
    @acc))

(defn- height [set] (inc (node/level (root set))))

(def ^:private batch
  {:conj    (fn [st ks] (reduce (fn [st k] (s/conj st [(inc k) 7] cmp)) st ks))
   :disj    (fn [st ks] (reduce (fn [st k] (s/disj st [k nil] cmp)) st ks))
   :replace (fn [st ks] (reduce (fn [st k] (s/replace st [k nil] [k 1234] cmp)) st ks))
   :mixed   (fn [st ks] (reduce (fn [st k]
                                  (case (mod (quot k 2) 3)
                                    0 (s/conj st [(inc k) 7] cmp)
                                    1 (s/disj st [k nil] cmp)
                                    2 (s/replace st [k nil] [k 1234] cmp)))
                                st ks))})

(defn- scattered [n cnt]
  (let [step (max 1 (quot n cnt))]
    (map #(* 2 (* step %)) (range 1 cnt))))

(deftest every-kind-of-cold-write-keeps-the-record
  (doseq [[ops-name ops] [[:stat stat-ops] [:concat concat-ops]]
          bf   [4 8]
          dbs  [0 16]
          kind [:conj :disj :replace :mixed]
          :let [n 400, lbl (str ops-name " bf=" bf " diff-buf=" dbs " " kind)]]
    (let [{:keys [st o addr]} (built ops bf dbs nil n)
          s1    ((batch kind) (cold o st addr) (scattered n 60))
          live  (audit s1 ops)
          addr1 (s/store s1 st)
          c     (cold o st addr1)
          a     (audit c ops)]
      (is (empty? (:wrong live)) (str lbl ": in memory, first " (first (:wrong live))))
      (is (empty? (:wrong a)) (str lbl ": stored, first " (first (:wrong a))))
      (is (zero? (:no-record a))
          (str lbl ": " (:no-record a) " of " (:branches a) " stored branches have no record"))
      (is (= (truth ops (seq c)) (s/measure (cold o st addr1))) (str lbl ": measure"))
      (is (= {:branches (:branches a) :unknown 0} (s/measure-census (cold o st addr1)))
          (str lbl ": census")))))

(deftest a-content-defined-tree-keeps-the-record
  (doseq [bf [8 16], kind [:conj :disj]
          :let [lbl (str "mst bf=" bf " " kind)]]
    (let [extra {:boundary (boundary/mst-boundary 3)}
          {:keys [st o addr]} (built stat-ops bf 0 extra 600)
          s1    ((batch kind) (cold o st addr) (scattered 600 80))
          addr1 (s/store s1 st)
          a     (audit (cold o st addr1) stat-ops)]
      (is (empty? (:wrong a)) (str lbl ": first " (first (:wrong a))))
      (is (zero? (:no-record a)) (str lbl ": " (:no-record a) " of " (:branches a)))
      (is (= (truth stat-ops (seq s1)) (s/measure (cold o st addr1))) lbl))))

;; A small deterministic generator: the sequence has to be the same on every run.
(defn- lcg [seed] (volatile! seed))
(defn- next-int [g bound]
  (let [x (mod (+ (* @g 1103515245) 12345) 2147483648)]
    (vreset! g x)
    (mod (quot x 65536) bound)))

(deftest a-long-sequence-of-writes-and-restarts
  (doseq [[ops-name ops] [[:stat stat-ops] [:concat concat-ops]]
          bf [4 8], dbs [0 16], seed [1 2]
          :let [lbl (str ops-name " bf=" bf " diff-buf=" dbs " seed=" seed)]]
    (let [{:keys [st o addr]} (built ops bf dbs nil 150)
          g (lcg seed)]
      (loop [step 0, st-set (cold o st addr), model (into (sorted-map) (elems 150))]
        (if (= step 300)
          (is (= (seq (map vec model)) (seq st-set)) (str lbl ": contents at the end"))
          (let [k  (next-int g 400)
                op (next-int g 3)
                [s2 model] (case op
                             0 [(s/conj st-set [k (get model k step)] cmp)
                                (if (contains? model k) model (assoc model k step))]
                             1 [(s/disj st-set [k nil] cmp) (dissoc model k)]
                             2 (if (contains? model k)
                                 [(s/replace st-set [k nil] [k (- step)] cmp) (assoc model k (- step))]
                                 [st-set model]))]
            (if (zero? (mod (inc step) 25))
              (let [a1 (s/store s2 st)
                    a  (audit (cold o st a1) ops)]
                (is (empty? (:wrong a)) (str lbl " step " step ": first " (first (:wrong a))))
                (is (zero? (:no-record a)) (str lbl " step " step ": branches without a record"))
                (is (= (truth ops (map vec model)) (s/measure (cold o st a1)))
                    (str lbl " step " step ": measure"))
                (recur (inc step) (cold o st a1) model))
              (recur (inc step) s2 model))))))))

(defn- reading [st f]
  (let [r0 (reads st), v (f)] [v (- (reads st) r0)]))

(deftest what-a-cold-reader-and-writer-pay
  (let [n 6000, bf 8
        {:keys [st o addr]} (built stat-ops bf 0 nil n)
        h   (height (cold o st addr))
        all (vec (elems n))]
    (is (>= h 4) "precondition: a tree deep enough for a per-level bill to mean something")
    (testing "`measure` is the root's own"
      (let [c (cold o st addr)
            [m r] (reading st #(s/measure c))]
        (is (= (truth stat-ops all) m))
        (is (<= r 1) (str r " reads"))))
    (testing "`get-nth` reads one node per level, wherever the position is"
      (doseq [i [0 1 777 (quot n 2) (- n 2) (dec n)]]
        (let [c (cold o st addr)
              [v r] (reading st #(s/get-nth c i))]
          (is (= [(nth all i) 0] v) (str "position " i))
          (is (<= r h) (str "position " i ": " r " reads, height " h)))))
    (testing "`measure-slice` reads the two boundary paths"
      (let [c (cold o st addr)
            [m r] (reading st #(s/measure-slice c (nth all 1234) (nth all 4567) cmp))]
        (is (= (truth stat-ops (subvec all 1234 4568)) m))
        (is (<= r (* 2 h)) (str r " reads, height " h))))
    (testing "a replace and an insert read their path"
      (let [c (cold o st addr)
            [_ r] (reading st #(s/replace c [2468 nil] [2468 5] cmp))]
        (is (<= r h) (str r " reads, height " h)))
      (let [c (cold o st addr)
            [_ r] (reading st #(s/conj c [2469 5] cmp))]
        (is (<= r h) (str r " reads, height " h))))
    (testing "a delete reads its path and the two neighbours it weighs at each level"
      (let [c (cold o st addr)
            [_ r] (reading st #(s/disj c [2468 nil] cmp))]
        (is (<= r (* 3 h)) (str r " reads, height " h))))))

(defn- age!
  "Rewrite every stored branch the way an earlier version wrote it."
  [st erode?]
  (swap! (:*disk st) update-vals
         (fn [blob]
           (if (:addresses blob)
             (cond-> (dissoc blob :child-measures) erode? (dissoc :measure))
             blob))))

(deftest an-older-tree-answers-and-recovers
  (doseq [erode? [false true]
          :let [lbl (if erode? "no records, no branch measures" "no records")]]
    (let [n 6000, bf 8
          {:keys [st o addr]} (built stat-ops bf 0 nil n)
          _   (age! st erode?)
          all (vec (elems n))
          h   (height (cold o st addr))
          census (s/measure-census (cold o st addr))]
      (testing lbl
        (is (pos? (:branches census)))
        (is (= (:branches census) (:unknown census)) "every branch is reported")
        (testing "answers"
          (is (= (truth stat-ops all) (s/measure (cold o st addr))))
          (doseq [i [0 777 (quot n 2) (dec n)]]
            (is (= [(nth all i) 0] (s/get-nth (cold o st addr) i)) (str "position " i)))
          (is (= (truth stat-ops (subvec all 1234 4568))
                 (s/measure-slice (cold o st addr) (nth all 1234) (nth all 4567) cmp))))
        (testing "and costs more than a tree with records"
          (let [c (cold o st addr)
                [_ r] (reading st #(s/get-nth c (dec n)))]
            (is (> r h) (str r " reads, height " h))))
        (testing "a read does not make a branch look repaired"
          (let [c (cold o st addr)]
            (s/get-nth c (quot n 2))
            (is (= census (s/measure-census c)))))
        (testing "a write through it stores the record for the branches it rewrites"
          (let [s1 (s/replace (cold o st addr) [2468 nil] [2468 5] cmp)
                a1 (s/store s1 st)
                c1 (s/measure-census (cold o st a1))]
            (is (= (- (:unknown census) (dec h)) (:unknown c1)) "the path's branches, and only those")
            (is (empty? (:wrong (audit (cold o st a1) stat-ops))))))
        (testing "`remeasure` repairs it in one pass"
          (let [s1 (s/remeasure (cold o st addr))
                _  (is (= {:branches (:branches census) :unknown 0} (s/measure-census s1)))
                a1 (s/store s1 st)
                c  (cold o st a1)
                a  (audit c stat-ops)]
            (is (= (seq all) (seq c)) "contents unchanged")
            (is (empty? (:wrong a)))
            (is (zero? (:no-record a)))
            (is (= {:branches (:branches census) :unknown 0} (s/measure-census (cold o st a1))))
            (is (identical? c (s/remeasure c)) "nothing left to do")
            (let [c2 (cold o st a1)
                  [_ r] (reading st #(s/get-nth c2 (dec n)))]
              (is (<= r h) (str "get-nth afterwards: " r " reads, height " h)))))))))

(deftest a-record-of-the-wrong-length-is-not-trusted
  (let [{:keys [st o addr]} (built stat-ops 8 0 nil 600)
        all (vec (elems 600))]
    (swap! (:*disk st) update-vals
           (fn [blob] (if (:child-measures blob) (update blob :child-measures pop) blob)))
    (let [c (cold o st addr)]
      (is (= (:branches (s/measure-census c)) (:unknown (s/measure-census c))))
      (is (= (truth stat-ops all) (s/measure (cold o st addr))))
      (is (= [(nth all 599) 0] (s/get-nth (cold o st addr) 599)))
      (let [s1 (s/replace (cold o st addr) [100 nil] [100 5] cmp)]
        (is (empty? (:wrong (audit s1 stat-ops))))
        (is (some? (.-_childMeasures (root s1)))
            "and the write replaced it with a record of its own, rather than leaving none")))))

(deftest a-buffered-write-does-not-make-an-older-branch-look-repaired
  ;; See the JVM twin: under a diff buffer the parent can keep an edit in a slot against the
  ;; child's OLD blob, so the child in memory holds a record its blob does not.
  (let [n 3000, bf 8
        {:keys [st o addr]} (built stat-ops bf 64 nil n)
        _      (age! st false)
        census (s/measure-census (cold o st addr))
        s1     (s/replace (cold o st addr) [2468 nil] [2468 5] cmp)
        a1     (s/store s1 st)
        stored (s/measure-census (cold o st a1))]
    (is (pos? (:unknown stored)) "precondition: the write left older branches behind")
    (is (= stored (s/measure-census s1)) "the set that was written says what a restore finds")
    (let [a2 (s/store (s/remeasure s1) st)]
      (is (= {:branches (:branches census) :unknown 0} (s/measure-census (cold o st a2))))
      (is (empty? (:wrong (audit (cold o st a2) stat-ops))))
      (is (zero? (:no-record (audit (cold o st a2) stat-ops)))))))

(deftest the-async-path-keeps-the-record-too
  (async done
         ((cps
           (let [a {:sync? false}
                 n 400
                 {:keys [st o addr]} (built stat-ops 4 0 nil n)
                 all (vec (elems n))
                 s1  (loop [st-set (cold o st addr), ks (scattered n 60)]
                       (if-let [k (first ks)]
                         (recur (case (mod (quot k 2) 3)
                                  0 (await (s/conj st-set [(inc k) 7] cmp a))
                                  1 (await (s/disj st-set [k nil] cmp a))
                                  2 (await (s/replace st-set [k nil] [k 1234] cmp a)))
                                (next ks))
                         st-set))
                 a1  (await (s/store s1 st a))
                 au  (audit (cold o st a1) stat-ops)]
             (is (empty? (:wrong au)) (str "first " (first (:wrong au))))
             (is (zero? (:no-record au)))
             (is (= {:branches (:branches au) :unknown 0}
                    (await (s/measure-census (cold o st a1) a))))
             (let [c (cold o st addr)
                   h (height (cold o st addr))
                   r0 (reads st)
                   v (await (s/get-nth c 333 a))]
               (is (= [(nth all 333) 0] v))
               (is (<= (- (reads st) r0) h)))
             (age! st false)
             (let [fixed (await (s/remeasure (cold o st a1) a))
                   a2    (await (s/store fixed st a))]
               (is (= 0 (:unknown (await (s/measure-census (cold o st a2) a)))))
               (is (empty? (:wrong (audit (cold o st a2) stat-ops)))))))
          ;; `done` from the continuation, not from inside the body: the rest of the run
          ;; would otherwise execute inside this body's call
          (fn [_] (done))
          (fn [e] (is (nil? e)) (js/console.error e) (done)))))
