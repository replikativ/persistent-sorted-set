(ns org.replikativ.persistent-sorted-set.test.stored-counts
  "THE INVARIANT: every branch that is stored is stored with its element count.

   `has-subtree-counts?` cannot check it (it asks the root), and the root-count tests in
   `durable-count-insert-side` could not see it fail: the root kept its count while the
   branches below lost theirs. A branch that DIVIDES — a split, a borrow — needs its
   children's counts, and when those children were on disk both results were stored
   unknown. Nothing on the write path heals an unknown branch, so each stayed unknown and
   every later write through it re-stored it that way. Measured before the fix, 60 cold
   write generations: 229 of 308 stored branches unknown at bf 16, 51 of 80 at bf 64.

   The oracle here reads the BLOBS, not the nodes: a count that exists only in memory is
   exactly the thing that must not pass.

   Both modes are pinned explicitly. The suite runs with -Dpss.diffBufSize=256, and under
   diff-buf a buffered write heals the child it passes through (by reading its children),
   which is what hid this from most of the suite."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
  (:import [org.replikativ.persistent_sorted_set IStorage ANode Branch PersistentSortedSet]))

(set! *warn-on-reflection* true)

;; The library's own codec, so diff-buf slots are persisted the way a real store does.
(defrecord BlobStorage [*disk *restores ctx]
  IStorage
  (store [_ node]
    (let [addr (str (java.util.UUID/randomUUID))]
      (swap! *disk assoc addr (nodes/node->blob node))
      addr))
  (accessed [_ _] nil)
  (restore [_ address]
    (swap! *restores inc)
    (let [blob (@*disk address)]
      (if (:addresses blob) (nodes/blob->branch ctx blob) (nodes/blob->leaf ctx blob)))))

(defn- storage
  ([disk] (storage disk (atom 0)))
  ([disk restores] (->BlobStorage disk restores (nodes/reader-context {}))))

(defn- opts [bf dbs] {:comparator compare :branching-factor bf :diff-buf-size dbs})

(defn- unknown? [blob] (neg? (long (or (:subtree-count blob) -1))))

(defn- disk-census
  "What a restore of `addr` finds: {:branches n :unknown m :leaves #{addr}}.

   Under diff-buf a branch is not only its blob. A parent's slot for it, when it carries a
   diff, is PROJECTED onto it at restore: the branch takes its count from the slot and its
   own slots from the slot's nested diff, superseding what its blob says. So a blob without
   a count is known all the same when such a slot stands above it."
  [disk addr]
  (let [acc (volatile! {:branches 0 :unknown 0 :leaves #{}})]
    ((fn walk [a entry]
       (let [{:keys [addresses] :as blob} (@disk a)
             projected? (and entry (some? (:diff entry)))]
         (if addresses
           (let [slots (if projected? (:diff entry) (:slots blob))]
             (vswap! acc update :branches inc)
             (when (if projected?
                     (neg? (long (:count entry)))
                     (unknown? blob))
               (vswap! acc update :unknown inc))
             (dotimes [i (count addresses)]
               (walk (nth addresses i) (when (> (long (:level blob)) 1) (get slots i)))))
           (vswap! acc update :leaves conj a))))
     addr nil)
    @acc))

(defn- wrong-counts
  "Every branch whose cached count is known and is not the number of elements below it."
  [^PersistentSortedSet s]
  (let [st  (.-_storage s)
        bad (volatile! [])]
    (letfn [(walk [^ANode n]
              (if (instance? Branch n)
                (let [^Branch b n
                      cached (.subtreeCount b)
                      actual (reduce + 0 (map #(walk (.child b st (int %))) (range (.len b))))]
                  (when (and (>= cached 0) (not= cached actual))
                    (vswap! bad conj {:level (.level b) :cached cached :actual actual}))
                  actual)
                (.len n)))]
      (walk (.root s))
      @bad)))

(defn- build [disk bf dbs n]
  (let [s (reduce #(set/conj %1 %2 compare) (set/sorted-set* (opts bf dbs)) (range 0 (* 2 n) 2))]
    (set/store s (storage disk))))

(defn- write-batch [cold news dels transient?]
  (if transient?
    (persistent! (reduce disj! (reduce conj! (transient cold) news) dels))
    (reduce #(set/disj %1 %2 compare) (reduce #(set/conj %1 %2 compare) cold news) dels)))

(defn- generations
  "`gens` rounds of (restore cold, insert `per` keys, delete `rem-per`, store), checking the
   invariant after every store. Returns the final {:addr :model :max-restores}."
  [{:keys [bf dbs n gens per rem-per transient? pattern seed]}]
  (let [disk (atom {})
        rnd  (java.util.Random. (long seed))
        a0   (build disk bf dbs n)]
    (is (zero? (:unknown (disk-census disk a0))) "a warm build stores every count")
    (loop [addr a0, g 0, model (into (sorted-set) (range 0 (* 2 n) 2)), max-restores 0]
      (if (= g gens)
        {:addr addr :model model :disk disk :max-restores max-restores}
        (let [restores (atom 0)
              cold  (set/restore-by compare addr (storage disk restores) (opts bf dbs))
              news  (case pattern
                      :append    (let [m (inc (long (or (last model) 0)))] (range m (+ m per)))
                      :scattered (repeatedly per #(inc (* 2 (.nextInt rnd (* 4 n))))))
              dels  (take rem-per (shuffle (seq model)))
              s     (write-batch cold news dels transient?)
              addr' (set/store s (storage disk restores))
              model (reduce disj (into model news) dels)
              cen   (disk-census disk addr')
              back  (set/restore-by compare addr' (storage disk) (opts bf dbs))]
          (is (zero? (:unknown cen))
              (str "generation " g ": " (:unknown cen) " of " (:branches cen) " stored branches have no count"))
          (when (zero? (mod g 5))
            (is (= {:branches (:branches cen) :unknown 0} (set/count-census back)))
            (is (= [] (wrong-counts back)))
            (is (= (count model) (count back)))
            (is (= (seq model) (seq back))))
          (recur addr' (inc g) model (max max-restores (long @restores))))))))

(deftest every-stored-branch-keeps-its-count-through-cold-writes
  (doseq [dbs        [0 64]
          bf         [8 16 64]
          transient? [false true]
          [pattern rem-frac] [[:scattered 5] [:append 0] [:scattered 1]]]
    (testing (str "bf " bf ", diff-buf " dbs ", " (if transient? "transient" "persistent")
                  ", " (name pattern) ", one delete per " rem-frac " inserts")
      (let [per (* 12 bf)]
        (generations {:bf bf :dbs dbs :n (* 60 bf) :gens 25 :per per
                      :rem-per (if (zero? rem-frac) 0 (quot per rem-frac))
                      :transient? transient? :pattern pattern :seed (+ bf dbs)})))))

(deftest a-write-reads-a-bounded-number-of-nodes
  (testing "one insert or delete on a cold tree: the path, its neighbours, and at most the
            children of the two nodes a split or a borrow divides — never a subtree"
    (doseq [dbs [0 64]
            bf  [8 32]]
      (let [disk (atom {})
            n    (* 200 bf)
            a0   (build disk bf dbs n)
            rnd  (java.util.Random. 3)]
        (loop [addr a0, i 0, worst 0]
          (if (= i 300)
            ;; height <= 4 here; 3 nodes a level on a delete, two nodes' children at one level
            (is (<= worst (+ (* 3 4) (* 2 bf) 2)) (str "bf " bf " diff-buf " dbs ": " worst " restores in one write"))
            (let [restores (atom 0)
                  cold (set/restore-by compare addr (storage disk restores) (opts bf dbs))
                  s    (if (even? i)
                         (set/conj cold (inc (* 2 (.nextInt rnd (* 2 n)))) compare)
                         (set/disj cold (first (set/slice cold (.nextInt rnd (* 2 n)) nil)) compare))
                  ;; before the store, which reads nothing but is not what is being bounded
                  used (long @restores)]
              (recur (set/store s (storage disk)) (inc i) (max worst used)))))))))

(defn- erase-counts!
  "Rewrite `frac` of the branch blobs below `addr` without their count, the state an earlier
   version left behind. Slot counts are left alone: they were always written."
  [disk addr ^java.util.Random rnd frac]
  (let [n (volatile! 0)]
    ((fn walk [a]
       (let [{:keys [addresses] :as blob} (@disk a)]
         (when addresses
           (when (< (.nextDouble rnd) (double frac))
             (swap! disk assoc-in [a :subtree-count] -1)
             (vswap! n inc))
           (run! walk addresses))))
     addr)
    @n))

(deftest recount-restores-every-count-and-stores-it
  (doseq [dbs  [0 64]
          bf   [8 32]
          frac [0.5 1.0]]
    (testing (str "bf " bf ", diff-buf " dbs ", " (int (* 100 frac)) "% of the branches unknown")
      (let [n      (* 120 bf)
            {:keys [disk addr]} (generations {:bf bf :dbs dbs :n n :gens 3 :per (* 4 bf) :rem-per bf
                                              :transient? true :pattern :scattered :seed 5})
            erased (erase-counts! disk addr (java.util.Random. 9) frac)
            cen0   (disk-census disk addr)
            cold   (set/restore-by compare addr (storage disk) (opts bf dbs))
            elems  (vec (seq cold))]
        (is (pos? erased))
        (is (= (:unknown cen0) (:unknown (set/count-census cold)))
            "the census sees what is stored")

        (testing "a count in memory hides nothing"
          (let [s (set/restore-by compare addr (storage disk) (opts bf dbs))]
            (is (= (count elems) (count s)))
            (is (= (count elems) (set/count-slice s nil nil)))
            (is (= (:unknown cen0) (:unknown (set/count-census s)))
                "counted in memory, still unknown on disk")
            (let [addr' (set/store (set/recount s) (storage disk))]
              (is (zero? (:unknown (disk-census disk addr')))
                  "and recount still repairs it"))))

        (let [restores (atom 0)
              s      (set/restore-by compare addr (storage disk restores) (opts bf dbs))
              fixed  (set/recount s)
              reads  (long @restores)
              addr'  (set/store fixed (storage disk))
              cen1   (disk-census disk addr')
              back   (set/restore-by compare addr' (storage disk) (opts bf dbs))]
          (is (not (identical? s fixed)))
          (is (zero? (:unknown cen1)) "every stored branch has its count")
          (is (= (:branches cen0) (:branches cen1)) "same structure")
          (is (= (:leaves cen0) (:leaves cen1)) "no leaf was rewritten")
          (is (<= reads (+ (:branches cen0) (count (:leaves cen0))))
              "it reads branches, and leaves only below a leaf-parent without a count")
          (is (= elems (vec (seq back))))
          (is (= (count elems) (count back)))
          (is (= [] (wrong-counts back)))
          (is (= {:branches (:branches cen1) :unknown 0} (set/count-census back)))
          (is (identical? back (set/recount back)) "nothing left to do")

          (testing "and writes keep it that way"
            (let [s2    (write-batch back (map #(inc (* 2 %)) (range 0 n 3)) (take bf elems) true)
                  addr2 (set/store s2 (storage disk))]
              (is (zero? (:unknown (disk-census disk addr2)))))))))))

(deftest a-split-of-an-uncounted-tree-does-not-walk-it
  (testing "children that are unknown themselves end the read: the result stays unknown,
            and the cost stays one node's children"
    (let [bf 8, disk (atom {}), n 4000
          addr (build disk bf 0 n)
          _    (erase-counts! disk addr (java.util.Random. 1) 1.0)
          rnd  (java.util.Random. 2)]
      (loop [addr addr, i 0, worst 0]
        (if (= i 200)
          (is (<= worst (+ 12 (* 2 bf) 2)))
          (let [restores (atom 0)
                cold (set/restore-by compare addr (storage disk restores) (opts bf 0))
                s    (set/conj cold (inc (* 2 (.nextInt rnd (* 2 n)))) compare)
                used (long @restores)]
            (recur (set/store s (storage disk)) (inc i) (max worst used))))))))

(deftest recount-edges
  (testing "a set with no storage, a single leaf, an empty set: nothing to do"
    (let [s (into (set/sorted-set* (opts 8 0)) (range 1000))]
      (is (identical? s (set/recount s)))
      (is (zero? (:unknown (set/count-census s)))))
    (let [s (into (set/sorted-set* (opts 8 0)) (range 3))]
      (is (identical? s (set/recount s)))
      (is (= {:branches 0 :unknown 0} (set/count-census s))))
    (let [s (set/sorted-set* (opts 8 0))]
      (is (identical? s (set/recount s)))))
  (testing "a transient is refused"
    (is (thrown? IllegalStateException
                 (set/recount (transient (into (set/sorted-set* (opts 8 0)) (range 100))))))))
