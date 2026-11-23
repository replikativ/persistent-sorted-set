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
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.boundary :as boundary]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
  (:import [org.replikativ.persistent_sorted_set IStorage ANode Branch ILeafProcessor PersistentSortedSet]
           [java.util List]))

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
  [{:keys [bf dbs n gens per rem-per transient? pattern seed extra dels-from]}]
  (let [disk (atom {})
        rnd  (java.util.Random. (long seed))
        opts (fn [bf dbs] (merge (opts bf dbs) extra))
        a0   (set/store (reduce #(set/conj %1 %2 compare) (set/sorted-set* (opts bf dbs)) (range 0 (* 2 n) 2))
                        (storage disk))]
    (is (zero? (:unknown (disk-census disk a0))) "a warm build stores every count")
    (loop [addr a0, g 0, model (into (sorted-set) (range 0 (* 2 n) 2)), max-restores 0]
      (if (= g gens)
        {:addr addr :model model :disk disk :max-restores max-restores}
        (let [restores (atom 0)
              cold  (set/restore-by compare addr (storage disk restores) (opts bf dbs))
              news  (case pattern
                      :append    (let [m (inc (long (or (last model) 0)))] (range m (+ m per)))
                      :scattered (repeatedly per #(inc (* 2 (.nextInt rnd (* 4 n))))))
              dels  (case (or dels-from :anywhere)
                      ;; seeded: an unseeded shuffle made the tree differ from run to run,
                      ;; and with it what the tests below could assume about it
                      :anywhere (let [l (java.util.ArrayList. ^java.util.Collection (vec model))]
                                  (java.util.Collections/shuffle l rnd)
                                  (take rem-per l))
                      ;; one end of the key range: every underflow has a sibling on one
                      ;; side only, so each borrow direction is taken on its own
                      :low      (take rem-per (seq model))
                      :high     (take rem-per (rseq model)))
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
  (testing "one insert or delete on a cold tree: the path, its neighbours, and the children
            of ONE of the two nodes a split or a borrow leaves — never both, never a subtree"
    (doseq [dbs [0 64]
            bf  [8 32]]
      (let [disk (atom {})
            n    (* 200 bf)
            a0   (build disk bf dbs n)
            rnd  (java.util.Random. 3)]
        (loop [addr a0, i 0, worst 0]
          (if (= i 300)
            ;; height <= 4 here: the lookup's path, 3 nodes a level on a delete, and ONE
            ;; divide's read — the smaller half of one node's children, never both halves
            (is (<= worst (+ 4 (* 3 4) (quot bf 2) 2)) (str "bf " bf " diff-buf " dbs ": " worst " restores in one write"))
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
        ;; under diff-buf a slot above an erased blob can supply its count, so erasing
        ;; blobs is not yet a tree with unknown branches
        (is (pos? (:unknown cen0)) "precondition: the stored tree has unknown branches")
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

(deftest each-borrow-direction-keeps-its-counts
  (doseq [dbs  [0 64]
          from [:low :high]]
    (testing (str "diff-buf " dbs ", deleting from the " (name from) " end")
      (generations {:bf 8 :dbs dbs :n 600 :gens 20 :per 10 :rem-per 50
                    :transient? false :pattern :scattered :seed 3 :dels-from from}))))

(defn- identity-processor
  "A processor that fires on every leaf and changes nothing: enough to take every write off
   the `one key = one element` shortcut and onto the arithmetic over the touched children."
  []
  (reify ILeafProcessor
    (shouldProcess [_ _leaf-size _settings] true)
    (processLeaf [_ entries _storage _settings] entries)))

(deftest a-leaf-processor-write-keeps-every-count
  (testing "with a processor a write cannot assume it changed the count by one, and used to
            fall back to summing ALL children — unknown at the first one on disk. A cold
            delete then turned a counted branch into an uncounted one."
    (doseq [transient? [false true]
            [pattern rem-frac] [[:scattered 3] [:scattered 1]]]
      (let [per 96]
        (generations {:bf 8 :dbs 0 :n 480 :gens 20 :per per :rem-per (quot per rem-frac)
                      :transient? transient? :pattern pattern :seed 17
                      :extra {:leaf-processor (identity-processor)}})))))

(deftest a-content-defined-tree-keeps-every-count
  (testing "MST mode cuts where the keys say, through the same split seam"
    (doseq [transient? [false true]]
      (generations {:bf 32 :dbs 0 :n 1500 :gens 15 :per 200 :rem-per 40
                    :transient? transient? :pattern :scattered :seed 23
                    :extra {:boundary (boundary/mst-boundary 3)}}))))

;; ---- what a count in memory may not do ----

(defn- caching-storage
  "One node OBJECT per address, shared by every set that restores through it — what a
   caching IStorage (datahike's) does."
  [disk]
  (let [inner (storage disk)
        cache (atom {})]
    (reify IStorage
      (store [_ node] (.store ^IStorage inner node))
      (accessed [_ _] nil)
      (restore [_ address]
        (or (@cache address)
            (let [n (.restore ^IStorage inner address)]
              (swap! cache assoc address n)
              n))))))

(defn- count-all!
  "Make every branch of `s` work out its count by reading, the way a caller's `count` or a
   range count does."
  [s]
  ((fn walk [^ANode n]
     (when (instance? Branch n)
       (.count n (.-_storage ^PersistentSortedSet s))
       (when (> (.level ^Branch n) 1)
         (dotimes [i (.len n)] (walk (.child ^Branch n (.-_storage ^PersistentSortedSet s) (int i)))))))
   (.root ^PersistentSortedSet s)))

(deftest a-count-in-memory-reaches-no-other-set
  (doseq [dbs [0 64]]
    (testing (str "diff-buf " dbs)
      (let [{:keys [disk addr]} (generations {:bf 8 :dbs dbs :n 900 :gens 2 :per 30 :rem-per 8
                                              :transient? true :pattern :scattered :seed 5})
            _      (erase-counts! disk addr (java.util.Random. 9) 1.0)
            want   (:unknown (disk-census disk addr))
            shared (caching-storage disk)
            a      (set/restore-by compare addr shared (opts 8 dbs))
            n      (count a)]
        (is (= n (set/count-slice a nil nil)))
        (testing "the same nodes, through the same cache"
          (let [b (set/restore-by compare addr shared (opts 8 dbs))]
            (is (= want (:unknown (set/count-census b))))
            (is (= want (:unknown (set/count-census a))))))
        (testing "and through a copy made for another comparator"
          (let [cmp2 (fn [x y] (compare x y))
                c    (set/restore-by cmp2 addr shared (assoc (opts 8 dbs) :comparator cmp2))]
            (is (= n (count c)))
            (is (= want (:unknown (set/count-census c))))
            (let [addr' (set/store (set/recount c) (storage disk))]
              (is (not= addr addr'))
              (is (zero? (:unknown (disk-census disk addr')))))))))))

(defn- root-branch ^Branch [s]
  (let [r (.root ^PersistentSortedSet s)] (when (instance? Branch r) r)))

(deftest a-root-that-is-promoted-is-not-changed-for-the-set-it-came-from
  (testing "in a content-defined tree a delete can empty one side of the root, and the other
            child is promoted to root AS IT IS — a node the original set still reaches at its
            old address. (In count mode the promoted node is always a fresh join.) If the new
            set writes its count into that node, the original believes its blob carries one,
            and its census and its recount pass over it."
    (let [bf    32
          opts  (assoc (opts bf 0) :boundary (boundary/mst-boundary 3))
          build (fn [n] (reduce #(set/conj %1 %2 compare) (set/sorted-set* opts) (range n)))
          ;; a tree whose root has exactly two children
          n     (first (filter (fn [n]
                                 (let [r (root-branch (build n))]
                                   (and r (>= (.level r) 2) (= 2 (.len r)))))
                               (range 200 4000 7)))
          _     (is (some? n) "precondition: such a tree exists in the searched range")
          disk  (atom {})
          addr  (set/store (build n) (storage disk))
          _     (erase-counts! disk addr (java.util.Random. 4) 1.0)
          ;; the root keeps its count: it is the nodes below that are legacy
          _     (swap! disk assoc-in [addr :subtree-count] n)
          want  (:unknown (disk-census disk addr))
          orig  (set/restore-by compare addr (caching-storage disk) opts)
          level0 (.level (root-branch orig))
          ;; delete from the high end until the root is lower: the last child empties and
          ;; the first, untouched, is what is left
          shrunk (loop [s orig, k (dec n)]
                   (let [r (root-branch s)]
                     (if (or (nil? r) (< (.level r) level0) (neg? k))
                       s
                       (recur (set/disj s k compare) (dec k)))))]
      (is (pos? want))
      (is (< (.level (root-branch shrunk)) level0) "precondition: the root collapsed")
      (is (= want (:unknown (set/count-census orig)))
          "the original set still sees its own blobs as they are")
      (let [addr' (set/store (set/recount orig) (storage disk))]
        (is (zero? (:unknown (disk-census disk addr')))))
      (testing "and the new set is stored with a counted root"
        (let [addr2 (set/store shrunk (storage disk))
              back  (set/restore-by compare addr2 (storage disk) opts)]
          (is (>= (long (:subtree-count (@disk addr2) -1)) 0))
          (is (= (count (seq back)) (count back)))
          (is (= [] (wrong-counts back))))))))

(deftest a-divide-over-an-uncounted-child-reads-no-further
  (testing "the node knows its total, one of the children it must read does not: the result
            stays unknown, nothing below that child is read, and no count is invented"
    (let [bf 8, disk (atom {}), n 6000
          addr (build disk bf 0 n)
          ;; erase the leaf-parents only: their parents still know their totals
          _    (doseq [[a blob] @disk :when (and (:addresses blob) (= 1 (:level blob)))]
                 (swap! disk assoc-in [a :subtree-count] -1))
          rnd  (java.util.Random. 6)]
      (loop [addr addr, i 0, worst 0]
        (if (= i 400)
          (do (is (<= worst (+ 12 (* 2 bf) 2)))
              (is (= [] (wrong-counts (set/restore-by compare addr (storage disk) (opts bf 0))))))
          (let [restores (atom 0)
                cold (set/restore-by compare addr (storage disk restores) (opts bf 0))
                s    (if (even? i)
                       (set/conj cold (inc (* 2 (.nextInt rnd n))) compare)
                       (set/disj cold (first (set/slice cold (.nextInt rnd (* 2 n)) nil)) compare))
                used (long @restores)]
            (recur (set/store s (storage disk)) (inc i) (max worst used))))))))

(deftest recount-of-a-set-with-unstored-writes
  (doseq [dbs [0 64]]
    (let [{:keys [disk addr model]} (generations {:bf 8 :dbs dbs :n 700 :gens 2 :per 30 :rem-per 8
                                                  :transient? true :pattern :scattered :seed 8})
          _     (erase-counts! disk addr (java.util.Random. 2) 0.7)
          cold  (set/restore-by compare addr (storage disk) (opts 8 dbs))
          news  (map #(inc (* 2 %)) (range 0 700 5))
          dirty (write-batch cold news (take 40 (seq model)) false)
          model (reduce disj (into model news) (take 40 (seq model)))
          addr' (set/store (set/recount dirty) (storage disk))
          back  (set/restore-by compare addr' (storage disk) (opts 8 dbs))]
      (is (zero? (:unknown (disk-census disk addr'))))
      (is (= (seq model) (seq back)))
      (is (= (count model) (count back)))
      (is (= [] (wrong-counts back))))))

(deftest a-warm-census-says-what-a-cold-one-does
  (testing "after buffered writes over a tree that is not yet recounted"
    (let [{:keys [disk addr]} (generations {:bf 8 :dbs 64 :n 900 :gens 2 :per 30 :rem-per 8
                                            :transient? true :pattern :scattered :seed 12})
          _    (erase-counts! disk addr (java.util.Random. 3) 1.0)
          cold (set/restore-by compare addr (storage disk) (opts 8 64))
          warm (reduce #(set/conj %1 %2 compare) cold (map #(inc (* 2 %)) (range 0 900 7)))
          addr' (set/store warm (storage disk))]
      (is (= (:unknown (disk-census disk addr'))
             (:unknown (set/count-census (set/restore-by compare addr' (storage disk) (opts 8 64))))
             (:unknown (set/count-census warm)))))))

(deftest a-root-that-collapses-onto-a-lazily-counted-node-is-stored-counted
  (testing "nothing on the delete path knows a total — the tree is uncounted, root included —
            but the node that becomes the root has worked its own count out by reading. That
            count is not what its blob says, and it is what the new root is stored with."
    (let [bf     32
          opts   (assoc (opts bf 0) :boundary (boundary/mst-boundary 3))
          build  (fn [n] (reduce #(set/conj %1 %2 compare) (set/sorted-set* opts) (range n)))
          n      (first (filter (fn [n] (let [r (root-branch (build n))]
                                          (and r (>= (.level r) 2) (= 2 (.len r)))))
                                (range 200 4000 7)))
          disk   (atom {})
          addr   (set/store (build n) (storage disk))
          _      (erase-counts! disk addr (java.util.Random. 4) 1.0)   ; root included
          level  #(some-> (root-branch %) .level)
          ;; the deletes, from the high end, up to the one that collapses the root
          run    (fn [s stop]
                   (loop [s s, k (dec n)]
                     (if (or (= k stop) (neg? k)) s (recur (set/disj s k compare) (dec k)))))
          level0 (level (set/restore-by compare addr (storage disk) opts))
          last-k (loop [s (set/restore-by compare addr (storage disk) opts), k (dec n)]
                   (let [s' (set/disj s k compare)]
                     (if (< (or (level s') 0) level0) k (recur s' (dec k)))))
          ^IStorage shared (caching-storage disk)
          before (run (set/restore-by compare addr shared opts) last-k)
          ^Branch promoted (.child (root-branch before) shared (int 0))
          _      (is (neg? (.subtreeCount promoted)) "precondition: its blob has no count")
          _      (is (neg? (.knownCount (root-branch before))) "precondition: nor does the root know a total")
          _      (.count promoted shared)
          shrunk (set/disj before last-k compare)
          addr'  (set/store shrunk (storage disk))]
      (is (< (level shrunk) level0) "precondition: the root collapsed")
      (is (neg? (.subtreeCount promoted)) "the shared node is as its blob says")
      (is (>= (long (:subtree-count (@disk addr') -1)) 0) "the new root is stored counted")
      (is (= [] (wrong-counts (set/restore-by compare addr' (storage disk) opts)))))))

(deftest a-count-worked-out-on-an-owned-node-is-written-with-it
  (testing "a transient over an uncounted tree splits a branch: the halves are its own nodes
            and it cannot count them. A reader then works their counts out. A replace that
            edits such a half in place deposits into the parent's slot — with the count —
            and the half itself must carry it too, or a warm census and a cold one disagree."
    (doseq [dbs [64 4]]
      (let [bf   8
            disk (atom {})
            addr (build disk bf dbs 900)
            _    (erase-counts! disk addr (java.util.Random. 3) 1.0)
            cold (set/restore-by compare addr (storage disk) (opts bf dbs))
            n0   (:branches (set/count-census cold))
            ;; insert until one branch has split, and stop there
            [t added] (loop [t (transient cold), k 1, added []]
                        (if (or (> (:branches (set/count-census t)) n0) (> k 1800))
                          [t added]
                          (recur (conj! t k) (+ k 2) (conj added k))))
            _    (is (> (:branches (set/count-census t)) n0) "precondition: a branch split")
            _    (set/store t (storage disk))
            _    (count-all! t)
            ;; an equal but distinct key object, so that the replace is a write
            t    (reduce #(set/replace %1 %2 (Integer/valueOf (int %2)) compare) t
                         (concat added (range 0 1800 2)))
            addr' (set/store t (storage disk))
            cold' (set/restore-by compare addr' (storage disk) (opts bf dbs))]
        (is (= (:unknown (disk-census disk addr'))
               (:unknown (set/count-census cold'))
               (:unknown (set/count-census t)))
            (str "diff-buf " dbs))
        (is (= [] (wrong-counts cold')))))))

(deftest recount-under-another-buffer-budget-writes-no-leaf
  (testing "a set reopened with a smaller budget than its nodes were written under: recount
            changes counts, it does not flush the slots it finds"
    (doseq [reopen [1 0 8]]
      (let [{:keys [disk addr model]} (generations {:bf 8 :dbs 64 :n 700 :gens 3 :per 20 :rem-per 5
                                                    :transient? false :pattern :scattered :seed 31})
            _      (erase-counts! disk addr (java.util.Random. 2) 1.0)
            before (disk-census disk addr)
            cold   (set/restore-by compare addr (storage disk) (opts 8 reopen))
            addr'  (set/store (set/recount cold) (storage disk))
            after  (disk-census disk addr')]
        (is (zero? (:unknown after)))
        (is (= (:leaves before) (:leaves after)) (str "reopened at " reopen ": no leaf was rewritten"))
        (is (= (seq model) (seq (set/restore-by compare addr' (storage disk) (opts 8 64)))))))))

(deftest storing-a-set-writes-no-count-into-a-node-it-shares
  (testing "the tree is uncounted, root included, so the root that a collapse promotes stays
            the node the original set reaches at its old address. The new set works its
            count out and is stored — a `store` of that root. The count must not land in
            the node: the original's census and recount would pass over it, and its blob
            has none."
    (let [bf     32
          opts   (assoc (opts bf 0) :boundary (boundary/mst-boundary 3))
          build  (fn [n] (reduce #(set/conj %1 %2 compare) (set/sorted-set* opts) (range n)))
          n      (first (filter (fn [n] (let [r (root-branch (build n))]
                                          (and r (>= (.level r) 2) (= 2 (.len r)))))
                                (range 200 4000 7)))
          disk   (atom {})
          addr   (set/store (build n) (storage disk))
          _      (erase-counts! disk addr (java.util.Random. 4) 1.0)   ; root included
          want   (:unknown (disk-census disk addr))
          orig   (set/restore-by compare addr (caching-storage disk) opts)
          level0 (.level (root-branch orig))
          shrunk (loop [s orig, k (dec n)]
                   (let [r (root-branch s)]
                     (if (or (nil? r) (< (.level r) level0) (neg? k))
                       s
                       (recur (set/disj s k compare) (dec k)))))
          _      (is (= (count (seq shrunk)) (count shrunk)))
          before (:unknown (set/count-census shrunk))
          addr2  (set/store shrunk (storage disk))]
      (is (< (.level (root-branch shrunk)) level0) "precondition: the root collapsed")
      (is (= before (:unknown (disk-census disk addr2)))
          "the census before the store said what the store wrote")
      (is (= want (:unknown (set/count-census orig))))
      (let [addr' (set/store (set/recount orig) (storage disk))]
        (is (zero? (:unknown (disk-census disk addr'))))))))

(deftest a-replace-in-place-keeps-a-count-that-was-worked-out
  (testing "diff-buf off, a transient over an uncounted tree, its count worked out by a
            reader: a replace edits the path in place and the nodes are written with it"
    (let [bf 8, disk (atom {})
          addr (build disk bf 0 400)
          _    (erase-counts! disk addr (java.util.Random. 3) 1.0)
          cold (set/restore-by compare addr (storage disk) (opts bf 0))
          t    (reduce conj! (transient cold) [1 3 5])          ; the transient owns a path
          _    (count-all! t)
          t    (set/replace t 3 (Integer/valueOf (int 3)) compare)
          addr' (set/store t (storage disk))]
      (is (>= (long (:subtree-count (@disk addr') -1)) 0) "the root is stored counted")
      (is (= [] (wrong-counts (set/restore-by compare addr' (storage disk) (opts bf 0))))))))

(deftest recount-of-a-tree-whose-budget-changed-loses-nothing
  (testing "written under a small budget, then written to under a larger one: branches hold
            buffered edits that no longer fit them. recount may flush those (documented);
            every count is repaired and every element is there."
    (let [{:keys [disk addr model]} (generations {:bf 8 :dbs 1 :n 900 :gens 2 :per 20 :rem-per 5
                                                  :transient? false :pattern :scattered :seed 41})
          warm   (reduce #(set/conj %1 %2 compare) (set/restore-by compare addr (storage disk) (opts 8 64)) [-1 -3 -5])
          addr1  (set/store warm (storage disk))
          _      (erase-counts! disk addr1 (java.util.Random. 2) 1.0)
          want   (seq (into model [-1 -3 -5]))]
      (doseq [reopen [1 64]]
        (let [addr' (set/store (set/recount (set/restore-by compare addr1 (storage disk) (opts 8 reopen)))
                               (storage disk))
              back  (set/restore-by compare addr' (storage disk) (opts 8 reopen))]
          (is (zero? (:unknown (disk-census disk addr'))) (str "reopened at " reopen))
          (is (= want (seq back)))
          (is (= (count want) (count back))))))))

(deftest a-processor-that-changes-cardinality-stores-no-wrong-count
  (testing "a processor that drops an element from the leaf it touches: one key is not one
            element, and the deletes run the tree through joins and borrows. Every stored
            count must be the true one, and none may be lost."
    (doseq [transient? [false true]]
      (let [bf    8
            disk  (atom {})
            addr  (build disk bf 0 700)
            proc  (reify ILeafProcessor
                    (shouldProcess [_ leaf-size _settings] (> (long leaf-size) 3))
                    (processLeaf [_ entries _storage _settings]
                      (let [^List entries entries]
                        (java.util.ArrayList. (.subList entries 0 (dec (.size entries)))))))
            popts (assoc (opts bf 0) :leaf-processor proc)]
        (loop [addr addr, i 0]
          (when (< i 120)
            (let [cold (set/restore-by compare addr (storage disk) popts)
                  ks   (take 3 (drop (* 5 (mod i 40)) (seq cold)))
                  s    (if transient?
                         (persistent! (reduce disj! (transient cold) ks))
                         (reduce #(set/disj %1 %2 compare) cold ks))
                  addr' (set/store s (storage disk))
                  back  (set/restore-by compare addr' (storage disk) (opts bf 0))]
              (when (seq ks)
                (is (= [] (wrong-counts back)) (str "round " i))
                (is (= (count (seq back)) (count back)) (str "round " i))
                (is (zero? (:unknown (disk-census disk addr'))) (str "round " i))
                (recur addr' (inc i))))))))))

(deftest recount-reads-each-branch-once
  (testing "only the root lacks its count: every branch is read once, no leaf"
    (let [bf 8, disk (atom {})
          addr (build disk bf 0 3000)
          _    (swap! disk assoc-in [addr :subtree-count] -1)
          cen  (disk-census disk addr)
          restores (atom 0)
          cold (set/restore-by compare addr (storage disk restores) (opts bf 0))
          fixed (set/recount cold)]
      (is (= 3000 (count fixed)))
      (is (= (:branches cen) @restores)))))

(deftest a-census-before-the-store-says-what-the-store-writes
  (doseq [dbs [0 64]
          transient? [false true]]
    (let [{:keys [disk addr model]} (generations {:bf 8 :dbs dbs :n 900 :gens 2 :per 30 :rem-per 8
                                                  :transient? false :pattern :scattered :seed 51})
          _     (erase-counts! disk addr (java.util.Random. 5) 0.6)
          cold  (set/restore-by compare addr (storage disk) (opts 8 dbs))
          _     (count-all! cold)
          dirty (write-batch cold (map #(inc (* 2 %)) (range 0 900 9)) (take 30 (seq model)) transient?)
          _     (count-all! dirty)
          warm  (:unknown (set/count-census dirty))
          addr' (set/store dirty (storage disk))]
      (is (= warm (:unknown (disk-census disk addr'))) (str "diff-buf " dbs " transient " transient?)))))

;; ===========================================================================
;; EVERY SINGLE COLD WRITE, ENUMERATED — the inductive step of the invariant above.
;;
;; THE INVARIANT is a one-step property: a stored tree in which every branch has its count,
;; plus ONE write, stored again, is a tree in which every branch has its count and none is
;; wrong. If that holds for every write from every tree, it holds for every sequence. So for
;; cold writes there is nothing to sample: the steps can be listed.
;;
;; WHAT DECIDES A STEP is local. At each level a write does one of a few things — updates
;; in place, splits, joins its left or its right neighbour, borrows from one — and which
;; one depends on four facts only: what came up from the child (nothing, a split, a child
;; lost to a join on either side), how full the node on the path is, and how full its left
;; and right neighbours under the same parent are. `shape-cases` lists every combination of
;; the three fills (every length a node can have, and `nil` for no neighbour) at every
;; level, under each of four cascades from below (splits along the first children, splits
;; along the last, joins to the left, joins to the right), with the path node first or last
;; among its parent's other children, and with every root size. Neighbouring subtrees are
;; filled unevenly, so that a count divided by proportion instead of by reading is wrong.
;;
;; WHY THE TREES ARE BUILT BY SHAPE and not by a history of inserts: measured, every single
;; write (5356 of them) from trees built ascending, descending, at random and by deleting a
;; larger tree down, at seven sizes, never grew the root, never split above level 1 and
;; never borrowed from the right. The stored blobs are plain maps, so a tree of any valid
;; shape can be written down directly.
;;
;; The oracle is a cold restore: every branch's count against the elements below it, and
;; with it what the tree holds, in which order, under which keys, in nodes of which size. A
;; classifier reads off what each step did (from the node lengths per level before and
;; after, and the path), and each test ends by asserting that every kind of step was seen
;; at every level, and every kind followed at the level above by every kind that can
;; follow it — so a change that makes a case unreachable fails here instead of quietly
;; covering less.
;;
;; Against the code before the fix (origin/main at 136f837) these fail on every branch
;; split and every branch borrow.
;;
;; What is NOT listed here is what depends on memory rather than on the stored tree — a
;; count a reader worked out, a transient stored half way, a node two sets share. Those are
;; the directed tests above, and `random-sequences-keep-every-count` below.
;; ===========================================================================

;; ---- the oracle ----

(defn- shape
  "The stored tree as a cold restore finds it: {:levels {level [node lengths, left to right]}
   :unknown branches without a count, :wrong branches whose count is not the number of
   elements below them, :n elements, :content the elements in tree order, :misplaced
   branch keys that are not the largest key of their child, :underfull / :overfull nodes
   outside [bf/2, bf] (the root: a branch of fewer than two children, or more than bf),
   :mislevelled children that are not one level below their parent}."
  [disk addr opts]
  (let [^IStorage st (storage disk (atom 0))
        s    (set/restore-by compare addr st opts)
        bf   (long (:branching-factor opts))
        mn   (quot bf 2)
        root (.root ^PersistentSortedSet s)
        acc  (volatile! {:levels (sorted-map) :unknown 0 :wrong 0 :misplaced 0 :underfull 0
                         :overfull 0 :mislevelled 0})
        out  (volatile! (transient []))
        fill (fn [^ANode n]
               (cond (> (.len n) bf) (vswap! acc update :overfull inc)
                     (identical? n root) (when (and (instance? Branch n) (< (.len n) 2))
                                           (vswap! acc update :underfull inc))
                     (< (.len n) mn) (vswap! acc update :underfull inc)))
        ;; returns [elements below, largest key below, level]
        walk (fn walk [^ANode n]
               (fill n)
               (if (instance? Branch n)
                 (let [^Branch br n
                       ^List ks (.keys n)
                       kids (mapv #(walk (.child br st (int %))) (range (.len n)))
                       c    (reduce + 0 (map first kids))]
                   (vswap! acc update-in [:levels (.level br)] (fnil conj []) (.len n))
                   (dotimes [i (.len n)]
                     (when-not (= (.get ks (int i)) (second (kids i))) (vswap! acc update :misplaced inc))
                     (when-not (= (nth (kids i) 2) (dec (.level br))) (vswap! acc update :mislevelled inc)))
                   (cond (neg? (.subtreeCount br))     (vswap! acc update :unknown inc)
                         (not= c (.subtreeCount br))   (vswap! acc update :wrong inc))
                   [c (second (peek kids)) (.level br)])
                 (let [^List ks (.keys n)]
                   (vswap! acc update-in [:levels 0] (fnil conj []) (.len n))
                   (dotimes [i (.len n)] (vswap! out conj! (.get ks (int i))))
                   [(.len n) (when (pos? (.len n)) (.get ks (int (dec (.len n))))) 0])))
        total (first (walk root))]
    (assoc @acc :n total :content (persistent! @out))))

(defn- after-op
  "The content a tree with `content` holds after `op`."
  [content [op k]]
  (case op
    :add (vec (into (apply sorted-set content) [k]))
    :del (vec (remove #(= % k) content))
    :rep content))

(defn- path-ordinals
  "Per level, the left-to-right position (within its level) of the node on the path to `k`,
   in the tree a restore finds — under a diff buffer that is not the tree the blobs spell
   out. `before` is that tree's `shape`."
  [disk addr opts before k]
  (let [^IStorage st (storage disk (atom 0))
        s (set/restore-by compare addr st opts)]
    (loop [^ANode n (.root ^PersistentSortedSet s), ord 0, out {}]
      (if (instance? Branch n)
        (let [^Branch br n
              L  (.level br)
              ^List ks (.keys n)
              i  (or (first (filter #(<= (compare k (.get ks (int %))) 0) (range (.len n))))
                     (dec (.len n)))]
          (recur (.child br st (int i))
                 ;; the nodes left of the child: the children of the nodes left of this one
                 (+ (long (reduce + 0 (take ord ((:levels before) L)))) (long i))
                 (assoc out L ord)))
        (assoc out 0 ord)))))

(defn- events
  "What one step did, read off the two shapes and the path:
   #{[:grow] [:collapse] [:split L] [:join-left L] [:join-right L] [:borrow-left L]
     [:borrow-right L]}, or #{[:in-place]}. A join is to the left when the left neighbour
   is not what it was; a borrow is from the neighbour that shrank while the path node did
   not."
  [before after path]
  (let [la (:levels before) lb (:levels after)
        ha (count la) hb (count lb)
        ev (volatile! #{})]
    (cond (> hb ha) (vswap! ev conj [:grow])
          (< hb ha) (vswap! ev conj [:collapse]))
    (doseq [L (range 0 (min ha hb))
            :let [x (la L) y (lb L) p (long (path L))]]
      (cond (> (count y) (count x)) (vswap! ev conj [:split L])
            (< (count y) (count x)) (vswap! ev conj (if (and (pos? p) (not= (get y (dec p)) (x (dec p))))
                                                      [:join-left L]
                                                      [:join-right L]))
            :else
            (do (when (and (pos? p) (< (y (dec p)) (x (dec p))) (>= (y p) (x p)))
                  (vswap! ev conj [:borrow-left L]))
                (when (and (< (inc p) (count x)) (< (y (inc p)) (x (inc p))) (>= (y p) (x p)))
                  (vswap! ev conj [:borrow-right L])))))
    (if (empty? @ev) #{[:in-place]} @ev)))

(defn- transitions
  "For every level that changed its parent's length, what the parent did about it:
   #{[kind L above]}, `above` being the parent's own event, :absorbed when it only updated
   in place, :grow for a split of the root, :collapse for a root left with one child."
  [before evs]
  (let [top  (dec (count (:levels before)))
        kind (into {} (for [[k L] evs :when L] [L k]))]
    (set (for [[L k] kind :when (#{:split :join-left :join-right} k)]
           [k L (cond (and (= k :split) (= L top))                                     :grow
                      (and (not= k :split) (= L (dec top)) (contains? evs [:collapse])) :collapse
                      :else (get kind (inc (long L)) :absorbed))]))))

;; ---- one cold write ----

(defn- step
  "Restore `addr` cold, do one write, store. `:reads` are the restores of the write itself,
   `:store-reads` those of the store."
  [disk addr opts before [op k]]
  (let [reads (atom 0)
        cold  (set/restore-by compare addr (storage disk reads) opts)
        tr?   (:transient? opts)
        s     (case op
                :add (if tr? (persistent! (conj! (transient cold) k)) (set/conj cold k compare))
                :del (if tr? (persistent! (disj! (transient cold) k)) (set/disj cold k compare))
                ;; an equal but distinct key object, so that the replace is a write
                :rep (set/replace cold k (Integer/valueOf (int k)) compare))
        used  (long @reads)
        addr' (set/store s (storage disk reads))
        after (shape disk addr' opts)
        evs   (events before after (path-ordinals disk addr opts before k))]
    {:addr addr' :reads used :store-reads (- (long @reads) used) :shape after
     :events evs :transitions (transitions before evs) :new-root? (not= addr addr')}))

;; ---- trees written down by shape ----

(defn- put! [disk blob]
  (let [a (str (java.util.UUID/randomUUID))] (swap! disk assoc a blob) a))

(defn- gen-leaf [disk ctr len base]
  (let [ks (vec (repeatedly len #(vswap! ctr + 2)))]
    {:addr (put! disk (assoc base :keys ks)) :count len :max (peek ks) :keys ks}))

(defn- gen-branch [disk level kids base]
  (let [n (reduce + (map :count kids))]
    {:addr  (put! disk (assoc base :level level :keys (mapv :max kids) :addresses (mapv :addr kids)
                              :subtree-count n))
     :count n
     :max   (:max (peek kids))}))

(defn- gen-sub
  "A subtree at `level` whose root has `len` children. Below it the nodes alternate between
   `fill` and `fill` + 1 children, so that neighbouring subtrees hold different numbers of
   elements."
  [disk ctr level len fill base]
  (if (zero? level)
    (gen-leaf disk ctr len base)
    (let [bf (long (:branching-factor base))]
      (gen-branch disk level
                  (mapv #(gen-sub disk ctr (dec level) (min bf (+ (long fill) (mod (long %) 2))) fill base)
                        (range len))
                  base))))

(defn- gen-spine
  "`specs` is indexed by level: {:own n :left n|nil :right n|nil :pad :left|:right} is the
   node on the path and its two neighbours; the last entry is the root, {:own n}. A node's
   other children are padding at minimum fill, laid out on its `:pad` side — so with
   padding on the right and no left neighbour the path node is its parent's first child,
   and the padding next to it is a neighbour all the same. Keys are the even numbers, so
   every odd number is a gap. nil when a level is too small to hold the children the level
   below asks for."
  [disk specs fill base]
  (let [ctr (volatile! 0) leaf-keys (volatile! nil)
        H    (dec (count specs))
        node (fn node [L]
               (let [{:keys [own pad]} (specs L)]
                 (if (zero? L)
                   (let [l (gen-leaf disk ctr own base)] (vreset! leaf-keys (:keys l)) l)
                   (let [{:keys [left right]} (specs (dec L))
                         need (+ 1 (if left 1 0) (if right 1 0))
                         sub  #(gen-sub disk ctr (dec L) % fill base)
                         pads #(vec (repeatedly (- own need) (fn [] (sub fill))))]
                     (when (>= own need)
                       ;; in key order: the keys come from one counter
                       (let [pl (when (= pad :left) (pads))
                             l  (when left (sub left))
                             c  (node (dec L))
                             r  (when (and c right) (sub right))
                             pr (when (and c (not= pad :left)) (pads))]
                         (when c
                           (gen-branch disk L (vec (concat pl (when l [l]) [c] (when r [r]) pr)) base))))))))
        root (node H)]
    (when root {:addr (:addr root) :leaf-keys @leaf-keys :n (:count root)})))

(defn- shape-cases
  "Every (level L, fill of the path node, fill of each neighbour or none) under each cascade
   from below, with the padding of L's parent on either side, and with every root size
   where L is the root's child."
  [bf H]
  (let [mn (quot bf 2) lens (range mn (inc bf)) sibs (cons nil lens)]
    (for [L (range 0 H), own lens, left sibs, right sibs
          cascade  (if (zero? L) [:none] [:split-first :split-last :join-left :join-right])
          pad      [:right :left]
          root-own (if (= L (dec H)) (range 2 (inc bf)) [(inc mn)])]
      (vec (for [i (range (inc H))]
             (assoc (cond (= i H) {:own root-own}
                          (= i L) {:own own :left left :right right}
                          (< i L) (case cascade
                                    ;; full nodes, so an insert splits all the way up to L:
                                    ;; along the first children, or along the last
                                    :split-first {:own bf :left nil :right mn}
                                    :split-last  {:own bf :left mn :right nil}
                                    ;; minimal nodes with one minimal neighbour, so a delete
                                    ;; joins all the way up to L, to that side
                                    :join-left   {:own mn :left mn :right nil}
                                    :join-right  {:own mn :left nil :right mn})
                          :else   {:own (inc mn) :left (inc mn) :right (inc mn)})
                    ;; `:pad` lays out a node's children: the cascade's levels, then L itself
                    :pad (cond (<= i L)      (if (= cascade :split-last) :left :right)
                               (= i (inc L)) pad
                               :else         :right)))))))

(defn- spine-ops
  "The writes tried at the path leaf: an insert before its first key, inside it, and after
   its last key (which lands in the next leaf where there is one); a delete of its first,
   middle and last key; a replace of its first and last."
  [ks]
  (let [f (first ks) l (peek ks) m (nth ks (quot (count ks) 2))]
    (distinct [[:add (dec f)] [:add (inc f)] [:add (inc l)]
               [:del f] [:del m] [:del l]
               [:rep f] [:rep l]])))

(defn- buffering-ops
  "The first writes of a two-write case: one insert, one delete, one replace — each a kind
   of edit a diff buffer can be left holding."
  [ks]
  [[:add (inc (first ks))] [:del (nth ks (quot (count ks) 2))] [:rep (first ks)]])

(defn- base [opts] {:branching-factor (:branching-factor opts) :diff-buf-size (:diff-buf-size opts)})

(defn- read-bound
  "What one write may read in a tree of `levels` levels, given what it did: its path; on a
   delete the two neighbours of every path node below the root; and for every branch that
   divided — a split, a borrow — the children of ONE of the two results, which hold at most
   three quarters of bf + 1 between a full neighbour and an underfull node. A replace is
   given one more: the suite runs with -ea, and the assertion that the replaced key has no
   equal neighbour across the leaf boundary reads the next leaf."
  [bf levels op evs]
  (let [divides (count (filter (fn [[k L]] (and L (pos? (long L)) (#{:split :borrow-left :borrow-right} k))) evs))]
    (+ (long levels)
       (case op :del (* 2 (dec (long levels))) :rep 1 0)
       (* divides (inc (quot (* 3 (long bf)) 4))))))

(defn- erase!
  "Take the count from the branches `which` selects: :all, :spine (on the path to `k`) or
   :off-spine."
  [disk addr which k]
  ((fn walk [a on?]
     (let [bl (@disk a)]
       (when-let [as (:addresses bl)]
         (when (case which :all true :spine on? :off-spine (not on?))
           (swap! disk assoc-in [a :subtree-count] -1))
         (let [ks (:keys bl)
               i  (or (first (keep-indexed (fn [i mk] (when (<= (compare k mk) 0) i)) ks))
                      (dec (count ks)))]
           (dotimes [j (count as)] (walk (nth as j) (and on? (= i j))))))))
   addr true))

(defn- leaf-addrs [disk] (set (filter #(nil? (:addresses (@disk %))) (keys @disk))))

(defn- buffered?
  "Does the tree stored at `addr` hold buffered edits?"
  [disk addr]
  (let [bl (@disk addr)]
    (boolean (or (seq (:slots bl)) (some #(buffered? disk %) (:addresses bl))))))

(defn- effect
  "[present' delta]: what `op` does to the set of keys present around the path leaf."
  [present [op k]]
  (case op
    :add (if (present k) [present 0] [(conj present k) 1])
    :del (if (present k) [(disj present k) -1] [present 0])
    :rep [present 0]))

(defn- record! [tot r op]
  (swap! tot (fn [t] (-> t
                         (update :steps inc)
                         (update :max-reads max (:reads r))
                         (update :events into (map #(vector op %) (:events r)))
                         (update :transitions into (:transitions r))))))

(defn- every-step
  "Run every case of `shape-cases`; `check` is called per step with the case, the step's
   input and result. With `second-step?` the first write is one of `buffering-ops`, and
   where it leaves buffered edits it is followed by every write, from the tree it stored. Returns {:trees :steps :max-reads :events :transitions}."
  [opts H {:keys [check prepare second-step?]}]
  (let [bf  (:branching-factor opts)
        mn  (quot bf 2)
        tot (atom {:trees 0 :steps 0 :max-reads 0 :events #{} :transitions #{}})]
    (doseq [specs (shape-cases bf H)]
      (let [disk (atom {})
            g    (gen-spine disk specs mn (base opts))]
        (when g
          (when prepare (prepare disk g))
          (let [before (shape disk (:addr g) opts)
                ops    (spine-ops (:leaf-keys g))]
            (swap! tot update :trees inc)
            (doseq [op1 (if second-step? (buffering-ops (:leaf-keys g)) ops)]
              (let [keep1 (set (keys @disk))
                    r1    (step disk (:addr g) opts before op1)
                    [present1 delta1] (effect (set (:leaf-keys g)) op1)]
                (record! tot r1 (first op1))
                (check {:specs specs :disk disk :g g :before before :ops [op1] :r r1 :delta delta1})
                ;; a second write, where the first one left buffered edits to write over
                (when (and second-step? (buffered? disk (:addr r1)))
                  (let [buffered true]
                    (doseq [op2 ops]
                      (let [keep2 (set (keys @disk))
                            r2    (step disk (:addr r1) opts (:shape r1) op2)]
                        (record! tot r2 (first op2))
                        (check {:specs specs :disk disk :g g :before (:shape r1) :ops [op1 op2] :r r2
                                :delta (second (effect present1 op2)) :input-buffered? buffered})
                        (swap! disk select-keys keep2)))))
                (swap! disk select-keys keep1)))))))
    @tot))

(defn- expected-events
  "Every kind of step at every level of a tree with branches at levels 1..H."
  [H]
  (set (concat [[:add [:in-place]] [:del [:in-place]] [:rep [:in-place]]
                [:add [:grow]] [:del [:collapse]]]
               (for [L (range 0 (inc H))] [:add [:split L]])
               (for [L (range 0 H)
                     e [:join-left :join-right :borrow-left :borrow-right]]
                 [:del [e L]]))))

(defn- expected-transitions
  "Every kind of length change at every level, followed at the level above by everything
   that can follow it: a split is absorbed or splits the parent (the root: grows the
   tree); a lost child is absorbed, or leaves the parent underfull to join either side or
   borrow from either side (the root: to collapse)."
  [H]
  (set (concat (for [L (range 0 H) a [:split :absorbed]] [:split L a])
               [[:split H :grow]]
               (for [j [:join-left :join-right]
                     L (range 0 (dec H))
                     a [:join-left :join-right :borrow-left :borrow-right :absorbed]]
                 [j L a])
               (for [j [:join-left :join-right] a [:collapse :absorbed]] [j (dec H) a]))))

(defn- missing [expected seen] (reduce disj expected seen))

(defn- counted-check
  "A fully counted tree stays fully counted, with no wrong count; it holds exactly the
   elements it should, in order, under branch keys that lead to them, in nodes of a valid
   size one level below their parent; a write that changed something stored a new root;
   and the write read no more than its bound, and the store nothing (without a diff
   buffer; with one a store may flush)."
  [opts]
  (let [bf (:branching-factor opts)]
    (fn [{:keys [specs ops r before delta]}]
      (let [sh    (:shape r)
            op    (peek ops)
            bound (read-bound bf (count (:levels before)) (first op) (:events r))
            want  (after-op (:content before) op)]
        (when-not (and (zero? (:unknown sh)) (zero? (:wrong sh))
                       (= (:n sh) (+ (:n before) delta))
                       (= (:content sh) want)
                       (zero? (:misplaced sh)) (zero? (:underfull sh)) (zero? (:overfull sh))
                       (zero? (:mislevelled sh))
                       ;; a write that is not a no-op stores a new root
                       (or (:new-root? r)
                           (and (zero? delta)
                                (not (and (= :rep (first op)) (some #{(second op)} (:content before))))))
                       (<= (:reads r) bound)
                       (or (pos? (:diff-buf-size opts)) (zero? (:store-reads r))))
          (is false (str "case " (pr-str specs) " ops " (pr-str ops) " events " (pr-str (:events r))
                         ": unknown " (:unknown sh) ", wrong " (:wrong sh) ", elements " (:n sh)
                         " from " (:n before) ", content as expected " (= (:content sh) want)
                         ", misplaced keys " (:misplaced sh) ", underfull " (:underfull sh)
                         ", overfull " (:overfull sh) ", mislevelled " (:mislevelled sh)
                         ", new root " (:new-root? r) ", reads " (:reads r) " (bound " bound
                         "), store reads " (:store-reads r))))))))

;; ---- Part 1: single cold writes, enumerated ----

(deftest every-single-cold-write-keeps-every-count
  ;; bf 4 is the smallest there is; 5 splits unevenly; 6 has a borrow of more than one
  (doseq [[bf H transient?] [[4 4 false] [4 4 true] [5 2 false] [6 3 false]]]
    (testing (str "bf " bf ", " (inc H) " levels, " (if transient? "transient" "persistent"))
      (let [opts {:comparator compare :branching-factor bf :diff-buf-size 0 :transient? transient?}
            r    (every-step opts H {:check (counted-check opts)})]
        (is (= #{} (missing (expected-events H) (:events r)))
            "every kind of step was taken at every level")
        (is (= #{} (missing (expected-transitions H) (:transitions r)))
            "and was followed at the level above by everything that can follow it")
        (is (pos? (:steps r)))))))

(deftest every-single-cold-write-on-a-root-that-is-a-leaf
  (doseq [bf [4 5 8] transient? [false true] size (range 1 (inc bf))]
    (let [opts   {:comparator compare :branching-factor bf :diff-buf-size 0 :transient? transient?}
          disk   (atom {})
          g      (gen-leaf disk (volatile! 0) size (base opts))
          before (shape disk (:addr g) opts)
          chk    (counted-check opts)]
      (doseq [op (spine-ops (:keys g))]
        (let [r (step disk (:addr g) opts before op)]
          (chk {:specs [:leaf-root bf size] :ops [op] :r r :before before
                :delta (second (effect (set (:keys g)) op))})
          (when (and (= size bf) (= :add (first op)))
            (is (= #{[:split 0] [:grow]} (:events r)) "a full leaf root splits and grows")))))))

(deftest every-cold-write-over-buffered-edits-keeps-every-count
  (testing "two writes in a row: the second starts from the tree the first one stored, which
            under a diff buffer holds buffered edits"
    (doseq [dbs [1 2 64]]
      (testing (str "diff-buf " dbs)
        (let [H    2
              opts {:comparator compare :branching-factor 4 :diff-buf-size dbs}
              seen (atom 0)
              chk  (counted-check opts)
              r    (every-step opts H {:second-step? true
                                       :check (fn [m]
                                                (when (:input-buffered? m) (swap! seen inc))
                                                (chk m))})]
          (is (pos? @seen) "precondition: some second steps started from buffered edits")
          (is (= #{} (missing (expected-events H) (:events r))))
          (is (= #{} (missing (expected-transitions H) (:transitions r)))))))))

(deftest every-single-cold-write-over-an-uncounted-tree
  (testing "a tree written by an earlier version — all of its counts missing, or those on the
            path to the path leaf, or those off it: a write never stores a WRONG count, and
            one recount + store leaves none missing and rewrites no leaf"
    (doseq [dbs   [0 2]
            which [:all :spine :off-spine]]
      (testing (str "diff-buf " dbs ", " (name which) " uncounted")
        (let [opts {:comparator compare :branching-factor 4 :diff-buf-size dbs}
              real (atom 0)
              r    (every-step
                    opts 2
                    {:prepare (fn [disk g] (erase! disk (:addr g) which (first (:leaf-keys g))))
                     :check
                     (fn [{:keys [specs ops disk r before delta]}]
                       (let [leaves (leaf-addrs disk)
                             cold   (set/restore-by compare (:addr r) (storage disk (atom 0)) opts)
                             addr'  (set/store (set/recount cold) (storage disk (atom 0)))
                             sh     (shape disk addr' opts)
                             want   (after-op (:content before) (peek ops))]
                         (when-not (zero? delta) (swap! real inc))
                         (when-not (and (zero? (:wrong (:shape r))) (zero? (:wrong sh))
                                        (zero? (:unknown sh))
                                        (= (:content (:shape r)) want) (= (:content sh) want)
                                        (zero? (:misplaced sh))
                                        (= leaves (leaf-addrs disk)))
                           (is false (str "case " (pr-str specs) " ops " (pr-str ops)
                                          ": wrong after the write " (:wrong (:shape r))
                                          ", after recount wrong " (:wrong sh) " unknown " (:unknown sh)
                                          ", content as expected " (= (:content sh) want)
                                          ", leaves written by recount "
                                          (count (remove leaves (leaf-addrs disk))))))))})]
          (is (pos? @real) "writes that changed the set were among the steps")
          (is (= #{} (missing (expected-events 2) (:events r)))))))))

(deftest every-single-cold-write-with-a-leaf-processor
  (testing "with a processor a write cannot assume it changed the count by one"
    (let [calls (atom 0)
          identity-processor (reify ILeafProcessor
                               (shouldProcess [_ _leaf-size _settings] true)
                               (processLeaf [_ entries _storage _settings] (swap! calls inc) entries))
          opts {:comparator compare :branching-factor 4 :diff-buf-size 0
                :leaf-processor identity-processor}
          r    (every-step opts 3 {:check (counted-check opts)})]
      (is (pos? @calls) "the processor ran")
      (is (= #{} (missing (expected-events 3) (:events r))))
      (is (= #{} (missing (expected-transitions 3) (:transitions r))))))
  (testing "and one that drops an element from the leaf it touches: no count is wrong or lost"
    (let [deltas   (atom #{})
          dropping (reify ILeafProcessor
                     (shouldProcess [_ leaf-size _settings] (> (long leaf-size) 5))
                     (processLeaf [_ entries _storage _settings]
                       (let [^List entries entries]
                         (java.util.ArrayList. (.subList entries 0 (dec (.size entries)))))))
          opts {:comparator compare :branching-factor 8 :diff-buf-size 0 :leaf-processor dropping}
          r    (every-step opts 2
                           {:check (fn [{:keys [specs ops r before]}]
                                     (let [sh (:shape r)]
                                       (swap! deltas conj (- (:n sh) (:n before)))
                                       (when-not (and (zero? (:unknown sh)) (zero? (:wrong sh))
                                                      (zero? (:misplaced sh)))
                                         (is false (str "case " (pr-str specs) " ops " (pr-str ops)
                                                        ": unknown " (:unknown sh) ", wrong " (:wrong sh)
                                                        ", misplaced keys " (:misplaced sh))))))})]
      (is (some #(< (long %) -1) @deltas)
          "precondition: some writes changed the number of elements by more than one")
      (is (pos? (:steps r))))))

;; ---- Part 2: content-defined trees ----
;;
;; Their shape follows the keys, so it cannot be written down by fill. Every single write
;; from trees built at random and from trees deleted down, at several sizes.

(defn- history-trees [n seed]
  (let [rnd (java.util.Random. (long seed))
        sh  (fn [c] (let [l (java.util.ArrayList. ^java.util.Collection (vec c))]
                      (java.util.Collections/shuffle l rnd)
                      (vec l)))]
    [{:build (sh (range 0 (* 2 n) 2))}
     {:build (sh (range 0 (* 6 n) 2)) :then-delete (drop n (sh (range 0 (* 6 n) 2)))}]))

(deftest every-single-cold-write-on-content-defined-trees
  (doseq [lz [2 3]]
    (testing (str "boundary level parameter " lz)
      (let [opts {:comparator compare :branching-factor 32 :diff-buf-size 0
                  :boundary (boundary/mst-boundary lz)}
            seen (atom #{})]
        (doseq [n [10 30 80 200 400]
                {:keys [build then-delete]} (history-trees n lz)]
          (let [disk (atom {})
                s    (reduce #(set/disj %1 %2 compare)
                             (reduce #(set/conj %1 %2 compare) (set/sorted-set* opts) build)
                             then-delete)
                ks   (vec (seq s))
                addr (set/store s (storage disk (atom 0)))
                before (shape disk addr opts)]
            (is (and (zero? (:unknown before)) (zero? (:wrong before))))
            (doseq [op (concat (map #(vector :add (dec (long %))) ks)
                               (map #(vector :del %) ks)
                               (map #(vector :rep %) ks))]
              (let [keep (set (keys @disk))
                    r    (step disk addr opts before op)
                    sh   (:shape r)]
                (swap! seen into (map #(vector (first op) ({:join-left :join :join-right :join} (first %) (first %)))
                                      (:events r)))
                (when-not (and (zero? (:unknown sh)) (zero? (:wrong sh))
                               (= (:content sh) (after-op (:content before) op))
                               (zero? (:misplaced sh)) (zero? (:mislevelled sh)))
                  (is false (str n " keys, " (pr-str op) " " (pr-str (:events r))
                                 ": unknown " (:unknown sh) ", wrong " (:wrong sh)
                                 ", content as expected " (= (:content sh) (after-op (:content before) op))
                                 ", misplaced keys " (:misplaced sh))))
                (swap! disk select-keys keep)))))
        (is (= #{} (missing #{[:add :split] [:add :grow] [:del :join] [:del :collapse]
                              [:add :in-place] [:del :in-place] [:rep :in-place]}
                            @seen)))))))

;; ---- Part 3: what depends on memory ----
;;
;; A sequence of writes, counts, stores and cold restores on one set. After every store the
;; stored tree is checked, and what `count-census` said just before the store is compared
;; with what the store wrote.

(def ^:private gen-step
  (gen/frequency
   [[6 (gen/tuple (gen/return :add) (gen/choose 0 400))]
    [4 (gen/tuple (gen/return :del) (gen/choose 0 400))]
    [2 (gen/tuple (gen/return :rep) (gen/choose 0 400))]
    [1 (gen/return [:count])]
    [1 (gen/tuple (gen/return :count-slice) (gen/choose 0 400))]
    [1 (gen/return [:store])]
    [1 (gen/return [:cold])]
    [1 (gen/return [:toggle])]]))

(defn- run-sequence
  "nil when the sequence kept the invariant, else a description of the first violation."
  [{:keys [bf dbs erased? init steps]}]
  (let [opts  {:comparator compare :branching-factor bf :diff-buf-size dbs}
        disk  (atom {})
        st    #(storage disk (atom 0))
        addr0 (set/store (reduce #(set/conj %1 %2 compare) (set/sorted-set* opts) init) (st))
        _     (when erased?
                (doseq [[a bl] @disk :when (:addresses bl)] (swap! disk assoc-in [a :subtree-count] -1)))
        check (fn [model addr]
                (let [sh (shape disk addr opts)]
                  (cond (pos? (:wrong sh))                        (str "wrong count: " (:wrong sh))
                        (and (not erased?) (pos? (:unknown sh)))  (str "missing count: " (:unknown sh))
                        (not= (:content sh) (vec model))          (str "content: " (:n sh) " elements vs " (count model))
                        (pos? (:misplaced sh))                    (str "misplaced keys: " (:misplaced sh)))))]
    (loop [s     (set/restore-by compare addr0 (st) opts)
           model (into (sorted-set) init)
           tr?   false
           steps steps]
      (if-let [[op k] (first steps)]
        (case op
          :add (recur (if tr? (conj! s k) (set/conj s k compare)) (conj model k) tr? (next steps))
          :del (recur (if tr? (disj! s k) (set/disj s k compare)) (disj model k) tr? (next steps))
          :rep (recur (if (contains? model k) (set/replace s k (Integer/valueOf (int k)) compare) s)
                      model tr? (next steps))
          :count (if (not= (count s) (count model))
                   (str "count " (count s) " vs " (count model))
                   (recur s model tr? (next steps)))
          :count-slice (if (not= (set/count-slice s nil k) (count (subseq model <= k)))
                         (str "count-slice to " k)
                         (recur s model tr? (next steps)))
          :toggle (recur (if tr? (persistent! s) (transient s)) model (not tr?) (next steps))
          (:store :cold)
          (let [census (:unknown (set/count-census s))
                addr   (set/store s (st))
                sh     (shape disk addr opts)]
            (or (check model addr)
                (when (not= census (:unknown sh))
                  (str "census before the store " census ", stored " (:unknown sh)))
                (recur (if (= op :cold)
                         (cond-> (set/restore-by compare addr (st) opts) tr? transient)
                         s)
                       model tr? (next steps)))))
        ;; the end: store, and for a tree that started uncounted, repair it
        (let [s    (if tr? (persistent! s) s)
              addr (set/store s (st))]
          (or (check model addr)
              (when erased?
                (let [addr' (set/store (set/recount (set/restore-by compare addr (st) opts)) (st))
                      sh    (shape disk addr' opts)]
                  (when (or (pos? (:unknown sh)) (pos? (:wrong sh)) (not= (:content sh) (vec model)))
                    (str "after recount: unknown " (:unknown sh) ", wrong " (:wrong sh)))))))))))

(deftest random-sequences-keep-every-count
  (let [p (prop/for-all [bf      (gen/elements [4 8])
                         dbs     (gen/elements [0 2 64])
                         erased? gen/boolean
                         init    (gen/vector-distinct (gen/choose 0 400) {:min-elements 0 :max-elements 250})
                         steps   (gen/vector gen-step 20 150)]
                        (nil? (run-sequence {:bf bf :dbs dbs :erased? erased? :init init :steps steps})))
        r (tc/quick-check 150 p :seed 20261123)]
    (is (:pass? r)
        (str "smallest failing case: " (pr-str (some-> r :shrunk :smallest))
             " -> " (some-> r :shrunk :smallest
                            ((fn [[bf dbs erased? init steps]]
                               (run-sequence {:bf bf :dbs dbs :erased? erased? :init init :steps steps}))))))))
