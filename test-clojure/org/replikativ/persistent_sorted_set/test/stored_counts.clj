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
                      :anywhere (take rem-per (shuffle (seq model)))
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
          shared (caching-storage disk)
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
            and the half itself must carry it too, or a warm census and a cold one disagree.
            The root, which nothing deposits for, takes its count when it is written."
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
        (is (>= (long (:subtree-count (@disk addr') -1)) 0) "the root was written with its count")
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
