(ns org.replikativ.persistent-sorted-set.test.stored-counts-exhaustive
  "Every single cold write, enumerated — the inductive step of `stored-counts`' invariant.

   THE INVARIANT is a one-step property: a stored tree in which every branch has its count,
   plus ONE write, stored again, is a tree in which every branch has its count and none is
   wrong. If that holds for every write from every tree, it holds for every sequence. So for
   cold writes there is nothing to sample: the steps can be listed.

   WHAT DECIDES A STEP is local. At each level a write does one of a few things — updates
   in place, splits, joins a neighbour, borrows from one — and which one depends on four
   facts only: what came up from the child (nothing, a split, a lost child), how full the
   node on the path is, and how full its left and right neighbours under the same parent
   are. `shape-cases` lists every combination of the three fills (every length a node can
   have, and `nil` for no neighbour) at every level, once under a cascade of splits from
   below and once under a cascade of joins, with every root size.

   WHY THE TREES ARE BUILT BY SHAPE and not by a history of inserts: measured, every single
   write (5356 of them) from trees built ascending, descending, at random and by deleting a
   larger tree down, at seven sizes, never grew the root, never split above level 1 and
   never borrowed from the right. The stored blobs are plain maps, so a tree of any valid
   shape can be written down directly.

   The oracle is a cold restore: every branch's count against the elements below it. A
   classifier reads off what each step did (from the node lengths per level before and
   after, and the path), and each test ends by asserting that every kind of step was seen
   at every level — so a change that makes a case unreachable fails here instead of quietly
   covering less.

   Against the code before the fix (origin/main at 136f837), the first test fails on 40
   steps at branching factor 4 and on 2612 at 8: every one a branch split or a branch
   borrow.

   What is NOT listed here is what depends on memory rather than on the stored tree — a
   count a reader worked out, a transient stored half way, a node two sets share. Those are
   the directed tests of `stored-counts`, and `random-sequences-keep-every-count` below."
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

;; ---- storage: blobs are the maps `nodes/node->blob` gives, restores are counted ----

(def ^:private ctx (nodes/reader-context {}))

(defn- storage [disk reads]
  (reify IStorage
    (store [_ node]
      (let [a (str (java.util.UUID/randomUUID))]
        (swap! disk assoc a (nodes/node->blob node))
        a))
    (accessed [_ _] nil)
    (restore [_ a]
      (swap! reads inc)
      (let [blob (@disk a)]
        (if (:addresses blob) (nodes/blob->branch ctx blob) (nodes/blob->leaf ctx blob))))))

;; ---- the oracle ----

(defn- shape
  "The stored tree as a cold restore finds it: {:levels {level [node lengths, left to right]}
   :unknown branches without a count, :wrong branches whose count is not the number of
   elements below them, :n elements, :content the elements in tree order, :misplaced
   branch keys that are not the largest key of their child, :underfull / :overfull nodes
   other than the root outside [bf/2, bf]}."
  [disk addr opts]
  (let [st   (storage disk (atom 0))
        s    (set/restore-by compare addr st opts)
        bf   (long (:branching-factor opts))
        mn   (quot bf 2)
        root (.root ^PersistentSortedSet s)
        acc  (volatile! {:levels (sorted-map) :unknown 0 :wrong 0 :misplaced 0 :underfull 0 :overfull 0})
        out  (volatile! (transient []))
        fill (fn [^ANode n]
               (when-not (identical? n root)
                 (cond (< (.len n) mn) (vswap! acc update :underfull inc)
                       (> (.len n) bf) (vswap! acc update :overfull inc))))
        ;; returns [elements below, largest key below]
        walk (fn walk [^ANode n]
               (fill n)
               (if (instance? Branch n)
                 (let [^Branch br n
                       ^List ks (.keys n)
                       kids (mapv #(walk (.child br st (int %))) (range (.len n)))
                       c    (reduce + 0 (map first kids))]
                   (vswap! acc update-in [:levels (.level br)] (fnil conj []) (.len n))
                   (dotimes [i (.len n)]
                     (when-not (= (.get ks (int i)) (second (kids i))) (vswap! acc update :misplaced inc)))
                   (cond (neg? (.subtreeCount br))     (vswap! acc update :unknown inc)
                         (not= c (.subtreeCount br))   (vswap! acc update :wrong inc))
                   [c (second (peek kids))])
                 (let [^List ks (.keys n)]
                   (vswap! acc update-in [:levels 0] (fnil conj []) (.len n))
                   (dotimes [i (.len n)] (vswap! out conj! (.get ks (int i))))
                   [(.len n) (when (pos? (.len n)) (.get ks (int (dec (.len n)))))])))
        total (first (walk root))]
    (assoc @acc :n total :content (persistent! @out))))

(defn- sound?
  "The tree holds exactly `expected`, in order, under keys that lead to it."
  [sh expected]
  (and (= (:content sh) expected) (zero? (:misplaced sh))))

(defn- after-op
  "The content a tree with `content` holds after `op`."
  [content [op k]]
  (case op
    :add (vec (into (apply sorted-set content) [k]))
    :del (vec (remove #(= % k) content))
    :rep content))

(defn- path-ordinals
  "Per level, the left-to-right position (within its level) of the node on the path to `k`."
  [disk addr k]
  (let [seen (volatile! {}) out (volatile! {})]
    ((fn walk [a on-path?]
       (let [bl  (@disk a)
             L   (or (:level bl) 0)
             ord (get @seen L 0)]
         (vswap! seen assoc L (inc ord))
         (when on-path? (vswap! out assoc L ord))
         (when-let [as (:addresses bl)]
           (let [ks (:keys bl)
                 i  (or (first (keep-indexed (fn [i mk] (when (<= (compare k mk) 0) i)) ks))
                        (dec (count ks)))]
             (dotimes [j (count as)] (walk (nth as j) (and on-path? (= i j))))))))
     addr true)
    @out))

(defn- events
  "What one step did, read off the two shapes and the path:
   #{[:grow] [:collapse] [:split L] [:join L] [:borrow-left L] [:borrow-right L] [:in-place]}."
  [before after path]
  (let [la (:levels before) lb (:levels after)
        ha (count la) hb (count lb)
        ev (volatile! #{})]
    (cond (> hb ha) (vswap! ev conj [:grow])
          (< hb ha) (vswap! ev conj [:collapse]))
    (doseq [L (range 0 (min ha hb))
            :let [x (la L) y (lb L) p (path L)]]
      (cond (> (count y) (count x)) (vswap! ev conj [:split L])
            (< (count y) (count x)) (vswap! ev conj [:join L])
            :else
            (do (when (and p (pos? p) (< (y (dec p)) (x (dec p))))
                  (vswap! ev conj [:borrow-left L]))
                (when (and p (< (inc p) (count x)) (< (y (inc p)) (x (inc p))))
                  (vswap! ev conj [:borrow-right L])))))
    (if (empty? @ev) #{[:in-place]} @ev)))

;; ---- one cold write ----

(defn- step
  "Restore `addr` cold, do one write, store. {:addr :events :reads :shape}; `:reads` are the
   restores of the write itself, not of the store."
  [disk addr opts before [op k]]
  (let [reads (atom 0)
        cold  (set/restore-by compare addr (storage disk reads) opts)
        tr?   (:transient? opts)
        s     (case op
                :add (if tr? (persistent! (conj! (transient cold) k)) (set/conj cold k compare))
                :del (if tr? (persistent! (disj! (transient cold) k)) (set/disj cold k compare))
                ;; an equal but distinct key object, so that the replace is a write
                :rep (set/replace cold k (Integer/valueOf (int k)) compare))
        used  @reads
        addr' (set/store s (storage disk reads))
        after (shape disk addr' opts)]
    {:addr addr' :reads used :shape after
     :events (events before after (path-ordinals disk addr k))}))

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
  "A subtree at `level` whose root has `len` children and everything below `fill`."
  [disk ctr level len fill base]
  (if (zero? level)
    (gen-leaf disk ctr len base)
    (gen-branch disk level (vec (repeatedly len #(gen-sub disk ctr (dec level) fill fill base))) base)))

(defn- gen-spine
  "`specs` is indexed by level: {:own n :left n|nil :right n|nil} is the node on the path
   and its two neighbours; the last entry is the root, {:own n}. The other nodes are at
   minimum fill. Keys are the even numbers, so every odd number is a gap. nil when a level
   is too small to hold the children the level below asks for."
  [disk specs fill base]
  (let [ctr (volatile! 0) leaf-keys (volatile! nil)
        H    (dec (count specs))
        node (fn node [L]
               (let [{:keys [own]} (specs L)]
                 (if (zero? L)
                   (let [l (gen-leaf disk ctr own base)] (vreset! leaf-keys (:keys l)) l)
                   (let [{:keys [left right]} (specs (dec L))
                         need (+ 1 (if left 1 0) (if right 1 0))]
                     (when (>= own need)
                       (let [l (when left (gen-sub disk ctr (dec L) left fill base))
                             c (node (dec L))]
                         (when c
                           (let [r   (when right (gen-sub disk ctr (dec L) right fill base))
                                 pad (vec (repeatedly (- own need) #(gen-sub disk ctr (dec L) fill fill base)))]
                             (gen-branch disk L (vec (concat (when l [l]) [c] (when r [r]) pad)) base)))))))))
        root (node H)]
    (when root {:addr (:addr root) :leaf-keys @leaf-keys :n (:count root)})))

(defn- shape-cases
  "Every (level, fill of the path node, fill of each neighbour or none) under each cascade
   from below, with every root size where the level under test is the root's child."
  [bf H]
  (let [mn (quot bf 2) lens (range mn (inc bf)) sibs (cons nil lens)]
    (for [L (range 0 H), own lens, left sibs, right sibs
          incoming [:split :underflow]
          root-own (if (= L (dec H)) (range 2 (inc bf)) [(inc mn)])]
      (vec (for [i (range (inc H))]
             (cond (= i H) {:own root-own}
                   (= i L) {:own own :left left :right right}
                   ;; below: full nodes, so an insert splits all the way up to L …
                   (< i L) (if (= incoming :split)
                             {:own bf :left nil :right mn}
                             ;; … or minimal nodes with a minimal neighbour, so a delete joins
                             {:own mn :left mn :right nil})
                   :else   {:own (inc mn) :left (inc mn) :right (inc mn)}))))))

(defn- spine-ops
  "The writes tried at the path leaf: an insert before, inside and after it; a delete of its
   first, middle and last key; a replace of its first and last."
  [ks]
  (let [f (first ks) l (peek ks) m (nth ks (quot (count ks) 2))]
    (distinct [[:add (dec f)] [:add (inc f)] [:add (inc l)]
               [:del f] [:del m] [:del l]
               [:rep f] [:rep l]])))

(defn- base [opts]
  (cond-> {:branching-factor (:branching-factor opts) :diff-buf-size (:diff-buf-size opts)}))

(defn- read-bound
  "The path, the two neighbours of each path node on a delete, and the children of ONE of
   the two nodes a split or a borrow leaves (at most half of bf + 1 of them)."
  [bf levels]
  (+ levels (* 2 (dec levels)) (quot (inc bf) 2) 1))

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
                         (update :events into (map #(vector op %) (:events r)))))))

(defn- every-step
  "Run every case of `shape-cases`; `check` is called per step with the case, the step's
   input and result. Returns {:trees :steps :max-reads :events}."
  [opts H {:keys [check prepare second-step?]}]
  (let [bf  (:branching-factor opts)
        mn  (quot bf 2)
        tot (atom {:trees 0 :steps 0 :max-reads 0 :events #{}})]
    (doseq [specs (shape-cases bf H)]
      (let [disk (atom {})
            g    (gen-spine disk specs mn (base opts))]
        (when g
          (when prepare (prepare disk g))
          (let [before (shape disk (:addr g) opts)
                ops    (spine-ops (:leaf-keys g))]
            (swap! tot update :trees inc)
            (doseq [op1 ops]
              (let [keep1 (set (keys @disk))
                    r1    (step disk (:addr g) opts before op1)
                    [present1 delta1] (effect (set (:leaf-keys g)) op1)]
                (if-not second-step?
                  (do (record! tot r1 (first op1))
                      (check {:specs specs :disk disk :g g :before before :ops [op1] :r r1 :delta delta1}))
                  ;; the second write starts from what the first one stored — under diff-buf
                  ;; a tree that holds buffered edits
                  (doseq [op2 ops]
                    (let [keep2 (set (keys @disk))
                          r2    (step disk (:addr r1) opts (:shape r1) op2)]
                      (record! tot r2 (first op2))
                      (check {:specs specs :disk disk :g g :before (:shape r1) :ops [op1 op2] :r r2
                              :delta (second (effect present1 op2))})
                      (swap! disk select-keys keep2))))
                (swap! disk select-keys keep1)))))))
    @tot))

(defn- expected-events
  "Every kind of step at every level of a tree with branches at levels 1..H."
  [H]
  (set (concat [[:add [:in-place]] [:del [:in-place]] [:rep [:in-place]]
                [:add [:grow]] [:del [:collapse]]]
               (for [L (range 0 (inc H))] [:add [:split L]])
               (for [L (range 0 H)
                     e [:join :borrow-left :borrow-right]]
                 [:del [e L]]))))

(defn- counted-check
  "A fully counted tree stays fully counted, with no wrong count; it holds exactly the
   elements it should, in order, under branch keys that lead to them, in nodes that are
   neither underfull nor overfull; and the write read no more than its bound."
  [opts H]
  (let [bound (read-bound (:branching-factor opts) (inc H))]
    (fn [{:keys [specs ops r before delta]}]
      (let [sh (:shape r)]
        (when-not (and (zero? (:unknown sh)) (zero? (:wrong sh))
                       (= (:n sh) (+ (:n before) delta))
                       (sound? sh (after-op (:content before) (peek ops)))
                       (zero? (:underfull sh)) (zero? (:overfull sh))
                       (<= (:reads r) bound))
          (is false (str "case " (pr-str specs) " ops " (pr-str ops) " events " (pr-str (:events r))
                         ": unknown " (:unknown sh) ", wrong " (:wrong sh) ", elements " (:n sh)
                         " from " (:n before) ", content as expected "
                         (= (:content sh) (after-op (:content before) (peek ops)))
                         ", misplaced keys " (:misplaced sh) ", underfull " (:underfull sh)
                         ", overfull " (:overfull sh) ", reads " (:reads r) " (bound " bound ")")))))))

;; ---- Part 1: single cold writes, enumerated ----

(deftest every-single-cold-write-keeps-every-count
  (doseq [[bf H] [[4 4] [8 3]]
          transient? [false true]]
    (testing (str "bf " bf ", " (inc H) " levels, " (if transient? "transient" "persistent"))
      (let [opts {:comparator compare :branching-factor bf :diff-buf-size 0 :transient? transient?}
            r    (every-step opts H {:check (counted-check opts H)})]
        (is (= #{} (reduce disj (expected-events H) (:events r)))
            "every kind of step was taken at every level")
        (is (pos? (:steps r)))))))

(deftest every-cold-write-over-buffered-edits-keeps-every-count
  (testing "two writes in a row: the second starts from the tree the first one stored, which
            under a diff buffer holds buffered edits"
    (doseq [dbs [1 2 64]]
      (testing (str "diff-buf " dbs)
        (let [H    3
              opts {:comparator compare :branching-factor 4 :diff-buf-size dbs}
              seen (atom 0)
              chk  (counted-check opts H)
              r    (every-step opts H {:second-step? true
                                       :check (fn [{:keys [disk] :as m}]
                                                (when (some :slots (vals @disk)) (swap! seen inc))
                                                (chk m))})]
          (is (pos? @seen) "precondition: some second steps started from buffered edits")
          (is (= #{} (reduce disj (expected-events H) (:events r)))))))))

(deftest every-single-cold-write-over-an-uncounted-tree
  (testing "a tree written by an earlier version — all of its counts missing, or those on the
            path, or those off it: a write never stores a WRONG count, and one recount +
            store leaves none missing and rewrites no leaf"
    (doseq [dbs   [0 2]
            which [:all :spine :off-spine]]
      (testing (str "diff-buf " dbs ", " (name which) " uncounted")
        (let [opts {:comparator compare :branching-factor 4 :diff-buf-size dbs}
              r    (every-step
                    opts 3
                    {:prepare (fn [disk g] (erase! disk (:addr g) which (first (:leaf-keys g))))
                     :check
                     (fn [{:keys [specs ops disk r]}]
                       (let [leaves (leaf-addrs disk)
                             cold   (set/restore-by compare (:addr r) (storage disk (atom 0)) opts)
                             addr'  (set/store (set/recount cold) (storage disk (atom 0)))
                             sh     (shape disk addr' opts)]
                         (when-not (and (zero? (:wrong (:shape r))) (zero? (:wrong sh))
                                        (zero? (:unknown sh)) (= (:n sh) (:n (:shape r)))
                                        (= leaves (leaf-addrs disk)))
                           (is false (str "case " (pr-str specs) " ops " (pr-str ops)
                                          ": wrong after the write " (:wrong (:shape r))
                                          ", after recount wrong " (:wrong sh) " unknown " (:unknown sh)
                                          ", leaves written by recount "
                                          (count (remove leaves (leaf-addrs disk))))))))})]
          (is (pos? (:steps r))))))))

(deftest every-single-cold-write-with-a-leaf-processor
  (testing "with a processor a write cannot assume it changed the count by one"
    (let [identity-processor (reify ILeafProcessor
                               (shouldProcess [_ _leaf-size _settings] true)
                               (processLeaf [_ entries _storage _settings] entries))
          opts {:comparator compare :branching-factor 4 :diff-buf-size 0
                :leaf-processor identity-processor}
          r    (every-step opts 3 {:check (counted-check opts 3)})]
      (is (= #{} (reduce disj (expected-events 3) (:events r))))))
  (testing "and one that drops an element from the leaf it touches: no count is wrong or lost"
    (let [dropping (reify ILeafProcessor
                     (shouldProcess [_ leaf-size _settings] (> (long leaf-size) 5))
                     (processLeaf [_ entries _storage _settings]
                       (let [^List entries entries]
                         (java.util.ArrayList. (.subList entries 0 (dec (.size entries)))))))
          opts {:comparator compare :branching-factor 8 :diff-buf-size 0 :leaf-processor dropping}
          r    (every-step opts 2
                           {:check (fn [{:keys [specs ops r]}]
                                     (let [sh (:shape r)]
                                       (when-not (and (zero? (:unknown sh)) (zero? (:wrong sh)))
                                         (is false (str "case " (pr-str specs) " ops " (pr-str ops)
                                                        ": unknown " (:unknown sh) ", wrong " (:wrong sh))))))})]
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
                (swap! seen into (map #(vector (first op) (first %)) (:events r)))
                (when-not (and (zero? (:unknown sh)) (zero? (:wrong sh))
                               (= (:n sh) (+ (:n before) (case (first op) :add 1 :del -1 0))))
                  (is false (str n " keys, " (pr-str op) " " (pr-str (:events r))
                                 ": unknown " (:unknown sh) ", wrong " (:wrong sh))))
                (swap! disk select-keys keep)))))
        (is (= #{} (reduce disj #{[:add :split] [:add :grow] [:del :join] [:del :collapse]
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
        check (fn [s model addr]
                (let [sh (shape disk addr opts)]
                  (cond (pos? (:wrong sh))                        (str "wrong count: " (:wrong sh))
                        (and (not erased?) (pos? (:unknown sh)))  (str "missing count: " (:unknown sh))
                        (not= (:n sh) (count model))              (str "elements " (:n sh) " vs " (count model)))))]
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
            (or (check s model addr)
                (when (not= census (:unknown sh))
                  (str "census before the store " census ", stored " (:unknown sh)))
                (recur (if (= op :cold)
                         (cond-> (set/restore-by compare addr (st) opts) tr? transient)
                         s)
                       model tr? (next steps)))))
        ;; the end: store, and for a tree that started uncounted, repair it
        (let [s    (if tr? (persistent! s) s)
              addr (set/store s (st))]
          (or (check s model addr)
              (when erased?
                (let [addr' (set/store (set/recount (set/restore-by compare addr (st) opts)) (st))
                      sh    (shape disk addr' opts)]
                  (when (or (pos? (:unknown sh)) (pos? (:wrong sh)) (not= (:n sh) (count model)))
                    (str "after recount: unknown " (:unknown sh) ", wrong " (:wrong sh)))))))))))

(deftest random-sequences-keep-every-count
  (let [p (prop/for-all [bf      (gen/elements [4 8])
                         dbs     (gen/elements [0 2 64])
                         erased? gen/boolean
                         init    (gen/vector-distinct (gen/choose 0 400) {:min-elements 0 :max-elements 250})
                         steps   (gen/vector gen-step 0 150)]
                        (nil? (run-sequence {:bf bf :dbs dbs :erased? erased? :init init :steps steps})))
        r (tc/quick-check 150 p :seed 20261123)]
    (is (:pass? r)
        (str "smallest failing case: " (pr-str (some-> r :shrunk :smallest))
             " -> " (some-> r :shrunk :smallest
                            ((fn [[bf dbs erased? init steps]]
                               (run-sequence {:bf bf :dbs dbs :erased? erased? :init init :steps steps}))))))))
