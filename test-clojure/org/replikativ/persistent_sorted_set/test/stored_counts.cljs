(ns org.replikativ.persistent-sorted-set.test.stored-counts
  "The ClojureScript twin of the JVM `stored-counts`.

   THE INVARIANT: every branch that is stored is stored with its element count. A branch
   that DIVIDES — a split, a borrow — needs its children's counts, and when those children
   were on disk both results were stored unknown; nothing on the write path heals that.

   The oracle reads the BLOBS. Both modes are pinned: the suite's default buffers, and under
   diff-buf a buffered write heals the child it passes through."
  (:require [cljs.test :as test :refer-macros [deftest testing is]]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.boundary :as boundary]
            [org.replikativ.persistent-sorted-set.branch :as branch :refer [Branch]]
            [org.replikativ.persistent-sorted-set.test.storage.util :as u]))

(defn- blob [disk a] (edn/read-string (@disk a)))

(defn- disk-census
  "What a restore of `addr` finds: {:branches n :unknown m :leaves #{addr}}. A parent's slot
   that carries a diff is PROJECTED onto the branch below it — count from the slot, slots
   from the slot's nested diff — superseding what that branch's own blob says."
  [disk addr]
  (let [acc (volatile! {:branches 0 :unknown 0 :leaves #{}})]
    ((fn walk [a entry]
       (let [{:keys [addresses level subtree-count] :as b} (blob disk a)
             projected? (and entry (some? (:diff entry)))]
         (if addresses
           (let [slots (if projected? (:diff entry) (:slots b))]
             (vswap! acc update :branches inc)
             (when (neg? (if projected? (:count entry) (or subtree-count -1)))
               (vswap! acc update :unknown inc))
             (dotimes [i (count addresses)]
               (walk (nth addresses i) (when (> level 1) (get slots i)))))
           (vswap! acc update :leaves conj a))))
     addr nil)
    @acc))

(defn- wrong-counts
  "Every branch blob whose stored count is not the number of elements below it. Only for a
   tree stored without buffers, where a blob is the whole truth about its subtree."
  [disk addr]
  (let [bad (volatile! [])]
    ((fn walk [a]
       (let [{:keys [addresses keys subtree-count]} (blob disk a)]
         (if addresses
           (let [n (reduce + 0 (map walk addresses))]
             (when (and (some? subtree-count) (>= subtree-count 0) (not= n subtree-count))
               (vswap! bad conj {:address a :stored subtree-count :actual n}))
             n)
           (count keys))))
     addr)
    @bad))

(defn- generations
  "`gens` rounds of (restore cold, ops, store), checking the invariant after every store."
  [bf dbs n gens ops-fn & [extra]]
  (let [opts (merge {:branching-factor bf :diff-buf-size dbs} extra)
        disk (atom {})
        st   #(u/storage (atom {}) disk opts)
        s0   (reduce s/conj (s/sorted-set* (assoc opts :storage (st))) (range 0 (* 2 n) 2))
        a0   (s/store s0 (st))]
    (is (zero? (:unknown (disk-census disk a0))) "a warm build stores every count")
    (loop [addr a0, gen 0, live (set (range 0 (* 2 n) 2))]
      (if (= gen gens)
        {:addr addr :live live :disk disk :opts opts}
        (let [cold  (s/restore addr (st) opts)
              ops   (ops-fn gen (vec (sort live)))
              s1    (reduce (fn [s [op k]] (if (= op :del) (s/disj s k) (s/conj s k))) cold ops)
              live' (reduce (fn [l [op k]] (if (= op :del) (disj l k) (conj l k))) live ops)
              addr' (s/store s1 (st))
              cen   (disk-census disk addr')
              back  (s/restore addr' (st) opts)]
          (is (zero? (:unknown cen))
              (str "generation " gen ": " (:unknown cen) " of " (:branches cen) " stored branches have no count"))
          (when (zero? dbs)
            (is (= [] (wrong-counts disk addr')) (str "generation " gen ": and no count is wrong")))
          (when (zero? (mod gen 4))
            (is (= {:branches (:branches cen) :unknown 0} (s/count-census back)))
            (is (= (count live') (count back)))
            (is (= (sort live') (vec (s/seq back)))))
          (recur addr' (inc gen) live'))))))

(defn- patterns [bf n]
  (let [per (* 12 bf)]
    {:append    (fn [gen sorted] (let [m (inc (peek sorted))] (map #(vector :add (+ m %)) (range per))))
     :scattered (fn [gen _] (map #(vector :add (inc (* 2 (mod (* 7919 (+ % (* gen per))) (* 2 n))))) (range per)))
     :mixed     (fn [gen sorted]
                  (concat (map #(vector :add (inc (* 2 (mod (* 7919 (+ % (* gen per))) (* 2 n))))) (range per))
                          (map #(vector :del %) (take (quot per 4) (take-nth (max 1 (quot (count sorted) (quot per 4))) sorted)))))
     :deletes   (fn [gen sorted] (map #(vector :del %) (take per (take-nth (max 1 (quot (count sorted) per)) sorted))))}))

(deftest every-stored-branch-keeps-its-count-through-cold-writes
  (doseq [dbs [0 64]
          bf  [8 16]
          :let [n (* 60 bf)]
          [label ops-fn] (patterns bf n)]
    (testing (str "bf " bf ", diff-buf " dbs ", " (name label))
      (generations bf dbs n 12 ops-fn))))

(defn- erase-counts!
  "Rewrite every branch blob without its own count, the state an earlier version left
   behind. Slot counts are left alone: they were always written."
  [disk]
  (swap! disk (fn [m] (into {} (map (fn [[a s]] [a (str/replace s #":subtree-count \d+" ":subtree-count -1")])) m))))

(deftest recount-restores-every-count-and-stores-it
  (doseq [dbs [0 64]
          bf  [8 16]]
    (testing (str "bf " bf ", diff-buf " dbs)
      (let [n (* 80 bf)
            {:keys [addr live disk opts]} (generations bf dbs n 3 (:mixed (patterns bf n)))
            st     #(u/storage (atom {}) disk opts)
            _      (erase-counts! disk)
            cen0   (disk-census disk addr)
            cold   (s/restore addr (st) opts)]
        (is (pos? (:unknown cen0)))
        (is (= (:unknown cen0) (:unknown (s/count-census cold))) "the census sees what is stored")
        (is (= (count live) (count cold)))
        (let [fixed (s/recount cold)
              addr' (s/store fixed (st))
              cen1  (disk-census disk addr')
              back  (s/restore addr' (st) opts)]
          (is (not (identical? cold fixed)))
          (is (zero? (:unknown cen1)) "every stored branch has its count")
          (is (= (:branches cen0) (:branches cen1)) "same structure")
          (is (= (:leaves cen0) (:leaves cen1)) "no leaf was rewritten")
          (is (= (sort live) (vec (s/seq back))))
          (is (= (count live) (count back)))
          (is (= {:branches (:branches cen1) :unknown 0} (s/count-census back)))
          (is (identical? back (s/recount back)) "nothing left to do")
          (testing "and writes keep it that way"
            (let [s2    (reduce s/conj back (map #(inc (* 2 %)) (range 0 n 3)))
                  addr2 (s/store s2 (st))]
              (is (zero? (:unknown (disk-census disk addr2)))))))))))

(deftest recount-edges
  (let [s0 (into (s/sorted-set* {:branching-factor 8 :diff-buf-size 0}) (range 1000))]
    (is (identical? s0 (s/recount s0)))
    (is (zero? (:unknown (s/count-census s0)))))
  (let [s0 (into (s/sorted-set* {:branching-factor 8 :diff-buf-size 0}) (range 3))]
    (is (identical? s0 (s/recount s0)))
    (is (= {:branches 0 :unknown 0} (s/count-census s0)))))

(deftest a-content-defined-tree-keeps-every-count
  (testing "MST mode cuts where the keys say; its split divides a count like any other"
    (let [bf 32 n 1500]
      (generations bf 0 n 10 (:scattered (patterns 16 n)) {:boundary (boundary/mst-boundary 3)})
      (generations bf 0 n 10 (:mixed (patterns 16 n)) {:boundary (boundary/mst-boundary 3)}))))

(deftest a-buffered-write-over-an-uncounted-tree-stores-what-it-knows
  (testing "a deposit works out the count of the child it passes; the child must be written
            with it, and a warm census must say what a cold one does"
    (let [bf 8 n 900
          {:keys [addr disk opts]} (generations bf 64 n 2 (:mixed (patterns bf n)))
          st    #(u/storage (atom {}) disk opts)
          _     (erase-counts! disk)
          cold  (s/restore addr (st) opts)
          warm  (reduce s/conj cold (map #(inc (* 2 %)) (range 0 n 7)))
          addr' (s/store warm (st))
          want  (:unknown (disk-census disk addr'))]
      (is (= want (:unknown (s/count-census (s/restore addr' (st) opts)))))
      (is (= want (:unknown (s/count-census warm)))))))

(deftest a-root-that-is-promoted-is-not-changed-for-the-set-it-came-from
  (testing "in a content-defined tree a delete can empty one side of the root, and the other
            child becomes the root AS IT IS — a node the original set still reaches at its
            old address. The new set must not write its count into that node."
    (let [bf    32
          opts  {:branching-factor bf :diff-buf-size 0 :boundary (boundary/mst-boundary 3)}
          disk  (atom {})
          st    #(u/storage (atom {}) disk opts)
          build (fn [n] (reduce s/conj (s/sorted-set* (assoc opts :storage (st))) (range n)))
          shape (fn [n] (let [a (s/store (build n) (st)) b (blob disk a)] [(:level b) (count (:addresses b))]))
          n     (first (filter #(let [[lvl len] (shape %)] (and (>= lvl 2) (= 2 len))) (range 200 4000 7)))
          _     (is (some? n) "precondition: a tree whose root has two children")
          addr  (s/store (build n) (st))
          _     (erase-counts! disk)
          _     (swap! disk update addr str/replace-first ":subtree-count -1" (str ":subtree-count " n))
          want  (:unknown (disk-census disk addr))
          level0 (:level (blob disk addr))
          cache (atom {})                        ; ONE node object per address, for both sets
          orig  (s/restore addr (u/storage cache disk opts) opts)
          shrunk (loop [s' orig, k (dec n)]
                   (if (or (neg? k)
                           (let [a (s/store s' (st))] (< (:level (blob disk a)) level0)))
                     s'
                     (recur (s/disj s' k) (dec k))))]
      (is (pos? want))
      (is (not (identical? orig shrunk)))
      (is (= want (:unknown (s/count-census orig)))
          "the original set still sees its own blobs as they are")
      (let [addr' (s/store (s/recount orig) (st))]
        (is (zero? (:unknown (disk-census disk addr'))))))))

(defn- async-round-trip []
  (async
   (let [bf 8 n 700
         {:keys [addr live disk opts]} (generations bf 64 n 3 (:mixed (patterns bf n)))
         ast   #(u/async-storage (atom {}) disk opts)
         a     {:sync? false}
         _     (erase-counts! disk)
         want  (:unknown (disk-census disk addr))
         cold  (s/restore addr (ast) opts)
         _     (is (pos? want))
         _     (is (= want (:unknown (await (s/count-census cold a)))))
         fixed (await (s/recount cold a))
         addr' (await (s/store fixed (ast) a))
         back  (s/restore addr' (ast) opts)
         _     (is (zero? (:unknown (disk-census disk addr'))))
         _     (is (= 0 (:unknown (await (s/count-census back a)))))
         ;; cold writes through async storage keep it that way
         s2    (loop [s' back, ks (map #(inc (* 2 %)) (range 0 n 5))]
                 (if (seq ks)
                   (recur (await (s/conj s' (first ks) compare a)) (next ks))
                   s'))
         s3    (loop [s' s2, ks (take 60 (sort live))]
                 (if (seq ks)
                   (recur (await (s/disj s' (first ks) compare a)) (next ks))
                   s'))
         addr2 (await (s/store s3 (ast) a))]
     (is (zero? (:unknown (disk-census disk addr2))))
     (is (= (:unknown (disk-census disk addr2))
            (:unknown (await (s/count-census (s/restore addr2 (ast) opts) a))))))))

(deftest the-async-arm-keeps-and-repairs-counts
  (test/async done
              ((async-round-trip)
               (fn [_] (done))
               (fn [err] (is (nil? err)) (js/console.error err) (done)))))

;; ===========================================================================
;; EVERY SINGLE COLD WRITE, ENUMERATED — every single cold write,
;; enumerated. See the JVM `stored-counts` for the argument — the invariant is a one-step property,
;; what a step does at each level is decided by the fill of the node on the path and of its
;; two neighbours, and trees are written down by shape because trees built by a history of
;; inserts do not reach every case.
;;
;; Smaller than the JVM run (one branching factor, one height per test) since the code under
;; test is a different implementation of the same algorithm, not a different algorithm.
;; ===========================================================================

;; ---- the oracle: a cold restore, every branch's count against the elements below it ----

(defn- shape
  "{:levels {level [node lengths, left to right]} :unknown :wrong :n, and what makes the
   tree sound beyond its counts: :content (the elements in tree order), :misplaced (branch
   keys that are not their child's largest key), :underfull / :overfull (nodes other than
   the root outside [bf/2, bf])."
  [disk addr opts]
  (let [st   (u/storage (atom {}) disk opts)
        set  (s/restore addr st opts)
        _    (s/count-census set)                       ; loads the root
        bf   (:branching-factor opts)
        mn   (quot bf 2)
        root (.-root set)
        acc  (volatile! {:levels (sorted-map) :unknown 0 :wrong 0 :misplaced 0 :underfull 0 :overfull 0})
        out  (volatile! (transient []))
        ;; returns [elements below, largest key below]
        walk (fn walk [n]
               (let [ks (.-keys n) len (alength ks)]
                 (when-not (identical? n root)
                   (cond (< len mn) (vswap! acc update :underfull inc)
                         (> len bf) (vswap! acc update :overfull inc)))
                 (if (instance? Branch n)
                   (let [kids (mapv #(walk (branch/child n st % {:sync? true})) (range len))
                         c    (reduce + 0 (map first kids))]
                     (vswap! acc update-in [:levels (.-level n)] (fnil conj []) len)
                     (dotimes [i len]
                       (when-not (= (aget ks i) (second (kids i))) (vswap! acc update :misplaced inc)))
                     (cond (neg? (.-subtree-count n))   (vswap! acc update :unknown inc)
                           (not= c (.-subtree-count n)) (vswap! acc update :wrong inc))
                     [c (second (peek kids))])
                   (do (vswap! acc update-in [:levels 0] (fnil conj []) len)
                       (dotimes [i len] (vswap! out conj! (aget ks i)))
                       [len (when (pos? len) (aget ks (dec len)))]))))
        total (first (walk root))]
    (assoc @acc :n total :content (persistent! @out))))

(defn- after-op [content [op k]]
  (case op
    :add (vec (into (apply sorted-set content) [k]))
    :del (vec (remove #(= % k) content))
    :rep content))

(defn- path-ordinals [disk addr k]
  (let [seen (volatile! {}) out (volatile! {})]
    ((fn walk [a on-path?]
       (let [bl  (blob disk a)
             L   (or (:level bl) 0)
             ord (get @seen L 0)]
         (vswap! seen assoc L (inc ord))
         (when on-path? (vswap! out assoc L ord))
         (when-let [as (:addresses bl)]
           (let [ks (vec (:keys bl))
                 i  (or (first (keep-indexed (fn [i mk] (when (<= k mk) i)) ks)) (dec (count ks)))]
             (dotimes [j (count as)] (walk (nth as j) (and on-path? (= i j))))))))
     addr true)
    @out))

(defn- events [before after path]
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

(defn- step [disk addr opts before [op k]]
  (let [st    (u/storage (atom {}) disk opts)
        cold  (s/restore addr st opts)
        r0    (:reads @u/*stats)
        s'    (case op
                :add (s/conj cold k)
                :del (s/disj cold k)
                :rep (s/replace cold k k))
        used  (- (:reads @u/*stats) r0)
        addr' (s/store s' (u/storage (atom {}) disk opts))
        after (shape disk addr' opts)]
    {:addr addr' :reads used :shape after
     :events (events before after (path-ordinals disk addr k))}))

(defn- step-async
  "`step` through the asynchronous arm: async storage, every write and the store awaited."
  [disk addr opts before [op k]]
  (async
   (let [a     {:sync? false}
         cold  (s/restore addr (u/async-storage (atom {}) disk opts) opts)
         r0    (:reads @u/*stats)
         s'    (case op
                 :add (await (s/conj cold k compare a))
                 :del (await (s/disj cold k compare a))
                 :rep (await (s/replace cold k k compare a)))
         used  (- (:reads @u/*stats) r0)
         addr' (await (s/store s' (u/async-storage (atom {}) disk opts) a))
         after (shape disk addr' opts)]
     {:addr addr' :reads used :shape after
      :events (events before after (path-ordinals disk addr k))})))

;; ---- trees written down by shape ----

(defn- put! [disk m]
  (let [a (random-uuid)] (swap! disk assoc a (pr-str m)) a))

(defn- gen-leaf [disk ctr len]
  (let [ks (vec (repeatedly len #(vswap! ctr + 2)))]
    {:addr (put! disk {:level 0 :keys (to-array ks) :addresses nil :subtree-count nil :measure nil})
     :count len :max (peek ks) :keys ks}))

(defn- gen-branch [disk level kids]
  (let [n (reduce + (map :count kids))]
    {:addr (put! disk {:level level :keys (to-array (map :max kids)) :addresses (to-array (map :addr kids))
                       :subtree-count n :measure nil :slots nil})
     :count n :max (:max (peek kids))}))

(defn- gen-sub [disk ctr level len fill]
  (if (zero? level)
    (gen-leaf disk ctr len)
    (gen-branch disk level (vec (repeatedly len #(gen-sub disk ctr (dec level) fill fill))))))

(defn- gen-spine [disk specs fill]
  (let [ctr (volatile! 0) leaf-keys (volatile! nil)
        H    (dec (count specs))
        node (fn node [L]
               (let [{:keys [own]} (specs L)]
                 (if (zero? L)
                   (let [l (gen-leaf disk ctr own)] (vreset! leaf-keys (:keys l)) l)
                   (let [{:keys [left right]} (specs (dec L))
                         need (+ 1 (if left 1 0) (if right 1 0))]
                     (when (>= own need)
                       (let [l (when left (gen-sub disk ctr (dec L) left fill))
                             c (node (dec L))]
                         (when c
                           (let [r   (when right (gen-sub disk ctr (dec L) right fill))
                                 pad (vec (repeatedly (- own need) #(gen-sub disk ctr (dec L) fill fill)))]
                             (gen-branch disk L (vec (concat (when l [l]) [c] (when r [r]) pad)))))))))))
        root (node H)]
    (when root {:addr (:addr root) :leaf-keys @leaf-keys :n (:count root)})))

(defn- shape-cases [bf H]
  (let [mn (quot bf 2) lens (range mn (inc bf)) sibs (cons nil lens)]
    (for [L (range 0 H), own lens, left sibs, right sibs
          incoming [:split :underflow]
          root-own (if (= L (dec H)) (range 2 (inc bf)) [(inc mn)])]
      (vec (for [i (range (inc H))]
             (cond (= i H) {:own root-own}
                   (= i L) {:own own :left left :right right}
                   (< i L) (if (= incoming :split)
                             {:own bf :left nil :right mn}
                             {:own mn :left mn :right nil})
                   :else   {:own (inc mn) :left (inc mn) :right (inc mn)}))))))

(defn- spine-ops [ks]
  (let [f (first ks) l (peek ks) m (nth ks (quot (count ks) 2))]
    (distinct [[:add (dec f)] [:add (inc f)] [:add (inc l)]
               [:del f] [:del m] [:del l]
               [:rep f] [:rep l]])))

(defn- effect [present [op k]]
  (case op
    :add (if (present k) [present 0] [(conj present k) 1])
    :del (if (present k) [(disj present k) -1] [present 0])
    :rep [present 0]))

(defn- erase-all! [disk]
  (swap! disk (fn [m] (into {} (map (fn [[a s']] [a (str/replace s' #":subtree-count \d+" ":subtree-count -1")])) m))))

(defn- leaf-addrs [disk] (set (filter #(nil? (:addresses (blob disk %))) (keys @disk))))

(defn- every-step
  [opts H {:keys [check prepare second-step?]}]
  (let [bf  (:branching-factor opts)
        mn  (quot bf 2)
        tot (volatile! {:trees 0 :steps 0 :events #{}})
        rec (fn [r op] (vswap! tot #(-> % (update :steps inc) (update :events into (map (fn [e] [op e]) (:events r))))))]
    (doseq [specs (shape-cases bf H)]
      (let [disk (atom {})
            g    (gen-spine disk specs mn)]
        (when g
          (when prepare (prepare disk))
          (let [before (shape disk (:addr g) opts)
                ops    (spine-ops (:leaf-keys g))]
            (vswap! tot update :trees inc)
            (doseq [op1 ops]
              (let [keep1 (set (keys @disk))
                    r1    (step disk (:addr g) opts before op1)
                    [present1 delta1] (effect (set (:leaf-keys g)) op1)]
                (if-not second-step?
                  (do (rec r1 (first op1))
                      (check {:specs specs :disk disk :before before :ops [op1] :r r1 :delta delta1}))
                  (doseq [op2 ops]
                    (let [keep2 (set (keys @disk))
                          r2    (step disk (:addr r1) opts (:shape r1) op2)]
                      (rec r2 (first op2))
                      (check {:specs specs :disk disk :before (:shape r1) :ops [op1 op2] :r r2
                              :delta (second (effect present1 op2))})
                      (swap! disk select-keys keep2))))
                (swap! disk select-keys keep1)))))))
    @tot))

(defn- every-step-async
  "`every-step` with each write going through `step-async`."
  [opts H {:keys [check second-step?]}]
  (async
   (let [bf  (:branching-factor opts)
         mn  (quot bf 2)
         tot (volatile! {:trees 0 :steps 0 :events #{}})
         rec (fn [r op] (vswap! tot #(-> % (update :steps inc) (update :events into (map (fn [e] [op e]) (:events r))))))]
     (loop [cases (shape-cases bf H)]
       (when (seq cases)
         (let [specs (first cases)
               disk  (atom {})
               g     (gen-spine disk specs mn)]
           (when g
             (let [before (shape disk (:addr g) opts)
                   ops    (vec (spine-ops (:leaf-keys g)))
                   base   (set (keys @disk))]
               (vswap! tot update :trees inc)
               (loop [i 0]
                 (when (< i (count ops))
                   (let [op1 (ops i)
                         r1  (await (step-async disk (:addr g) opts before op1))
                         [present1 delta1] (effect (set (:leaf-keys g)) op1)]
                     (if-not second-step?
                       (do (rec r1 (first op1))
                           (check {:specs specs :disk disk :before before :ops [op1] :r r1 :delta delta1}))
                       (let [keep2 (set (keys @disk))]
                         (loop [j 0]
                           (when (< j (count ops))
                             (let [op2 (ops j)
                                   r2  (await (step-async disk (:addr r1) opts (:shape r1) op2))]
                               (rec r2 (first op2))
                               (check {:specs specs :disk disk :before (:shape r1) :ops [op1 op2] :r r2
                                       :delta (second (effect present1 op2))})
                               (swap! disk select-keys keep2)
                               (recur (inc j)))))))
                     (swap! disk select-keys base)
                     (recur (inc i)))))))
           (recur (rest cases)))))
     @tot)))

(defn- expected-events [H]
  (set (concat [[:add [:in-place]] [:del [:in-place]] [:rep [:in-place]]
                [:add [:grow]] [:del [:collapse]]]
               (for [L (range 0 (inc H))] [:add [:split L]])
               (for [L (range 0 H)
                     e [:join :borrow-left :borrow-right]]
                 [:del [e L]]))))

(defn- counted-check [bf levels]
  ;; the path, two neighbours a level on a delete, and one half of one node's children
  (let [bound (+ levels (* 2 (dec levels)) (quot (inc bf) 2) 1)]
    (fn [{:keys [specs ops r before delta]}]
      (let [sh (:shape r)]
        (when-not (and (zero? (:unknown sh)) (zero? (:wrong sh))
                       (= (:n sh) (+ (:n before) delta))
                       (= (:content sh) (after-op (:content before) (peek ops)))
                       (zero? (:misplaced sh)) (zero? (:underfull sh)) (zero? (:overfull sh))
                       (<= (:reads r) bound))
          (is false (str "case " (pr-str specs) " ops " (pr-str ops) " events " (pr-str (:events r))
                         ": unknown " (:unknown sh) ", wrong " (:wrong sh) ", elements " (:n sh)
                         " from " (:n before) ", content as expected "
                         (= (:content sh) (after-op (:content before) (peek ops)))
                         ", misplaced keys " (:misplaced sh) ", underfull " (:underfull sh)
                         ", overfull " (:overfull sh) ", reads " (:reads r) " (bound " bound ")")))))))

(deftest every-single-cold-write-keeps-every-count
  (let [H 3
        opts {:branching-factor 4 :diff-buf-size 0}
        r (every-step opts H {:check (counted-check 4 (inc H))})]
    (is (= #{} (reduce disj (expected-events H) (:events r)))
        "every kind of step was taken at every level")
    (is (pos? (:steps r)))))

(deftest every-cold-write-over-buffered-edits-keeps-every-count
  (testing "two writes in a row: the second starts from a tree that holds buffered edits"
    (let [H 2
          opts {:branching-factor 4 :diff-buf-size 2}
          seen (volatile! 0)
          chk  (counted-check 4 (inc H))
          r (every-step opts H {:second-step? true
                                :check (fn [{:keys [disk] :as m}]
                                         (when (some #(seq (:slots (blob disk %))) (keys @disk)) (vswap! seen inc))
                                         (chk m))})]
      (is (pos? @seen) "precondition: some second steps started from buffered edits")
      (is (= #{} (reduce disj (expected-events H) (:events r)))))))

(deftest every-single-cold-write-over-an-uncounted-tree
  (testing "no count is ever wrong, and one recount + store leaves none missing and rewrites
            no leaf"
    (doseq [dbs [0 2]]
      (let [opts {:branching-factor 4 :diff-buf-size dbs}
            r (every-step
               opts 2
               {:prepare erase-all!
                :check (fn [{:keys [specs ops disk r]}]
                         (let [leaves (leaf-addrs disk)
                               st     #(u/storage (atom {}) disk opts)
                               addr'  (s/store (s/recount (s/restore (:addr r) (st) opts)) (st))
                               sh     (shape disk addr' opts)]
                           (when-not (and (zero? (:wrong (:shape r))) (zero? (:wrong sh))
                                          (zero? (:unknown sh)) (= (:n sh) (:n (:shape r)))
                                          (= leaves (leaf-addrs disk)))
                             (is false (str "diff-buf " dbs " case " (pr-str specs) " ops " (pr-str ops)
                                            ": wrong " (:wrong (:shape r)) ", after recount wrong "
                                            (:wrong sh) " unknown " (:unknown sh))))))})]
        (is (pos? (:steps r)))))))

(deftest every-single-cold-write-on-content-defined-trees
  (let [opts {:branching-factor 32 :diff-buf-size 0 :boundary (boundary/mst-boundary 2)}
        seen (volatile! #{})]
    (doseq [n [10 30 80 200]]
      (let [disk (atom {})
            st   #(u/storage (atom {}) disk opts)
            ;; a fixed scatter of the even numbers below 2n
            ks   (vec (sort (distinct (map #(* 2 (mod (* 7919 %) n)) (range n)))))
            s0   (reduce s/conj (s/sorted-set* (assoc opts :storage (st))) (map #(nth ks (mod (* 31 %) (count ks))) (range (count ks))))
            addr (s/store s0 (st))
            before (shape disk addr opts)]
        (is (and (zero? (:unknown before)) (zero? (:wrong before))))
        (doseq [op (concat (map #(vector :add (dec %)) ks)
                           (map #(vector :del %) ks))]
          (let [keep (set (keys @disk))
                r    (step disk addr opts before op)
                sh   (:shape r)]
            (vswap! seen into (map #(vector (first op) (first %)) (:events r)))
            (when-not (and (zero? (:unknown sh)) (zero? (:wrong sh))
                           (= (:n sh) (+ (:n before) (case (first op) :add 1 :del -1))))
              (is false (str n " keys, " (pr-str op) " " (pr-str (:events r))
                             ": unknown " (:unknown sh) ", wrong " (:wrong sh))))
            (swap! disk select-keys keep)))))
    (is (= #{} (reduce disj #{[:add :split] [:del :join] [:add :in-place] [:del :in-place]} @seen)))))

;; ---- the asynchronous arm ----
;;
;; Every function that reads storage exists twice, once per `:sync?`, and the two are
;; separate code after macro expansion. Measured before these tests existed, the whole
;; suite called `conj` asynchronously once, `disj` twice and `replace` never.

(deftest every-single-cold-write-keeps-every-count-async
  (test/async done
              (let [H 3 opts {:branching-factor 4 :diff-buf-size 0}]
                ((every-step-async opts H {:check (counted-check 4 (inc H))})
                 (fn [r]
                   (is (= #{} (reduce disj (expected-events H) (:events r)))
                       "every kind of step was taken at every level")
                   (is (pos? (:steps r)))
                   (done))
                 (fn [err] (is (nil? err)) (js/console.error err) (done))))))

(deftest every-cold-write-over-buffered-edits-keeps-every-count-async
  (test/async done
              (let [H 2 opts {:branching-factor 4 :diff-buf-size 2}
                    seen (volatile! 0)
                    chk  (counted-check 4 (inc H))]
                ((every-step-async opts H {:second-step? true
                                           :check (fn [{:keys [disk] :as m}]
                                                    (when (some #(seq (:slots (blob disk %))) (keys @disk)) (vswap! seen inc))
                                                    (chk m))})
                 (fn [r]
                   (is (pos? @seen) "precondition: some second steps started from buffered edits")
                   (is (= #{} (reduce disj (expected-events H) (:events r))))
                   (done))
                 (fn [err] (is (nil? err)) (js/console.error err) (done))))))
