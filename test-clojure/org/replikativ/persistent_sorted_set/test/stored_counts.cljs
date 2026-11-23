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
;; EVERY SINGLE COLD WRITE, ENUMERATED — the ClojureScript twin of the second half of the
;; JVM `stored-counts`; the argument is spelled out there. The invariant is a one-step
;; property; what a step does at each level is decided by what came up from the child and
;; by the fill of the node on the path and of its two neighbours; and trees are written
;; down by shape because trees built by a history of inserts do not reach every case.
;;
;; A smaller grid than on the JVM, since this is a second implementation of one algorithm —
;; but run twice, synchronously and asynchronously: every function that reads storage
;; exists once per `:sync?`, and the two are separate code after macro expansion.
;; ===========================================================================

;; ---- the oracle: a cold restore ----

(defn- shape
  "{:levels {level [node lengths, left to right]} :unknown :wrong :n, and what makes the
   tree sound beyond its counts: :content (the elements in tree order), :misplaced (branch
   keys that are not their child's largest key), :underfull / :overfull (nodes outside
   [bf/2, bf]; the root: a branch of fewer than two children, or more than bf),
   :mislevelled (children that are not one level below their parent)."
  [disk addr opts]
  (let [st   (u/storage (atom {}) disk opts)
        set  (s/restore addr st opts)
        _    (s/count-census set)                       ; loads the root
        bf   (:branching-factor opts)
        mn   (quot bf 2)
        root (.-root set)
        acc  (volatile! {:levels (sorted-map) :unknown 0 :wrong 0 :misplaced 0 :underfull 0
                         :overfull 0 :mislevelled 0})
        out  (volatile! (transient []))
        ;; returns [elements below, largest key below, level]
        walk (fn walk [n]
               (let [ks (.-keys n) len (alength ks) br? (instance? Branch n)]
                 (cond (> len bf)          (vswap! acc update :overfull inc)
                       (identical? n root) (when (and br? (< len 2)) (vswap! acc update :underfull inc))
                       (< len mn)          (vswap! acc update :underfull inc))
                 (if br?
                   (let [kids (mapv #(walk (branch/child n st % {:sync? true})) (range len))
                         c    (reduce + 0 (map first kids))]
                     (vswap! acc update-in [:levels (.-level n)] (fnil conj []) len)
                     (dotimes [i len]
                       (when-not (= (aget ks i) (second (kids i))) (vswap! acc update :misplaced inc))
                       (when-not (= (nth (kids i) 2) (dec (.-level n))) (vswap! acc update :mislevelled inc)))
                     (cond (neg? (.-subtree-count n))   (vswap! acc update :unknown inc)
                           (not= c (.-subtree-count n)) (vswap! acc update :wrong inc))
                     [c (second (peek kids)) (.-level n)])
                   (do (vswap! acc update-in [:levels 0] (fnil conj []) len)
                       (dotimes [i len] (vswap! out conj! (aget ks i)))
                       [len (when (pos? len) (aget ks (dec len))) 0]))))
        total (first (walk root))]
    (assoc @acc :n total :content (persistent! @out))))

(defn- after-op [content [op k]]
  (case op
    :add (vec (into (apply sorted-set content) [k]))
    :del (vec (remove #(= % k) content))
    :rep content))

(defn- path-ordinals
  "Per level, the left-to-right position of the node on the path to `k`, in the tree a
   restore finds (under a diff buffer, not the tree the blobs spell out)."
  [disk addr opts before k]
  (let [st  (u/storage (atom {}) disk opts)
        set (s/restore addr st opts)
        _   (s/count-census set)]
    (loop [n (.-root set), ord 0, out {}]
      (if (instance? Branch n)
        (let [L   (.-level n)
              ks  (.-keys n)
              len (alength ks)
              i   (or (first (filter #(<= k (aget ks %)) (range len))) (dec len))]
          (recur (branch/child n st i {:sync? true})
                 (+ (reduce + 0 (take ord ((:levels before) L))) i)
                 (assoc out L ord)))
        (assoc out 0 ord)))))

(defn- events
  "#{[:grow] [:collapse] [:split L] [:join-left L] [:join-right L] [:borrow-left L]
     [:borrow-right L]}, or #{[:in-place]}."
  [before after path]
  (let [la (:levels before) lb (:levels after)
        ha (count la) hb (count lb)
        ev (volatile! #{})]
    (cond (> hb ha) (vswap! ev conj [:grow])
          (< hb ha) (vswap! ev conj [:collapse]))
    (doseq [L (range 0 (min ha hb))
            :let [x (la L) y (lb L) p (path L)]]
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
  "#{[kind L above]}: for every level that changed its parent's length, what the parent did."
  [before evs]
  (let [top  (dec (count (:levels before)))
        kind (into {} (for [[k L] evs :when L] [L k]))]
    (set (for [[L k] kind :when (#{:split :join-left :join-right} k)]
           [k L (cond (and (= k :split) (= L top))                                     :grow
                      (and (not= k :split) (= L (dec top)) (contains? evs [:collapse])) :collapse
                      :else (get kind (inc L) :absorbed))]))))

(defn- edge
  "[first? last?]: is the path node at level L its parent's first child, its last?"
  [before path L]
  (let [p (path L) top (dec (count (:levels before)))]
    (if (= L top)
      [true true]
      (let [pp    (path (inc L))
            lens  ((:levels before) (inc L))
            start (reduce + 0 (take pp lens))]
        [(= p start) (= p (+ start (dec (lens pp))))]))))

(defn- policy-violations
  "The rebalance a node chose against the one it must choose: only a node left underfull
   rebalances at all, and it joins
   its left neighbour if the two fit in one node, else its right one if those fit, else
   borrows from the fuller of the two, the left one when they are equal. Without this a
   borrow from the wrong side passes every other check."
  [bf before path evs]
  (for [[k L] evs
        :when (and L (#{:join-left :join-right :borrow-left :borrow-right} k))
        :let [x  ((:levels before) L)
              p  (path L)
              [first? last?] (edge before path L)
              c  (dec (x p))
              fits-left  (and (not first?) (<= (+ (x (dec p)) c) bf))
              fits-right (and (not last?) (<= (+ c (x (inc p))) bf))
              left-fuller (and (not first?) (or last? (>= (x (dec p)) (x (inc p)))))]
        :when (or (>= c (quot bf 2))            ; not underfull: nothing to rebalance
                  (case k
                    :join-left    (not fits-left)
                    :join-right   (or fits-left (not fits-right))
                    :borrow-left  (or fits-left fits-right (not left-fuller))
                    :borrow-right (or fits-left fits-right last? left-fuller)))]
    [k L]))

(defn- result [disk addr addr' opts before k reads store-reads]
  (let [after (shape disk addr' opts)
        path  (path-ordinals disk addr opts before k)
        evs   (events before after path)]
    {:addr addr' :reads reads :store-reads store-reads :shape after :events evs :path path
     :transitions (transitions before evs) :new-root? (not= addr addr')}))

(defn- step [disk addr opts before [op k]]
  (let [cold  (s/restore addr (u/storage (atom {}) disk opts) opts)
        r0    (:reads @u/*stats)
        s'    (case op
                :add (s/conj cold k)
                :del (s/disj cold k)
                :rep (s/replace cold k k))
        r1    (:reads @u/*stats)
        addr' (s/store s' (u/storage (atom {}) disk opts))]
    (result disk addr addr' opts before k (- r1 r0) (- (:reads @u/*stats) r1))))

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
         r1    (:reads @u/*stats)
         addr' (await (s/store s' (u/async-storage (atom {}) disk opts) a))]
     (result disk addr addr' opts before k (- r1 r0) (- (:reads @u/*stats) r1)))))

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

(defn- gen-sub
  "A subtree whose root has `len` children; below it the nodes alternate between `fill` and
   `fill` + 1 children, so that neighbouring subtrees hold different numbers of elements."
  [disk ctr level len fill bf]
  (if (zero? level)
    (gen-leaf disk ctr len)
    (gen-branch disk level
                (mapv #(gen-sub disk ctr (dec level) (min bf (+ fill (mod % 2))) fill bf) (range len)))))

(defn- gen-spine
  "`specs` by level: {:own n :left n|nil :right n|nil :pad :left|:right}, the last the root.
   A node's other children are padding at minimum fill on its `:pad` side."
  [disk specs fill bf]
  (let [ctr (volatile! 0) leaf-keys (volatile! nil)
        H    (dec (count specs))
        node (fn node [L]
               (let [{:keys [own pad]} (specs L)]
                 (if (zero? L)
                   (let [l (gen-leaf disk ctr own)] (vreset! leaf-keys (:keys l)) l)
                   (let [{:keys [left right]} (specs (dec L))
                         need (+ 1 (if left 1 0) (if right 1 0))
                         sub  #(gen-sub disk ctr (dec L) % fill bf)
                         npad (- own need)
                         ;; `:pad` is a side, or the number of padding children on the left
                         nl   (cond (= pad :left) npad (number? pad) (min npad pad) :else 0)
                         pads #(vec (repeatedly % (fn [] (sub fill))))]
                     (when (>= own need)
                       ;; in key order: the keys come from one counter
                       (let [pl (pads nl)
                             l  (when left (sub left))
                             c  (node (dec L))
                             r  (when (and c right) (sub right))
                             pr (when c (pads (- npad nl)))]
                         (when c
                           (gen-branch disk L (vec (concat pl (when l [l]) [c] (when r [r]) pr))))))))))
        root (node H)]
    (when root {:addr (:addr root) :leaf-keys @leaf-keys :n (:count root)})))

(defn- shape-cases [bf H]
  (let [mn (quot bf 2) lens (range mn (inc bf)) sibs (cons nil lens)]
    (for [L (range 0 H), own lens, left sibs, right sibs
          cascade  (if (zero? L) [:none] [:split-first :split-mid :split-last :join-left :join-right])
          pad      [:right :left]
          root-own (if (= L (dec H)) (range 2 (inc bf)) [(inc mn)])]
      (vec (for [i (range (inc H))]
             (assoc (cond (= i H) {:own root-own}
                          (= i L) {:own own :left left :right right}
                          (< i L) (case cascade
                                    :split-first {:own bf :left nil :right mn}
                                    :split-last  {:own bf :left mn :right nil}
                                    :split-mid   {:own bf :left mn :right mn}
                                    :join-left   {:own mn :left mn :right nil}
                                    :join-right  {:own mn :left nil :right mn})
                          :else   {:own (inc mn) :left (inc mn) :right (inc mn)})
                    :pad (cond (<= i L)      (case cascade
                                               :split-last :left
                                               ;; the path child one before the cut, so that
                                               ;; its two halves fall one on each side
                                               :split-mid  (- (quot (inc bf) 2) 2)
                                               :right)
                               (= i (inc L)) pad
                               :else         :right)))))))

(defn- spine-ops [ks]
  (let [f (first ks) l (peek ks) m (nth ks (quot (count ks) 2))]
    (distinct [[:add (dec f)] [:add (inc f)] [:add (inc l)]
               [:del f] [:del m] [:del l]
               [:rep f] [:rep l]])))

(defn- buffering-ops
  "The first writes of a two-write case: an insert, a delete, a replace in the path leaf,
   and an insert into the leaf before it and a delete from the one after it — so that a
   neighbour the second write weighs is not the size its blob says."
  [ks]
  (let [f (first ks) l (peek ks)]
    [[:add (inc f)] [:del (nth ks (quot (count ks) 2))] [:rep f]
     [:add (- f 3)] [:del (+ l 2)]]))

(defn- neighbour-write?
  "Did the first write of a two-write case go into a leaf next to the path leaf, and the
   second then rebalance the path leaf? Then the neighbour it weighed was not the size its
   blob says."
  [{:keys [ops r leaf-keys input-buffered?]}]
  (let [[_ k1] (first ops)]
    (boolean (and input-buffered? (= 2 (count ops))
                  (or (< k1 (dec (first leaf-keys))) (> k1 (peek leaf-keys)))
                  (some (fn [[k L]] (and (= 0 L) (#{:join-left :join-right :borrow-left :borrow-right} k)))
                        (:events r))))))

(defn- effective?
  "A write that did nothing must not stand in for an in-place write."
  [present [op k] delta]
  (or (not (zero? delta)) (and (= op :rep) (contains? present k))))

(defn- effect [present [op k]]
  (case op
    :add (if (present k) [present 0] [(conj present k) 1])
    :del (if (present k) [(disj present k) -1] [present 0])
    :rep [present 0]))

(defn- erase-all! [disk]
  (swap! disk (fn [m] (into {} (map (fn [[a s']] [a (str/replace s' #":subtree-count \d+" ":subtree-count -1")])) m))))

(defn- leaf-addrs [disk] (set (filter #(nil? (:addresses (blob disk %))) (keys @disk))))

(defn- buffered? [disk addr]
  (let [bl (blob disk addr)]
    (boolean (or (seq (:slots bl)) (some #(buffered? disk %) (:addresses bl))))))

(defn- every-step
  [opts H {:keys [check prepare second-step?]}]
  (let [bf  (:branching-factor opts)
        mn  (quot bf 2)
        tot (volatile! {:trees 0 :steps 0 :events #{} :transitions #{} :events2 #{} :transitions2 #{}})
        rec (fn [r op ev tr eff?] (vswap! tot #(cond-> (update % :steps inc)
                                                 eff? (-> (update ev into (map (fn [e] [op e]) (:events r)))
                                                          (update tr into (:transitions r))))))]
    (doseq [specs (shape-cases bf H)]
      (let [disk (atom {})
            g    (gen-spine disk specs mn bf)]
        (when g
          (when prepare (prepare disk))
          (let [before (shape disk (:addr g) opts)
                ops    (spine-ops (:leaf-keys g))]
            (vswap! tot update :trees inc)
            (doseq [op1 (if second-step? (buffering-ops (:leaf-keys g)) ops)]
              (let [keep1 (set (keys @disk))
                    r1    (step disk (:addr g) opts before op1)
                    present0 (set (:content before))
                    [present1 delta1] (effect present0 op1)]
                (rec r1 (first op1) :events :transitions (effective? present0 op1 delta1))
                (check {:specs specs :disk disk :before before :ops [op1] :r r1 :delta delta1})
                ;; a second write, where the first one left buffered edits to write over
                (when (and second-step? (buffered? disk (:addr r1)))
                  (let [buffered true]
                    (doseq [op2 ops]
                      (let [keep2 (set (keys @disk))
                            r2    (step disk (:addr r1) opts (:shape r1) op2)]
                        (rec r2 (first op2) :events2 :transitions2 (effective? present1 op2 (second (effect present1 op2))))
                        (check {:specs specs :disk disk :before (:shape r1) :ops [op1 op2] :r r2
                                :delta (second (effect present1 op2)) :input-buffered? buffered
                                :leaf-keys (:leaf-keys g)})
                        (swap! disk select-keys keep2)))))
                (swap! disk select-keys keep1)))))))
    @tot))

(defn- every-step-async
  "`every-step` with each write going through `step-async`."
  [opts H {:keys [check second-step?]}]
  (async
   (let [bf  (:branching-factor opts)
         mn  (quot bf 2)
         tot (volatile! {:trees 0 :steps 0 :events #{} :transitions #{} :events2 #{} :transitions2 #{}})
         rec (fn [r op ev tr eff?] (vswap! tot #(cond-> (update % :steps inc)
                                                  eff? (-> (update ev into (map (fn [e] [op e]) (:events r)))
                                                           (update tr into (:transitions r))))))]
     (loop [cases (shape-cases bf H)]
       (when (seq cases)
         (let [specs (first cases)
               disk  (atom {})
               g     (gen-spine disk specs mn bf)]
           (when g
             (let [before (shape disk (:addr g) opts)
                   ops    (vec (spine-ops (:leaf-keys g)))
                   ops1   (if second-step? (buffering-ops (:leaf-keys g)) ops)
                   base   (set (keys @disk))]
               (vswap! tot update :trees inc)
               (loop [i 0]
                 (when (< i (count ops1))
                   (let [op1 (ops1 i)
                         r1  (await (step-async disk (:addr g) opts before op1))
                         present0 (set (:content before))
                         [present1 delta1] (effect present0 op1)]
                     (rec r1 (first op1) :events :transitions (effective? present0 op1 delta1))
                     (check {:specs specs :disk disk :before before :ops [op1] :r r1 :delta delta1})
                     (when (and second-step? (buffered? disk (:addr r1)))
                       (let [keep2    (set (keys @disk))
                             buffered true]
                         (loop [j 0]
                           (when (< j (count ops))
                             (let [op2 (ops j)
                                   r2  (await (step-async disk (:addr r1) opts (:shape r1) op2))]
                               (rec r2 (first op2) :events2 :transitions2 (effective? present1 op2 (second (effect present1 op2))))
                               (check {:specs specs :disk disk :before (:shape r1) :ops [op1 op2] :r r2
                                       :delta (second (effect present1 op2)) :input-buffered? buffered
                                       :leaf-keys (:leaf-keys g)})
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
                     e [:join-left :join-right :borrow-left :borrow-right]]
                 [:del [e L]]))))

(defn- expected-transitions [H]
  (set (concat (for [L (range 0 H) a [:split :absorbed]] [:split L a])
               [[:split H :grow]]
               (for [j [:join-left :join-right]
                     L (range 0 (dec H))
                     a [:join-left :join-right :borrow-left :borrow-right :absorbed]]
                 [j L a])
               (for [j [:join-left :join-right] a [:collapse :absorbed]] [j (dec H) a]))))

(defn- missing [expected seen] (reduce disj expected seen))

(defn- read-bound
  "The path; on a delete the two neighbours of every path node below the root; and for
   every branch that divided, the children of one of the two results."
  [bf levels op evs]
  (let [at-branch (fn [kinds] (count (filter (fn [[k L]] (and L (pos? L) (kinds k))) evs)))]
    (+ levels
       (if (= op :del) (* 2 (dec levels)) 0)
       (* (at-branch #{:split}) (quot (inc bf) 2))
       (* (at-branch #{:borrow-left :borrow-right}) (inc (quot (* 3 bf) 4))))))

(defn- counted-check [opts]
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
                       (or (pos? (:diff-buf-size opts)) (zero? (:store-reads r)))
                       (empty? (policy-violations bf before (:path r) (:events r))))
          (is false (str "case " (pr-str specs) " ops " (pr-str ops) " events " (pr-str (:events r))
                         ": unknown " (:unknown sh) ", wrong " (:wrong sh) ", elements " (:n sh)
                         " from " (:n before) ", content as expected " (= (:content sh) want)
                         ", misplaced keys " (:misplaced sh) ", underfull " (:underfull sh)
                         ", overfull " (:overfull sh) ", mislevelled " (:mislevelled sh)
                         ", new root " (:new-root? r) ", reads " (:reads r) " (bound " bound
                         "), store reads " (:store-reads r) ", rebalanced against the policy "
                         (pr-str (policy-violations bf before (:path r) (:events r))))))))))

(deftest every-single-cold-write-keeps-every-count
  (doseq [[bf H] [[4 3]]]
    (testing (str "bf " bf ", " (inc H) " levels")
      (let [opts {:branching-factor bf :diff-buf-size 0}
            r (every-step opts H {:check (counted-check opts)})]
        (is (= #{} (missing (expected-events H) (:events r)))
            "every kind of step was taken at every level")
        (is (= #{} (missing (expected-transitions H) (:transitions r)))
            "and was followed at the level above by everything that can follow it")
        (is (pos? (:steps r)))))))

(deftest every-single-cold-write-on-a-root-that-is-a-leaf
  (doseq [bf [4 5 8] size (range 1 (inc bf))]
    (let [opts   {:branching-factor bf :diff-buf-size 0}
          disk   (atom {})
          g      (gen-leaf disk (volatile! 0) size)
          before (shape disk (:addr g) opts)
          chk    (counted-check opts)]
      (doseq [op (spine-ops (:keys g))]
        (let [r (step disk (:addr g) opts before op)]
          (chk {:specs [:leaf-root bf size] :ops [op] :r r :before before
                :delta (second (effect (set (:keys g)) op))})
          (when (and (= size bf) (= :add (first op)))
            (is (= #{[:split 0] [:grow]} (:events r)) "a full leaf root splits and grows")))))))

(deftest every-cold-write-over-buffered-edits-keeps-every-count
  (testing "two writes in a row: the second starts from a tree that holds buffered edits"
    (let [H 2
          opts {:branching-factor 4 :diff-buf-size 2}
          seen (volatile! 0)
          next-door (volatile! 0)
          chk  (counted-check opts)
          r (every-step opts H {:second-step? true
                                :check (fn [m]
                                         (when (:input-buffered? m) (vswap! seen inc))
                                         (when (neighbour-write? m) (vswap! next-door inc))
                                         (chk m))})]
      (is (pos? @seen) "precondition: some second steps started from buffered edits")
      (is (pos? @next-door)
          "precondition: some rebalanced against a neighbour that held a buffered edit")
      (is (= #{} (missing (expected-events H) (:events2 r)))
          "the writes over buffered edits took every kind of step at every level")
      (is (= #{} (missing (expected-transitions H) (:transitions2 r)))))))

(deftest every-single-cold-write-over-an-uncounted-tree
  (testing "no count is ever wrong, and one recount + store leaves none missing and rewrites
            no leaf"
    ;; under a diff buffer only: the JVM twin runs both, and without a buffer `recount`
    ;; has its own tests above
    (doseq [dbs [2]]
      (let [opts {:branching-factor 4 :diff-buf-size dbs}
            real (volatile! 0)
            r (every-step
               opts 2
               {:prepare erase-all!
                :check (fn [{:keys [specs ops disk r before delta]}]
                         (when-not (zero? delta) (vswap! real inc))
                         (let [leaves (leaf-addrs disk)
                               st     #(u/storage (atom {}) disk opts)
                               addr'  (s/store (s/recount (s/restore (:addr r) (st) opts)) (st))
                               sh     (shape disk addr' opts)
                               want   (after-op (:content before) (peek ops))]
                           (when-not (and (zero? (:wrong (:shape r))) (zero? (:wrong sh))
                                          (zero? (:unknown sh))
                                          (= (:content (:shape r)) want) (= (:content sh) want)
                                          (zero? (:misplaced sh))
                                          (= leaves (leaf-addrs disk)))
                             (is false (str "diff-buf " dbs " case " (pr-str specs) " ops " (pr-str ops)
                                            ": wrong " (:wrong (:shape r)) ", after recount wrong "
                                            (:wrong sh) " unknown " (:unknown sh)
                                            ", content as expected " (= (:content sh) want))))))})]
        (is (pos? @real) "writes that changed the set were among the steps")
        (is (= #{} (missing (expected-events 2) (:events r))))))))

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
            ;; a level also gets shorter when an emptied child is dropped, which is no join
            (vswap! seen into (map #(vector (first op) ({:join-left :shorter :join-right :shorter} (first %) (first %)))
                                   (:events r)))
            (when-not (and (zero? (:unknown sh)) (zero? (:wrong sh))
                           (= (:content sh) (after-op (:content before) op))
                           (zero? (:misplaced sh)) (zero? (:mislevelled sh)))
              (is false (str n " keys, " (pr-str op) " " (pr-str (:events r))
                             ": unknown " (:unknown sh) ", wrong " (:wrong sh)
                             ", misplaced keys " (:misplaced sh))))
            (swap! disk select-keys keep)))))
    (is (= #{} (missing #{[:add :split] [:del :shorter] [:add :in-place] [:del :in-place]} @seen)))))

;; ---- the asynchronous arm ----
;;
;; Measured before these tests existed, the whole suite called `conj` asynchronously once,
;; `disj` twice and `replace` never.

(deftest every-single-cold-write-keeps-every-count-async
  (test/async done
              (let [H 2 opts {:branching-factor 4 :diff-buf-size 0}]
                ((every-step-async opts H {:check (counted-check opts)})
                 (fn [r]
                   (is (= #{} (missing (expected-events H) (:events r)))
                       "every kind of step was taken at every level")
                   (is (= #{} (missing (expected-transitions H) (:transitions r))))
                   (is (pos? (:steps r)))
                   (done))
                 (fn [err] (is (nil? err)) (js/console.error err) (done))))))

(deftest every-cold-write-over-buffered-edits-keeps-every-count-async
  (test/async done
              (let [H 1 opts {:branching-factor 4 :diff-buf-size 2}
                    seen (volatile! 0)
                    next-door (volatile! 0)
                    chk  (counted-check opts)]
                ((every-step-async opts H {:second-step? true
                                           :check (fn [m]
                                                    (when (:input-buffered? m) (vswap! seen inc))
                                                    (when (neighbour-write? m) (vswap! next-door inc))
                                                    (chk m))})
                 (fn [r]
                   (is (pos? @seen) "precondition: some second steps started from buffered edits")
                   (is (pos? @next-door)
                       "precondition: some rebalanced against a neighbour that held a buffered edit")
                   (is (= #{} (missing (expected-events H) (:events2 r))))
                   (is (= #{} (missing (expected-transitions H) (:transitions2 r))))
                   (done))
                 (fn [err] (is (nil? err)) (js/console.error err) (done))))))
