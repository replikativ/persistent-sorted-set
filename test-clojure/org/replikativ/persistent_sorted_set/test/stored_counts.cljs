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

(deftest recount-under-another-buffer-budget-writes-no-leaf
  (testing "written under a small budget, reopened and written to under a larger one. (The
            other direction is not testable with this storage: it hands every node back under
            ITS budget, so a node written at 64 comes back at 1 with slots that no longer fit
            — the documented case, where any write flushes.)"
    (doseq [[written reopened] [[1 64] [4 64]]]
      (let [bf 8 n 700
            {:keys [addr disk opts]} (generations bf written n 3 (:mixed (patterns bf n)))
            opts'  (assoc opts :diff-buf-size reopened)
            st     #(u/storage (atom {}) disk opts')
            warm   (reduce s/conj (s/restore addr (st) opts') [-1 -3])
            addr1  (s/store warm (st))
            _      (erase-counts! disk)
            before (disk-census disk addr1)
            addr'  (s/store (s/recount (s/restore addr1 (st) opts')) (st))
            after  (disk-census disk addr')]
        (is (pos? (:unknown before)))
        (is (zero? (:unknown after)) (str written " -> " reopened))
        (is (= (:leaves before) (:leaves after)) (str written " -> " reopened ": no leaf was rewritten"))))))

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
