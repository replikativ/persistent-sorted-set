(ns org.replikativ.persistent-sorted-set.test.stored-counts-exhaustive
  "The ClojureScript twin of the JVM `stored-counts-exhaustive`: every single cold write,
   enumerated. See that namespace for the argument — the invariant is a one-step property,
   what a step does at each level is decided by the fill of the node on the path and of its
   two neighbours, and trees are written down by shape because trees built by a history of
   inserts do not reach every case.

   Smaller than the JVM run (one branching factor, one height per test) since the code under
   test is a different implementation of the same algorithm, not a different algorithm."
  (:require [cljs.test :refer-macros [deftest testing is]]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.boundary :as boundary]
            [org.replikativ.persistent-sorted-set.branch :as branch :refer [Branch]]
            [org.replikativ.persistent-sorted-set.test.storage.util :as u]))

(defn- blob [disk a] (edn/read-string (@disk a)))

;; ---- the oracle: a cold restore, every branch's count against the elements below it ----

(defn- shape [disk addr opts]
  (let [st   (u/storage (atom {}) disk opts)
        set  (s/restore addr st opts)
        _    (s/count-census set)                       ; loads the root
        acc  (volatile! {:levels (sorted-map) :unknown 0 :wrong 0})
        walk (fn walk [n]
               (let [len (alength (.-keys n))]
                 (if (instance? Branch n)
                   (let [c (reduce + 0 (map #(walk (branch/child n st % {:sync? true})) (range len)))]
                     (vswap! acc update-in [:levels (.-level n)] (fnil conj []) len)
                     (cond (neg? (.-subtree-count n))   (vswap! acc update :unknown inc)
                           (not= c (.-subtree-count n)) (vswap! acc update :wrong inc))
                     c)
                   (do (vswap! acc update-in [:levels 0] (fnil conj []) len)
                       len))))
        total (walk (.-root set))]
    (assoc @acc :n total)))

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
                       (<= (:reads r) bound))
          (is false (str "case " (pr-str specs) " ops " (pr-str ops) " events " (pr-str (:events r))
                         ": unknown " (:unknown sh) ", wrong " (:wrong sh) ", elements " (:n sh)
                         " from " (:n before) ", reads " (:reads r) " (bound " bound ")")))))))

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
