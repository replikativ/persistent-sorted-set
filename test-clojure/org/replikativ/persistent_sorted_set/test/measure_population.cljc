(ns org.replikativ.persistent-sorted-set.test.measure-population
  "Where each runtime actually HOLDS a measure.

   A branch can only fold its children's measures if the children have them. The
   JVM's `Branch.remove` recomputes from children and works; the same change on
   ClojureScript produced a nil root measure every time, which says the cljs
   children are not populated the way the JVM's are.

   This namespace measures that directly — how many nodes at each level carry a
   non-nil `_measure` — rather than inferring it from a downstream symptom. The
   two runtimes should agree; where they do not, this says by how much and at
   which level, which is what a fix has to target."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.measure :as measure])
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.node :as node]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set
                    PersistentSortedSet ANode Branch NumericStatsOps])))

(defn- by-decade [a b] (compare (quot (long a) 10) (quot (long b) 10)))

#?(:clj  (def ^:private ops (NumericStatsOps/instance))
   :cljs (do (defrecord SumOps []
               measure/IMeasure
               (identity-measure [_] 0)
               (extract [_ k] k)
               (merge-measure [_ a b] (+ a b))
               (remove-measure [_ current k _recompute] (- current k)))
             (def ^:private ops (->SumOps))))

(defn- census
  "{level {:with n :without n}} over every resident node of the tree."
  [set]
  (let [acc (atom {})
        note (fn [lvl has?] (swap! acc update-in [lvl (if has? :with :without)] (fnil inc 0)))]
    #?(:clj
       (letfn [(walk [^ANode n]
                 (let [lvl (.level n)]
                   (note lvl (some? (.-_measure n)))
                   (when (instance? Branch n)
                     (dotimes [i (.len ^Branch n)]
                       (walk (.child ^Branch n nil i))))))]
         (walk (.root ^PersistentSortedSet set)))
       :cljs
       ;; a cljs Leaf has no `level` FIELD — only Branch does — so read it through
       ;; the node protocol. Reading `.-level` directly reported every leaf as
       ;; level `undefined` and made this census silently find no leaves at all.
       (letfn [(walk [n]
                 (let [lvl (node/level n)]
                   (note lvl (some? (.-_measure n)))
                   (when (pos? lvl)
                     (let [kids (.-children n)]
                       (dotimes [i (alength (.-keys n))]
                         (when-let [c (and kids (aget kids i))] (walk c)))))))]
         (walk (.-root set))))
    @acc))

(defn- build [n bf]
  (s/from-sequential compare (vec (map #(+ 3 (* 10 %)) (range n)))
                     {:branching-factor bf :measure ops}))

(deftest leaves-carry-a-measure-after-an-eager-build
  (testing "`from-sequential` with `:measure` must leave every LEAF holding one.
            This is the precondition for a branch being able to fold its children
            at all, and it is where the two runtimes were suspected to differ."
    (doseq [[n bf] [[200 8] [200 16]]]
      (let [c (census (build n bf))
            leaf (get c 0)]
        (is (some? leaf) (str "n=" n " bf=" bf ": the walk found leaves"))
        (is (zero? (get leaf :without 0))
            (str "n=" n " bf=" bf ": leaves WITHOUT a measure after an eager build: "
                 (pr-str c)))))))

(deftest a-remove-does-not-strip-the-measures-below-it
  (testing "after removals the surviving leaves must still hold measures —
            otherwise every branch above them can only postpone, and the
            aggregate silently disappears from the whole tree"
    (doseq [[n bf drop-n] [[200 8 50] [200 16 137]]]
      (let [base (build n bf)
            v (reduce (fn [acc i] (s/disj acc (* 10 i) by-decade)) base (range drop-n))
            c (census v)]
        (is (zero? (get-in c [0 :without] 0))
            (str "n=" n " bf=" bf " dropped=" drop-n
                 ": leaves lost their measure: " (pr-str c)))))))
