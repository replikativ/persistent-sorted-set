(ns org.replikativ.persistent-sorted-set.test.replace-separator
  "A `replace` under a COARSE operation comparator must still refresh every
   separator that routing depends on.

   `replace` locates the element with an OPERATION comparator that may be coarser
   than the SET's — datahike's value-changing upsert searches `[e a _ _]` and
   replaces the whole datom. The branch then asked that same coarse comparator
   whether the child's max had moved, and skipped the separator update when it
   said no. But routing uses the SET's comparator, which can see the change. The
   separator kept naming the OLD element, and a later descent comparing the NEW
   element against it routes past the child that holds it.

   The element is still there. `seq` lists it, the set is sorted, `count` is
   right, and a lookup with the coarse comparator finds it — only a lookup with
   the set's own comparator misses. Measured before the fix:

       JVM, inside a transient    n=40 bf=4:  9    n=100 bf=4: 24   n=3000 bf=16: 46
       ClojureScript, persistent  n=40 bf=4: 13    n=100 bf=4: 33   n=3000 bf=16: 250

   and through datahike at branching-factor 8, 375 of 3000 datoms invisible to
   `d/datoms db :eavt e a v` while `d/q` and `d/pull` still found them.

   ## Why the two runtimes failed on different paths

   The JVM writes `_keys[idx] = newMaxKey` unconditionally and its persistent
   path always returns the successor, so the flag only ever suppressed
   PROPAGATION to the grandparent — hence transient-only, and only in trees of
   THREE levels or more (with two levels the parent is the root, whose separator
   is the one written unconditionally). ClojureScript gated the keys rebuild on
   the same flag, so its default persistent path was affected, once per leaf.

   That is why this file is `.cljc` and asserts the same property on both: the
   defect was one idea with two different symptoms, and a JVM-only test would
   have reported the ClojureScript half as fixed."
  (:require [clojure.test :refer [deftest testing is]]
            #?(:clj [org.replikativ.persistent-sorted-set :as s]
               :cljs [org.replikativ.persistent-sorted-set :as s]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set PersistentSortedSet])))

;; elements are [k v]; the SET orders by both, REPLACE keys on k alone
(defn- full-cmp [a b]
  (let [c (compare (nth a 0) (nth b 0))]
    (if-not (zero? c) c (compare (nth a 1) (nth b 1)))))

(defn- by-k [a b] (compare (nth a 0) (nth b 0)))

(defn- has?
  "Membership under the SET's comparator — the one every ordinary lookup uses.
   `contains?`/`seq` are clojure.core on the JVM and namespaced fns on cljs."
  [set x]
  #?(:clj (clojure.core/contains? set x) :cljs (s/contains? set x)))

(defn- elems [set]
  #?(:clj (clojure.core/seq set) :cljs (s/seq set)))

(defn- unfindable
  "How many of the replaced elements the set can no longer find."
  [set n]
  (count (remove #(has? set [% 5]) (range n))))

(defn- build [n bf]
  (reduce #(s/conj %1 [%2 0] full-cmp)
          (s/sorted-set* {:comparator full-cmp :branching-factor bf})
          (range n)))

(defn- replace-all-persistent [set n]
  (reduce (fn [s i] (s/replace s [i 0] [i 5] by-k)) set (range n)))

;; ---------------------------------------------------------------------------

(deftest every-replaced-element-is-still-findable-persistent
  (testing "the ClojureScript half: its default path skipped the separator
            rebuild whenever the coarse comparator called the max unchanged"
    (doseq [[n bf] [[40 4] [100 4] [3000 16]]]
      (let [v (replace-all-persistent (build n bf) n)]
        (is (zero? (unfindable v n))
            (str "n=" n " bf=" bf ": elements present but unfindable"))
        (is (= n (count v)) (str "n=" n " bf=" bf))
        (is (= (vec (elems v)) (vec (sort full-cmp (elems v))))
            (str "n=" n " bf=" bf ": still sorted"))))))

#?(:clj
   (deftest every-replaced-element-is-still-findable-transient
     (testing "the JVM half: `_keys[idx]` was always written, so the immediate
               separator was fine — what was suppressed was PROPAGATION, leaving
               the GRANDPARENT stale. Needs three levels; at two the parent is
               the root and the bug cannot appear, which is why the shapes below
               are deep rather than wide."
       (doseq [[n bf] [[40 4] [100 4] [3000 16]]]
         (let [base (build n bf)
               v (let [t (reduce (fn [^PersistentSortedSet t i]
                                   (.replace t [i 0] [i 5] by-k))
                                 (.asTransient ^PersistentSortedSet base) (range n))]
                   (.persistent ^PersistentSortedSet t))]
           (is (zero? (unfindable v n))
               (str "n=" n " bf=" bf ": elements present but unfindable"))
           (is (= n (count v)) (str "n=" n " bf=" bf))
           (is (= (vec (elems v)) (vec (sort full-cmp (elems v))))
               (str "n=" n " bf=" bf ": still sorted")))))))

(deftest the-shapes-above-are-deep-enough-to-show-it
  (testing "precondition. With only two levels the parent IS the root, whose
            separator is refreshed unconditionally, so a shallow tree cannot
            exhibit the defect and a test built on one would pass against the
            unfixed code."
    #?(:clj (let [v (build 3000 16)]
              (is (>= (.level (.root ^PersistentSortedSet v)) 2)
                  "3000 elements at bf 16 must be at least three levels"))
       :cljs (is true "depth is asserted on the JVM; the cljs failure was
                       per-leaf and visible at every shape above"))))

;; ---------------------------------------------------------------------------
;; The half of C2 that value equality alone cannot see
;;
;; The cases above use elements whose `=` agrees with the set comparator, so
;; `not= new-max-key keys[idx]` is a sufficient test and ClojureScript passed
;; them. The defect needs a type whose `=` is COARSER than the comparator —
;; datahike's Datom, where `equiv-datom` compares e/a/v while `cmp-datoms-eavt`
;; orders by e/a/v/tx. Then `=` says "same element", the separator is left naming
;; the OLD one, and a later descent routes past the child that holds the new one.
;;
;; The JVM got `separatorMoved` (value equality OR the projection comparator);
;; ClojureScript kept the bare `not=` until this test was written. Measured,
;; elements that `seq` lists but `lookup` cannot find:
;;
;;     bf  4 n   40    JVM 0    cljs 19
;;     bf  8 n  400    JVM 0    cljs 99
;;     bf 16 n 3000    JVM 0    cljs 374
;;
;; `count` and `seq` were both correct throughout — only lookup failed.

(deftype CoarseE [a b]
  Object
  (toString [_] (str "CoarseE[" a " " b "]"))
  #?@(:clj  [clojure.lang.IPersistentCollection
             (equiv [_ o] (and (instance? CoarseE o) (= a (.-a ^CoarseE o))))]
      :cljs [IEquiv
             (-equiv [_ o] (and (instance? CoarseE o) (= a (.-a o))))]))

(defn- coarse-set-cmp [^CoarseE x ^CoarseE y]
  (let [c (compare (.-a x) (.-a y))]
    (if (zero? c) (compare (.-b x) (.-b y)) c)))

(defn- coarse-op-cmp [^CoarseE x ^CoarseE y] (compare (.-a x) (.-a y)))

(deftest a-separator-must-move-when-the-comparator-says-so-even-if-equals-does-not
  (testing "an element type whose `=` ignores a field the set orders by. Every
            replaced element must still be findable; the CONTROL below uses plain
            vectors, whose `=` is exact, and must be clean on both runtimes — if
            it is not, the probe is wrong rather than the code."
    (doseq [[bf n] [[4 40] [8 400] [16 3000]]]
      (let [base  (reduce #(s/conj %1 %2 coarse-set-cmp)
                          (s/sorted-set* {:comparator coarse-set-cmp
                                          :branching-factor bf})
                          (map #(CoarseE. % 0) (range n)))
            after (reduce (fn [st i] (s/replace st (CoarseE. i 0) (CoarseE. i 5) coarse-op-cmp))
                          base (range n))
            missing (for [i (range n)
                          :when (nil? (s/lookup after (CoarseE. i 5) coarse-set-cmp))]
                      i)]
        (is (= n (count after)) (str "bf=" bf " n=" n ": count"))
        (is (= n (count (elems after))) (str "bf=" bf " n=" n ": seq length"))
        (is (empty? missing)
            (str "bf=" bf " n=" n ": " (count missing) " elements that `seq` lists"
                 " but `lookup` cannot find, first " (vec (take 8 missing))))))))

(deftest the-coarse-equality-probe-is-sound
  (testing "the same experiment with plain vectors, whose `=` is exact — this must
            be clean regardless, and was on both runtimes even before the fix"
    (doseq [[bf n] [[4 40] [8 400]]]
      (let [base  (reduce #(s/conj %1 %2 compare)
                          (s/sorted-set* {:comparator compare :branching-factor bf})
                          (map #(vector % 0) (range n)))
            after (reduce (fn [st i] (s/replace st [i 0] [i 5]
                                                (fn [x y] (compare (first x) (first y)))))
                          base (range n))]
        (is (empty? (for [i (range n) :when (nil? (s/lookup after [i 5] compare))] i))
            (str "bf=" bf " n=" n ": control must be clean"))))))
