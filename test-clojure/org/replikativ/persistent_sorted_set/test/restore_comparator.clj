(ns org.replikativ.persistent-sorted-set.test.restore-comparator
  "`restore` must honour `:comparator`, because on ClojureScript it always has.

   `restore-by` is JVM-ONLY. Portable `.cljc` code that restores a set built with a custom
   comparator therefore has exactly one spelling available to it:

       (restore addr storage {:comparator cmp})

   ClojureScript honours that key (`btset/restore` reads `(or (:comparator opts) (:cmp opts)
   compare)`). The JVM hard-coded `RT/DEFAULT_COMPARATOR` and threw the key away, so the same
   source produced a working set on one runtime and a broken one on the other.

   Broken in a particularly quiet way, because the TREE is fine — only the comparator the set
   navigates it with is wrong. Measured on 0..19 under a descending comparator, stored and
   restored:

       (vec r)          [19 18 ... 1 0]   correct
       (count r)        20                correct
       (contains? r 5)  FALSE             (restore-by desc => true)
       (disj r 5)       a no-op
       (conj r 5)       21 elements, a DUPLICATE 5 — and durable once stored

   Every cheap oracle passes. `seq`, `count` and printing all look right, which is exactly the
   shape of defect this suite keeps having to learn to see."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set Settings]))

(def ^:private desc (comparator (fn [a b] (> (compare a b) 0))))

(defn- storage [disk bf] (ts/->Storage (atom {}) disk (Settings. (int bf) nil nil nil (int 0))))

(defn- stored [bf n]
  (let [disk (atom {})
        s    (reduce #(set/conj %1 %2 desc)
                     (set/sorted-set* {:comparator desc :branching-factor bf})
                     (range n))]
    {:addr (set/store s (storage disk bf)) :disk disk :bf bf :n n}))

(deftest restore-honours-an-explicit-comparator
  (testing "the set must navigate with the comparator it was given, not the default"
    (doseq [bf [8 16]]
      (let [{:keys [addr disk n]} (stored bf 20)
            r (set/restore addr (storage disk bf) {:comparator desc :branching-factor bf})]
        (is (= (reverse (range n)) (vec r))
            (str "bf=" bf ": the order is descending either way — this is the part that
                  looks fine and is why the defect stayed hidden"))
        (is (= n (count r)) (str "bf=" bf ": count"))
        (doseq [k [0 5 13 19]]
          (is (contains? r k)
              (str "bf=" bf ": " k " is present and must be findable")))
        (is (not (contains? r 20)) (str "bf=" bf ": absent keys must stay absent"))))))

(deftest a-wrong-comparator-corrupts-on-the-next-write
  (testing "the consequence that outlives the process: a lookup miss turns a conj into a
            DUPLICATE insert, and storing that makes it durable"
    (doseq [bf [8 16]]
      (let [{:keys [addr disk n]} (stored bf 20)
            r (set/restore addr (storage disk bf) {:comparator desc :branching-factor bf})]
        ;; clojure.core/conj and disj DELIBERATELY, i.e. the set's own comparator. An earlier
        ;; version passed `desc` as the OPERATION comparator, which overrides the set's wrong
        ;; `_cmp` and repairs the very thing under test — so it passed against the unfixed
        ;; build while the commit message's numbers (21 and 20) came from these 2-arity forms.
        ;; Caught by an adversarial review.
        (is (= n (count (clojure.core/conj r 5)))
            (str "bf=" bf ": conjing an element that is already present must be a no-op, "
                 "not a second copy"))
        (is (= (dec n) (count (clojure.core/disj r 5)))
            (str "bf=" bf ": and disj must actually remove it"))))))

(deftest restore-honours-cmp-as-well-as-comparator
  (testing "`btset/restore` reads (or (:comparator opts) (:cmp opts) compare) and this
            namespace's own `sorted-set*` accepts `:cmp`, so fixing only `:comparator` left
            the identical defect live on the JVM for anyone who spelled it `:cmp`"
    (doseq [bf [8 16]]
      (let [{:keys [addr disk n]} (stored bf 20)
            r (set/restore addr (storage disk bf) {:cmp desc :branching-factor bf})]
        (is (= (reverse (range n)) (vec r)) (str "bf=" bf ": contents"))
        (doseq [k [0 5 13 19]]
          (is (contains? r k) (str "bf=" bf ": " k " must be findable under :cmp too")))))))

(deftest restore-by-and-restore-agree
  (testing "the two spellings must give the same set — restore-by is the JVM-only one that
            always worked, so it is the oracle here"
    (doseq [bf [8 16]]
      (let [{:keys [addr disk]} (stored bf 20)
            a (set/restore-by desc addr (storage disk bf) {:branching-factor bf})
            b (set/restore addr (storage disk bf) {:comparator desc :branching-factor bf})]
        (is (= (vec a) (vec b)) (str "bf=" bf ": same contents"))
        (doseq [k (range 20)]
          (is (= (contains? a k) (contains? b k))
              (str "bf=" bf ": same answer for " k)))))))

(deftest the-default-is-unchanged
  (testing "the control — omitting :comparator must still give the natural comparator, or
            this fix would silently change every existing caller"
    (let [disk (atom {})
          bf   16
          s    (reduce #(set/conj %1 %2 compare)
                       (set/sorted-set* {:branching-factor bf}) (range 20))
          addr (set/store s (storage disk bf))
          r    (set/restore addr (storage disk bf) {:branching-factor bf})]
      (is (= (range 20) (vec r)) "ascending, the natural order")
      (is (contains? r 5))
      (is (= 20 (count r))))))
