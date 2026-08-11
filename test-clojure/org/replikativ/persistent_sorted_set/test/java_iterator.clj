(ns org.replikativ.persistent-sorted-set.test.java-iterator
  "`java.util.Iterator.next()` must throw `NoSuchElementException` when exhausted.
   `JavaIter.next` ignored its own `_over` flag, so it did neither of the two things the
   contract requires.

   ## Past the end of a non-empty set

   `next()` re-read `_seq.first()` and handed back the LAST element again — indefinitely.
   Measured on `(range 5)`, calling `next()` eight times: 0 1 2 3 4 4 4 4.

   Anything driven by `hasNext()` never reaches this, which is exactly why it survived:
   `for (Object o : set)`, `Iterators.addAll`, `while (it.hasNext())` all stop first. What
   it breaks is the other documented way to consume an Iterator — call `next()` and let the
   exception end the loop — and every generic wrapper that relies on the exception to know
   it is done. Those get a silent infinite supply of a duplicate element instead of a
   terminating iteration.

   ## On an empty set

   The constructor sets `_over` from `seq == null`, so an empty set gives `_seq == null` and
   the first `next()` dereferenced it: NullPointerException, not NoSuchElementException.

   ## Why it is reachable from Clojure at all

   Two construction sites — `PersistentSortedSet.iterator()` and `Seq.iterator()` — so both
   `(.iterator s)` and `(.iterator (seq s))` produce one. Clojure's own `seq`/`reduce` go
   through `Seq` directly and never touch `JavaIter`, so this is Java-interop surface: it
   costs a single predicate on a path Clojure does not take."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set])
  (:import [java.util NoSuchElementException]))

(deftest exhausted-iterator-throws
  (testing "past the end, next() must throw rather than repeat the last element"
    (doseq [n [1 5 100]]
      (let [s  (into (set/sorted-set) (range n))
            it (.iterator ^Iterable s)
            drained (vec (repeatedly n #(.next it)))]
        (is (= (range n) drained) (str "n=" n ": the elements themselves"))
        (is (false? (.hasNext it)) (str "n=" n ": hasNext agrees it is done"))
        (is (thrown? NoSuchElementException (.next it))
            (str "n=" n ": and next() must say so — this used to return "
                 (dec n) " again, forever"))
        (is (thrown? NoSuchElementException (.next it))
            (str "n=" n ": and keep saying so"))))))

(deftest empty-set-iterator-throws-the-right-exception
  (testing "an empty set has no Seq at all, so this used to be a NullPointerException"
    (let [it (.iterator ^Iterable (set/sorted-set))]
      (is (false? (.hasNext it)))
      (is (thrown? NoSuchElementException (.next it))))))

(deftest the-seq-iterator-has-the-same-contract
  (testing "Seq.iterator() is the second construction site and must behave identically"
    (let [s  (into (set/sorted-set) (range 5))
          it (.iterator ^Iterable (seq s))]
      (is (= (range 5) (vec (repeatedly 5 #(.next it)))))
      (is (false? (.hasNext it)))
      (is (thrown? NoSuchElementException (.next it))))))

(deftest ordinary-iteration-is-unchanged
  (testing "the control: everything that checks hasNext must behave exactly as before, or
            this fix would be a behaviour change rather than a contract repair"
    (doseq [n [0 1 5 1000]]
      (let [s (into (set/sorted-set) (range n))]
        ;; `vec`, not `=` on the seqs: iterator-seq of an exhausted iterator is nil, and
        ;; (= () nil) is false.
        (is (= (vec (range n)) (vec (iterator-seq (.iterator ^Iterable s))))
            (str "n=" n ": iterator-seq, which stops on hasNext"))
        (is (= (range n) (vec (into [] s)))
            (str "n=" n ": and reduce, which does not use JavaIter at all"))))))
