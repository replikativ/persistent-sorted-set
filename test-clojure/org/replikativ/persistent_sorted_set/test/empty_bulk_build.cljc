(ns org.replikativ.persistent-sorted-set.test.empty-bulk-build
  "Building from an EMPTY collection must give an EMPTY set.

   `ArrayUtil.distinct` compacts an array to its distinct prefix and returns that prefix's
   length. Its loop starts at index 1, so for a zero-length array it never runs and the
   trailing `return to + 1` reported ONE distinct element in an array that has none.
   `from-sequential` then built from a 1-element array whose sole slot was null:

       (from-sequential compare [])
       ;; seq (nil), count 1, (= s #{}) false, (empty? s) false, (contains? s nil) true
       ;; (conj s 5) => [nil 5]; store then restore => [nil]   -- the nil is DURABLE

   nil is the one value the set refuses everywhere else: the namespace docstring says it
   \"can't store nil\", and `from-sequential` itself throws IllegalArgumentException on a nil
   ELEMENT. With no elements at all it fabricated one.

   Two things kept the suite green. `assert-sorted!` passes VACUOUSLY at length 1, so the
   builder's own precondition check could not see it. And the variadic entry points
   `(sorted-set)` / `(sorted-set-by cmp)` dispatch to the 0- and 1-arity constructors, so
   the library never reached this path itself — only a caller passing a possibly-empty
   collection did.

   ClojureScript was always correct here: `sorted-arr-distinct` short-circuits on
   `alength <= 1`. So this also closes a cross-runtime divergence, and the test is .cljc to
   keep both honest."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]))

(deftest an-empty-input-gives-an-empty-set
  (testing "every empty-ish input, through every builder that accepts one"
    (doseq [[label coll] [["vector" []] ["set" #{}] ["nil" nil] ["lazy" (filter odd? [2 4])]]]
      (let [s (set/from-sequential compare coll)]
        (is (= 0 (count s))           (str label ": count"))
        (is (empty? s)                (str label ": empty?"))
        (is (nil? (seq s))            (str label ": seq is nil"))
        (is (= [] (vec s))            (str label ": vec"))
        (is (= s #{})                 (str label ": equal to the empty set"))
        (is (not (contains? s nil))   (str label ": does not contain nil"))))))

(deftest a-fabricated-nil-would-have-been-durable
  (testing "and it would not have stayed in memory: growing the set keeps the phantom"
    (let [s (set/conj (set/from-sequential compare []) 5 compare)]
      (is (= [5] (vec s)) "conj onto an empty built set must not carry a nil along")
      (is (= 1 (count s))))))

(deftest non-empty-builds-are-unchanged
  (testing "the control — `distinct` still has to dedupe and still has to keep singletons"
    (is (= [7]       (vec (set/from-sequential compare [7]))))
    (is (= [1 2 3]   (vec (set/from-sequential compare [3 1 2 3 1]))))
    (is (= [1]       (vec (set/from-sequential compare [1 1 1 1]))))
    (is (= (range 100) (vec (set/from-sequential compare (shuffle (range 100))))))))

(deftest nil-elements-are-still-refused
  (testing "the fix must not turn the nil REFUSAL into acceptance"
    (is (thrown? #?(:clj IllegalArgumentException :cljs js/Error)
                 (set/from-sequential compare [1 nil 2])))))
