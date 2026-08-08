(ns org.replikativ.persistent-sorted-set.test.branching-factor
  "Which branching factors are supported, stated once and enforced on both runtimes.

   **4 is the floor.** `minBranchingFactor` is `bf >>> 1`, so bf 2 and bf 3 both
   give a minimum fill of ONE. A non-root branch of length 1 then counts as
   legally filled, its single child has no sibling to rebalance with, and a
   removal leaves a length-0 leaf. bf 4 is the first factor whose minimum fill is
   2 — the smallest that lets a merge restore a valid node. So the floor is
   forced by the structure, not chosen.

   What that looked like before it was refused:

     * JVM — `ArrayIndexOutOfBoundsException: Index -1 out of bounds for length 0`
       from `maxKey()` on the empty leaf, on ordinary add/remove sequences. Over
       40 random seeds x 400 ops: bf 2 failed 40/40, bf 3 failed 30/40, while
       bf 4, 5, 6, 7, 8, 15, 16, 17, 32, 33 and 64 all passed 40/40.
     * ClojureScript — no throw, silent corruption. It keeps the empty leaf and
       starts yielding `nil` ELEMENTS from a set whose constructor explicitly
       refuses nil. Measured at bf 2, one seed: `count` 41 against 39 real
       elements, five length-0 leaves, and 11 violations from
       `diagnostics/validate` including `:not-strictly-sorted` with `:curr nil`.

   The library previously advertised **3** as the minimum (`from-sorted-array`'s
   assertion message said so), which is one below the first factor that works.

   **Non-positive means UNSET, not an error.** `Settings()` has always delegated
   with 0, and `map->settings` normalizes a missing value, so 0 / negative / absent
   all take the default of 512. Only an explicit 1, 2 or 3 is refused — a caller
   who names one is asking for a tree that cannot work. This also closes a
   ClojureScript-only hazard: `{:branching-factor nil}` used to reach
   `arr-partition-approx` with a chunk length of 0 and exhaust the Node heap,
   where the JVM normalized it to 512."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]))

(defn- effective-bf
  "The branching factor a constructed set actually carries."
  [opts]
  #?(:clj  (.branchingFactor
            (.-_settings ^org.replikativ.persistent_sorted_set.PersistentSortedSet
             (s/sorted-set* (assoc opts :comparator compare))))
     :cljs (:branching-factor (.-settings (s/sorted-set* (assoc opts :comparator compare))))))

(defn- refused? [opts]
  (try (effective-bf opts) false
       (catch #?(:clj IllegalArgumentException :cljs :default) _ true)))

(deftest branching-factors-below-four-are-refused
  (testing "1, 2 and 3 cannot form a working B-tree — refuse them where the set is
            built, not at the crash site several operations later"
    (doseq [bf [1 2 3]]
      (is (refused? {:branching-factor bf})
          (str "branching-factor " bf " must be refused")))))

(deftest four-and-above-are-supported
  (testing "including the odd factors, whose minimum fill is (quot bf 2)"
    (doseq [bf [4 5 6 7 8 15 16 17 32 33 64 512]]
      (is (= bf (effective-bf {:branching-factor bf}))
          (str "branching-factor " bf " must be accepted and carried unchanged")))))

(deftest an-unset-branching-factor-takes-the-default
  (testing "absent, nil, zero and negative all mean UNSET. `Settings()` has always
            delegated with 0, so this is long-standing behaviour and must not
            become an error — and on ClojureScript a nil used to reach
            `arr-partition-approx` with chunk length 0 and hang."
    (doseq [opts [{} {:branching-factor nil} {:branching-factor 0} {:branching-factor -4}]]
      (is (= 512 (effective-bf opts))
          (str (pr-str opts) " must take the default of 512")))))

(deftest a-refused-factor-does-not-build-a-broken-set
  (testing "the refusal has to happen before any elements go in, so the caller
            never holds a set that will corrupt later"
    (doseq [bf [2 3]]
      (is (thrown? #?(:clj IllegalArgumentException :cljs :default)
                   (reduce #(s/conj %1 %2 compare)
                           (s/sorted-set* {:comparator compare :branching-factor bf})
                           (range 40)))
          (str "bf " bf " must throw at construction, not silently build")))))

(deftest the-smallest-supported-factor-really-works
  (testing "bf 4 is the floor, so exercise it properly rather than assuming — the
            sequences below are the ones that crashed at bf 3"
    (let [n 200
          built (reduce #(s/conj %1 %2 compare)
                        (s/sorted-set* {:comparator compare :branching-factor 4})
                        (range n))
          after (reduce #(s/disj %1 %2 compare) built (range 0 n 2))]
      (is (= n (count built)))
      (is (= (vec (range 1 n 2)) (vec #?(:clj (seq after) :cljs (s/seq after)))))
      (is (every? some? (vec #?(:clj (seq after) :cljs (s/seq after))))
          "no nil elements — the ClojureScript symptom at bf 2/3")
      (is (= (quot n 2) (count after)) "count agrees with seq"))))
