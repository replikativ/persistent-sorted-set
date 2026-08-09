(ns org.replikativ.persistent-sorted-set.test.false-element-diff
  "`false` is a legal ELEMENT, and the buffered remove/replace paths recorded the wrong one.

   Both sites took the actually-removed element out of the `removed-out` box with

       (or (some-> removed-out (arrays/aget 0)) key)

   so whenever the stored element was `false`, `or` fell through to the caller's
   comparator-EQUIVALENT probe. The in-memory answer stayed correct — the element really was
   removed from the live tree — while the DIFF recorded an absence for a key that had never
   been there. A cold restore then applies that diff and the `false` comes back.

   Measured at branching factor 4, diff budget 128:

       remove `false` via an equivalent probe   in memory count 5, absent
                                                cold restore count 6, RESURRECTED
       replace `false` with :replacement        in memory count 16, replacement only
                                                cold restore count 17, BOTH present
       same at budget 0                         correct (no diff is recorded at all)
       same on the JVM                          correct (it null-checks the array slot)

   A shape sweep over bf [4 6 8 10 16], sizes bf+1..48 and nine element positions failed
   1489 of 1721 cells; the survivors are shapes that structurally flush the operation instead
   of buffering it, which is what makes this shape-dependent rather than universal.

   Same class as the `false`-measure defect fixed in 071038c. That pass converted the twenty
   MEASURE sites to `some?` and missed these two ELEMENT sites, so the bug moved rather than
   died. Found by an independent second-opinion review asked to generalise the earlier fix.

   ClojureScript-only: the JVM stores the removed element into an `Object[1]` and tests the
   slot for null, which `false` passes."
  (:require [cljs.test :refer-macros [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.test.storage.util :as u]))

;; Two comparators over the same elements. `stable-cmp` is the SET's: it separates `false`,
;; `:probe` and `:replacement`. `op-cmp` is the one passed to the operation and is COARSER —
;; it ranks all three equal, which is the documented "[id value] compared by id" pattern and
;; the only way to reach an element by a probe that is not the element itself.
(defn- rank [x]
  (cond (false? x) 0.25 (= x :probe) 0.5 (= x :replacement) 0.75 :else x))
(defn- stable-cmp [a b] (compare (rank a) (rank b)))
(defn- op-rank [x]
  (if (or (false? x) (= x :probe) (= x :replacement)) 0.25 x))
(defn- op-cmp [a b] (compare (op-rank a) (op-rank b)))

(defn- cold-roundtrip
  "Build a set containing `false`, apply `f`, store, drop the node cache, restore. Returns
   the in-memory and the cold-restored element vectors."
  [budget f]
  (let [opts    {:branching-factor 4 :diff-buf-size budget :comparator stable-cmp}
        memory  (atom {})
        disk    (atom {})
        storage (u/storage memory disk opts)
        base    (reduce #(s/conj %1 %2 stable-cmp)
                        (s/sorted-set* opts)
                        (concat (range 5) [false]))
        _       (s/store base storage)
        changed (f base)
        address (s/store changed storage)
        _       (reset! memory {})                 ; force a genuinely COLD read
        cold    (s/restore address storage opts)]
    {:memory (vec (s/seq changed)) :cold (vec (s/seq cold))}))

(deftest removing-a-false-element-through-an-equivalent-probe-survives-a-cold-restore
  (testing "the diff must record the element that was actually removed, not the probe"
    (doseq [budget [128 0]]
      (let [{:keys [memory cold]} (cold-roundtrip budget #(s/disj % :probe op-cmp))]
        (is (= memory cold)
            (str "budget=" budget ": the cold restore must agree with memory, got "
                 (pr-str memory) " vs " (pr-str cold)))
        (is (not (some false? cold))
            (str "budget=" budget ": `false` was removed and must not come back: "
                 (pr-str cold)))))))

(deftest replacing-a-false-element-does-not-leave-both-behind
  (testing "the same box, on the replace path — a wrong key here duplicates rather than
            resurrects, because the replacement is also inserted"
    (doseq [budget [128 0]]
      (let [{:keys [memory cold]}
            (cold-roundtrip budget #(s/replace % :probe :replacement op-cmp))]
        (is (= memory cold)
            (str "budget=" budget ": " (pr-str memory) " vs " (pr-str cold)))
        (is (not (some false? cold))
            (str "budget=" budget ": `false` was replaced and must be gone: "
                 (pr-str cold)))
        (is (some #(= :replacement %) cold)
            (str "budget=" budget ": and the replacement must be present"))))))

(deftest a-false-element-round-trips-untouched
  (testing "the control: without any operation, `false` must survive store and restore, or
            the tests above could pass for the wrong reason"
    (let [{:keys [memory cold]} (cold-roundtrip 128 identity)]
      (is (= memory cold))
      (is (some false? cold) "precondition: the set really does contain `false`"))))
