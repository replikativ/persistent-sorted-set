(ns org.replikativ.persistent-sorted-set.test.disj-deposit
  "A buffered `disj` must record the element it actually REMOVED.

   `Branch.remove` deposited `Absent(<the caller's search key>)`. Under an
   operation comparator coarser than the set's, that is not the element the leaf
   holds. `projectLeaf` replays the diff under the SET's comparator, so the
   `Absent` cancels nothing and the removed element comes back on the next
   reload — while `contains?` still answers false, because the separator WAS
   updated. So `seq` yields an element that `contains?`, `lookup` and `slice`
   cannot find, and `count` disagrees with `seq`.

   Measured before the fix — 40 elements `[i 0]`, bulk-built at bf 8 with
   diff-buf 256, cold-restored, then `(disj s [17 999] by-first)`:

       in memory   count 39, [17 0] absent
       RELOADED    count 40, seq 40, extra #{[17 0]}, contains? [17 0] => false

   ## Why this went unfound for so long

   It needs a LEVEL-1 root. At level 2 and above the slot is a branch marker
   whose diff is null, so the deposited key is never used and the reload is
   clean. Six earlier attempts all used `conj`-built trees, which at these sizes
   are deeper with underfull leaves — every one of them put the deposit at level
   2 and came back green, and the conclusion drawn from them (that the deposit
   was unreachable) was written into the source as fact.

   `from-sorted-array` packs leaves to `avg`, so 40 elements at bf 8 is a
   level-1 root. That single difference is what separates a green run from
   durable data resurrection, which is why the builder and the cold restore are
   spelled out below rather than left to a helper."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            #?(:clj  [org.replikativ.persistent-sorted-set.test.storage :as ts]
               :cljs [org.replikativ.persistent-sorted-set.test.storage.util :as u]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set
                    PersistentSortedSet ANode Settings])))

(def ^:private BF 8)
(def ^:private DBS 256)

;; The SET orders by the whole vector; the DISJ comparator keys on the first
;; element alone, so `[17 999]` finds and removes the stored `[17 0]`.
(defn- by-first [a b] (compare (first a) (first b)))

(defn- elems [n] (vec (for [i (range n)] [i 0])))

#?(:clj
   (do
     (defn- node-settings ^Settings [] (Settings. (int BF) nil nil nil (int DBS)))

     (defn- fresh [disk addr]
       (s/restore-by compare addr
                     (ts/->Storage (atom {}) disk (node-settings))
                     {:branching-factor BF :diff-buf-size DBS :comparator compare}))

     (deftest a-buffered-disj-records-the-element-it-removed
       (testing "the reload must agree with memory. Before the fix the removed
                 element was resurrected, giving a set whose `seq` and `count`
                 disagree and whose resurrected member is unfindable."
         (let [n 40
               disk (atom {})
               st (ts/->Storage (atom {}) disk (node-settings))
               xs (elems n)
               s0 (s/from-sorted-array compare (to-array xs) n
                                       {:branching-factor BF :diff-buf-size DBS :storage st})
               addr0 (s/store s0 st)
               cold (fresh disk addr0)
               s2 (s/disj cold [17 999] by-first)
               addr1 (s/store s2 st)
               back (fresh disk addr1)
               want (vec (remove #(= % [17 0]) xs))]
           (is (= 1 (.level ^ANode (.root ^PersistentSortedSet cold)))
               "precondition: a LEVEL-1 root. At level 2 the slot is a branch
                marker with a null diff and this cannot fail — which is exactly
                how six earlier conj-built attempts came back green.")
           (is (= (dec n) (count s2)) "in memory")
           (is (= want (vec (seq back))) "the reload must equal memory")
           (is (= (dec n) (count back)) "count after reload")
           (is (= (count (vec (seq back))) (count back))
               "count and seq must describe the same set")
           (is (not (contains? back [17 0])) "and the removed element stays gone"))))

     (deftest the-baseline-is-clean
       (testing "the same cycle with buffering OFF — what localises the defect to
                 the diff-buf deposit rather than to disj or to restore"
         (let [n 40
               disk (atom {})
               st (ts/->Storage (atom {}) disk (Settings. (int BF) nil nil nil (int 0)))
               xs (elems n)
               s0 (s/from-sorted-array compare (to-array xs) n
                                       {:branching-factor BF :diff-buf-size 0 :storage st})
               a0 (s/store s0 st)
               cold (s/restore-by compare a0 (ts/->Storage (atom {}) disk (Settings. (int BF) nil nil nil (int 0)))
                                  {:branching-factor BF :diff-buf-size 0 :comparator compare})
               s2 (s/disj cold [17 999] by-first)
               a1 (s/store s2 st)
               back (s/restore-by compare a1 (ts/->Storage (atom {}) disk (Settings. (int BF) nil nil nil (int 0)))
                                  {:branching-factor BF :diff-buf-size 0 :comparator compare})]
           (is (= (vec (remove #(= % [17 0]) xs)) (vec (seq back))))
           (is (= (dec n) (count back))))))))

#?(:cljs
   (deftest a-buffered-disj-records-the-element-it-removed
     (testing "the ClojureScript twin. `branch.cljs` already threaded the removed
               element through `:removed-out` for the measure, bound it, and then
               deposited the search key anyway — the correct value was computed
               and discarded. It was also allocated only when a `:measure` was
               configured, so without one it fell back to the search key."
       (let [n 40
             opts {:comparator compare :branching-factor BF :diff-buf-size DBS}
             disk (atom {})
             st (u/storage (atom {}) disk opts)
             xs (elems n)
             s0 (s/from-sequential compare xs (assoc opts :storage st))
             _ (s/store s0 st)
             s2 (s/disj s0 [17 999] by-first)
             a1 (s/store s2 st)
             back (s/restore a1 (u/storage (atom {}) disk opts) opts)
             want (vec (remove #(= % [17 0]) xs))]
         (is (= (dec n) (count s2)) "in memory")
         (is (= want (vec (s/seq back))) "the reload must equal memory")
         (is (= (dec n) (count back)) "count after reload")
         (is (= (count (vec (s/seq back))) (count back))
             "count and seq must describe the same set")))))
