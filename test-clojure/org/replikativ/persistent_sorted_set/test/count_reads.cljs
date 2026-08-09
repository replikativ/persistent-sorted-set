(ns org.replikativ.persistent-sorted-set.test.count-reads
  "`count` on a stored set must not load the tree.

   A branch KNOWS its element count: `subtree-count` is maintained by the write paths and
   round-trips through the blob. The JVM uses it — `Branch.count(IStorage)` returns
   `_subtreeCount` whenever it is >= 0 and only walks when it is -1. ClojureScript's
   `branch/$count` recursed unconditionally, so `(count s)` on a freshly restored set
   loaded EVERY node in the tree to add up numbers it was already holding.

   Measured by reverting the fix and rebuilding, one `count` on a cold restore:

       bf  8, n  500     162 blob reads  ->  1
       bf  8, n 2000     661 blob reads  ->  1
       bf 64, n  500      16 blob reads  ->  1
       bf 64, n 2000      63 blob reads  ->  1

   Every count VALUE was correct in both builds. That is the signature of this defect:
   the answer was never wrong, only the work absurd, so nothing in the suite could see it.

   (The figure originally reported for this defect was 1819 reads. It is not reproduced
   here at any shape tried, and should not be repeated; the table above is what this
   namespace actually measures.)

   That is a real cost for datahike on Node, where every read is a konserve round trip and
   `count` is called casually. It is also invisible to every correctness oracle in this
   suite: the ANSWER was always right, only the work was absurd. Which is why the assertion
   here is on `:reads`, not on the count — though it checks the count too, because a fast
   path that returns the wrong number would be far worse than a slow one.

   TRAP, and the reason each case builds its own storage: `u/storage`'s `*memory` atom
   caches by address and `:reads` counts only cache MISSES. Reusing the storage the set was
   written with means every node is already resident, the read count is 0 either way, and
   the test passes against both builds. Every restore below therefore gets a FRESH memory
   cache over the same `*disk`."
  (:require [cljs.test :refer-macros [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.btset :as btset]
            [org.replikativ.persistent-sorted-set.branch :as branch]
            [org.replikativ.persistent-sorted-set.impl.node :as node]
            [org.replikativ.persistent-sorted-set.test.storage.util :as u]))

(defn- written [n bf]
  (let [opts (assoc {} :branching-factor bf)
        disk (atom {})
        st   (u/storage (atom {}) disk opts)
        s0   (reduce s/conj (s/sorted-set* (assoc opts :storage st)) (range n))
        addr (s/store s0 st)]
    {:addr addr :disk disk :opts opts :n n}))

(defn- cold [{:keys [addr disk opts]}]
  ;; fresh *memory* over the same *disk* — see the TRAP note above
  (s/restore addr (u/storage (atom {}) disk opts) opts))

(defn- reads-during [f]
  (let [before (:reads @u/*stats)
        v      (f)]
    {:value v :reads (- (:reads @u/*stats) before)}))

(deftest count-on-a-cold-stored-set-reads-only-the-root
  (doseq [bf [8 64]
          n  [500 2000]]
    (let [w (written n bf)
          set* (cold w)
          {:keys [value reads]} (reads-during #(count set*))]
      (is (= n value)
          (str "bf=" bf " n=" n ": the count must be right — a wrong fast path is worse "
               "than a slow correct one"))
      (is (= 1 reads)
          (str "bf=" bf " n=" n ": count must read the ROOT and nothing else; it read "
               reads " blobs")))))

(deftest the-restored-root-actually-carries-its-count
  (testing "the precondition the fast path rests on. If a restored root came back with
            subtree-count -1 the fast path would never be taken, the test above would
            measure the walk in both builds, and it would pass against the defect."
    (doseq [bf [8 64]]
      (let [set* (cold (written 2000 bf))
            root (btset/root-node set*)]
        (is (instance? branch/Branch root)
            (str "bf=" bf ": 2000 elements must give a Branch root, not a Leaf — a Leaf
                  root counts its own key array and never reaches branch/$count"))
        (is (>= (node/subtree-count root) 0)
            (str "bf=" bf ": the root must come back from the blob with a known count"))
        (is (= 2000 (node/subtree-count root))
            (str "bf=" bf ": and it must be the RIGHT count"))))))

(deftest an-unknown-count-still-walks-and-is-still-right
  (testing "-1 is the honest answer for a branch whose children are not all known, and the
            walk must remain reachable and correct for it"
    (let [set* (cold (written 2000 8))]
      ;; A full traversal is the independent oracle: it never consults subtree-count.
      (is (= (range 2000) (vec (s/seq set*))))
      (is (= 2000 (count set*)) "and agrees with count"))))
