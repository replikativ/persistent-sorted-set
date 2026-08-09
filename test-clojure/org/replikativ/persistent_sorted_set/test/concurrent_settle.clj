(ns org.replikativ.persistent-sorted-set.test.concurrent-settle
  "Two threads must not settle the same node. The contract is not enforced — this DETECTS it.

   `store()` publishes the settled per-child state with a PLAIN write rather than a CAS, and
   publishes BEFORE handing the node to `IStorage.store`. Both are fine under the documented
   contract — one settle at a time per LINEAGE (`doc/CONCURRENCY.md`) — and neither is safe
   when two threads settle versions that share dirty nodes.

   Structural sharing makes that easy to reach by accident. Measured on the pipelining-writer
   shape (derive v1 from a base, then v2 from v1, touching different subtrees) at bf 8 /
   n 1000: 3 Branch objects were reachable from BOTH roots and dirty in both, so storing
   either settles the same objects. Over 1600 barrier-synchronised rounds the race fired
   consistently — 20 writes where a sequential run does 15, in 196 of 200 rounds — without
   producing a content error, which is exactly why it needs a detector rather than a test that
   waits for corruption.

   Serialising `store()` would tax every single-threaded caller for a contract violation, so
   the guard is an assertion: `Branch.beginSettle`/`endSettle` run only inside `assert`, so
   with -da there is no map, no allocation and no lookup.

   ## Why the interleaving here is deterministic

   Racing two threads and hoping their windows overlap makes a flaky test. Instead the storage
   BLOCKS inside `store`: thread A enters, latches, and waits; thread B then enters the same
   node's `store` and trips the detector. A hammer loop would sometimes pass for the wrong
   reason and could not distinguish 'no defect' from 'no overlap'.

   ## What this cannot catch

   Two threads settling DISJOINT trees over one storage — legal, and it must not fire there,
   which the second deftest pins. And any race whose windows never overlap. It is a detector,
   not a proof."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as ss])
  (:import [org.replikativ.persistent_sorted_set Settings IStorage ANode Branch Leaf
            PersistentSortedSet]
           [java.util.concurrent CountDownLatch TimeUnit]))

(set! *warn-on-reflection* true)

(defn- settings ^Settings [bf] (Settings. (int bf) nil nil nil (int 0)))

;; A storage that parks the FIRST thread to store a branch, holding it inside `store()` until
;; released. That keeps thread A's settle window open while thread B enters the same node.
(defrecord BlockingStorage [*disk ^Settings settings ^CountDownLatch entered ^CountDownLatch release *blocked?]
  IStorage
  (store [_ node]
    (let [^ANode node node]
      (when (and (instance? Branch node) (compare-and-set! *blocked? false true))
        (.countDown entered)
        (.await release 5 TimeUnit/SECONDS))
      (let [addr (str (java.util.UUID/randomUUID))]
        (swap! *disk assoc addr {:level (.level node) :keys (vec (.keys node))})
        addr)))
  (accessed [_ _] nil)
  (restore [_ _] (throw (ex-info "restore not used in this test" {}))))

(defn- build [bf n]
  (reduce #(ss/conj %1 %2 compare)
          (ss/sorted-set* {:comparator compare :branching-factor bf}) (range n)))

(deftest two-threads-settling-the-same-node-are-detected
  (testing "the contract is per LINEAGE, not per tree — two versions can share dirty nodes,
            and nothing serialises store(). Detected under -ea rather than enforced."
    (let [bf 8
          s   (build bf 200)
          disk (atom {})
          entered (CountDownLatch. 1)
          release (CountDownLatch. 1)
          st  (->BlockingStorage disk (settings bf) entered release (atom false))
          err (atom nil)
          ;; both threads settle THE SAME set, hence the same node objects
          t1  (Thread. #(try (ss/store s st) (catch Throwable t (reset! err t))))
          t2  (Thread. #(try (ss/store s st) (catch Throwable t (reset! err t))))]
      (.start t1)
      ;; wait until t1 is parked INSIDE store() with its settle window open
      (is (.await entered 5 TimeUnit/SECONDS)
          "PRECONDITION: the storage must have parked a thread inside store(); without that
           the two windows need not overlap and this test would prove nothing")
      (.start t2)
      (.join t2 5000)
      (.countDown release)
      (.join t1 5000)
      (let [e @err]
        (is (instance? AssertionError e)
            (str "expected the concurrent settle to be detected, got: " (pr-str e)))
        (when (instance? AssertionError e)
          (is (re-find #"concurrent settle" (str (.getMessage ^AssertionError e)))
              "and the message must name the contract"))))))

(deftest settling-disjoint-trees-concurrently-is-allowed
  (testing "the detector must not fire for two threads storing DIFFERENT trees to one
            storage — that is legal and common. A detector that fired here would be worse
            than none, and a test that only checked the positive case would not notice."
    (let [bf 8
          a (build bf 200)
          b (build bf 200)              ; independent tree, no shared nodes
          disk (atom {})
          st (->BlockingStorage disk (settings bf) (CountDownLatch. 1) (CountDownLatch. 0)
                                (atom true))   ; pre-tripped: never block
          err (atom nil)
          t1 (Thread. #(try (ss/store a st) (catch Throwable t (reset! err t))))
          t2 (Thread. #(try (ss/store b st) (catch Throwable t (reset! err t))))]
      (.start t1) (.start t2)
      (.join t1 10000) (.join t2 10000)
      (is (nil? @err)
          (str "storing disjoint trees concurrently must not trip the detector, got: "
               (pr-str @err))))))

(deftest the-detector-is-reentrant-for-one-thread
  (testing "store() recurses into children on the same thread; that must not look like a
            race. Ordinary single-threaded storing is the overwhelmingly common path."
    (let [bf 8 disk (atom {})
          st (->BlockingStorage disk (settings bf) (CountDownLatch. 1) (CountDownLatch. 0)
                                (atom true))]
      (is (some? (ss/store (build bf 500) st)) "a plain store must succeed")
      (is (pos? (count @disk)) "and must actually have written nodes"))))
