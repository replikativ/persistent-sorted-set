(ns org.replikativ.persistent-sorted-set.test.root-publication
  "`root()` lazily materializes a stored tree, and that materialization is a PUBLICATION:
   it hands another thread both the node and the `Settings` this method just adopted from
   it. Two things had to be true for that to be safe and neither was.

   ## 1. The read order was the inverse of the write order — the real defect

   `root()` restores, then ADJUSTS `_settings` — branching factor, boundary, diff-buf
   budget, each taken from what the restored node turns out to carry — and writes `_root`
   LAST, deliberately, so the adopted settings are in place before anything can use the
   root. The read was spelled:

       _settings.readReference(_root)

   and Java evaluates the RECEIVER before the argument (JLS 15.12.4.1). So the reader read
   `_settings` first and `_root` second: the exact inverse. A thread could therefore pair
   the newly published root with the settings from BEFORE the adoption, and the
   branching-factor adoption is not cosmetic — `root()`'s own comment records what it
   prevents, an array overrun when a set runs at a smaller bf than the nodes it was handed
   (`ArrayIndexOutOfBoundsException: last destination index 31 out of bounds for object
   array[8]`). This needs no store reordering, so it is reachable on x86's TSO and not only
   on a weak memory model. The fix is the local `rootRef` read first in `root()`/`store()`.

   ## 2. `_root` was not volatile

   Publishing `_settings` before `_root` only establishes happens-before if the `_root`
   write is a release and the read an acquire. Every read and every write of a set goes
   through `root()`, so one thread materializing while another reads is the ordinary case
   for a set shared between threads — which is how datahike uses one.

   ## What is and is not tested here, precisely

   `the-publication-field-is-volatile` is a real red check: it fails against a build with
   the modifier removed (verified by removing it, rebuilding, and watching it go red).

   There is deliberately NO test asserting that a non-volatile field is observably stale,
   and this is worth recording because a plausible one was written and discarded. Two
   spinning-reader variants were tried against a rebuilt non-volatile build on x86-64
   HotSpot — one polling a nanoTime deadline, one call-free with a bounded counter so that
   a hoisted load would show up as an early exit. BOTH PASSED against the broken build:
   the write became visible anyway. A test that cannot fail is worse than no test, so it is
   not here.

   That also corrects an inherited claim. This fix was queued off a report that a reader
   thread had been measured spinning 76 CPU-seconds on a null root. That measurement could
   not be reproduced here in two attempts and should not be repeated as fact. The fix
   stands on the read-order inversion in (1), which is a language-level guarantee rather
   than an empirical one, and on (2) being what the JMM requires for (1) to mean anything —
   not on that number."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet]
           [java.lang.reflect Modifier]
           [java.util.concurrent CountDownLatch TimeUnit]))

(deftest the-publication-field-is-volatile
  (testing "`_root` carries the happens-before edge for the `_settings` adoptions above it,
            so its modifier is load-bearing rather than decorative"
    (let [f (.getDeclaredField PersistentSortedSet "_root")]
      (is (Modifier/isVolatile (.getModifiers f))
          "_root must be volatile: root() writes _settings then _root, and only a volatile
           write makes the former visible to a thread that acquires the latter"))))

(deftest a-restored-set-materializes-lazily
  (testing "the precondition every claim above rests on — if restore materialized eagerly
            there would be no publication to get wrong"
    (let [st   (ts/storage)
          addr (set/store (into (set/sorted-set) (range 2000)) st)
          ^PersistentSortedSet s (set/restore addr st)]
      (is (nil? (.-_root s)) "restore leaves _root unmaterialized")
      (is (some? (.-_address s)) "and holds an address instead")
      (is (some? (.root s)) "root() materializes it")
      (is (some? (.-_root s)) "and publishes it")
      (is (= (range 2000) (vec s)) "contents survive the round trip"))))

(deftest concurrent-entry-agrees-on-the-settings-and-the-contents
  (testing "many threads entering root() on the same unmaterialized set must agree on the
            settings adopted from it, and on what the tree contains.

            NOT asserted, because it is not true and not promised: that they get the same
            NODE OBJECT. Measured here, 8 threads over 25 rounds: 24 rounds produced 2-4
            DISTINCT root objects. `root()` is a racy cache — losers of the race restore
            their own copy, use it, and whichever finishes last wins `_root`. That is
            benign (the copies are equal, no thread mutates a shared root, and a caching
            IStorage such as datahike's CachedStorage returns one object and collapses the
            race entirely) and costs only duplicated IO bounded by the thread count. An
            earlier version of this test asserted object identity and failed against
            correct code; recording the real behaviour instead of asserting a guarantee the
            implementation never made."
    (dotimes [_ 25]
      (let [st    (ts/storage)
            addr  (set/store (into (set/sorted-set) (range 500)) st)
            ^PersistentSortedSet s (set/restore addr st)
            n     8
            go    (CountDownLatch. 1)
            done  (CountDownLatch. n)
            seen  (atom [])
            ts*   (doall
                   (for [_ (range n)]
                     (doto (Thread.
                            (fn []
                              (.await go)
                              (let [r  (.root s)
                                    bf (.branchingFactor (.-_settings s))]
                                ;; maxKey, not identity: the copies are equal but need not
                                ;; be the same object (see the docstring above).
                                (swap! seen conj [(.maxKey r) bf]))
                              (.countDown done)))
                       (.start))))]
        (.countDown go)
        (is (.await done 10 TimeUnit/SECONDS) "all readers returned")
        (doseq [t ts*] (.join t 1000))
        (is (= n (count @seen)) "every thread reported")
        (is (= [499] (distinct (map first @seen)))
            "every thread's root must describe the same tree")
        (is (= 1 (count (distinct (map second @seen))))
            "and every thread must agree on the branching factor adopted from it")
        (is (= (range 500) (vec s)) "and the set still reads correctly afterwards")))))
