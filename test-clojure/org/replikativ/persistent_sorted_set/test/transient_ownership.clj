(ns org.replikativ.persistent-sorted-set.test.transient-ownership
  "A transient belongs to the thread that made it.

   `doc/CONCURRENCY.md` says so, and — following Clojure — it is a CONTRACT rather
   than an enforcement. Clojure's `PersistentVector.persistent()` carries the owner
   check commented out on purpose: an owner comparison sees thread IDENTITY, so it
   cannot distinguish a HANDOFF (one thread finishes, publishes across a
   happens-before edge, another continues — safe, and used by `fold` and by any
   core.async block that parks) from CONCURRENT mutation, which is what actually
   corrupts. Enforcing it would forbid the safe pattern to prevent the unsafe one.

   So ownership is checked only under `-Dpss.strictTransients=true`, for suites
   that know no handoff occurs. What IS always checked is the case Clojure checks
   and this library never did: using a transient after `persistent!`.

   Measured before the check existed — 4 threads x 5000 `conj!` on one transient,
   three trials:

       expected 20000   seq 19193   count 19166   sorted? false
       expected 20000   seq 19562   count 14742   sorted? false
       expected 20000   seq 19262   count 13942   sorted? false

   Note `sorted?`. This is not only lost elements: the keys come out of order, so
   every later `binarySearch` on the set is arbitrary — and it is durable the
   moment the set is stored."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]))

(defn- opts [] {:branching-factor 32 :comparator compare})

(deftest a-stale-transient-handle-is-refused
  (testing "using a transient after persistent! throws, as every Clojure transient
            does. It used to answer `editable() == false` and quietly take the
            PERSISTENT path — so `conj!` returned a new set and the caller's
            mutation went somewhere they were not looking."
    (let [t (transient (s/sorted-set* (opts)))
          _ (conj! t 1)
          p (persistent! t)]
      (is (not (identical? p t))
          "persistent! returns a NEW set — which is what makes the stale handle detectable")
      (is (= [1] (vec (seq p))) "and the result is a usable persistent set")
      (doseq [[label op] [["conj!" #(conj! t 2)] ["disj!" #(disj! t 1)]]]
        (let [e (try (op) nil (catch Throwable e e))]
          (is (instance? IllegalAccessError e) (str label ": expected IllegalAccessError"))
          (is (= "Transient used after persistent! call" (ex-message e)) label))))))

(deftest a-set-that-was-never-transient-is-unaffected
  (testing "the stale-handle guard must not touch ordinary persistent use"
    (let [s0 (reduce #(s/conj %1 %2 compare) (s/sorted-set* (opts)) (range 100))]
      (is (= 101 (count (seq (s/conj s0 999 compare))))))))

(deftest ownership-is-enforced-only-under-the-strict-flag
  (testing "off by default so a legitimate handoff is not forbidden; on under
            -Dpss.strictTransients=true so a suite can catch real misuse.

            Asserts the FLAG's effect rather than a fixed outcome, so it states
            the contract in both configurations."
    (let [strict? (Boolean/getBoolean "pss.strictTransients")
          t   (transient (s/sorted-set* (opts)))
          err (atom nil)
          th  (Thread. #(try (conj! t 1) (catch Throwable e (reset! err e))))]
      (.start th) (.join th)
      (if strict?
        (do (is (instance? IllegalAccessError @err))
            (is (= "Transient used by non-owner thread" (ex-message @err))))
        (is (nil? @err) "permissive by default: a foreign thread is served, as in Clojure")))))

(deftest the-owning-thread-is-unaffected
  (testing "the check must not cost the normal case anything — a transient used
            by its owner behaves exactly as before"
    (let [t (transient (s/sorted-set* (opts)))]
      (dotimes [i 5000] (conj! t i))
      (dotimes [i 1000] (disj! t (* 2 i)))
      (let [p (persistent! t)
            xs (vec (seq p))]
        (is (= (remove even? (range 5000)) (filter odd? xs)))
        (is (= xs (vec (sort compare xs))) "and it is sorted")
        (is (= (count xs) (count p)) "and seq agrees with count")))))

(deftest ^:strict-transients concurrent-mutation-is-refused-under-the-flag
  (testing "under -Dpss.strictTransients=true a racing multi-threaded mutation
            cannot corrupt the set. WITHOUT the flag this is undefined behaviour
            and the set really does come out broken — measured: sorted? false,
            seq 19318 vs count 14793, and one trial threw
            ArrayIndexOutOfBoundsException from Seq/first — so the assertion only
            holds in strict mode and is skipped otherwise."
    (when (Boolean/getBoolean "pss.strictTransients")
      (let [t  (transient (s/sorted-set* (opts)))
            ths (mapv (fn [k] (Thread. #(dotimes [j 2000]
                                          (try (conj! t (+ (* k 100000) j))
                                               (catch Throwable _ nil)))))
                      (range 4))]
        (run! #(.start %) ths)
        (run! #(.join %) ths)
        (let [p  (persistent! t)
              xs (vec (seq p))]
          (is (= xs (vec (sort compare xs))) "sorted")
          (is (= (count xs) (count (distinct xs))) "no duplicates")
          (is (= (count xs) (count p)) "seq agrees with count"))))))
