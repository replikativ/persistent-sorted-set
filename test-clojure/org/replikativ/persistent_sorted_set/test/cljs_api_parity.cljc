(ns org.replikativ.persistent-sorted-set.test.cljs-api-parity
  "Four public-API defects that existed only on ClojureScript, pinned as PARITY.

   Each of these had a correct JVM half, so a `.clj` test could never have caught them and a
   `.cljs` test would have had to guess at the right answer. Written as `.cljc` so the JVM
   side states the expected behaviour and the ClojureScript side has to match it.

   1. `from-sorted-array` IGNORED its `len` argument. The parameter was spelled `_len` and
      both builders partitioned the whole array, taking the count from `alength`:

          (from-sorted-array compare #js [1 2 3 4 5] 3)
          JVM  => [1 2 3]       count 3
          cljs => [1 2 3 4 5]   count 5

      A caller passing a reusable buffer with only its first `len` slots valid got the
      buffer's stale tail as set members, with no error.

   2. `compact` dropped metadata and storage, contradicting its own docstring
      (\"Preserves comparator, settings, and metadata\"). `(.-settings set)` is the NODE
      settings subset — `[:branching-factor :measure :boundary :diff-buf-size]` — so neither
      `:meta` nor `:storage` was reachable through it. `(store (compact s))` threw. The JVM
      half of exactly this was found and fixed earlier; the cljs half never was.

   3. `seek` BACKWARD was a no-op. `Iter -seek` returned `this` whenever the current element
      already compared `>= key`, so seeking from 5000 back to 2500 answered from 5000.

   4. `seek` did not respect the seq's retained bound: `(seek (slice s 2500 7500) 9000)`
      answered `[9000]` on cljs and `nil` on the JVM. Every other method on the same type
      guards on `right`; only `-seek` did not.

   (3) and (4) pull in opposite directions — one too conservative, one not conservative
   enough — which is why both survived: any test that fixed a mental model on one of them
   would look consistent with the other."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            #?(:clj  [org.replikativ.persistent-sorted-set.test.storage :as tstore]
               :cljs [org.replikativ.persistent-sorted-set.test.storage.util :as util]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set Settings])))

;; `sq` takes the seq of a SET. It is NOT for slices or seek results: on ClojureScript
;; `set/seq` is BTSet's own seq, and handing it an Iter throws inside the iterator rather
;; than reporting a test failure. Iterators are already seqs on both runtimes, so they are
;; compared and nil-checked directly.
(defn- elems [s] (vec #?(:clj (seq s) :cljs (set/seq s))))
(defn- sq [s] #?(:clj (seq s) :cljs (set/seq s)))

(defn- arr-of
  "A native array of `xs` — Object[] on the JVM, js array on ClojureScript."
  [xs]
  #?(:clj (to-array xs) :cljs (into-array xs)))

(defn- storage []
  #?(:clj  (tstore/->Storage (atom {}) (atom {}) (Settings. (int 8) nil nil nil (int 0)))
     :cljs (util/storage (atom {}) (atom {}) {:branching-factor 8 :diff-buf-size 0})))

;; ---------------------------------------------------------------------------
;; 1. from-sorted-array honours `len`

(deftest from-sorted-array-uses-only-the-first-len-elements
  (testing "the tail of the array beyond `len` must not become set members"
    (is (= [1 2 3] (elems (set/from-sorted-array compare (arr-of [1 2 3 4 5]) 3))))
    (is (= 3 (count (set/from-sorted-array compare (arr-of [1 2 3 4 5]) 3)))))
  (testing "and it must hold once the tree is deeper than one leaf, where the builder
            partitions rather than taking a single shortcut"
    (doseq [bf [4 8]]
      (let [s (set/from-sorted-array compare (arr-of (range 100)) 30 {:branching-factor bf})]
        (is (= 30 (count s)) (str "bf=" bf ": count"))
        (is (= (range 30) (elems s)) (str "bf=" bf ": contents"))
        (is (not (contains? s 30)) (str "bf=" bf ": element 30 is past len"))))))

(deftest from-sorted-array-edge-lengths
  (testing "len 0 gives the empty set; len == alength is the whole array"
    (is (= [] (elems (set/from-sorted-array compare (arr-of [1 2 3]) 0))))
    (is (= 0 (count (set/from-sorted-array compare (arr-of [1 2 3]) 0))))
    (is (= [1 2 3] (elems (set/from-sorted-array compare (arr-of [1 2 3]) 3))))
    (is (= [] (elems (set/from-sorted-array compare (arr-of []) 0))))))

(deftest an-out-of-range-len-is-refused-identically-on-both-runtimes
  (testing "`len` must name a real prefix. The empty-array case is the one that mattered:
            `(from-sorted-array cmp [] 1)` built from a 1-element array whose only slot was
            null and returned a set CONTAINING NIL, count 1 — the same defect fixed for
            `from-sequential`, hidden by the same trap, since `assert-sorted!` passes
            VACUOUSLY at len 1. The other two directions used to surface as
            ArrayIndexOutOfBoundsException and IllegalArgumentException on the JVM, neither
            catchable alongside the ClojureScript half, hence one `ex-info` on both."
    (doseq [[label arr len] [["empty array, len 1" [] 1]
                             ["len past the end"   [1 2 3] 5]
                             ["negative len"       [1 2 3] -1]]]
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core/ExceptionInfo)
                   (set/from-sorted-array compare (arr-of arr) len))
          (str label ": must be refused, not silently accepted")))))

(deftest unsorted-input-is-refused-on-both-runtimes
  (testing "the JVM has always asserted strictly-ascending, distinct input; ClojureScript had
            NO check, so the same call was refused on one runtime and silently accepted on
            the other. Measured on cljs before the fix: 1000 shuffled elements gave count
            1000 with only 3 of them findable by `contains?`, and `#js [1 nil 3]` made nil a
            durable member of a set whose docstring says it cannot store nil."
    (is (thrown? #?(:clj AssertionError :cljs js/Error)
                 (set/from-sorted-array compare (arr-of [3 1 2]) 3))
        "descending input")
    (is (thrown? #?(:clj AssertionError :cljs js/Error)
                 (set/from-sorted-array compare (arr-of [1 2 2 3]) 4))
        "duplicates")
    ;; A nil element is now refused by a dedicated, UNCONDITIONAL check that runs before the
    ;; ordering assert, so it reports as a nil violation rather than as unsorted input. That
    ;; matters: the ordering assert is elidable and could only ever catch a nil that happened
    ;; to sit out of order, whereas `from-sorted-array` was one of the two entry points
    ;; through which nil could enter a set the library says cannot hold one.
    (is (thrown? #?(:clj IllegalArgumentException :cljs cljs.core/ExceptionInfo)
                 (set/from-sorted-array compare (arr-of [1 nil 3]) 3))
        "a nil element anywhere")
    (is (thrown? #?(:clj IllegalArgumentException :cljs cljs.core/ExceptionInfo)
                 (set/from-sorted-array compare (arr-of [nil 1]) 2))
        "including a LEADING nil, which under `compare` is legitimately ascending and so is
         invisible to the ordering check — the case that needed no exotic comparator at all")))

;; ---------------------------------------------------------------------------
;; 2. compact preserves what it says it preserves

(deftest compact-preserves-metadata
  (testing "the docstring promises metadata; it carries :pss/storage-id for the wire codec"
    (let [s (with-meta (set/from-sorted-array compare (arr-of (range 50)) 50) {:x 1})]
      (is (= {:x 1} (meta s)) "precondition: the source set carries the metadata")
      (is (= {:x 1} (meta (set/compact s))) "compact must carry it through")
      (is (= (range 50) (elems (set/compact s))) "and must not disturb the contents"))))

(deftest compact-preserves-storage-so-the-result-can-be-stored
  (testing "dropping the storage made (store (compact s)) throw — the failure mode is a
            crash on the next write, well away from the compact call that caused it"
    (let [st (storage)
          s  (set/from-sorted-array compare (arr-of (range 200)) 200
                                    {:branching-factor 8 :storage st})
          c  (set/compact s)]
      (is (= (range 200) (elems c)) "contents survive")
      ;; The ONE-arity store, deliberately: it uses the set's OWN storage, which is the
      ;; thing compact was dropping. Passing `st` explicitly (the two-arity) supplies the
      ;; storage the defect removed, so that version of this assertion passed against the
      ;; unfixed build — it looked like a test and was not one.
      (is (some? (set/store c))
          "the compacted set must carry its storage, so a bare (store c) works"))))

(deftest compact-on-degenerate-sets
  (testing "empty and single-element sets, where a builder is easiest to get wrong"
    (is (= [] (elems (set/compact (set/from-sorted-array compare (arr-of []) 0)))))
    (is (= [7] (elems (set/compact (set/from-sorted-array compare (arr-of [7]) 1)))))))

;; ---------------------------------------------------------------------------
;; 3 + 4. seek

(defn- big [bf] (set/from-sorted-array compare (arr-of (range 10000)) 10000
                                       {:branching-factor bf}))

(deftest seek-repositions-backward
  (testing "seeking below the current position must answer from `to`, not from where the
            iterator happens to be sitting"
    (doseq [bf [8 64]]
      (let [s (big bf)
            b (-> (sq s) (set/seek 5000) (set/seek 2500))]
        (is (= 2500 (first b)) (str "bf=" bf ": first element"))
        (is (= 7500 (count b)) (str "bf=" bf ": element count"))
        (is (= (range 2500 10000) b) (str "bf=" bf ": contents"))))))

(deftest reverse-seek-repositions-upward
  (testing "the mirror on a descending seq"
    (doseq [bf [8 64]]
      (let [s (big bf)
            r (-> (set/rslice s 9999 nil) (set/seek 5000) (set/seek 7500))]
        (is (= 7500 (first r)) (str "bf=" bf ": first element"))
        (is (= 7501 (count r)) (str "bf=" bf ": element count"))))))

(deftest forward-seek-is-unchanged
  (testing "the control — the direction that always worked, and the one a merge-join drives"
    (doseq [bf [8 64]]
      (let [s (big bf)]
        (is (= (range 5000 10000) (-> (sq s) (set/seek 5000))) (str "bf=" bf))
        (is (= (range 7000 10000) (-> (sq s) (set/seek 5000) (set/seek 7000)))
            (str "bf=" bf ": forward then further forward"))))))

(deftest seek-respects-the-seqs-retained-bound
  (testing "seek moves the START of a seq; it must never widen it past the bound it carries"
    (doseq [bf [8 64]]
      (let [s  (big bf)
            sl (set/slice s 2500 7500)]
        (is (nil? (set/seek sl 9000))
            (str "bf=" bf ": seeking past the upper bound yields nothing"))
        ;; The WHOLE RANGE, not `last`. Asserting only `last` reads the RETAINED bound,
        ;; which the backward-seek defect never moved — so both of these lines stayed green
        ;; against the broken build while their neighbours failed, certifying nothing.
        ;; 52351a3 removed exactly this pattern from `seek_backward.clj` and left its twin
        ;; here; caught by a test-vacuity audit.
        (is (= (range 5000 7501) (set/seek sl 5000))
            (str "bf=" bf ": a forward seek stays bounded above"))
        (is (= (range 3000 7501) (set/seek (set/seek sl 5000) 3000))
            (str "bf=" bf ": and so does a backward one"))
        ;; A seq retains only ONE bound: for an ascending seq that is the upper one. Seeking
        ;; below a slice's lower bound therefore yields elements under it — 1000..7500 here,
        ;; not 2500..7500 — which is the same rule a forward seek follows (it moves the start
        ;; and keeps `_keyTo`). Measured, not assumed: my first guess at this line was
        ;; 2500..7500 and it was wrong.
        (is (= (range 1000 7501) (set/seek sl 1000))
            (str "bf=" bf ": seeking below a slice's lower bound reaches below it — the "
                 "lower bound is not part of the seq — but the upper bound still holds"))))))

(deftest seek-agrees-with-a-fresh-slice-over-a-grid
  (testing "the general property rather than a handful of points: for any pair of targets,
            seeking twice must equal slicing once from the last target. A shape-dependent
            defect hides from single-point tests."
    (doseq [bf [4 8 64]]
      (let [s (set/from-sorted-array compare (arr-of (range 2000)) 2000
                                     {:branching-factor bf})]
        (doseq [a [37 500 999 1500 1963]
                b [0 37 499 500 501 1000 1500 1999]]
          (is (= (set/slice s b nil) (-> (sq s) (set/seek a) (set/seek b)))
              (str "bf=" bf " seek " a " then " b)))))))

(deftest replace-refuses-a-nil-new-key
  (testing "the other entry point through which nil could enter. Every other mutation path
            refuses nil, so a set that documents itself as unable to store one could still be
            made to hold one: `(replace (sorted-set-by cmp 1) 1 nil)` gave count 1, [nil]."
    (let [s (set/from-sorted-array compare (arr-of [1 2 3]) 3)]
      (is (thrown? #?(:clj IllegalArgumentException :cljs cljs.core/ExceptionInfo)
                   (set/replace s 2 nil))
          "a nil replacement must be refused")
      (is (= [1 2 3] (elems s)) "and the set is untouched"))))
