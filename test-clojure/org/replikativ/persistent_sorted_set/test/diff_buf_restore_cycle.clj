(ns org.replikativ.persistent-sorted-set.test.diff-buf-restore-cycle
  "The store -> restore -> modify -> store cycle, under diff-buf.

   This is the cycle every consumer runs on every reconnect, and until these
   tests existed nothing exercised it: `stress_diff_buf.clj` drives ONE live
   set, so the restore-side projection never fed its own output back into a
   store. Two defects lived there, both PRE-EXISTING since 1256f81 (the commit
   that introduced diff-buf), both single-threaded, and both fixed with these
   tests.

   ## A. `replace` recorded the wrong element as removed

   `Branch.replace` deposited `Absent(oldKey)` using the CALLER's search key.
   With an operation comparator coarser than the set's — datahike's
   value-changing datom upsert searches `[e a _ _]` and replaces the whole
   datom — that is not the element the leaf actually holds once a previous
   buffered replace has already changed it. The Absent then cancelled nothing
   and the new Present was added ALONGSIDE the old one.

   Measured before the fix, 16 elements, two replaces of one key in a single
   transient cycle, store, restore: key 5 came back as `[[5 1] [5 2]]`, `count`
   16 against a `seq` of 17. In memory it looked correct throughout — the
   transient leaf is mutated in place, so nothing projects the diff until a
   reload, and the corruption only appears after one.

   Fixed by capturing the element the replace will actually remove, before the
   mutation removes it.

   ## B. A null diff does not always mean a branch anchor

   `Slot`'s javadoc says a null `diff` marks \"a BRANCH anchor marker\", and
   `assembleNested` recursed on that reading, casting the child to a `Branch`.
   But `depositKV` also leaves the diff null when the slot's `anchor` is null —
   the \"no durable base, write this child wholesale\" case — which happens on
   LEAF children too. A plain store then threw `ClassCastException`.

   Fixed by skipping anchor-less slots (a wholesale-written child has no
   buffered difference to assemble, at any level), with an assertion pinning
   the remaining case.

   ## What the harness must spell out

   `set-dbs` and `node-dbs` are separate arguments. The test storage's default
   is `(Settings.)`, whose `diffBufSize` comes from the `pss.diffBufSize` system
   property — which the `:test` alias sets to 256. A case that passes
   `:diff-buf-size 0` in the set's opts and leaves the storage on the default is
   therefore NOT testing the baseline; it runs at 256. That cost a wrong
   conclusion while these defects were being characterised (the baseline was
   briefly reported as broken), and it is the same flaw the audit found in
   `stress_diff_buf.clj`, which passes one budget into both sides so they can
   never disagree."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet RefType Settings]))

(set! *warn-on-reflection* true)

(defn- full-cmp [[k1 v1 t1] [k2 v2 t2]]
  (let [c (compare k1 k2)]
    (if-not (zero? c) c
            (let [c (compare v1 v2)]
              (if-not (zero? c) c (compare t1 t2))))))

(defn- by-k [a b] (compare (nth a 0) (nth b 0)))

(defn- node-settings
  "Settings the STORAGE builds restored nodes with. Must be spelled out: the
   test storage's default is `(Settings.)`, whose `diffBufSize` comes from the
   `pss.diffBufSize` system property — which the `:test` alias sets to 256. A
   run that passes `:diff-buf-size 0` in the set's opts and leaves the storage
   on the default is therefore NOT testing the baseline: the nodes come back at
   256 and the restored set adopts that. Measured — it is what made an earlier
   version of this namespace report the baseline as broken."
  ^Settings [bf dbs]
  (Settings. (int bf) RefType/STRONG nil nil (int dbs)))

(defn- cycle-once
  "`rounds` of (restore from the address -> `ups` key-only replaces -> store).

   `set-dbs` is what the SET declares, `node-dbs` what the STORAGE builds
   restored nodes with — SEPARATE arguments on purpose, see the ns docstring.
   Returns :clean, or the first round whose restored set is not what was stored."
  [n bf set-dbs node-dbs rounds ups]
  (let [disk (atom {})
        st   (ts/->Storage (atom {}) disk (node-settings bf node-dbs))
        opts {:comparator full-cmp :branching-factor bf
              :diff-buf-size set-dbs :ref-type :strong}
        s0   (into (ss/sorted-set* (assoc opts :storage st))
                   (map (fn [k] [k 0 0]) (range n)))]
    (try
      (loop [round 0, addr (ss/store s0 st)]
        (if (= round rounds)
          :clean
          (let [st2  (ts/->Storage (atom {}) disk (node-settings bf node-dbs))
                cold (ss/restore-by full-cmp addr st2 (assoc opts :storage st2))
                c    (count cold)
                sq   (vec (seq cold))
                dups (->> (map first sq) frequencies (filter #(> (val %) 1)) (into {}))]
            (if (or (not= c (count sq)) (seq dups))
              {:round round :count c :seq-count (count sq) :duplicate-keys dups}
              (let [rnd (java.util.Random. (+ 100 round))
                    t (reduce (fn [^PersistentSortedSet t _]
                                (let [k (.nextInt rnd (int n))]
                                  (.replace t [k 0 0] [k (inc round) round] by-k)))
                              (.asTransient ^PersistentSortedSet cold) (range ups))
                    s' (.persistent ^PersistentSortedSet t)]
                (recur (inc round) (ss/store s' st2)))))))
      (catch Throwable e
        {:threw (.getName (class e))
         :at (str (first (filter #(re-find #"persistent_sorted_set" (str %))
                                 (.getStackTrace e))))}))))

(deftest a-restore-cycle-preserves-the-set-under-diff-buf
  (testing "DEFECT A, fixed. Was round 2 every time and independent of depth —
            bf 4 and bf 512 duplicated alike."
    (is (= :clean (cycle-once 40   4   128 128 8 2))  "minimal: 40 elements, bf 4")
    (is (= :clean (cycle-once 8000 64  128 128 8 48)) "three levels")
    (is (= :clean (cycle-once 8000 512 128 128 8 48)) "two levels")))

(deftest the-baseline-restore-cycle-is-clean
  (testing "the same cycle with buffering off, at the same three shapes — what
            localises defect A to diff-buf rather than to restore or replace.

            NOT excluded from the suite: if this ever fails, the defect is
            broader than diff-buf and this namespace's claims are wrong."
    (is (= :clean (cycle-once 40   4   0 0 8 2)))
    (is (= :clean (cycle-once 8000 64  0 0 8 48)))
    (is (= :clean (cycle-once 8000 512 0 0 8 48)))))

(deftest a-set-may-declare-a-budget-its-nodes-do-not-carry
  (testing "DEFECT B. Set buffers at 128, the storage rebuilds nodes at 0.

            Was a ClassCastException out of `Branch.assembleNested`. Fixing that
            stopped the crash but not the wrongness: the diff-buf stress sweep
            then found this configuration returning wrong CONTENT with a
            MATCHING count — substituted elements — on 5 of 5 seeds.

            So it is REFUSED rather than accommodated. The node's settings are
            the storage's to get right: they describe a blob the storage itself
            wrote, and a library that silently returns the wrong set is worse
            than one that names which half of the contract broke."
    (let [r (cycle-once 8000 64 128 0 8 48)]
      (is (= "java.lang.IllegalStateException" (:threw r))
          (str "expected a refusal, got " (pr-str r)))
      (is (re-find #"installSlots" (str (:at r)))
          "refused where the incoherence is first visible")))
  (testing "the converse — a set that declares NO budget over nodes that carry
            one — is the REALIZABLE direction (a blob knows its budget, a caller
            restoring without opts does not) and must keep working, via root()'s
            upward adoption. This is D-F1."
    (is (= :clean (cycle-once 8000 64 0 128 8 48)))))

;; ---------------------------------------------------------------------------

(defn- by-k2 [a b] (compare (nth a 0) (nth b 0)))

(deftest replace-refuses-an-ambiguous-slot-under--ea
  (testing "`replace` PRECONDITION: at most one element per operation-comparator
            key. It writes the new element into the OLD element's slot, which
            keeps the set sorted only while the replacement belongs in that same
            position. When `cmp` is coarser than the SET's comparator, a search
            picks an ARBITRARY one of several equal elements, and writing over it
            can move it past a sibling.

            Measured before the assertion existed, bf 4, [k 0] for k in 0..39
            plus [5 7]: `(replace s [5 0] [5 9] by-k)` returned [[5 9] [5 7]] —
            out of order — and `contains? s [5 7]` then returned FALSE for an
            element still in the set, because every later lookup binary searches
            an array that is no longer ordered. Silent, and durable once stored.

            An ASSERTION, not a runtime check: `-ea` in this alias, elided in
            production, so it costs users nothing and turns the misuse into a
            named failure in their tests.

            NOTE the two equal elements land in SEPARATE leaves here —
            [[4 0] [5 0]] | [[5 7] [6 0] [7 0]] — which is why the check cannot
            live in `Leaf.replace` alone. `Branch.noEqualSiblingAcrossBoundary`
            covers the boundary, `Leaf.noEqualSibling` the interior."
    (let [opts {:comparator (fn [a b]
                              (let [c (compare (nth a 0) (nth b 0))]
                                (if-not (zero? c) c (compare (nth a 1) (nth b 1)))))
                :branching-factor 4 :diff-buf-size 128}
          base (-> (reduce #(ss/conj %1 [%2 0] (:comparator opts))
                           (ss/sorted-set* opts) (range 40))
                   (ss/conj [5 7] (:comparator opts)))]
      (is (= 41 (count base)) "precondition: both elements for key 5 are present")
      (is (thrown-with-msg? AssertionError #"may be UNSORTED"
                            (ss/replace base [5 0] [5 9] by-k2))
          "the ambiguous replace is named rather than silently corrupting the set")))

  (testing "the supported case — one element per operation-comparator key — is
            untouched, at the same shape"
    (let [cmp (fn [a b]
                (let [c (compare (nth a 0) (nth b 0))]
                  (if-not (zero? c) c (compare (nth a 1) (nth b 1)))))
          opts {:comparator cmp :branching-factor 4 :diff-buf-size 128}
          base (reduce #(ss/conj %1 [%2 0] cmp) (ss/sorted-set* opts) (range 40))
          v1   (ss/replace base [5 0] [5 9] by-k2)]
      (is (= [[5 9]] (vec (filter #(= 5 (nth % 0)) (seq v1)))))
      (is (= 40 (count v1)))
      (is (= (vec (seq v1)) (vec (sort cmp (seq v1)))) "and still sorted"))))
