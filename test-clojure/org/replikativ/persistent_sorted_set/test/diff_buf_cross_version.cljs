(ns org.replikativ.persistent-sorted-set.test.diff-buf-cross-version
  "Projecting a buffered diff must not touch the node it projects FROM.

   A caching `IStorage` returns restored nodes SHARED BY ADDRESS across tree
   versions: consecutive commits buffer against the same durable anchor with
   different accumulated diffs, so version N's exact child at address B is the
   very object version N+1 projects `{B, δ}` onto. Projection is
   VERSION-SPECIFIC. Mutating that shared object in place — installing slots,
   rewriting separators, count and measure, or cache-filling projected children
   into it — leaks one version's projection into every other version's reads.

   The JVM hit this and fixed it (#19); `Branch.projectBranch` carries the
   reasoning at length. The ClojureScript port kept mutating.

   FOUR things have to coincide, which is why nothing caught it:

     1. diff-buf enabled,
     2. a storage whose node cache is SHARED across versions,
     3. each version derived from a RESTORED set — so the buffering happens
        against cached nodes rather than resident ones,
     4. inserts SCATTERED across many leaves, so many children carry slots.

   `stress_diff_buf.cljs` builds a fresh `*memory` per restore, so (2) never
   holds there — the same blind spot that hid the JVM's #19."
  (:require [cljs.test :refer-macros [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.test.storage.util :as u]))

(def ^:private n 4000)
(def ^:private scatter1 (vec (range 1 n 37)))
(def ^:private scatter2 (vec (range 2 n 53)))

(defn- scenario
  "base → v1 → v2, each derived from a RESTORED set, all through ONE storage so
   the node cache is shared. Returns what each version reads back, and what it
   should read, with `read-order` deciding which is materialized first."
  [{:keys [diff-buf read-order]}]
  (let [opts {:branching-factor 8 :diff-buf-size diff-buf :comparator compare}
        disk (atom {})
        mem  (atom {})
        st   (u/storage mem disk opts)
        base (reduce #(s/conj %1 %2 compare) (s/sorted-set* opts) (range 0 n 4))
        a0   (s/store base st)
        v1   (reduce #(s/conj %1 %2 compare) (s/restore a0 st opts) scatter1)
        a1   (s/store v1 st)
        v2   (reduce #(s/conj %1 %2 compare) (s/restore a1 st opts) scatter2)
        a2   (s/store v2 st)
        exp1 (vec (sort (distinct (concat (range 0 n 4) scatter1))))
        exp2 (vec (sort (distinct (concat (range 0 n 4) scatter1 scatter2))))
        r1   (s/restore a1 st opts)
        r2   (s/restore a2 st opts)
        [g1 g2] (if (= :v1-first read-order)
                  (let [x (vec (s/seq r1))] [x (vec (s/seq r2))])
                  (let [y (vec (s/seq r2))] [(vec (s/seq r1)) y]))]
    {:v1-missing (count (remove (set g1) exp1))
     :v1-extra   (count (remove (set exp1) g1))
     :v2-missing (count (remove (set g2) exp2))
     :v2-extra   (count (remove (set exp2) g2))}))

(deftest projection-does-not-clobber-the-version-it-projects-from
  (testing "two versions restored through ONE cache, in both read orders"
    (doseq [order [:v1-first :v2-first]]
      (let [r (scenario {:diff-buf 512 :read-order order})]
        (is (= {:v1-missing 0 :v1-extra 0 :v2-missing 0 :v2-extra 0} r)
            (str "read-order " order ": " (pr-str r)))))))

(deftest diff-buf-off-is-the-control
  (testing "with no buffering there is no projection, so nothing can leak — if
            this fails the fault is not in projection"
    (doseq [order [:v1-first :v2-first]]
      (let [r (scenario {:diff-buf 0 :read-order order})]
        (is (= {:v1-missing 0 :v1-extra 0 :v2-missing 0 :v2-extra 0} r)
            (str "read-order " order ": " (pr-str r)))))))
