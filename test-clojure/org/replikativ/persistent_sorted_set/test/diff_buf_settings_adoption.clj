(ns org.replikativ.persistent-sorted-set.test.diff-buf-settings-adoption
  "A restored set must take its diff-buf budget from the NODES when the caller did
   not name one.

   Nodes are self-describing: each blob carries its own `:diff-buf-size`, exactly
   as it carries its boundary. `root()` has always adopted the boundary; it did
   not adopt the budget. So the documented bare call —

       (pss/restore-by cmp addr storage)

   — produced a set at budget 0 over nodes at budget N. Reads stayed correct,
   because projection is driven by the NODE's settings through `child()`. The
   next WRITE rebuilt through the SET's settings and dropped every surviving
   sibling's buffered elements.

   The mismatch is the whole bug, which is why nothing caught it:
   `stress_diff_buf.clj` passes `:diff-buf-size` into the restore opts AND into
   the storage's `Settings`, so its two halves can never disagree. These tests
   deliberately make them disagree."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet Settings]))

(def ^:private bf 16)
(def ^:private budget 512)
(def ^:private n 6000)
(def ^:private scatter (vec (range 3 n 37)))

(defn- diff-buf-of [set]
  (let [f (.getDeclaredField PersistentSortedSet "_settings")]
    (.setAccessible f true)
    (.diffBufSize ^Settings (.get f set))))

(defn- scenario
  "A tree stored WITH buffered slots, then restored under `restore-opts` and
   written to once more. Returns what a cold read-back finds versus what it
   should. `nil` restore-opts means the bare documented call."
  [restore-opts]
  (let [disk  (atom {})
        ;; the NODES carry the budget, as a real self-describing blob does
        nodes (Settings. (int bf) nil nil nil (int budget))
        st    #(ts/->Storage (atom {}) disk nodes)
        full  {:branching-factor bf :diff-buf-size budget :comparator compare}
        v0    (reduce #(s/conj %1 %2 compare) (s/sorted-set* full) (range 0 n 2))
        a0    (s/store v0 (st))
        ;; buffer a scattered batch through a correctly-configured set and store it:
        ;; the blob now carries SLOTS, which is what the next write can drop
        v1    (reduce #(s/conj %1 %2 compare) (s/restore-by compare a0 (st) full) scatter)
        a1    (s/store v1 (st))
        r     (s/restore-by compare a1 (st) restore-opts)
        v2    (reduce #(s/conj %1 %2 compare) r [7777777])
        a2    (s/store v2 (st))
        got   (vec (seq (s/restore-by compare a2 (st) full)))
        want  (vec (sort (distinct (concat (range 0 n 2) scatter [7777777]))))]
    {:missing (count (remove (set got) want))
     :extra   (count (remove (set want) got))
     :adopted (diff-buf-of r)}))

(deftest a-bare-restore-adopts-the-nodes-budget
  (testing "restoring without :diff-buf-size must not lose the buffered elements
            the nodes already carry — measured at 81 lost before the adoption.

            The assertion is about the DATA, not about a particular number: what
            makes the difference between loss and no loss is whether buffering is
            ENABLED at all, not whether the two budgets match. A set at 0 over
            nodes at N rebuilds through a no-buffering path and drops their
            slots; a set at any positive budget does not. Adoption therefore only
            ever raises from 0, and this asserts exactly that — which also keeps
            the test honest under `-Dpss.diffBufSize=256`, where a bare restore
            already starts at the ambient default rather than 0."
    (let [r (scenario {:branching-factor bf})]
      (is (zero? (:missing r)) (str "elements lost: " (:missing r)))
      (is (zero? (:extra r)))
      (is (pos? (:adopted r))
          "buffering is enabled rather than left off over buffered nodes"))))

(deftest an-explicit-budget-still-wins
  (testing "adoption only ever raises from 0 — a caller who names a budget keeps it"
    (let [r (scenario {:branching-factor bf :diff-buf-size budget})]
      (is (zero? (:missing r)))
      (is (= budget (:adopted r))))))

(deftest adoption-does-not-invent-buffering
  (testing "nodes written WITHOUT a budget do not push one onto the restored set.

            The set still lands on the ambient default — `Settings/defaultDiffBufSize`,
            which the suite sets to 256 and a bare JVM leaves at 0 — because that is
            what `map->settings` has always applied. Adoption must not CHANGE that,
            so this compares against the ambient value rather than hardcoding 0,
            and it round-trips the data either way."
    (let [disk  (atom {})
          nodes (Settings. (int bf) nil nil nil (int 0))
          st    #(ts/->Storage (atom {}) disk nodes)
          base  {:branching-factor bf :diff-buf-size 0 :comparator compare}
          v0    (reduce #(s/conj %1 %2 compare) (s/sorted-set* base) (range 1000))
          a0    (s/store v0 (st))
          r     (s/restore-by compare a0 (st) {:branching-factor bf})]
      (is (= (vec (range 1000)) (vec (seq r))))
      (is (= (Settings/defaultDiffBufSize) (diff-buf-of r))
          "unchanged by adoption: the nodes had nothing to adopt"))))
