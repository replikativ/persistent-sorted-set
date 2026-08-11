(ns org.replikativ.persistent-sorted-set.test.early-exit-count
  "`count` must not outrun the elements when a leafProcessor is configured.

   A processor may compact or expand the leaf it is handed, so adding or removing one KEY
   need not change the element count by one. Three arms applied ±1 unconditionally:

       Branch.add     EARLY_EXIT arm    _subtreeCount += 1
       Branch.remove  EARLY_EXIT arm    _subtreeCount -= 1
       PersistentSortedSet.disjoin      _count = alterCount(-1)

   `991bdcf` guarded the two REBUILD arms in `Branch.remove`; these three siblings were
   missed. The trap is that EARLY_EXIT does not mean \"the processor did not fire\" — a
   level-1 Branch handles the processor correctly and STILL returns EARLY_EXIT to its
   parent, which then applied ±1 blindly. So the corruption needs a tree of level >= 2 AND
   an editable (transient) spine, which is why the whole suite stayed green.

   Measured before the fix:

       disj  bf  8  n   200   count   179   seq   178   drift level 3 179/178, level 2 75/74
       disj  bf  8  n  1000   count   979   seq   978   drift levels 4, 3, 2
       disj  bf 16  n  2000   count  1979   seq  1978
       disj  bf 64  n 20000   count 19979   seq 19978
       conj  bf  8  n   200   count   221   seq   220   drift level 3 221/220, level 2 85/84

   Every level from 2 up drifts, because EARLY_EXIT climbs the whole spine.

   And it is DURABLE, which is the part that makes it more than an in-memory nuisance:
   `node->identity` excludes `:subtree-count`, so the address does NOT change and the wrong
   count rides along inside the blob, invisible to a merkle audit. A cold restore reports
   the same wrong count."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.diagnostics :as diag])
  (:import [java.util List ArrayList]
           [org.replikativ.persistent_sorted_set ILeafProcessor ANode Branch IStorage
            Settings PersistentSortedSet]))

(defn- armable-processor
  "Inert until `armed`, then drops the LAST entry of any leaf it is handed. Never expands,
   never empties, always sorted — inside the ILeafProcessor contract. Being able to ARM it
   is what matters: the tree is built and warmed with the processor inert, so the spine is
   editable and the counts are correct, and then ONE op changes the count by 2 instead of 1."
  [armed]
  (reify ILeafProcessor
    (shouldProcess [_ leaf-size _settings] (and @armed (> (long leaf-size) 1)))
    (processLeaf [_ entries _storage _settings]
      (let [^List entries entries]
        (if (and @armed (> (.size entries) 1))
          (ArrayList. ^java.util.Collection (.subList entries 0 (dec (.size entries))))
          entries)))))

(defn- real-count [^ANode n]
  (if (instance? Branch n)
    (let [^Branch b n]
      (reduce + 0 (map #(real-count (.child b nil (int %))) (range (.-_len b)))))
    (.-_len n)))

(defn- run
  "Build with the processor inert, warm the spine into editable state, arm, do one more op."
  [op bf n]
  (let [armed (atom false)
        s0    (reduce #(set/conj %1 %2 compare)
                      (set/sorted-set* {:comparator compare
                                        :branching-factor bf
                                        :leaf-processor (armable-processor armed)})
                      (range n))
        t     (transient s0)
        ;; warm-up: make the spine editable with the processor still inert
        t     (reduce (fn [acc i] (if (= :disj op)
                                    (disj! acc i)
                                    (conj! acc (+ n i))))
                      t (range 20))
        _     (reset! armed true)
        t     (if (= :disj op) (disj! t 100) (conj! t (+ n 500)))
        s     (persistent! t)]
    {:count (count s) :seq-count (clojure.core/count (seq s))
     :real  (real-count (.root ^PersistentSortedSet s))
     :validate (diag/validate-full s)}))

(deftest count-agrees-with-the-elements-on-the-early-exit-arms
  (testing "a processor that changes the leaf by more than the one key touched must not
            leave count and seq disagreeing — at any level, on either op"
    (doseq [op [:disj :conj]
            [bf n] [[8 200] [8 1000] [16 2000]]]
      (let [{:keys [count seq-count real validate]} (run op bf n)]
        (is (= count seq-count)
            (str op " bf=" bf " n=" n ": count " count " but " seq-count " elements"))
        (is (= count real)
            (str op " bf=" bf " n=" n ": count " count " but " real " reachable from the root"))
        (is (true? validate)
            (str op " bf=" bf " n=" n ": " validate))))))

;; A storage that PERSISTS :subtree-count, the way datahike's does. Against a storage that
;; drops the field the durability half of this defect is invisible, so the in-tree test
;; Storage (which drops it) would make this test vacuous.
(defrecord CountingStorage [*disk ^Settings settings]
  IStorage
  (store [_ node]
    (let [^ANode node node
          addr (str (java.util.UUID/randomUUID))]
      (swap! *disk assoc addr
             {:level         (.level node)
              :n-keys        (clojure.core/count (.keys node))
              :subtree-count (when (instance? Branch node) (.subtreeCount ^Branch node))})
      addr))
  (accessed [_ _] nil)
  (restore [_ _] (throw (ex-info "restore not exercised here" {}))))

(deftest the-drifted-count-travels-into-the-blob
  (testing "node->identity excludes :subtree-count, so a wrong count does NOT change the
            node's address — it rides along inside the blob where a merkle audit cannot see
            it. Assert on what was actually written, not on the in-memory value."
    (let [armed (atom false)
          proc  (armable-processor armed)
          bf    8
          n     200
          s0    (reduce #(set/conj %1 %2 compare)
                        (set/sorted-set* {:comparator compare
                                          :branching-factor bf
                                          :leaf-processor proc})
                        (range n))
          t     (reduce disj! (transient s0) (range 20))
          _     (reset! armed true)
          s     (persistent! (disj! t 100))
          disk  (atom {})
          _     (set/store s (->CountingStorage disk (Settings. (int bf) nil nil nil (int 0))))
          elems (clojure.core/count (seq s))
          roots (->> (vals @disk)
                     (filter #(some-> (:subtree-count %) (>= 0)))
                     (map :subtree-count)
                     (apply max))]
      (is (= elems roots)
          (str "the largest persisted :subtree-count is the root's and must equal the "
               elems " elements actually in the set, got " roots)))))
