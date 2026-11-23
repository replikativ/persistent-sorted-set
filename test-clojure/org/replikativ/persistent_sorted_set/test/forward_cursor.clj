(ns org.replikativ.persistent-sorted-set.test.forward-cursor
  "`PersistentSortedSet.ForwardCursor` against a sorted-set model.

   The cursor is internal API, but datahike's query engine seeks with it, and before this
   namespace nothing in the suite executed a line of it.

   A cursor's answer depends on where it stands and where it is asked to go, so the cases
   are pairs: from every position, a seek to every position at or after it — within the
   leaf, into the next leaf, across several, past the end. On trees small enough to list
   every pair, of every size up to a few levels, in memory, restored cold, and restored
   cold after a second generation of many edits and of two (under a diff buffer: buffered
   edits the cursor has to see)."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
  (:import [org.replikativ.persistent_sorted_set IStorage PersistentSortedSet
            PersistentSortedSet$ForwardCursor]))

(set! *warn-on-reflection* true)

(def ^:private ctx (nodes/reader-context {}))

(defn- storage [disk]
  (reify IStorage
    (store [_ node]
      (let [a (str (java.util.UUID/randomUUID))] (swap! disk assoc a (nodes/node->blob node)) a))
    (accessed [_ _] nil)
    (restore [_ a]
      (let [b (@disk a)] (if (:addresses b) (nodes/blob->branch ctx b) (nodes/blob->leaf ctx b))))))

(defn- ge [model k] (first (subseq model >= k)))

(defn- cursor ^PersistentSortedSet$ForwardCursor [^PersistentSortedSet s] (.forwardCursor s))

(defn- mismatches
  "Every seek, every seek after a seek, and a walk with `next` to the end from every
   position. `fresh` gives a set the cursor has not touched."
  [fresh model]
  (let [probes (range -2 (+ 3 (long (or (last model) 0))))
        bad    (volatile! [])]
    (doseq [k1 probes]
      (let [c (cursor (fresh))
            r (.seekGE c (int k1))]
        (when (not= r (ge model k1)) (vswap! bad conj [:seek k1 r]))
        (when (not= (.current c) (ge model k1)) (vswap! bad conj [:current k1 (.current c)]))
        (let [got (loop [acc []] (if-some [x (.next c)] (recur (conj acc x)) acc))]
          (when (not= got (vec (rest (subseq model >= k1)))) (vswap! bad conj [:next-from k1 got]))
          (when (some? (.next c)) (vswap! bad conj [:next-after-the-end k1]))))
      (doseq [k2 probes :when (>= k2 k1)]
        (let [c (cursor (fresh))]
          (.seekGE c (int k1))
          (let [r (.seekGE c (int k2))]
            (when (not= r (ge model k2)) (vswap! bad conj [:seek-after-seek k1 k2 r]))
            (when (not= (.next c) (second (subseq model >= k2)))
              (vswap! bad conj [:next-after-seek-after-seek k1 k2]))))))
    @bad))

(deftest forward-cursor-matches-a-sorted-set
  ;; bf 64 for leaves wide enough that the search inside one is a binary search
  (doseq [[bf sizes] [[4 (concat (range 0 42) [70 150])]
                      [8 (concat (range 0 42) [70 150])]
                      [64 [40 64 65 200]]]
          dbs [0 2]]
    (testing (str "bf " bf ", diff-buf " dbs)
      (let [seen-slots (volatile! false)]
        (doseq [size sizes]
          (let [opts   {:comparator compare :branching-factor bf :diff-buf-size dbs}
                ks     (map #(int (* 2 %)) (range size))
                model  (into (sorted-set) ks)
                s      (reduce #(set/conj %1 %2 compare) (set/sorted-set* opts) ks)
                disk   (atom {})
                addr   (set/store s (storage disk))
                ;; a second generation: every third key deleted, an odd key next to every fifth
                gone   (take-nth 3 ks)
                extra  (map #(int (inc (long %))) (take-nth 5 ks))
                s2     (as-> (set/restore-by compare addr (storage disk) opts) x
                         (reduce #(set/disj %1 %2 compare) x gone)
                         (reduce #(set/conj %1 %2 compare) x extra))
                model2 (into (reduce disj model gone) extra)
                addr2  (set/store s2 (storage disk))
                ;; and one with two edits only, few enough to stay buffered at any budget
                mid    (when (seq ks) (nth ks (quot size 2)))
                s3     (cond-> (set/restore-by compare addr (storage disk) opts)
                         mid (-> (set/disj mid compare) (set/conj (int -1) compare)))
                model3 (cond-> model mid (-> (disj mid) (conj (int -1))))
                addr3  (set/store s3 (storage disk))]
            (when (some :slots (vals @disk)) (vreset! seen-slots true))
            (doseq [[tag fresh m] [[:memory (fn [] s) model]
                                   [:cold (fn [] (set/restore-by compare addr (storage disk) opts)) model]
                                   [:cold-second-generation
                                    (fn [] (set/restore-by compare addr2 (storage disk) opts)) model2]
                                   [:cold-two-edits
                                    (fn [] (set/restore-by compare addr3 (storage disk) opts)) model3]]]
              (let [bad (mismatches fresh m)]
                (is (empty? bad) (str size " elements, " (name tag) ": " (pr-str (take 5 bad))))))))
        (when (pos? dbs)
          (is @seen-slots "precondition: some stored trees held buffered edits"))))))

(deftest forward-cursor-with-its-own-comparator
  (testing "a seek by a coarser comparator than the set's: [e a] pairs sought by e alone"
    (let [cmp   (fn [[e1 a1] [e2 a2]] (let [c (compare e1 e2)] (if (zero? c) (compare a1 a2) c)))
          by-e  (fn [[e1 _] [e2 _]] (compare e1 e2))
          elems (for [e (range 0 60 2) a (range 3)] [e a])
          s     (reduce #(set/conj %1 %2 cmp) (set/sorted-set* {:comparator cmp :branching-factor 4}) elems)]
      (doseq [k1 (range -1 62) k2 (range k1 62)]
        (let [c (.forwardCursor ^PersistentSortedSet s by-e)]
          (.seekGE c [k1 nil])
          (let [r    (.seekGE c [k2 nil])
                want (first (filter #(>= (long (first %)) k2) elems))]
            (when (not= r want)
              (is false (str "seek " k1 " then " k2 ": " (pr-str r) ", expected " (pr-str want))))))))))
