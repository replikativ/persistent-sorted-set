(ns org.replikativ.persistent-sorted-set.test.bulk-build
  "`from-sorted-seq` on ClojureScript — the streaming bulk builder.

   The claim is the same as the JVM's: for the same input and settings it
   produces the SAME TREE as the in-memory `from-sorted-array`, while holding
   only O(depth × branching-factor) rather than O(n).

   Contents alone would not establish that. A tree with the right elements and
   the wrong fanout passes every `=` check and then behaves differently under
   slicing, counting and later inserts — so the structural tests compare SHAPE.
   And shape alone would not establish the memory claim either, since an
   implementation that buffered every level and then built the tree would have
   identical shape. That is what `nodes-are-written-mid-stream` is for: it reads
   the ORDER nodes were written in, which distinguishes a streaming build from a
   level-at-a-time one without measuring heap.

   Heap sampling is deliberately absent. The JVM suite samples live memory
   mid-build; on Node there is no equivalent worth trusting, and the write-order
   test is a stronger assertion anyway because it is deterministic."
  (:require [cljs.test :refer-macros [deftest testing is]]
            [is.simm.partial-cps.async :refer-macros [async]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage :refer [IStorage]]
            [org.replikativ.persistent-sorted-set.impl.node :as node]
            [org.replikativ.persistent-sorted-set.branch :refer [Branch]]))

;; ---------------------------------------------------------------------------

(defn- recording-storage
  "Keeps every node in a map and, separately, an ordered LOG of what was written.
   The log is the whole point — see `nodes-are-written-mid-stream`."
  []
  (let [disk (atom {})
        log (atom [])
        restores (atom 0)
        n (atom 0)]
    {:disk disk :log log :restores restores
     :storage (reify IStorage
                (store [_ node _opts]
                  (let [a (str "n" (swap! n inc))]
                    (swap! disk assoc a node)
                    (swap! log conj {:address a
                                     :level (node/level node)
                                     :len (alength (.-keys node))})
                    a))
                (restore [_ a _opts] (swap! restores inc) (@disk a))
                (accessed [_ _] nil)
                (markFreed [_ _] nil)
                (isFreed [_ _] false)
                (freedInfo [_ _] nil))}))

(defn- shape
  "Level and fanout of every node, recursively — TREE STRUCTURE, not contents.
   `child` resolves a branch's i-th child, which differs between a resident tree
   and an address-only one."
  [node child]
  (if (instance? Branch node)
    {:level (node/level node)
     :len (alength (.-keys node))
     :children (mapv #(shape (child node %) child) (range (alength (.-keys node))))}
    {:level 0 :len (alength (.-keys node))}))

(defn- resident-child [node i] (aget (.-children node) i))

(defn- stored-child [storage]
  (fn [node i] (storage/restore storage (aget (.-addresses node) i) {:sync? true})))

(defn- root-of
  "The root node of an address-rooted set, restored."
  [s storage]
  (or (.-root s) (storage/restore storage (.-address s) {:sync? true})))

;; ---------------------------------------------------------------------------
;; equivalence with the in-memory builder

(deftest same-tree-as-from-sorted-array
  (testing "identical contents AND identical structure across the lengths where
            the cut rule's three cases meet — it changes at 2*avg and at max, so
            those boundaries are where a streaming reimplementation would drift"
    (doseq [bf [4 8 16 32]
            n [0 1 2 3 7 15 16 17 31 32 33 47 48 63 64 65 100 257 1000 5000]]
      (let [xs (vec (range n))
            opts {:branching-factor bf}
            {:keys [storage]} (recording-storage)
            arr (set/from-sorted-array compare (to-array xs) (count xs) opts)
            seqd (set/from-sorted-seq compare xs (assoc opts :storage storage))]
        (is (= (vec arr) (vec seqd))
            (str "contents differ at bf=" bf " n=" n))
        (when (pos? n)
          (is (= (shape (.-root arr) resident-child)
                 (shape (root-of seqd storage) (stored-child storage)))
              (str "SHAPE differs at bf=" bf " n=" n
                   " — same elements, different tree")))))))

(deftest same-tree-for-non-contiguous-keys
  (testing "the equivalence is about the cut rule, not about the keys being ints
            that happen to be dense"
    (doseq [n [50 200 777]]
      (let [xs (vec (sort (map #(* % 37) (range n))))
            opts {:branching-factor 16}
            {:keys [storage]} (recording-storage)
            arr (set/from-sorted-array compare (to-array xs) (count xs) opts)
            seqd (set/from-sorted-seq compare xs (assoc opts :storage storage))]
        (is (= (vec arr) (vec seqd)))
        (is (= (shape (.-root arr) resident-child)
               (shape (root-of seqd storage) (stored-child storage))))))))

(deftest string-keys-round-trip
  (testing "a non-numeric comparator"
    (let [xs (vec (sort (map #(str "k" %) (range 500))))
          {:keys [storage]} (recording-storage)
          s (set/from-sorted-seq compare xs {:storage storage :branching-factor 16})]
      (is (= xs (vec s))))))

;; ---------------------------------------------------------------------------
;; the property that justifies the function existing

(deftest nodes-are-written-mid-stream
  (testing "a STREAMING build finishes a leaf, hands it up, and finishes a branch
            as soon as enough leaves exist — so level-1 writes are interleaved
            among the leaves. A build that buffered each level and then wrote it
            would produce an identical tree and an entirely different log: every
            leaf first, then every branch.

            This is the assertion that distinguishes them, and it needs no heap
            measurement to do it."
    (let [{:keys [storage log]} (recording-storage)
          bf 16
          avg (bit-shift-right (+ bf (bit-shift-right bf 1)) 1)]
      (set/from-sorted-seq compare (range 5000) {:storage storage :branching-factor bf})
      (let [entries @log
            levels (mapv :level entries)
            first-branch (.indexOf (to-array levels) 1)
            last-leaf (loop [i (dec (count levels))]
                        (if (or (neg? i) (zero? (nth levels i))) i (recur (dec i))))]
        (is (pos? first-branch) "leaves come before the first branch")
        (is (< first-branch last-leaf)
            "a level-1 node was written BEFORE the last leaf — i.e. the build
             interleaves rather than finishing level 0 first")
        (testing "and no more than 2*avg leaves separate two level-1 writes"
          (let [runs (loop [ls levels run 0 acc []]
                       (cond
                         (empty? ls) (conj acc run)
                         (zero? (first ls)) (recur (rest ls) (inc run) acc)
                         :else (recur (rest ls) 0 (conj acc run))))]
            (is (every? #(<= % (* 2 avg)) runs)
                (str "leaf runs between branch writes: " (pr-str runs)))))))))

(deftest build-never-reads-back
  (testing "a bulk build writes; it must not restore. A `restore` during the
            build would mean a node was dropped and re-fetched, which is the
            memory bug this function exists to avoid, wearing a disguise."
    (let [{:keys [storage restores]} (recording-storage)]
      (set/from-sorted-seq compare (range 10000) {:storage storage :branching-factor 16})
      (is (zero? @restores)))))

(deftest the-flush-hook-runs-after-every-store
  (testing "`:flush-fn` is the seam a caller that BUFFERS writes needs: without
            it the builder's memory bound is real for the tree and nominal for
            the caller, whose buffer still grows to the whole index."
    (let [{:keys [storage log]} (recording-storage)
          flushes (atom 0)]
      (set/from-sorted-seq compare (range 3000)
                           {:storage storage :branching-factor 16
                            :flush-fn (fn [] (swap! flushes inc) nil)})
      (is (pos? @flushes))
      (is (= (count @log) @flushes) "one flush per stored node"))))

;; ---------------------------------------------------------------------------
;; storage round-trip and later use

(deftest stored-tree-restores
  (testing "the built tree is a real stored tree: take its root address, restore
            from a fresh set, get the elements back"
    (let [xs (vec (range 2000))
          {:keys [storage]} (recording-storage)
          s (set/from-sorted-seq compare xs {:storage storage :branching-factor 16})
          addr (set/store s storage)
          restored (set/restore addr storage {:branching-factor 16})]
      (is (some? addr))
      (is (= xs (vec restored))))))

(deftest the-root-address-is-not-re-stored
  (testing "the set is already address-rooted, so `store` hands back the address
            the build ended with rather than writing the root a second time"
    (let [{:keys [storage log]} (recording-storage)
          s (set/from-sorted-seq compare (range 500) {:storage storage :branching-factor 16})
          before (count @log)]
      (is (= (.-address s) (set/store s storage)))
      (is (= before (count @log)) "no additional write"))))

(deftest built-tree-supports-further-operations
  (testing "a bulk-built tree is an ordinary set afterwards — conj, disj, slice.

            Worth asserting because the nodes come back address-only with no
            resident children, which is a state the mutation paths must handle."
    (let [xs (vec (range 1000))
          {:keys [storage]} (recording-storage)
          s (set/from-sorted-seq compare xs {:storage storage :branching-factor 16})]
      (is (= 1000 (count s)))
      (is (contains? s 500))
      (is (= (conj xs 1000) (vec (conj s 1000))))
      (is (= (remove #{500} xs) (vec (disj s 500))))
      (is (= (range 100 110) (vec (set/slice s 100 109)))))))

(deftest the-count-is-exact-without-walking
  (testing "an address-rooted set carrying cnt = -1 restores the entire tree on
            the first `count`, which would undo the build. The root's subtree
            count is known, so it is passed through."
    (let [{:keys [storage restores]} (recording-storage)
          s (set/from-sorted-seq compare (range 5000) {:storage storage :branching-factor 16})]
      (is (= 5000 (count s)))
      (is (zero? @restores) "counting restored nothing"))))

;; ---------------------------------------------------------------------------
;; both arms

(deftest the-async-arm-builds-the-same-tree
  (testing "`async+sync` emits both arms from one source, so they must agree.

            Compared by WRITE LOG rather than by reading the sets back. Reading
            an async-built set back would need async reads too — `(vec s)` does a
            synchronous restore, which an async storage rightly refuses — so a
            contents comparison would only be testing the reader. The log is the
            better assertion anyway: same nodes, same shapes, same ORDER means
            the two arms ran the same state machine, not merely that they landed
            on the same elements.

            The storages assert their own mode, so neither arm can pass by
            accidentally running the other."
    (cljs.test/async
     done
     (let [sync-log (atom [])
           async-log (atom [])
           mk (fn [log async?]
                (let [n (atom 0)]
                  (reify IStorage
                    (store [_ node opts]
                      (is (= async? (false? (:sync? opts)))
                          "the arm under test is the one that ran")
                      (let [a (str "n" (swap! n inc))]
                        (swap! log conj [(node/level node) (alength (.-keys node))])
                        (if async? (async a) a)))
                    (restore [_ _ _] (throw (ex-info "build must not read back" {})))
                    (accessed [_ _] nil)
                    (markFreed [_ _] nil)
                    (isFreed [_ _] false)
                    (freedInfo [_ _] nil))))
           opts {:branching-factor 16}]
       (set/from-sorted-seq compare (range 3000)
                            (assoc opts :storage (mk sync-log false)))
       ((set/from-sorted-seq compare (range 3000)
                             (assoc opts :storage (mk async-log true) :sync? false))
        (fn [_]
          (is (pos? (count @sync-log)))
          (is (= @sync-log @async-log)
              "both arms wrote the same nodes, in the same order")
          (done))
        (fn [e]
          (is false (str "async arm failed: " e))
          (done)))))))

;; ---------------------------------------------------------------------------
;; input contract

(deftest rejects-input-that-would-corrupt-the-tree
  (testing "unsorted input must FAIL rather than build a wrong tree.

            RocksDB's SstFileWriter — the same bulk-build pattern — refuses with
            \"Keys must be added in strict ascending order\" for exactly this
            reason: the alternative is a structure that looks fine and answers
            queries wrongly."
    (let [{:keys [storage]} (recording-storage)]
      (testing "descending"
        (is (thrown-with-msg? js/Error #"strictly ascending"
                              (count (set/from-sorted-seq compare [3 2 1] {:storage storage})))))
      (testing "a single inversion late in an otherwise sorted run"
        (is (thrown-with-msg? js/Error #"strictly ascending"
                              (count (set/from-sorted-seq compare (concat (range 1000) [998])
                                                          {:storage storage})))))
      (testing "duplicates — a set cannot hold them, and silently dropping one
                would make the count disagree with the input"
        (is (thrown-with-msg? js/Error #"strictly ascending"
                              (count (set/from-sorted-seq compare [1 2 2 3] {:storage storage})))))
      (testing "nil"
        (is (thrown-with-msg? js/Error #"cannot store nil"
                              (count (set/from-sorted-seq compare [1 nil 3] {:storage storage}))))))))

(deftest requires-storage
  (testing "without storage there is nothing to stream to, and the caller wants
            from-sorted-array instead — say so rather than fail later"
    (is (thrown-with-msg? js/Error #"requires :storage"
                          (count (set/from-sorted-seq compare [1 2 3] {}))))))

(deftest guards-throw-at-the-call-site-in-the-async-arm-too
  (testing "the guards run BEFORE async+sync, so misuse throws where it happened
            rather than turning into a rejected continuation the caller may never
            look at."
    (is (thrown-with-msg? js/Error #"requires :storage"
                          (set/from-sorted-seq compare [1 2 3] {:sync? false})))))

(deftest empty-input-yields-empty-set
  (let [{:keys [storage log]} (recording-storage)
        s (set/from-sorted-seq compare [] {:storage storage})]
    (is (= 0 (count s)))
    (is (= [] (vec s)))
    (is (zero? (count @log)) "nothing to store, so nothing stored")))

(deftest single-element
  (let [{:keys [storage]} (recording-storage)
        s (set/from-sorted-seq compare [42] {:storage storage})]
    (is (= [42] (vec s)))
    (is (= 1 (count s)))))

(deftest tiny-branching-factors-are-refused-not-hung
  (testing "a fanout of 1 never reduces the level count, so the build grows
            upward forever.

            `avg = (min + max) / 2` with `min = bf >>> 1`, so bf 1 and 2 both
            give avg 1. This is a `throw` and not an `assert` on purpose:
            :advanced elides asserts in a consumer release, and an elided guard
            here means the build spins until it OOMs rather than failing — the
            worst way for a bulk build to be wrong, because it looks like the
            slow-but-working case it is meant to replace."
    (let [{:keys [storage]} (recording-storage)]
      (doseq [bf [1 2]]
        (is (thrown-with-msg? js/Error #"branching-factor must be >= 3"
                              (count (set/from-sorted-seq compare (range 200)
                                                          {:storage storage :branching-factor bf})))
            (str "bf=" bf " must be refused")))
      (testing "and the smallest workable factor does work"
        (doseq [bf [3 5 7]]
          (is (= 200 (count (set/from-sorted-seq compare (range 200)
                                                 {:storage storage :branching-factor bf})))
              (str "bf=" bf)))))))
