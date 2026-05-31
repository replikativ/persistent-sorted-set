(ns org.replikativ.persistent-sorted-set.test.diff-buf
  "Diff-buffering correctness (see doc/diff-buffering.md). Each test sets :diff-buf-size
   explicitly, so it gates the buffering path regardless of the library default. Asserts the
   diff-buf-specific properties the general suite doesn't: deposit fires at leaf-parents with
   exact ĝ, anchors are captured and restore to the durable leaf, a content-only commit is
   ~1 PUT with 0 reads, the stored root carries nested :slots, store→fresh-restore is content
   exact (incl. multi-cycle, remove/merge slot-carry, and a generative scan), and markFreed
   never frees a live (reachable) node. JVM-only — it inspects Branch/Slot internals."
  (:require [clojure.test :as t :refer [deftest is testing]]
            [clojure.edn]
            [clojure.set]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set Branch ANode Slot PersistentSortedSet IStorage Settings]
           [clojure.lang PersistentTreeMap]
           [java.lang.ref Reference]
           [java.util Random]))

;; Elements are pairs [k v]; the comparator keys on the first element, so
;; (replace [k a] [k b]) is cmp-equal — a pure content-only change.
(def cmp (fn [a b] (compare (first a) (first b))))

(defn- opbuf-settings ^Settings [bf b]
  (let [s (Settings. (int bf) nil nil nil (int b))]
    (set! (.-_comparator s) ^java.util.Comparator cmp)
    s))

(defn- deref-child [c]
  (cond (instance? ANode c) c
        (instance? Reference c) (.get ^Reference c)
        :else c))

(defn- branch-children [^Branch b]
  (when-let [ch (.-_children b)]
    (->> (range (.-_len b)) (map #(deref-child (aget ^objects ch %))) (remove nil?))))

(defn- walk-branches [node]
  (when (instance? Branch node)
    (cons node (mapcat walk-branches (branch-children node)))))

(defn- root-of [^PersistentSortedSet s] (.root s))

(defn- leaf-diff ^PersistentTreeMap [^Slot sl]
  (let [d (.-diff sl)] (when (instance? PersistentTreeMap d) d)))

(defn- slot-val [^Slot sl k]
  (when-let [d (leaf-diff sl)]
    (let [v (.valAt d [k nil] ::miss)] (when (not= v ::miss) v))))

(defn- find-leaf-slot [brs k]
  (some (fn [^Branch b]
          (when-let [slots (.-_slots b)]
            (some (fn [i]
                    (when-let [^Slot sl (aget ^objects slots i)]
                      (when (some? (slot-val sl k)) [b i sl])))
                  (range (.-_len b)))))
        brs))

(defn- fresh-restore
  "Re-open the set from disk with an EMPTY node cache, forcing durable reads + projection."
  [addr disk bf b]
  (ss/restore-by cmp addr
                 (tstore/->Storage (atom {}) disk (opbuf-settings bf b))
                 {:branching-factor bf :diff-buf-size b :comparator cmp}))

(defn- tree-depth [^PersistentSortedSet s]
  (loop [n (root-of s), d 0]
    (if (instance? Branch n) (recur (first (branch-children n)) (inc d)) d)))

(deftest content-correctness
  (testing "conj/replace/disj under diff-buf yield exact content"
    (let [n   200
          ref (atom (sorted-set-by cmp))
          s   (reduce (fn [s i] (swap! ref conj [i 0]) (conj s [i 0]))
                      (ss/sorted-set* {:comparator cmp :branching-factor 4 :diff-buf-size 100})
                      (shuffle (range n)))
          s   (reduce (fn [s i] (swap! ref #(-> % (disj [i 0]) (conj [i (inc i)])))
                        (ss/replace s [i 0] [i (inc i)]))
                      s (range 0 n 2))
          s   (reduce (fn [s i] (swap! ref disj [i 0]) (disj s [i 0]))
                      s (range 1 n 4))]
      (is (= (vec (seq @ref)) (vec (seq s)))))))

(deftest leaf-deposit
  (testing "a content-only replace deposits Present at a leaf-parent (level 1) with exact ĝ; latest-wins"
    (let [s0  (reduce (fn [s i] (conj s [i 0]))
                      (ss/sorted-set* {:comparator cmp :branching-factor 4 :diff-buf-size 100})
                      (range 64))
          k   17
          s1  (ss/replace s0 [k 0] [k 111])
          hit (find-leaf-slot (walk-branches (root-of s1)) k)]
      (is (some? hit) "a leaf-parent slot carries a leaf-diff for the replaced key")
      (when hit
        (let [[^Branch b _ ^Slot sl] hit
              child (deref-child (aget ^objects (.-_children b) (first (keep-indexed (fn [i c] (when (some? (slot-val ^Slot c k)) i)) [sl]))))]
          (is (= 1 (.-_level b)) "leaf-diff sits at a leaf-parent (level 1)")
          (is (= [k 111] (slot-val sl k)) "Present payload is the replacement element")
          (is (= (.-count sl) (.count ^ANode (deref-child (aget ^objects (.-_children b) 0)) nil))
              "ĝ.count tracks the child subtree count")))
      ;; latest-wins: a second replace on the same key overwrites the buffered Present
      (let [s2 (ss/replace s1 [k 111] [k 222])
            v2 (some #(slot-val % k)
                     (mapcat (fn [^Branch bb]
                               (when-let [sl (.-_slots bb)]
                                 (keep #(aget ^objects sl %) (range (.-_len bb)))))
                             (walk-branches (root-of s2))))]
        (is (= [k 222] v2) "later op on the same key overwrites (net latest-wins)")))))

(deftest anchor-capture
  (testing "a buffered leaf slot's anchor restores to the pre-mutation durable leaf"
    (let [st  (tstore/storage-with-settings (opbuf-settings 4 100))
          s0  (reduce (fn [s i] (conj s [i 0]))
                      (ss/sorted-set* {:comparator cmp :branching-factor 4 :diff-buf-size 100})
                      (range 64))
          _   (ss/store s0 st)
          k   17
          s1  (ss/replace s0 [k 0] [k 999])
          hit (find-leaf-slot (walk-branches (root-of s1)) k)]
      (is (some? hit))
      (when hit
        (let [^Slot sl  (nth hit 2)
              anchor    (.-anchor sl)]
          (is (some? anchor) "the slot captured the child's durable anchor")
          (when anchor
            (let [restored (.restore ^IStorage st anchor)
                  ks       (vec (take (.-_len ^ANode restored) (.-_keys ^ANode restored)))]
              (is (some #(= [k 0] %) ks) "anchor leaf still holds the pre-replace element"))))))))

(deftest markers-and-anchors
  (testing "on a content-only path every level deposits a slot: leaf-diffs at level 1, anchor markers above, all with an anchor"
    (let [st (tstore/storage-with-settings (opbuf-settings 4 100))
          s  (reduce (fn [s i] (conj s [i 0]))
                     (ss/sorted-set* {:comparator cmp :branching-factor 4 :diff-buf-size 100})
                     (range 256))
          _  (ss/store s st)
          s  (reduce (fn [s i] (ss/replace s [i 0] [i 1])) s (range 0 256 5))
          brs (walk-branches (root-of s))
          depth (apply max (map (fn [^Branch b] (.-_level b)) brs))
          slotted (for [^Branch b brs
                        :when (.-_slots b)
                        i (range (.-_len b))
                        :let [^Slot sl (aget ^objects (.-_slots b) i)]
                        :when sl]
                    [(.-_level b) sl])
          leaf-slots   (filter (fn [[lvl ^Slot sl]] (and (= 1 lvl) (instance? PersistentTreeMap (.-diff sl)))) slotted)
          branch-marks (filter (fn [[lvl ^Slot sl]] (and (> lvl 1) (nil? (.-diff sl)))) slotted)
          bad-leaf     (filter (fn [[lvl ^Slot sl]] (and (= 1 lvl) (not (instance? PersistentTreeMap (.-diff sl))))) slotted)
          bad-branch   (filter (fn [[lvl ^Slot sl]] (and (> lvl 1) (some? (.-diff sl)))) slotted)
          no-anchor    (filter (fn [[_ ^Slot sl]] (nil? (.-anchor sl))) slotted)]
      (is (>= depth 2) "tree is deep enough to have branch-level markers")
      (is (empty? bad-leaf) "every level-1 slot carries a leaf-diff map")
      (is (empty? bad-branch) "every branch-level slot is an anchor marker (nil diff)")
      (is (seq leaf-slots) "leaf-parent leaf-diffs present")
      (is (seq branch-marks) "branch-level anchor markers present (every-level capture)")
      (is (empty? no-anchor) "every slot captured an anchor"))))

(deftest write-amplification
  (testing "a content-only commit writes ~1 object (root) and reads 0"
    (let [st (tstore/storage-with-settings (opbuf-settings 4 100))
          s0 (reduce (fn [s i] (conj s [i 0]))
                     (ss/sorted-set* {:comparator cmp :branching-factor 4 :diff-buf-size 100})
                     (range 200))
          _  (ss/store s0 st)
          depth (tree-depth s0)
          _  (reset! tstore/*stats {:reads 0 :writes 0 :accessed 0})
          s1 (ss/replace s0 [37 0] [37 1])
          _  (ss/store s1 st)
          {:keys [reads writes]} @tstore/*stats]
      (is (>= depth 2) "multi-level tree (so baseline would write > 1)")
      (is (zero? reads) "store reads nothing (written nodes are resident)")
      (is (= 1 writes) "content-only commit writes only the root"))))

(deftest serialized-slots
  (testing "the stored root object carries a nested :slots map bottoming out in a leaf-diff"
    (let [st   (tstore/storage-with-settings (opbuf-settings 4 100))
          s0   (reduce (fn [s i] (conj s [i 0]))
                       (ss/sorted-set* {:comparator cmp :branching-factor 4 :diff-buf-size 100})
                       (range 64))
          _    (ss/store s0 st)
          s1   (ss/replace s0 [17 0] [17 1])
          addr (ss/store s1 st)
          obj  (clojure.edn/read-string (get @(:*disk st) addr))
          slots (:slots obj)]
      (is (some? slots) "root object has :slots")
      (when slots
        (let [leaf-diffs (letfn [(walk [m] (mapcat (fn [[_ {:keys [diff]}]]
                                                     (if (and (map? diff) (every? number? (keys diff)))
                                                       (walk diff)
                                                       [diff]))
                                                   m))]
                           (walk slots))]
          (is (seq (remove empty? leaf-diffs)) "a leaf-diff is present at the bottom of the nesting"))))))

(deftest store-restore-roundtrip
  (testing "store -> fresh restore (cold reads + projection) is content exact"
    (let [n 200, bf 4, b 100
          st  (tstore/storage-with-settings (opbuf-settings bf b))
          ref (atom (sorted-set-by cmp))
          s0  (reduce (fn [s i] (swap! ref conj [i 0]) (conj s [i 0]))
                      (ss/sorted-set* {:comparator cmp :branching-factor bf :diff-buf-size b}) (range n))
          _   (ss/store s0 st)
          s1  (reduce (fn [s i] (swap! ref #(-> % (disj [i 0]) (conj [i 1]))) (ss/replace s [i 0] [i 1]))
                      s0 (range 0 n 3))
          s1  (reduce (fn [s i] (swap! ref disj [i 0]) (disj s [i 0])) s1 (range 2 n 7))
          addr (ss/store s1 st)
          loaded (fresh-restore addr (:*disk st) bf b)]
      (is (= (vec (seq @ref)) (vec (seq loaded)))))))

(deftest multicycle
  (testing "8 cycles of fresh-restore -> mutate -> store stay content exact"
    (let [bf 4, b 60, n 150
          disk (atom {})
          ref  (atom (sorted-set-by cmp))
          s0   (reduce (fn [s i] (swap! ref conj [i 0]) (conj s [i 0]))
                       (ss/sorted-set* {:comparator cmp :branching-factor bf :diff-buf-size b}) (range n))
          addr0 (ss/store s0 (tstore/->Storage (atom {}) disk (opbuf-settings bf b)))]
      (loop [cyc 0, addr addr0]
        (if (= cyc 8)
          (is (= (vec (seq @ref)) (vec (seq (fresh-restore addr disk bf b)))) "final cycle exact")
          (let [loaded (fresh-restore addr disk bf b)
                _ (is (= (vec (seq @ref)) (vec (seq loaded))) (str "cycle " cyc " exact"))
                s1 (reduce (fn [s i] (swap! ref #(-> % (disj [i 0]) (conj [i (inc cyc)]))) (ss/replace s [i 0] [i (inc cyc)]))
                           loaded (range cyc n 11))
                s1 (reduce (fn [s i] (swap! ref conj [i 0]) (conj s [i 0])) s1 (range (+ n cyc) (+ n cyc 5)))
                addr' (ss/store s1 (tstore/->Storage (atom {}) disk (opbuf-settings bf b)))]
            (recur (inc cyc) addr')))))))

(deftest remove-merge-slot-carry
  (testing "a committed-buffered sibling keeps its diff when a neighbor merges in a later commit"
    (let [bf 4, b 200, n 200, disk (atom {})
          mkst #(tstore/->Storage (atom {}) disk (opbuf-settings bf b))
          ref (atom (apply sorted-set-by cmp (map (fn [i] [i 0]) (range n))))
          s0 (reduce (fn [s i] (conj s [i 0])) (ss/sorted-set* {:comparator cmp :branching-factor bf :diff-buf-size b}) (range n))
          a0 (ss/store s0 (mkst))
          l1 (fresh-restore a0 disk bf b)
          s1 (reduce (fn [s i] (swap! ref #(-> % (disj [i 0]) (conj [i 9]))) (ss/replace s [i 0] [i 9])) l1 (range 0 n 2))
          a1 (ss/store s1 (mkst))
          l2 (fresh-restore a1 disk bf b)
          s2 (reduce (fn [s i] (swap! ref disj [i 0]) (disj s [i 0])) l2 (range 1 n 2))
          a2 (ss/store s2 (mkst))
          l3 (fresh-restore a2 disk bf b)]
      (is (= (vec (seq @ref)) (vec (seq l3)))))))

(deftest generative
  (testing "random conj/disj/replace over a small key range with small B (frequent flushes) + fresh reload each cycle"
    (let [bf 4, b 8, keyrange 80, cycles 30, ops 40, disk (atom {})
          mkst #(tstore/->Storage (atom {}) disk (opbuf-settings bf b))
          ref (atom (sorted-set-by cmp))
          s0 (reduce (fn [s i] (swap! ref conj [i 0]) (conj s [i 0]))
                     (ss/sorted-set* {:comparator cmp :branching-factor bf :diff-buf-size b}) (range 0 keyrange 2))]
      (loop [c 0, addr (ss/store s0 (mkst))]
        (if (= c cycles)
          (is (= (vec (seq @ref)) (vec (seq (fresh-restore addr disk bf b)))) "final cycle exact")
          (let [loaded (fresh-restore addr disk bf b)
                _ (is (= (vec (seq @ref)) (vec (seq loaded))) (str "generative cycle " c " exact"))
                s2 (reduce (fn [s _]
                             (let [k (rand-int keyrange), op (rand-int 3)]
                               (cond
                                 (= op 1) (do (swap! ref disj [k 0]) (disj s [k 0]))
                                 (and (= op 2) (contains? @ref [k 0]))
                                 (do (swap! ref #(-> % (disj [k 0]) (conj [k (inc c)]))) (ss/replace s [k 0] [k (inc c)]))
                                 :else (do (swap! ref conj [k 0]) (conj s [k 0])))))
                           loaded (range ops))]
            (recur (inc c) (ss/store s2 (mkst)))))))))

;; --- markFreed safety: a buffered child re-points to its anchor, but the mutation path
;; markFreed's that anchor; if it stays reachable from the final root, an online GC would
;; delete a live node. Intersect the reachable set with everything markFreed during a churn.

(defn- recording-storage [disk bf b freed]
  (let [inner (tstore/->Storage (atom {}) disk (opbuf-settings bf b))]
    (reify IStorage
      (comparator [_] cmp)
      (restore [_ a] (.restore inner a))
      (store   [_ n] (.store inner n))
      (accessed [_ a] (.accessed inner a))
      (markFreed [_ a] (swap! freed conj a))
      (isFreed [_ a] (contains? @freed a)))))

(deftest markfreed-never-frees-live
  (testing "no reachable node is ever markFreed (over-free) across a transient churn under diff-buf"
    (let [bf 16, keyrange 400, cycles 12, ops 20]
      (doseq [b [0 64 512]]
        (let [rng   (Random. 1)
              disk  (atom {})
              freed (atom #{})
              mkst  #(recording-storage disk bf b freed)
              s0    (reduce (fn [s i] (conj s [i 0]))
                            (ss/sorted-set* {:comparator cmp :branching-factor bf :diff-buf-size b}) (range 0 keyrange 2))
              final-addr
              (loop [c 0, addr (ss/store s0 (mkst))]
                (if (= c cycles)
                  addr
                  (let [loaded (ss/restore-by cmp addr (mkst) {:branching-factor bf :diff-buf-size b :comparator cmp})
                        s2 (persistent! (reduce (fn [tr _] (conj! tr [(.nextInt rng keyrange) 0]))
                                                (transient loaded) (range ops)))]
                    (recur (inc c) (ss/store s2 (mkst))))))
              loaded    (ss/restore-by cmp final-addr (mkst) {:branching-factor bf :diff-buf-size b :comparator cmp})
              reachable (atom #{})
              _         (ss/walk-addresses loaded (fn [a] (swap! reachable conj a)))
              live-freed (clojure.set/intersection @reachable @freed)]
          (is (empty? live-freed) (str "diffBufSize=" b ": " (count live-freed) " reachable node(s) were markFreed (over-free)")))))))
