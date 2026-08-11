(ns org.replikativ.persistent-sorted-set.test.ref-type
  "`:ref-type` must bound the resident set on every write path, not just the baseline one.

   Two independent escapes were found and are pinned here, one per section:

     * CONTENT-DEFINED (MST) mode -- `Branch.removeContent` and `Branch.mstMergeWith` copied
       each unchanged sibling forward as a BARE `ANode` while keeping its address, so no
       `Reference` was ever created on those paths and one bare child near the root pinned
       everything beneath it. Measured at bf 8 / n 20000 / `:ref-type :soft`: 0 references
       cleared, 1375 of 1375 nodes resident.

     * DIFF-BUF -- the diff-buf settle published its children array unchanged while the
       baseline settle wrapped, so with buffering on every once-dirty child stayed strong
       forever. Root children were `{:ref 5}` at budget 0 and `{:BARE-STRONG 5}` at 256.

   Contents were always correct in both cases; this is retention, not corruption.

   FIXTURES ARE DELIBERATELY SEPARATE. The MST section uses `mst-run` (no second store --
   a checkpoint re-wraps everything and hides the defect) and the diff-buf section uses
   `diff-buf-build` (a SECOND generation, so the settle has buffered children to deal with).
   The two clearing helpers likewise differ in signature and entry point -- `clear-references!`
   walks from a set, `clear-all-references!` from a branch plus its storage. Do not unify
   them: each section's shape is what makes its own defect observable."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.boundary :as b]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet Branch ANode IStorage]
           [java.lang.ref Reference]))

(set! *warn-on-reflection* true)

;; ===========================================================================
;; 1. content-defined (MST) mode
;; ===========================================================================

(defn- resident-nodes [^PersistentSortedSet s]
  (let [n (atom 0)]
    (letfn [(w [x]
              (when (instance? Branch x)
                (swap! n inc)
                (let [^Branch br x
                      ch (.childrenArray br)]
                  (dotimes [i (.len br)]
                    (let [c (when ch (aget ^objects ch i))
                          c (if (instance? Reference c) (.get ^Reference c) c)]
                      (when (some? c)
                        (if (instance? Branch c) (w c) (swap! n inc))))))))]
      (w (.root s)))
    @n))

(defn- clear-references!
  "Clear every Reference reachable from the root, descending through references only —
   a bare strong child legitimately stops the walk, because it pins its subtree."
  [^PersistentSortedSet s]
  (let [n (atom 0)]
    (letfn [(w [^Branch br]
              (let [ch (.childrenArray br)]
                (dotimes [i (.len br)]
                  (let [x (when ch (aget ^objects ch i))]
                    (when (instance? Reference x)
                      (let [c (.get ^Reference x)]
                        (when (instance? Branch c) (w c)))
                      (.clear ^Reference x)
                      (swap! n inc))))))]
      (w (.root s)))
    @n))

(defn- mst-run [n mst?]
  (let [bf   8
        disk (atom {})
        opts (cond-> {:comparator compare :branching-factor bf :ref-type :soft}
               mst? (assoc :boundary (b/mst-boundary 4)))
        st   (ts/storage (atom {}) disk)
        s0   (reduce #(ss/conj %1 %2 compare) (ss/sorted-set* opts) (range n))
        _    (ss/store s0 st)
        vs   (range 0 (quot n 10) 100)
        s1   (reduce #(ss/disj %1 %2 compare) s0 vs)   ; NO second store — see the ns docstring
        before  (resident-nodes s1)
        cleared (clear-references! s1)
        after   (resident-nodes s1)]
    {:resident-before before :cleared cleared :resident-after after
     :expected (remove (set vs) (range n)) :actual (seq s1)}))

(deftest mst-mode-honours-ref-type
  (testing "a content-defined boundary must not pin the tree between checkpoints"
    (let [{:keys [resident-before cleared resident-after expected actual]} (mst-run 20000 true)]
      (is (pos? cleared)
          (str "MST cleared " cleared " references of " resident-before
               " resident nodes. Zero means nothing in the tree is evictable and `:ref-type`"
               " is inert."))
      (is (< resident-after (* 0.75 resident-before))
          (str "MST pinned " resident-after " of " resident-before
               " nodes after eviction — `:ref-type :soft` must bound the resident set."))
      (is (= expected actual) "contents unaffected — this is retention, not corruption"))))

(deftest count-mode-is-unchanged
  (testing "the MST wrapping must not disturb the count path, which already wrapped
            correctly — a fix that moved BOTH numbers would be doing something else"
    (let [{:keys [resident-before cleared resident-after expected actual]} (mst-run 20000 false)]
      (is (pos? cleared) (str "count mode cleared " cleared))
      (is (< resident-after (* 0.8 resident-before))
          (str "count mode pinned " resident-after " of " resident-before
               " — a BOUND, not merely `<`, which one evicted node would satisfy"))
      (is (= expected actual) "contents"))))

;; SCOPE OF THIS NAMESPACE, since it is easy to expect more of it than it gives.
;;
;; `count-mode-is-unchanged` is a CONTROL: it shows the MST wrapping did not move the count
;; path. It is not, and cannot be, a guard on the count-mode wraps themselves — those run
;; inside `store()`, and this namespace deliberately does no second store (see the ns
;; docstring). Verified rather than assumed: disabling both count-mode passthrough wraps
;; leaves every number here identical (bare-strong-with-address 24, resident 4033/6598,
;; cleared 2565 — the same with them on).
;;
;; The wraps ARE guarded, by `diff-buf-honours-ref-type` BELOW IN THIS NAMESPACE (it was a
;; separate namespace, ref_type_diff_buf, until these two were merged), which checks the
;; post-settle state. Disabling the same two lines makes it fail with
;; "diff-buf 0: root holds bare strong children: {:reference 3, :bare-strong 2}". A review
;; reported that the suite stayed green when those wraps were removed; that is not what I
;; measured — the coverage exists, in that namespace rather than this one.

;; ===========================================================================
;; 2. diff-buf
;; ===========================================================================

(defn- child-kinds
  "Per-child reference kind of a branch, from one snapshot."
  [^Branch b]
  (let [ch (.childrenArray b)]
    (frequencies
     (map (fn [i]
            (let [x (when ch (aget ^objects ch i))]
              (cond (nil? x) :absent
                    (instance? ANode x) :bare-strong
                    :else :reference)))
          (range (.len b))))))

(defn- clear-all-references!
  "Deterministic eviction: clear every Reference in the tree, as the GC would
   under pressure. Returns how many were cleared."
  [^Branch root storage]
  (let [n (atom 0)]
    (letfn [(walk [^Branch b]
              (let [ch (.childrenArray b)]
                (dotimes [i (.len b)]
                  (let [x (when ch (aget ^objects ch i))]
                    (when (instance? Reference x)
                      ;; descend BEFORE clearing, while the child is still reachable
                      (let [c (.get ^Reference x)]
                        (when (instance? Branch c) (walk c)))
                      (.clear ^Reference x)
                      (swap! n inc))))))]
      (walk root))
    @n))

(defn- diff-buf-build [dbs ref-type n]
  (let [disk (atom {})
        st   (ts/storage (atom {}) disk)
        opts {:comparator compare :branching-factor 8
              :diff-buf-size dbs :ref-type ref-type}
        s0   (into (ss/sorted-set* (assoc opts :storage st)) (range n))
        _    (ss/store s0 st)
        ;; a second generation, so the settle has buffered children to deal with
        s1   (reduce #(ss/conj %1 %2 compare) s0 (range n (+ n 100)))
        addr (ss/store s1 st)]
    {:set s1 :storage st :disk disk :addr addr :opts opts
     :expected (vec (range (+ n 100)))}))

;; ---------------------------------------------------------------------------

(deftest diff-buf-honours-ref-type
  (testing "the settle must wrap children per `:ref-type`, with buffering on as
            well as off. A bare strong child is not evictable, so the configured
            bound silently does not apply."
    (doseq [dbs [0 256]]
      (let [{:keys [set storage]} (diff-buf-build dbs :soft 300)
            ^IStorage storage storage
            ^Branch root (.root ^PersistentSortedSet set)]
        (is (zero? (get (child-kinds root) :bare-strong 0))
            (str "diff-buf " dbs ": root holds bare strong children: "
                 (pr-str (child-kinds root))))
        (is (zero? (get (child-kinds (.child root storage (int 0))) :bare-strong 0))
            (str "diff-buf " dbs ": level-1 holds bare strong children")))))

  (testing ":strong must still be bare — the option means what it says in both
            directions, and a test that only checked one would pass against a
            build that wrapped unconditionally"
    (let [{:keys [set]} (diff-buf-build 256 :strong 300)
          ^Branch root (.root ^PersistentSortedSet set)]
      (is (zero? (get (child-kinds root) :reference 0))
          (str "strong must not wrap: " (pr-str (child-kinds root)))))))

(deftest an-evicted-buffered-child-is-re-derived-correctly
  (testing "THE path the wrapping switches on, and which had never executed:
            `store()` writes the assembled nested diff back into the slot so a
            cleared child can be rebuilt as restore(anchor) + project(slot).

            Eviction is forced rather than waited for — every Reference in the
            tree is cleared explicitly, which is what the GC would do under
            pressure and what no test could previously reach, since a buffered
            child was never weakly held in the first place."
    (doseq [dbs [0 256]]
      (let [{:keys [set storage expected]} (diff-buf-build dbs :soft 300)
            ^Branch root (.root ^PersistentSortedSet set)
            cleared (clear-all-references! root storage)]
        (is (pos? cleared)
            (str "diff-buf " dbs ": precondition — there were references to clear."
                 " Without wrapping this is 0 and the rest of this test is vacuous."))
        ;; every read below must now go through restore + projection
        (is (= expected (vec (seq set)))
            (str "diff-buf " dbs ": contents after eviction"))
        (is (= (count expected) (count set))
            (str "diff-buf " dbs ": count after eviction"))
        (is (contains? set 350) (str "diff-buf " dbs ": lookup after eviction"))
        (is (= (range 100 110) (vec (ss/slice set 100 109)))
            (str "diff-buf " dbs ": slice after eviction"))))))

(deftest an-evicted-tree-still-stores-correctly
  (testing "and re-deriving must produce a tree that can be stored and restored
            again — otherwise eviction would corrupt the next commit rather than
            just costing a read"
    (let [{:keys [set storage disk opts expected]} (diff-buf-build 256 :soft 300)
          ^Branch root (.root ^PersistentSortedSet set)]
      (clear-all-references! root storage)
      (let [s2   (reduce #(ss/conj %1 %2 compare) set (range 1000 1050))
            addr (ss/store s2 storage)
            cold (ss/restore-by compare addr (ts/storage (atom {}) disk)
                                (assoc opts :storage storage))
            want (vec (concat expected (range 1000 1050)))]
        (is (= want (vec (seq s2))) "in memory after modifying an evicted tree")
        (is (= want (vec (seq cold))) "and after a store/restore round trip")
        (is (= (count want) (count cold)) "count agrees with seq")))))
