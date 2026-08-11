(ns org.replikativ.persistent-sorted-set.test.projection-sharing
  "What happens when two sets, or two versions, meet over ONE caching storage.

     * ALIASING -- `projectBranch` handed the projected copy the SHARED base node's address
       array, so writing through the copy mutated the node every other set reads.
     * PROJ-CMP SHARING -- `_projCmp` is a FIELD on a node that a caching storage shares by
       address, so two sets whose comparators order ties differently overwrote each other's
       stamp and whichever read last won.
     * COPY INVARIANTS -- what a projected copy must and must not carry: no durable address
       for a child it has re-ordered, and the ref-type of the SET doing the projecting.

   THE NODE-SHARING AXIS IS THE FIXTURE, and the three sections need opposite settings of it:
   `proj-cmp-sharing` needs ONE memoising storage so both sets get the same root object,
   while `proj-cmp-copy-invariants` builds a fresh `*memory` per call so nodes are
   deliberately NOT shared. Do not unify their storage constructors -- `node-settings` and
   `settings` differ in arity for the same reason, and `cmp1`/`cmp2` (ties REVERSED) are not
   `cmp-a`/`cmp-b` (identical ordering, two objects).

   Several aliases name the same namespace (`s`/`ss`/`set`, `ts`/`tstore`). That is
   deliberate: it let the three files merge with their bodies byte-identical."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.test.storage :as ts]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set
            ANode Branch Slot IStorage Settings PersistentSortedSet RefType]
           [java.lang.ref Reference]))

(set! *warn-on-reflection* true)

;; ===========================================================================
;; 1. a projected copy must not alias the shared base node's arrays
;; ===========================================================================

(def ^:private BF 16)
(def ^:private DBS 256)

(defn- node-settings ^Settings []
  (Settings. (int BF) RefType/STRONG nil nil (int DBS)))

(defn- caching-storage
  "Returns the SAME node object for a given address every time — what a real
   caching storage does, and what makes aliasing observable at all. The default
   test storage rebuilds a node per restore, which is why nothing caught this."
  [disk]
  (let [^IStorage inner (tstore/->Storage (atom {}) disk (node-settings))
        cache (atom {})]
    (reify IStorage
      (store [_ node] (.store inner node))
      (accessed [_ a] (.accessed inner a))
      (restore [_ a]
        (or (@cache a)
            (let [n (.restore inner a)] (swap! cache assoc a n) n)))
      (markFreed [_ _] nil)
      (isFreed [_ _] false)
      (freedInfo [_ _] nil))))

(defn- seeded
  "A stored tree that really carries diff-buf slots. Returns [disk addr]."
  []
  (let [st   (tstore/storage-with-settings (node-settings))
        opts {:comparator compare :branching-factor BF :ref-type :strong
              :diff-buf-size DBS}
        s0   (s/from-sorted-array compare (object-array (range 0 3000 10)) 300
                                  (assoc opts :storage st))
        a0   (s/store s0 st)
        cold (s/restore-by compare a0 st opts)
        s1   (reduce #(s/conj %1 %2 compare) cold [5 15 25 35])]
    [(:*disk st) (s/store s1 st)]))

(defn- addresses-of [^Branch b] (aget ^objects (.addressesAndSlots b) 0))
(defn- slots-of    [^Branch b] (aget ^objects (.addressesAndSlots b) 1))

(deftest a-projected-branch-owns-its-address-array
  (testing "the projection must not hand out the shared base's array — with a
            caching storage that array belongs to every other version too"
    (let [[disk addr] (seeded)
          st    (caching-storage disk)
          back  (s/restore-by compare addr st
                              {:comparator compare :branching-factor BF
                               :ref-type :strong :diff-buf-size DBS})
          ^Branch root (.root ^PersistentSortedSet back)
          slots (slots-of root)
          idx   (first (for [i (range (.len root))
                             :let [^Slot sl (when slots (aget ^objects slots i))]
                             :when (and sl (.-diff sl))]
                         i))]
      (is (= 2 (.level ^ANode root))
          "precondition: a level-2 root, so a projected child is a Branch")
      (is (some? idx)
          "precondition: some slot carries a diff, so projectBranch actually runs.
           Without one `child()` returns the base unchanged and this is vacuous.")
      (let [base-addr (aget ^objects (addresses-of root) idx)
            proj      (.child root ^IStorage st (int idx))
            base      (.restore ^IStorage st base-addr)]
        (is (instance? Branch proj) "the projected child is a Branch")
        (is (not (identical? proj base))
            "precondition: a projection really happened — projectBranch returns a
             COPY, so an identical object means the diff was not applied")
        (is (not (identical? (addresses-of ^Branch proj) (addresses-of ^Branch base)))
            "THE assertion: the projection must own its addresses array, not alias
             the shared base's")
        (is (= (seq (addresses-of ^Branch base)) (seq (addresses-of ^Branch proj)))
            "and it must be a faithful copy — the anchors are the same addresses")))))

(deftest writing-a-child-in-place-requires-an-editable-node
  (testing "`child(int,ANode)` writes into the snapshot's arrays in place; that is
            safe only on an owner-thread, unshared node. The contract was written
            in a comment and checked nowhere. Under -ea it is now enforced —
            which is what keeps a projected copy (never editable) out of it."
    (let [[disk addr] (seeded)
          st    (caching-storage disk)
          back  (s/restore-by compare addr st
                              {:comparator compare :branching-factor BF
                               :ref-type :strong :diff-buf-size DBS})
          ^Branch root (.root ^PersistentSortedSet back)]
      (is (not (.editable root))
          "precondition: a restored root is shared, not editable")
      (let [kid (.child root ^IStorage st (int 0))
            e   (try (.child root (int 0) ^ANode kid) nil
                     (catch AssertionError e e))]
        (is (some? e)
            "an in-place child write on a shared node must be refused under -ea")))))

;; ===========================================================================
;; 2. one set's comparator must not decide another set's leaf order
;; ===========================================================================

;; Same key order, ties REVERSED — so the two disagree only where a leaf holds several
;; elements sharing a first component, which is exactly what projectLeaf reorders.
(defn- cmp1 [[k1 v1] [k2 v2]]
  (let [c (compare k1 k2)] (if-not (zero? c) c (compare v1 v2))))
(defn- cmp2 [[k1 v1] [k2 v2]]
  (let [c (compare k1 k2)] (if-not (zero? c) c (compare v2 v1))))

(defn- sorted-under? [cmp xs]
  (every? neg? (map (fn [[x y]] (cmp x y)) (partition 2 1 xs))))

(defn- scenario
  "Build a diff-buf tree whose leaf-parents carry leaf diffs, open it under BOTH comparators
   over ONE caching storage, and interleave the reads so B's root() lands between two steps
   of A's lazy seq."
  [{:keys [bf dbs n]}]
  (let [st   (ts/storage-with-settings (Settings. (int bf) nil nil nil (int dbs)))
        opts {:comparator cmp1 :branching-factor bf :diff-buf-size dbs
              :ref-type :strong :storage st}
        base (into (ss/sorted-set* opts) (mapcat (fn [k] [[k 0] [k 1]]) (range n)))
        _    (ss/store base st)
        ;; second generation: content-only inserts, so leaf-parents buffer leaf diffs
        s1   (persistent! (reduce (fn [t k] (conj! t [k 2])) (transient base) (range 0 n 7)))
        addr (ss/store s1 st)
        truth (vec (seq s1))
        A    (ss/restore-by cmp1 addr st opts)
        B    (ss/restore-by cmp2 addr st (assoc opts :comparator cmp2))
        sA   (seq A)
        _    (first sA)                        ; A materialises its first leaf under cmp1
        rootA (.root ^PersistentSortedSet A)
        rootB (.root ^PersistentSortedSet B)   ; B's root() must not re-stamp A's node
        ;; Read the stamp HERE, immediately after B's read. Every later `A` operation calls
        ;; A.root() again, which re-seeds the field to cmp1 and REPAIRS the damage — so
        ;; capturing it in the returned map (evaluated after the `store` below) observed the
        ;; repaired value and passed against the unfixed build. An adversarial review caught
        ;; that; the binding order is the assertion here.
        proj-after-B (.-_projCmp ^Branch rootA)
        got  (vec sA)                          ; A finishes its walk
        ;; durability: mutate A and store, then read back through a FRESH cache
        dk   (or (ffirst (keep (fn [[x y]] (when (pos? (cmp1 x y)) [x y]))
                               (partition 2 1 got)))
                 [-1 0])
        addr2 (ss/store (ss/conj A [(first dk) 9] cmp1) st)
        st2  (assoc st :*memory (atom {}))
        back (ss/restore-by cmp1 addr2 st2 (assoc opts :comparator cmp1 :storage st2))
        bseq (vec (seq back))]
    {:root-shared    (identical? rootA rootB)
     :proj-cmp-of-A  proj-after-B
     :seq            got
     :truth          truth
     :count-A        (count A)
     :unfindable     (into [] (comp (remove #(contains? A %)) (take 5)) truth)
     :reloaded       bseq
     :reloaded-unfindable (into [] (comp (remove #(contains? back %)) (take 5)) truth)}))

(def ^:private configs [{:bf 8 :dbs 64 :n 60} {:bf 8 :dbs 256 :n 200} {:bf 16 :dbs 128 :n 300}])

(deftest a-second-set-does-not-resteal-the-first-sets-projection-comparator
  (testing "reading through B must leave A projecting under A's own comparator"
    (doseq [{:keys [bf dbs n] :as cfg} configs]
      (let [{:keys [root-shared proj-cmp-of-A]} (scenario cfg)
            label (str "bf=" bf " dbs=" dbs " n=" n)]
        (is (false? root-shared)
            (str label ": the two sets must not share one root object, since a node carries "
                 "a single projection comparator"))
        (is (identical? cmp1 proj-cmp-of-A)
            (str label ": A's root must still project under cmp1"))))))

(deftest the-first-sets-contents-stay-correct-and-findable
  (testing "count agreeing is not enough — the ORDER and the lookups have to hold too"
    (doseq [{:keys [bf dbs n] :as cfg} configs]
      (let [{:keys [seq truth count-A unfindable]} (scenario cfg)
            label (str "bf=" bf " dbs=" dbs " n=" n)]
        (is (= (clojure.core/count truth) count-A) (str label ": count"))
        (is (sorted-under? cmp1 seq) (str label ": A's seq must be sorted under cmp1"))
        (is (= truth seq) (str label ": A's seq must be exactly what was stored"))
        (is (= [] unfindable)
            (str label ": every stored element must be findable — these were in seq but "
                 "contains? answered false: " unfindable))))))

(deftest and-nothing-mis-ordered-reaches-the-disk
  (testing "the part that made this more than an in-memory nuisance: a dirty leaf is written
            wholesale, so a mis-ordering survives store and a cold reload through a fresh
            cache — one element per run was permanently unfindable"
    (doseq [{:keys [bf dbs n] :as cfg} configs]
      (let [{:keys [reloaded reloaded-unfindable]} (scenario cfg)
            label (str "bf=" bf " dbs=" dbs " n=" n)]
        (is (sorted-under? cmp1 reloaded) (str label ": the reloaded seq must be sorted"))
        (is (= [] reloaded-unfindable)
            (str label ": unfindable after a cold reload: " reloaded-unfindable))))))

;; ===========================================================================
;; 3. invariants a projected copy must satisfy
;; ===========================================================================

;; Two comparator OBJECTS that order identically. That is the point: the copy fires on
;; object identity, and this is the routine case the fix must handle correctly rather than
;; the exotic one.
(def ^:private cmp-a (comparator (fn [a b] (< (compare a b) 0))))
(def ^:private cmp-b (comparator (fn [a b] (< (compare a b) 0))))

(defn- settings ^Settings [bf ref-type dbs]
  (Settings. (int bf) ref-type nil nil (int dbs)))

(defn- dirty-rooted-set
  "A set whose root is a Branch carrying at least one child with NO durable address —
   i.e. a subtree reachable only through the children array."
  [bf dbs]
  (let [disk (atom {})
        st   #(ts/->Storage (atom {}) disk (settings bf nil dbs))
        base (reduce #(set/conj %1 %2 cmp-a)
                     (set/sorted-set* {:comparator cmp-a :branching-factor bf
                                       :diff-buf-size dbs})
                     (range 200))
        addr (set/store base (st))
        cold (set/restore-by cmp-a addr (st) {:branching-factor bf :diff-buf-size dbs})]
    ;; one insert leaves the touched child dirty (address nulled, child held strongly)
    {:set (set/conj cold 1000 cmp-a) :storage (st) :disk disk}))

(defn- null-address-count [^Branch b]
  (let [addrs (.addresses b)]
    (if (nil? addrs)
      (.-_len b)
      (count (filter nil? (seq addrs))))))

(deftest the-copy-keeps-children-that-have-no-address
  (testing "a child with a null address is reachable ONLY through the children array;
            dropping it loses the subtree"
    (doseq [dbs [0 64]]
      (let [bf 8
            {:keys [set storage]} (dirty-rooted-set bf dbs)
            ^PersistentSortedSet s set
            ^Branch root (.root s)
            expected (vec (seq s))]
        (is (pos? (null-address-count root))
            (str "dbs=" dbs ": PRECONDITION — the root must have at least one child with no "
                 "durable address, or this test cannot reach the defect"))
        ;; A set over the SAME root object under a different comparator OBJECT: root() sees a
        ;; foreign _projCmp and takes the copy path.
        (let [^PersistentSortedSet t
              (PersistentSortedSet. nil cmp-b nil ^IStorage storage root (count s)
                                    (settings bf nil dbs) 0)]
          (is (= expected (vec (seq t)))
              (str "dbs=" dbs ": every element must survive the copy"))
          (is (some? (set/store t storage))
              (str "dbs=" dbs ": and the copy must be storable")))))))

(deftest a-dirty-root-stays-strongly-held-through-the-copy
  (testing "`_address == null` implies a strong hold — root() throws if such a reference is
            ever cleared, so wrapping the copy in a Soft/WeakReference arms that throw"
    (doseq [ref-type [RefType/SOFT RefType/WEAK]]
      (let [bf 8
            {:keys [set storage]} (dirty-rooted-set bf 0)
            ^PersistentSortedSet s set
            ^Branch root (.root s)
            ^PersistentSortedSet t
            (PersistentSortedSet. nil cmp-b nil ^IStorage storage root (count s)
                                  (settings bf ref-type 0) 0)]
        (is (nil? (.-_address t)) "PRECONDITION: the root is dirty (no address)")
        (.root t)                                   ; triggers the copy + publish
        (is (not (instance? Reference (.-_root t)))
            (str ref-type ": a dirty root must be published strongly, got "
                 (class (.-_root t))))
        (is (= (vec (seq s)) (vec (seq t)))
            (str ref-type ": and it must still read correctly"))))))
