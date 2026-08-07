(ns org.replikativ.persistent-sorted-set.test.projection-aliasing
  "A diff-buf projection must not share array state with the node it projects FROM.

   `projectBranch` builds a version-specific view of a durable branch: the same
   blob at address B is the child of version N and, with a different accumulated
   diff, of version N+1. A caching `IStorage` — datahike's `CachedStorage` — hands
   back the SAME object for B every time, so anything the projection shares with
   its base is shared across every version that reads B.

   `projectLeaf` has always returned a fresh `Leaf`, and `projectBranch` already
   copies `_keys` (it has to: the diff moves separators). It did NOT copy the
   ADDRESSES array — the copy was handed `base`'s own. The justification in the
   comment was that the copy is \"sealed, never edited in place\", and that was
   true, but only via a three-step argument:

     * `child(int,ANode)` writes `addresses[idx] = null` IN PLACE,
     * it is documented owner-thread-only and is only ever called on editable nodes,
     * a projected copy carries `base._settings`, whose `_edit` is null, so it is
       never editable.

   Measured before this change: 0 violations over 38,813,363 `child(int,ANode)`
   calls and 3,899 projections across the diff-buf namespaces — the invariant did
   hold. It is now enforced two ways instead of argued: `child(int,ANode)` asserts
   `editable()` under -ea (which `:test` sets), and the projection owns its array.

   The two tests below are structural rather than behavioural on purpose. With the
   assertion in place the corruption is no longer reachable to demonstrate, so
   asserting on observable set contents would pass against the unfixed code and
   claim coverage it does not have. These check the two properties directly."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set
            ANode Branch Slot IStorage Settings PersistentSortedSet RefType]))

(set! *warn-on-reflection* true)

(def ^:private BF 16)
(def ^:private DBS 256)

(defn- node-settings ^Settings []
  (Settings. (int BF) RefType/STRONG nil nil (int DBS)))

(defn- caching-storage
  "Returns the SAME node object for a given address every time — what a real
   caching storage does, and what makes aliasing observable at all. The default
   test storage rebuilds a node per restore, which is why nothing caught this."
  [disk]
  (let [inner (tstore/->Storage (atom {}) disk (node-settings))
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
            proj      (.child root st (int idx))
            base      (.restore st base-addr)]
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
      (let [kid (.child root st (int 0))
            e   (try (.child root (int 0) ^ANode kid) nil
                     (catch AssertionError e e))]
        (is (some? e)
            "an in-place child write on a shared node must be refused under -ea")))))
