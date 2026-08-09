(ns org.replikativ.persistent-sorted-set.test.proj-cmp-copy-invariants
  "`Branch.withProjCmp` must preserve the two invariants a node copy can quietly break.

   `withProjCmp` copies a branch when a second set stamps a different projection comparator
   onto a node their shared caching storage handed both of them (see `proj_cmp_sharing`).
   Two things about that copy were wrong, and neither is reachable through the pure Clojure
   API today — a dirty root always inherits `_projCmp == _cmp`, so the conflict needs a set
   constructed directly over a foreign-stamped root. Both are pinned here anyway, because
   `root()` does not check either one and the surrounding code leans on both.

   ## 1. The copy dropped UNSTORED children

   The copy took a fresh (null) children array, which is right for a child that can be
   restored from its address and wrong for one that cannot. For any index with
   `addresses[i] == null`, `children[i]` is a bare dirty ANode and the ONLY reference to that
   subtree; nulling it produced the state the invariant forbids — address null AND child
   null. Measured before the fix, root `_len` 7 with a null address at index 6, at
   diffBufSize 0 and 64 alike:

       (seq copy)   -> AssertionError at Branch.child's precondition
       (store copy) -> AssertionError \"dirty child must be a bare resident ANode ...\"

   and under -da an NPE or a silently truncated subtree.

   ## 2. `root()` demoted a DIRTY root to a clearable Reference

   `_root = _settings.makeReference(root)` ran unconditionally, but `_address == null` is
   supposed to imply a STRONG hold — there is no durable copy to fall back on, and `root()`
   itself throws IllegalStateException if such a reference is ever cleared. Measured: under
   `:soft` and `:weak` the Branch became a Soft/WeakReference, and clearing it made the very
   next `root()` throw \"a dirty root's reference was cleared\".

   Both found by an adversarial review of the commit that introduced `withProjCmp`."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet Branch ANode Settings
            RefType IStorage]
           [java.lang.ref Reference]))

(set! *warn-on-reflection* true)

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
