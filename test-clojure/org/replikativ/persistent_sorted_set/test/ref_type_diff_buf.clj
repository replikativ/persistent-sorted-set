(ns org.replikativ.persistent-sorted-set.test.ref-type-diff-buf
  "`:ref-type` must bound the resident set under diff-buf too — and the eviction it
   enables must actually work.

   The diff-buf settle published its children array unchanged, with no
   `makeReference`. The baseline settle wraps. So the moment buffering was on,
   every child that had been dirty in any commit stayed a bare STRONG reference
   from its parent, copy-on-write carried that into every successor branch, and
   the bound the user configured stopped applying to the hot part of the index —
   in exactly the deployment diff-buf exists for. Measured, bf 8, `:ref-type :soft`:

       diff-buf 0     root children {:ref 5}
       diff-buf 256   root children {:BARE-STRONG 5}

   ## Why the fix needed this test and not just the wrapping

   Because a buffered child was never weakly held, it could never be evicted, so
   the path that re-derives one had never run in-process. That path is
   load-bearing: `store()` writes the ASSEMBLED nested diff back into the slot
   (`new Slot(cnested[i], …)`) precisely so a cleared child can be rebuilt as
   `restore(anchor) + project(slot)`. Wrapping switches it on for the first time.
   Shipping the one-line change alone would have been enabling untested code on a
   data path.

   The eviction here is DETERMINISTIC: rather than hoping the GC clears a
   SoftReference under memory pressure, the test clears the references itself.
   That exercises the same reader path — `child()` finds a cleared reference,
   restores from the address, and projects the parent's slot onto it."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet Branch ANode IStorage]
           [java.lang.ref Reference]))

(set! *warn-on-reflection* true)

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

(defn- build [dbs ref-type n]
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
      (let [{:keys [set storage]} (build dbs :soft 300)
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
    (let [{:keys [set]} (build 256 :strong 300)
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
      (let [{:keys [set storage expected]} (build dbs :soft 300)
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
    (let [{:keys [set storage disk opts expected]} (build 256 :soft 300)
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
