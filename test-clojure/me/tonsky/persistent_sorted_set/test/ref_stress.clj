(ns me.tonsky.persistent-sorted-set.test.ref-stress
  "Stress tests for SoftReference/WeakReference eviction in persistent sorted sets.
   Uses :ref-type :weak to aggressively clear references and verify that
   store/restore/navigation operations remain correct under GC pressure."
  (:require
   [clojure.test :refer [deftest is testing]]
   [me.tonsky.persistent-sorted-set :as set])
  (:import
   [me.tonsky.persistent_sorted_set ANode Branch IStorage Leaf PersistentSortedSet Settings RefType]
   [java.util UUID]))

;; --- Storage implementation ---

(defn make-storage
  "Creates an in-memory IStorage backed by an atom map."
  []
  (let [disk (atom {})]
    {:disk disk
     :storage
     (reify IStorage
       (store [_ node]
         (let [address (UUID/randomUUID)]
           (swap! disk assoc address node)
           address))
       (restore [_ address]
         (let [node (@disk address)]
           (when-not node
             (throw (ex-info "Node not found in storage" {:address address})))
           node))
       (markFreed [_ address]
         (swap! disk dissoc address))
       (accessed [_ address] nil))}))

;; --- Helpers ---

(defn force-gc
  "Force garbage collection and give the JVM time to clear weak references."
  []
  (System/gc)
  (Thread/sleep 10)
  (System/gc))

(defn allocate-garbage
  "Allocate some garbage to increase GC pressure."
  [n]
  (let [_ (doall (repeatedly n #(byte-array 1024)))]
    nil))

(defn tree-elements
  "Collect all elements from a tree into a sorted vector."
  [tree storage]
  (vec (set/slice tree (first tree) (last tree))))

;; --- Tests ---

(deftest test-weak-ref-store-restore-navigate
  (testing "Tree with weak refs can be stored, restored, GC'd, and navigated"
    (let [{:keys [storage]} (make-storage)
          bf 32
          n 5000
          keys (vec (range n))
          tree (set/conj-all
                (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                keys)
          addr (set/store tree storage)
          restored (set/restore addr storage {:branching-factor bf :ref-type :weak})]
      ;; Force GC to clear weak references
      (force-gc)
      (allocate-garbage 10000)
      (force-gc)
      ;; Navigate the tree - should restore from storage transparently
      (is (= n (count restored)))
      (is (= 0 (first restored)))
      (is (= (dec n) (last restored)))
      ;; Verify all elements accessible
      (is (= keys (vec (set/slice restored 0 (dec n))))))))

(deftest test-weak-ref-modify-after-gc
  (testing "Tree with weak refs can be modified after GC clears references"
    (let [{:keys [storage]} (make-storage)
          bf 32
          n 2000
          keys (vec (range 0 (* n 2) 2))  ;; Even numbers 0,2,4,...
          tree (set/conj-all
                (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                keys)
          addr (set/store tree storage)
          restored (set/restore addr storage {:branching-factor bf :ref-type :weak})]
      ;; Force GC to clear weak references
      (force-gc)
      (allocate-garbage 10000)
      (force-gc)
      ;; Add odd numbers (interspersed insertions)
      (let [new-keys (range 1 (* n 2) 2)
            modified (set/conj-all restored (vec new-keys))
            ;; Store the modified tree - this exercises the store() fix
            new-addr (set/store modified storage)]
        (is (= (* 2 n) (count modified)))
        ;; Verify all elements present and ordered
        (is (= (vec (range (* 2 n))) (vec (set/slice modified 0 (dec (* 2 n))))))
        ;; Restore again and verify
        (let [re-restored (set/restore new-addr storage {:branching-factor bf :ref-type :weak})]
          (force-gc)
          (is (= (* 2 n) (count re-restored))))))))

(deftest test-weak-ref-repeated-store-restore-cycles
  (testing "Multiple store/restore/modify cycles with weak refs and GC pressure"
    (let [{:keys [storage]} (make-storage)
          bf 16
          initial-keys (vec (range 0 500 5))]  ;; 100 keys: 0,5,10,...,495
      (loop [tree (set/conj-all
                   (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                   initial-keys)
             cycle 0
             next-key 1]
        (when (< cycle 10)
          (let [addr (set/store tree storage)
                _ (force-gc)
                restored (set/restore addr storage {:branching-factor bf :ref-type :weak})
                _ (force-gc)
                _ (allocate-garbage 5000)
                _ (force-gc)
                ;; Add some keys
                new-keys (vec (range next-key (+ next-key 10)))
                modified (set/conj-all restored new-keys)
                ;; Remove some keys
                to-remove (take 3 (filter #(contains? modified %) (range (* cycle 5) (* (inc cycle) 5))))
                final (reduce disj modified to-remove)]
            (is (pos? (count final)) (str "Cycle " cycle " should have elements"))
            (recur final (inc cycle) (+ next-key 10))))))))

(deftest test-weak-ref-large-tree-gc-pressure
  (testing "Large tree under heavy GC pressure with weak references"
    (let [{:keys [storage]} (make-storage)
          bf 64
          n 10000
          keys (vec (range n))
          tree (set/conj-all
                (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                keys)
          addr (set/store tree storage)]
      ;; Multiple rounds of restore-GC-access
      (dotimes [round 5]
        (let [restored (set/restore addr storage {:branching-factor bf :ref-type :weak})]
          ;; Heavy GC pressure
          (force-gc)
          (allocate-garbage 20000)
          (force-gc)
          ;; Random access patterns
          (is (= 0 (first restored)))
          (is (= (dec n) (last restored)))
          (is (= (count (set/slice restored 1000 2000))
                 1001))
          ;; Spot check specific elements
          (is (contains? restored 5000))
          (is (contains? restored 9999))
          (is (not (contains? restored n))))))))

(deftest test-weak-ref-conj-all-with-splits
  (testing "conjAll causing splits on a GC-pressured weak-ref tree"
    (let [{:keys [storage]} (make-storage)
          bf 16  ;; Small bf to force many splits
          ;; Start with a tree that fills nodes
          base-keys (vec (range 0 1000 4))  ;; 250 keys
          tree (set/conj-all
                (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                base-keys)
          addr (set/store tree storage)
          restored (set/restore addr storage {:branching-factor bf :ref-type :weak})]
      ;; GC to clear all weak refs
      (force-gc)
      (allocate-garbage 10000)
      (force-gc)
      ;; Now insert interspersed keys that will cause many splits
      (let [insert-keys (vec (filter #(pos? (mod % 4)) (range 1000)))  ;; ~750 keys
            result (set/conj-all restored insert-keys)
            result-addr (set/store result storage)]
        (is (= 1000 (count result)))
        (is (= (vec (range 1000)) (vec (set/slice result 0 999))))
        ;; Restore and verify again after GC
        (let [final (set/restore result-addr storage {:branching-factor bf :ref-type :weak})]
          (force-gc)
          (is (= 1000 (count final)))
          (is (= 500 (nth (vec (set/slice final 0 999)) 500))))))))

(deftest test-weak-ref-disj-after-gc
  (testing "disj operations on a weak-ref tree after GC"
    (let [{:keys [storage]} (make-storage)
          bf 32
          n 1000
          keys (vec (range n))
          tree (set/conj-all
                (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                keys)
          addr (set/store tree storage)
          restored (set/restore addr storage {:branching-factor bf :ref-type :weak})]
      ;; GC to clear references
      (force-gc)
      (allocate-garbage 10000)
      (force-gc)
      ;; Disj many elements
      (let [to-remove (range 0 n 3)  ;; Remove every 3rd element
            result (reduce disj restored to-remove)
            expected (vec (remove #(zero? (mod % 3)) (range n)))]
        (is (= (count expected) (count result)))
        ;; Store and restore
        (let [result-addr (set/store result storage)]
          (force-gc)
          (let [final (set/restore result-addr storage {:branching-factor bf :ref-type :weak})]
            (is (= (count expected) (count final)))))))))

(deftest test-weak-ref-concurrent-trees
  (testing "Multiple trees sharing storage with weak refs and GC pressure"
    (let [{:keys [storage]} (make-storage)
          bf 32
          ;; Create several trees
          tree1 (set/conj-all
                 (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                 (vec (range 0 500)))
          tree2 (set/conj-all
                 (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                 (vec (range 500 1000)))
          addr1 (set/store tree1 storage)
          addr2 (set/store tree2 storage)]
      ;; Restore both, GC, then access interleaved
      (let [r1 (set/restore addr1 storage {:branching-factor bf :ref-type :weak})
            r2 (set/restore addr2 storage {:branching-factor bf :ref-type :weak})]
        (force-gc)
        (allocate-garbage 10000)
        (force-gc)
        ;; Access both trees alternately
        (is (= 500 (count r1)))
        (force-gc)
        (is (= 500 (count r2)))
        (is (= 0 (first r1)))
        (force-gc)
        (is (= 500 (first r2)))
        (is (= 499 (last r1)))
        (force-gc)
        (is (= 999 (last r2)))))))

(deftest test-weak-ref-store-modified-subtree
  (testing "Storing a tree where some subtrees have been modified and others are stored references"
    (let [{:keys [storage]} (make-storage)
          bf 16
          n 500
          keys (vec (range 0 n 2))  ;; 250 even keys
          tree (set/conj-all
                (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                keys)
          addr (set/store tree storage)
          restored (set/restore addr storage {:branching-factor bf :ref-type :weak})]
      ;; GC to clear all weak refs to stored nodes
      (force-gc)
      (allocate-garbage 10000)
      (force-gc)
      ;; Modify only a portion of the tree (left side)
      (let [left-inserts (vec (range 1 100 2))  ;; Odd numbers 1-99
            modified (set/conj-all restored left-inserts)]
        ;; The right side of the tree should still be stored (addresses non-null)
        ;; The left side has new unstored nodes
        ;; GC again to clear any newly-created weak refs
        (force-gc)
        ;; Store should work: stored subtrees are fine (have addresses),
        ;; modified subtrees have raw ANode children
        (let [new-addr (set/store modified storage)]
          (is (some? new-addr))
          ;; Verify
          (let [final (set/restore new-addr storage {:branching-factor bf :ref-type :weak})]
            (force-gc)
            (is (= (+ 250 50) (count final)))
            (is (= (vec (sort (concat keys left-inserts)))
                   (vec (set/slice final 0 (dec n)))))))))))

(deftest test-soft-ref-basic-eviction
  (testing "Soft references are cleared under memory pressure"
    (let [{:keys [storage disk]} (make-storage)
          bf 32
          n 5000
          keys (vec (range n))
          tree (set/conj-all
                (set/sorted-set* {:branching-factor bf :ref-type :soft :storage storage})
                keys)
          addr (set/store tree storage)]
      ;; Verify tree is stored
      (is (pos? (count @disk)))
      ;; Restore and access under memory pressure
      (let [restored (set/restore addr storage {:branching-factor bf :ref-type :soft})]
        ;; Access some elements to load nodes
        (is (= 0 (first restored)))
        (is (= (dec n) (last restored)))
        ;; Even under GC pressure, soft refs should be recoverable from storage
        (force-gc)
        (allocate-garbage 50000)
        (force-gc)
        ;; All elements should still be accessible
        (is (= n (count restored)))
        (is (= keys (vec (set/slice restored 0 (dec n)))))))))

(deftest test-weak-ref-conj-single-after-gc
  (testing "Single conj operations on weak-ref tree after GC"
    (let [{:keys [storage]} (make-storage)
          bf 32
          keys (vec (range 0 1000 2))
          tree (set/conj-all
                (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                keys)
          addr (set/store tree storage)
          restored (set/restore addr storage {:branching-factor bf :ref-type :weak})]
      ;; GC clears weak refs
      (force-gc)
      ;; Single conj operations (not conjAll)
      (let [modified (-> restored
                         (.cons (Integer/valueOf 1))
                         (.cons (Integer/valueOf 3))
                         (.cons (Integer/valueOf 5))
                         (.cons (Integer/valueOf 7))
                         (.cons (Integer/valueOf 9)))]
        (force-gc)
        (let [new-addr (set/store modified storage)]
          (is (= 505 (count modified)))
          (let [final (set/restore new-addr storage {:branching-factor bf :ref-type :weak})]
            (force-gc)
            (is (= 505 (count final)))
            (is (contains? final 1))
            (is (contains? final 3))))))))

(deftest test-weak-ref-many-versions
  (testing "Many versions of a tree stored with weak refs, all accessible after GC"
    (let [{:keys [storage]} (make-storage)
          bf 32
          base-tree (set/conj-all
                     (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                     (vec (range 100)))
          base-addr (set/store base-tree storage)]
      ;; Create many versions by adding different elements
      (let [versions (reduce
                      (fn [versions i]
                        (let [prev-addr (peek versions)
                              restored (set/restore prev-addr storage {:branching-factor bf :ref-type :weak})
                              modified (set/conj-all restored (vec (range (* i 100) (* (inc i) 100))))
                              new-addr (set/store modified storage)]
                          (force-gc)
                          (conj versions new-addr)))
                      [base-addr]
                      (range 1 20))]
        ;; GC heavily
        (force-gc)
        (allocate-garbage 30000)
        (force-gc)
        ;; Verify the last version has all elements
        (let [final (set/restore (peek versions) storage {:branching-factor bf :ref-type :weak})]
          (is (= 2000 (count final)))
          (is (= (vec (range 2000)) (vec (set/slice final 0 1999)))))))))

(deftest test-branch-store-cleared-weakref-throws-not-npe
  (testing "Branch.store() with cleared WeakReference throws IllegalStateException, not NPE"
    ;; This directly reproduces the scenario from issue #13:
    ;; A Branch has _addresses[i] == null and _children[i] is a cleared WeakReference
    (let [{:keys [storage]} (make-storage)
          settings (Settings. (int 16) RefType/WEAK)
          ;; Create a Leaf to use as a child
          leaf-keys (into-array Object (range 5))
          leaf (Leaf. (int 5) leaf-keys settings)
          ;; Store the leaf to get an address
          leaf-addr (.store leaf (:storage (make-storage)))
          ;; Create a WeakReference to the leaf
          weak-ref (java.lang.ref.WeakReference. leaf)
          ;; Create a Branch with the WeakReference as a child but null address
          ;; This simulates the corrupted state: address nulled but child is still a Reference
          branch-keys (into-array Object [4])
          branch-children (object-array [weak-ref])
          branch (Branch. (int 1) (int 1) branch-keys nil branch-children settings)]
      ;; Clear the local reference to allow GC to clear the WeakReference
      ;; (leaf is still referenced by leaf-keys, leaf-addr, etc. - so we need to be creative)
      ;; Instead, directly set the WeakReference's referent to null via clearing
      (.clear weak-ref)
      ;; Now _children[0] is a cleared WeakReference and _addresses[0] is null
      ;; Branch.store() should throw IllegalStateException, not NPE
      (is (thrown-with-msg? IllegalStateException
                            #"tree structure corruption"
                            (.store branch (:storage (make-storage))))))))

(deftest test-branch-store-with-valid-weakref-works
  (testing "Branch.store() with valid (non-cleared) WeakReference child works"
    ;; Even if _children[i] is a WeakReference (not raw ANode), store() should
    ;; unwrap it and store successfully.
    (let [{:keys [storage]} (make-storage)
          settings (Settings. (int 16) RefType/WEAK)
          leaf-keys (into-array Object (range 5))
          leaf (Leaf. (int 5) leaf-keys settings)
          ;; Create a WeakReference to the leaf (NOT cleared)
          weak-ref (java.lang.ref.WeakReference. leaf)
          branch-keys (into-array Object [4])
          branch-children (object-array [weak-ref])
          branch (Branch. (int 1) (int 1) branch-keys nil branch-children settings)]
      ;; store() should unwrap the WeakReference and store the leaf
      (let [addr (.store branch storage)]
        (is (some? addr))))))

(deftest test-issue-13-repro-conjall-gc-store
  (testing "Issue #13 scenario: conjAll on restored tree, GC, then store"
    ;; This test exercises the exact issue #13 scenario at the Branch level:
    ;; 1. Store a tree (children become WeakReferences)
    ;; 2. Restore it
    ;; 3. Navigate to some nodes (populates _children with WeakRefs)
    ;; 4. Modify via conjAll (creates new nodes, nulls addresses)
    ;; 5. GC clears WeakReferences
    ;; 6. Store the modified tree - should work because:
    ;;    - Modified nodes have raw ANode children (not References)
    ;;    - Unmodified nodes have non-null addresses (already stored)
    (dotimes [trial 20]
      (let [{:keys [storage]} (make-storage)
            bf 16
            base-keys (vec (range 0 200 3))
            tree (set/conj-all
                  (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                  base-keys)
            addr (set/store tree storage)
            restored (set/restore addr storage {:branching-factor bf :ref-type :weak})]
        ;; Navigate to populate children with WeakRefs
        (first restored)
        (last restored)
        (contains? restored 99)
        ;; Heavy GC to clear WeakRefs
        (force-gc)
        (allocate-garbage 20000)
        (force-gc)
        ;; conjAll with interspersed keys
        (let [new-keys (vec (filter #(pos? (mod % 3)) (range 200)))
              modified (set/conj-all restored new-keys)]
          ;; GC again
          (force-gc)
          (allocate-garbage 10000)
          (force-gc)
          ;; Store should succeed - this is where issue #13 NPE would occur
          (let [new-addr (set/store modified storage)]
            (is (some? new-addr))
            ;; Verify correctness
            (let [final (set/restore new-addr storage {:branching-factor bf :ref-type :weak})]
              (is (= 200 (count final))))))))))

(deftest test-issue-13-repro-cons-gc-store
  (testing "Issue #13 scenario with single cons operations (EARLY_EXIT path)"
    ;; Tests the cons EARLY_EXIT path where root stays as WeakReference
    (dotimes [trial 20]
      (let [{:keys [storage]} (make-storage)
            bf 16
            base-keys (vec (range 0 200 2))
            tree (set/conj-all
                  (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                  base-keys)
            addr (set/store tree storage)
            restored (set/restore addr storage {:branching-factor bf :ref-type :weak})]
        ;; GC to clear WeakRefs
        (force-gc)
        (allocate-garbage 10000)
        (force-gc)
        ;; Single cons operations (uses EARLY_EXIT for in-place transient updates)
        (let [modified (reduce (fn [t k] (.cons ^PersistentSortedSet t (Integer/valueOf k)))
                               restored
                               (range 1 50 2))]  ;; Add odd numbers 1-49
          (force-gc)
          ;; Store should succeed
          (let [new-addr (set/store modified storage)]
            (is (some? new-addr))
            (let [final (set/restore new-addr storage {:branching-factor bf :ref-type :weak})]
              (is (= (+ 100 25) (count final))))))))))

(deftest test-issue-13-repro-disj-gc-store
  (testing "Issue #13 scenario with disj operations (EARLY_EXIT path)"
    (dotimes [trial 20]
      (let [{:keys [storage]} (make-storage)
            bf 16
            base-keys (vec (range 500))
            tree (set/conj-all
                  (set/sorted-set* {:branching-factor bf :ref-type :weak :storage storage})
                  base-keys)
            addr (set/store tree storage)
            restored (set/restore addr storage {:branching-factor bf :ref-type :weak})]
        ;; GC
        (force-gc)
        (allocate-garbage 10000)
        (force-gc)
        ;; disj operations
        (let [to-remove (range 0 500 5)  ;; Remove every 5th
              modified (reduce disj restored to-remove)]
          (force-gc)
          (let [new-addr (set/store modified storage)]
            (is (some? new-addr))
            (let [final (set/restore new-addr storage {:branching-factor bf :ref-type :weak})]
              (is (= 400 (count final))))))))))
