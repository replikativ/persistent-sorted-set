(ns me.tonsky.persistent-sorted-set.test.auto-removal
  "CLJS tests for auto-removal of obsolete addresses during batch operations.

   This mirrors the CLJ auto_removal.clj tests to verify the marking machinery
   works correctly in ClojureScript.

   This feature allows storage implementations to track and delete addresses
   that become obsolete when nodes are modified, preventing garbage accumulation
   during large batch operations."
  (:require [cljs.test :as t :refer-macros [is are deftest testing async]]
            [clojure.edn :as edn]
            [clojure.set :as cset]
            [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.impl.storage :as storage :refer [IStorage]]
            [me.tonsky.persistent-sorted-set.impl.node :as node]
            [me.tonsky.persistent-sorted-set.btset :refer [BTSet]]
            [me.tonsky.persistent-sorted-set.leaf :refer [Leaf]]
            [me.tonsky.persistent-sorted-set.branch :refer [Branch] :as branch]))

(def ^:dynamic *debug* false)

(defn gen-addr []
  (random-uuid))

;;; Storage implementation with auto-removal support

(defrecord AutoRemovalStorage [*memory *disk *freed settings]
  IStorage
  (store [_ node opts]
    (let [address (gen-addr)]
      (swap! *disk assoc address
             (pr-str
              {:level     (node/level node)
               :keys      (vec (.-keys node))
               :addresses (when (instance? Branch node)
                            (vec (.-addresses node)))}))
      (swap! *memory assoc address node)
      address))

  (accessed [_ address]
    nil)

  (restore [_ address opts]
    (or
     (@*memory address)
     (let [{:keys [level keys addresses] :as m} (edn/read-string (@*disk address))
           node (if addresses
                  (branch/from-map (assoc m :settings settings))
                  (Leaf. (clj->js keys) settings))]
       (swap! *memory assoc address node)
       node)))

  ;; Auto-removal methods
  (delete [_ addresses]
    (when (seq addresses)
      (doseq [addr addresses]
        (swap! *disk dissoc addr)
        (swap! *memory dissoc addr))))

  (markFreed [_ address]
    (when *debug*
      (println "markFreed called with address:" address))
    (when address
      (swap! *freed conj address)))

  (isFreed [_ address]
    (contains? @*freed address))

  (freedInfo [_ address]
    (when (contains? @*freed address)
      "Freed in CLJS test")))

(defn auto-removal-storage
  "Create storage with auto-removal tracking."
  ([] (auto-removal-storage {}))
  ([opts]
   (->AutoRemovalStorage (atom {}) (atom {}) (atom #{})
                         (merge {:branching-factor 512} opts))))

(defn storage-size
  "Return number of entries in disk storage."
  [^AutoRemovalStorage storage]
  (count @(:*disk storage)))

(defn freed-count
  "Return number of addresses marked as freed."
  [^AutoRemovalStorage storage]
  (count @(:*freed storage)))

(defn clear-freed!
  "Clear the freed set."
  [^AutoRemovalStorage storage]
  (reset! (:*freed storage) #{}))

(defn delete-freed!
  "Delete all freed addresses from storage."
  [^AutoRemovalStorage storage]
  (let [freed @(:*freed storage)]
    (when (seq freed)
      (storage/delete storage freed)
      (reset! (:*freed storage) #{}))))

;;; Tests

(deftest test-basic-storage-operations
  (testing "Basic store/restore without auto-removal"
    (let [stg (auto-removal-storage)
          set1 (-> (set/sorted-set)
                   (conj 1) (conj 2) (conj 3))
          addr1 (set/store set1 stg)
          set2 (set/restore addr1 stg)]
      (is (= (vec set1) (vec set2)))
      (is (pos? (storage-size stg))))))

(deftest test-mark-freed
  (testing "markFreed collects addresses"
    (let [stg (auto-removal-storage)
          addr1 (gen-addr)
          addr2 (gen-addr)]
      (storage/markFreed stg addr1)
      (is (= 1 (freed-count stg)))
      (storage/markFreed stg addr2)
      (is (= 2 (freed-count stg)))
      ;; nil should be ignored
      (storage/markFreed stg nil)
      (is (= 2 (freed-count stg))))))

(deftest test-is-freed
  (testing "isFreed checks if address was marked"
    (let [stg (auto-removal-storage)
          addr1 (gen-addr)
          addr2 (gen-addr)]
      (is (not (storage/isFreed stg addr1)))
      (storage/markFreed stg addr1)
      (is (storage/isFreed stg addr1))
      (is (not (storage/isFreed stg addr2))))))

(deftest test-delete-freed
  (testing "deleteFreed removes marked addresses"
    (let [stg (auto-removal-storage)
          ;; Store some nodes first
          set1 (-> (set/sorted-set)
                   (conj 1) (conj 2) (conj 3))
          addr1 (set/store set1 stg)
          initial-size (storage-size stg)]
      (is (pos? initial-size))

      ;; Mark the root address as freed
      (storage/markFreed stg addr1)
      (is (= 1 (freed-count stg)))

      ;; Delete freed addresses
      (delete-freed! stg)

      ;; Freed set should be cleared
      (is (= 0 (freed-count stg)))

      ;; Storage should have fewer entries (at least root was deleted)
      (is (< (storage-size stg) initial-size)))))

(deftest test-delete-batch
  (testing "delete removes multiple addresses in batch"
    (let [stg (auto-removal-storage)
          ;; Create and store multiple sets
          sets (for [i (range 5)]
                 (-> (set/sorted-set)
                     (conj (* i 10))
                     (conj (+ (* i 10) 1))))
          addrs (mapv #(set/store % stg) sets)
          initial-size (storage-size stg)]
      (is (>= initial-size 5))

      ;; Delete first 3 addresses
      (storage/delete stg (take 3 addrs))

      ;; Storage should be smaller
      (is (< (storage-size stg) initial-size)))))

(deftest test-auto-removal-workflow
  (testing "Workflow: track old addresses, modify, delete freed"
    (let [stg (auto-removal-storage)
          ;; Create initial set with many elements to ensure multiple nodes
          initial-set (reduce conj (set/sorted-set) (range 100))
          addr1 (set/store initial-set stg)
          size-after-initial (storage-size stg)]

      (when *debug*
        (println "Initial storage size:" size-after-initial))

      ;; Get all addresses in the initial tree
      (let [initial-addrs (atom #{})]
        (set/walk-addresses initial-set #(swap! initial-addrs conj %))

        (when *debug*
          (println "Initial addresses:" (count @initial-addrs)))

        ;; Mark all current addresses as "old" (would be freed after modification)
        (doseq [addr @initial-addrs]
          (storage/markFreed stg addr))

        (is (= (count @initial-addrs) (freed-count stg)))

        ;; Now simulate modification - create new version with different content
        (let [modified-set (-> initial-set
                               (disj 50) (disj 51) (disj 52)
                               (conj 1000) (conj 1001))
              addr2 (set/store modified-set stg)
              size-after-modify (storage-size stg)]

          (when *debug*
            (println "After modify storage size:" size-after-modify)
            (println "Freed count:" (freed-count stg)))

          ;; Storage grew because we have both old and new nodes
          (is (>= size-after-modify size-after-initial))

          ;; Now delete the freed addresses
          (delete-freed! stg)

          (let [size-after-cleanup (storage-size stg)]
            (when *debug*
              (println "After cleanup storage size:" size-after-cleanup))

            ;; Freed set should be empty
            (is (= 0 (freed-count stg)))

            ;; Storage should be smaller than after modification
            ;; (old nodes deleted, only new nodes remain)
            (is (< size-after-cleanup size-after-modify))

            ;; Modified set should still be restorable
            (let [restored (set/restore addr2 stg)]
              (is (= (vec modified-set) (vec restored))))))))))

(deftest test-incremental-updates-with-cleanup
  (testing "Multiple updates with incremental cleanup"
    (let [stg (auto-removal-storage)
          ;; Start with initial set
          set0 (reduce conj (set/sorted-set) (range 50))
          _ (set/store set0 stg)]

      ;; Do multiple updates, cleaning up after each
      (loop [current-set set0
             iteration 0]
        (when (< iteration 5)
          (let [;; Track current addresses before modification
                old-addrs (atom #{})
                _ (set/walk-addresses current-set #(swap! old-addrs conj %))

                ;; Mark old addresses as freed
                _ (doseq [addr @old-addrs]
                    (storage/markFreed stg addr))

                ;; Modify set
                new-set (-> current-set
                            (disj (* iteration 10))
                            (conj (+ 100 iteration)))
                _ (set/store new-set stg)

                size-before-cleanup (storage-size stg)

                ;; Cleanup
                _ (delete-freed! stg)

                size-after-cleanup (storage-size stg)]

            (when *debug*
              (println "Iteration" iteration
                       "before:" size-before-cleanup
                       "after:" size-after-cleanup))

            ;; Each cleanup should reduce storage size
            (is (<= size-after-cleanup size-before-cleanup))

            (recur new-set (inc iteration))))))))

(deftest test-automatic-marking-during-conj
  (testing "Addresses are automatically marked as freed during conj operations"
    (let [stg (auto-removal-storage)
          ;; Create set WITH storage so operations can call markFreed
          initial-set (reduce conj (set/sorted-set* {:storage stg}) (range 100))
          stored-addr (set/store initial-set stg)
          initial-size (storage-size stg)
          initial-freed (freed-count stg)]

      (when *debug*
        (println "After initial store - size:" initial-size "freed:" initial-freed)
        (println "Stored address:" stored-addr)
        (println "Set address field:" (.-address initial-set))
        (println "Set storage field:" (.-storage initial-set)))

      ;; Freed count should be 0 after initial store (no modifications yet)
      (is (= 0 initial-freed))

      ;; Now modify the set - this should automatically mark old leaf addresses as freed
      ;; The storage is attached to the set, so conj will call markFreed
      (let [_ (when *debug* (println "Before conj - freed:" (freed-count stg)))
            set-after-first-conj (conj initial-set 500)
            _ (when *debug*
                (println "After first conj - freed:" (freed-count stg))
                (println "First conj result address:" (.-address set-after-first-conj))
                (println "First conj result storage:" (if (.-storage set-after-first-conj) "present" "nil")))
            modified-set (conj set-after-first-conj 501)
            _ (when *debug* (println "After second conj - freed:" (freed-count stg)))
            _ (set/store modified-set stg)
            freed-after-modify (freed-count stg)]

        (when *debug*
          (println "After modify - freed:" freed-after-modify))

        ;; The old leaf node(s) that were modified should be marked as freed
        ;; (At least 1 address should be freed since we modified a leaf)
        (is (pos? freed-after-modify) "At least one address should be marked as freed after modification")

        ;; Delete the freed addresses
        (delete-freed! stg)

        ;; Modified set should still be restorable
        (let [restored (set/restore (set/store modified-set stg) stg)]
          (is (contains? (set restored) 500))
          (is (contains? (set restored) 501)))))))

(deftest test-automatic-marking-during-disj
  (testing "Addresses are automatically marked as freed during disj operations"
    (let [stg (auto-removal-storage)
          ;; Create set WITH storage
          initial-set (reduce conj (set/sorted-set* {:storage stg}) (range 100))
          _ (set/store initial-set stg)]

      ;; Remove elements - should trigger automatic marking
      (let [modified-set (-> initial-set
                             (disj 50)
                             (disj 51)
                             (disj 52))
            _ (set/store modified-set stg)
            freed-after-disj (freed-count stg)]

        (when *debug*
          (println "After disj - freed:" freed-after-disj))

        ;; Disj operations should mark old addresses as freed
        (is (pos? freed-after-disj) "At least one address should be marked as freed after disj")))))

;; =============================================================================
;; GC Invariant Tests (ported from gc_stress.clj)
;; =============================================================================

;; Use the same AutoRemovalStorage but with custom branching factor
(defn gc-tracking-storage
  "Create storage that tracks markFreed calls for GC invariant tests.
   Returns {:storage <IStorage> :freed-set <atom #{}> :disk <atom {}>}"
  [branching-factor]
  (let [stg (auto-removal-storage {:branching-factor branching-factor})]
    {:storage stg
     :freed-set (:*freed stg)
     :disk (:*disk stg)}))

(defn build-stored-tree
  "Build a tree from keys, store it, restore it (so all nodes have addresses).
   Returns {:tree <restored-pss> :storage <IStorage> :freed-set <atom> :disk <atom>}"
  [keys branching-factor]
  (let [{:keys [storage freed-set disk]} (gc-tracking-storage branching-factor)
        tree (reduce conj
                     (set/sorted-set* {:branching-factor branching-factor :storage storage})
                     (sort (distinct keys)))
        address (set/store tree storage)
        restored (set/restore address storage {:branching-factor branching-factor})]
    ;; Clear freed-set from initial build (we only care about subsequent ops)
    (reset! freed-set #{})
    {:tree restored :storage storage :freed-set freed-set :disk disk
     :root-address address}))

(defn collect-tree-addresses
  "Walk a stored tree and collect all addresses referenced by it."
  [^BTSet tree]
  (let [addresses (atom #{})]
    (set/walk-addresses tree
                        (fn [addr]
                          (when addr
                            (swap! addresses conj addr))
                          true))
    ;; Also include root address
    (when-let [root-addr (.-address tree)]
      (swap! addresses conj root-addr))
    @addresses))

(defn check-gc-invariant
  "After conj + store, verify:
   1. No address in the tree is in the freed-set
   2. Tree contents are correct
   Returns {:pass? bool :details ...}"
  [tree freed-set expected-keys]
  (let [tree-addrs (collect-tree-addresses tree)
        freed @freed-set
        conflicts (cset/intersection tree-addrs freed)]
    (cond
      (seq conflicts)
      {:pass? false
       :error :freed-address-in-tree
       :conflicts (count conflicts)
       :tree-addr-count (count tree-addrs)
       :freed-count (count freed)
       :sample-conflicts (take 5 conflicts)}

      (not= (count expected-keys) (count tree))
      {:pass? false
       :error :count-mismatch
       :expected (count expected-keys)
       :actual (count tree)}

      (not= (vec expected-keys) (vec tree))
      {:pass? false
       :error :content-mismatch
       :first-diff (first (remove (set (vec tree)) expected-keys))}

      :else
      {:pass? true})))

(deftest test-build-stored-tree-basic
  (testing "Basic build-stored-tree correctness"
    (let [stg (auto-removal-storage {:branching-factor 32})
          ;; Build tree directly
          tree (reduce conj (set/sorted-set* {:branching-factor 32 :storage stg}) (range 100))
          _ (is (= 100 (count tree)) "Initial tree should have 100 elements")
          addr (set/store tree stg)
          restored (set/restore addr stg {:branching-factor 32})
          _ (is (= 100 (count restored)) "Restored tree should have 100 elements")
          ;; Add more elements to restored tree
          tree2 (reduce conj restored (range 100 200))]
      (is (= 200 (count tree2)) "After adding 100 more, should have 200 elements"))))

(deftest test-build-stored-tree-via-helper
  (testing "build-stored-tree helper works correctly"
    (let [{:keys [tree storage freed-set]} (build-stored-tree (range 100) 32)]
      (is (= 100 (count tree)) (str "Restored tree should have 100 elements, got " (count tree)))
      ;; Add more elements
      (let [tree2 (reduce conj tree (range 100 200))]
        (is (= 200 (count tree2)) (str "After adding 100 more, should have 200, got " (count tree2)))))))

(deftest test-gc-invariant-simple-append
  (testing "Simple append: freed addresses not in stored tree"
    (doseq [bf [16 32 64]]
      (let [{:keys [tree storage freed-set]} (build-stored-tree (range 100) bf)
            _ (is (= 100 (count tree)) (str "bf=" bf " tree should have 100 elements"))
            new-keys (vec (range 100 200))
            result (reduce conj tree new-keys)
            _ (is (= 200 (count result)) (str "bf=" bf " result should have 200 elements, got " (count result)))
            _ (set/store result storage)
            result-check (check-gc-invariant result freed-set
                                             (vec (range 200)))]
        (is (:pass? result-check)
            (str "bf=" bf " " (pr-str result-check)))))))

(deftest test-gc-invariant-interspersed
  (testing "Interspersed keys: frees at various positions in the tree"
    (doseq [bf [16 32]]
      (let [base-keys (vec (range 0 200 2))  ; evens = 100 elements
            _ (is (= 100 (count base-keys)) "base-keys should be 100")
            {:keys [tree storage freed-set]} (build-stored-tree base-keys bf)
            _ (is (= 100 (count tree)) (str "bf=" bf " tree should have 100 elements"))
            ;; Insert odds between existing keys
            new-keys (vec (range 1 200 2))  ; odds = 100 elements
            _ (is (= 100 (count new-keys)) "new-keys should be 100")
            result (reduce conj tree new-keys)
            _ (is (= 200 (count result)) (str "bf=" bf " result should have 200, got " (count result)))
            _ (set/store result storage)
            result-check (check-gc-invariant result freed-set
                                             (vec (range 200)))]
        (is (:pass? result-check)
            (str "bf=" bf " interspersed " (pr-str result-check)))))))

(deftest test-gc-invariant-splits-heavy
  (testing "Heavy splitting: small BF with large inserts"
    (let [bf 16
          {:keys [tree storage freed-set]} (build-stored-tree (range 50) bf)
          _ (is (= 50 (count tree)) (str "tree should have 50, got " (count tree)))
          ;; Insert enough to cause many splits
          new-keys (vec (range 50 200))
          _ (is (= 150 (count new-keys)) "new-keys should be 150")
          result (reduce conj tree new-keys)
          _ (is (= 200 (count result)) (str "result should have 200, got " (count result)))
          _ (set/store result storage)
          result-check (check-gc-invariant result freed-set
                                           (vec (range 200)))]
      (is (:pass? result-check)
          (str "splits-heavy " (pr-str result-check))))))

(deftest test-gc-invariant-repeated-store
  (testing "Store, conj, store again - simulates successive commits"
    (let [bf 32
          {:keys [tree storage freed-set disk]} (build-stored-tree (range 100) bf)
          _ (is (= 100 (count tree)) (str "tree should have 100, got " (count tree)))
          ;; First conj + store
          tree1 (reduce conj tree (range 100 200))
          _ (is (= 200 (count tree1)) (str "tree1 should have 200, got " (count tree1)))
          addr1 (set/store tree1 storage)
          freed1 @freed-set
          ;; Restore from new address
          tree1-restored (set/restore addr1 storage {:branching-factor bf})
          _ (is (= 200 (count tree1-restored)) (str "tree1-restored should have 200, got " (count tree1-restored)))
          ;; Reset freed for second round
          _ (reset! freed-set #{})
          ;; Second conj + store
          tree2 (reduce conj tree1-restored (range 200 300))
          _ (is (= 300 (count tree2)) (str "tree2 should have 300, got " (count tree2)))
          addr2 (set/store tree2 storage)
          ;; Check: tree2's addresses should not be in freed-set from round 2
          result-check (check-gc-invariant tree2 freed-set
                                           (vec (range 300)))]
      ;; Also verify first round was clean
      (is (empty? (cset/intersection (collect-tree-addresses tree1) freed1))
          "Round 1 should also have no conflicts")
      (is (:pass? result-check)
          (str "repeated-store " (pr-str result-check))))))

(deftest test-gc-restore-after-gc
  (testing "Simulate GC: remove freed addresses from disk, then restore tree"
    (let [bf 32
          {:keys [tree storage freed-set disk]} (build-stored-tree (range 200) bf)
          ;; conj + store
          result (reduce conj tree (range 200 400))
          root-addr (set/store result storage)
          ;; Simulate GC: remove freed addresses from disk
          freed @freed-set
          _ (swap! disk #(reduce dissoc % freed))
          ;; Now restore the tree - should work because no freed addr is referenced
          restored (set/restore root-addr storage {:branching-factor bf})]
      ;; Verify we can iterate the entire tree (forces all lazy loads)
      (is (= 400 (count restored)))
      (is (= (vec (range 400)) (vec restored))))))

(deftest test-gc-invariant-disj
  (testing "Disj operations maintain GC invariant"
    (let [bf 32
          {:keys [tree storage freed-set]} (build-stored-tree (range 200) bf)
          ;; Remove some elements
          result (reduce disj tree (range 50 100))
          _ (set/store result storage)
          expected (vec (concat (range 50) (range 100 200)))
          result-check (check-gc-invariant result freed-set expected)]
      (is (:pass? result-check)
          (str "disj " (pr-str result-check))))))

(deftest test-gc-invariant-mixed-ops
  (testing "Mixed conj and disj operations maintain GC invariant"
    (let [bf 32
          {:keys [tree storage freed-set]} (build-stored-tree (range 200) bf)
          _ (is (= 200 (count tree)) (str "tree should have 200, got " (count tree)))
          ;; Mix of operations - step by step
          tree-after-conj1 (conj tree 500)
          _ (is (= 201 (count tree-after-conj1)) (str "after conj 500, got " (count tree-after-conj1)))
          tree-after-conj2 (conj tree-after-conj1 501)
          _ (is (= 202 (count tree-after-conj2)) (str "after conj 501, got " (count tree-after-conj2)))
          tree-after-conj3 (conj tree-after-conj2 502)
          _ (is (= 203 (count tree-after-conj3)) (str "after conj 502, got " (count tree-after-conj3)))
          tree-after-disj1 (disj tree-after-conj3 50)
          _ (is (= 202 (count tree-after-disj1)) (str "after disj 50, got " (count tree-after-disj1)))
          tree-after-disj2 (disj tree-after-disj1 51)
          _ (is (= 201 (count tree-after-disj2)) (str "after disj 51, got " (count tree-after-disj2)))
          tree-after-conj4 (conj tree-after-disj2 600)
          _ (is (= 202 (count tree-after-conj4)) (str "after conj 600, got " (count tree-after-conj4)))
          result (disj tree-after-conj4 100)
          _ (is (= 201 (count result)) (str "after disj 100, got " (count result)))
          _ (set/store result storage)
          expected (-> (set (range 200))
                       (conj 500) (conj 501) (conj 502) (conj 600)
                       (disj 50) (disj 51) (disj 100)
                       sort vec)
          result-check (check-gc-invariant result freed-set expected)]
      (is (:pass? result-check)
          (str "mixed-ops " (pr-str result-check))))))

;; Run tests
(comment
  (t/run-tests 'me.tonsky.persistent-sorted-set.test.auto-removal))
