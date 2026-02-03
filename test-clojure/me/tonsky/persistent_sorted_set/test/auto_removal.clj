(ns me.tonsky.persistent-sorted-set.test.auto-removal
  "Tests for auto-removal of obsolete addresses during batch operations.

   This feature allows storage implementations to track and delete addresses
   that become obsolete when nodes are modified, preventing garbage accumulation
   during large batch operations."
  (:require
   [clojure.edn :as edn]
   [clojure.test :as t :refer [is are deftest testing]]
   [me.tonsky.persistent-sorted-set :as set])
  (:import
   [java.util Comparator]
   [me.tonsky.persistent_sorted_set ANode Branch IStorage Leaf PersistentSortedSet Settings]))

(def ^:dynamic *debug* false)

(defn gen-addr []
  (random-uuid))

;;; Storage implementation with auto-removal support

(defrecord AutoRemovalStorage [*memory *disk *freed ^Settings settings]
  IStorage
  (store [_ node]
    (let [node    ^ANode node
          address (gen-addr)]
      (swap! *disk assoc address
             (pr-str
              {:level     (.level node)
               :keys      (.keys node)
               :addresses (when (instance? Branch node)
                            (.addresses ^Branch node))}))
      (swap! *memory assoc address node)
      address))

  (accessed [_ address]
    nil)

  (restore [_ address]
    (or
     (@*memory address)
     (let [{:keys [level
                   ^java.util.List keys
                   ^java.util.List addresses]} (edn/read-string (@*disk address))
           node (if addresses
                  (Branch. (int level) ^java.util.List keys ^java.util.List addresses settings)
                  (Leaf. keys settings))]
       (swap! *memory assoc address node)
       node)))

  (markFreed [_ address]
    (when *debug*
      (println "markFreed called with address:" address))
    (when address
      (swap! *freed conj address))))

;; Standalone functions for delete and deleteFreed (not part of IStorage interface)
(defn delete
  "Delete addresses from storage."
  [^AutoRemovalStorage storage addresses]
  (when (seq addresses)
    (doseq [addr addresses]
      (swap! (:*disk storage) dissoc addr)
      (swap! (:*memory storage) dissoc addr))))

(defn delete-freed
  "Delete all addresses that have been marked as freed."
  [^AutoRemovalStorage storage]
  (let [freed @(:*freed storage)]
    (when (seq freed)
      (delete storage freed)
      (reset! (:*freed storage) #{}))))

(defn auto-removal-storage
  "Create storage with auto-removal tracking."
  ^IStorage []
  (->AutoRemovalStorage (atom {}) (atom {}) (atom #{}) (Settings.)))

(defn storage-size
  "Return number of entries in disk storage."
  [^AutoRemovalStorage storage]
  (count @(:*disk storage)))

(defn freed-count
  "Return number of addresses marked as freed."
  [^AutoRemovalStorage storage]
  (count @(:*freed storage)))

;;; Tests

(deftest test-basic-storage-operations
  (testing "Basic store/restore without auto-removal"
    (let [storage (auto-removal-storage)
          set1 (-> (set/sorted-set)
                   (conj 1) (conj 2) (conj 3))
          addr1 (set/store set1 storage)
          set2 (set/restore addr1 storage)]
      (is (= (vec set1) (vec set2)))
      (is (pos? (storage-size storage))))))

(deftest test-mark-freed
  (testing "markFreed collects addresses"
    (let [storage (auto-removal-storage)
          addr1 (gen-addr)
          addr2 (gen-addr)]
      (.markFreed storage addr1)
      (is (= 1 (freed-count storage)))
      (.markFreed storage addr2)
      (is (= 2 (freed-count storage)))
      ;; nil should be ignored
      (.markFreed storage nil)
      (is (= 2 (freed-count storage))))))

(deftest test-delete-freed
  (testing "deleteFreed removes marked addresses"
    (let [storage (auto-removal-storage)
          ;; Store some nodes first
          set1 (-> (set/sorted-set)
                   (conj 1) (conj 2) (conj 3))
          addr1 (set/store set1 storage)
          initial-size (storage-size storage)]
      (is (pos? initial-size))

      ;; Mark the root address as freed
      (.markFreed storage addr1)
      (is (= 1 (freed-count storage)))

      ;; Delete freed addresses
      (delete-freed storage)

      ;; Freed set should be cleared
      (is (= 0 (freed-count storage)))

      ;; Storage should have fewer entries (at least root was deleted)
      (is (< (storage-size storage) initial-size)))))

(deftest test-delete-batch
  (testing "delete removes multiple addresses in batch"
    (let [storage (auto-removal-storage)
          ;; Create and store multiple sets
          sets (for [i (range 5)]
                 (-> (set/sorted-set)
                     (conj (* i 10))
                     (conj (+ (* i 10) 1))))
          addrs (mapv #(set/store % storage) sets)
          initial-size (storage-size storage)]
      (is (>= initial-size 5))

      ;; Delete first 3 addresses
      (delete storage (take 3 addrs))

      ;; Storage should be smaller
      (is (< (storage-size storage) initial-size)))))

(deftest test-auto-removal-workflow
  (testing "Workflow: track old addresses, modify, delete freed"
    (let [storage (auto-removal-storage)
          ;; Create initial set with many elements to ensure multiple nodes
          initial-set (reduce conj (set/sorted-set) (range 100))
          addr1 (set/store initial-set storage)
          size-after-initial (storage-size storage)]

      (when *debug*
        (println "Initial storage size:" size-after-initial))

      ;; Get all addresses in the initial tree
      (let [initial-addrs (atom #{})]
        (set/walk-addresses initial-set #(swap! initial-addrs conj %))

        (when *debug*
          (println "Initial addresses:" (count @initial-addrs)))

        ;; Mark all current addresses as "old" (would be freed after modification)
        (doseq [addr @initial-addrs]
          (.markFreed storage addr))

        (is (= (count @initial-addrs) (freed-count storage)))

        ;; Now simulate modification - create new version with different content
        (let [modified-set (-> initial-set
                               (disj 50) (disj 51) (disj 52)
                               (conj 1000) (conj 1001))
              addr2 (set/store modified-set storage)
              size-after-modify (storage-size storage)]

          (when *debug*
            (println "After modify storage size:" size-after-modify)
            (println "Freed count:" (freed-count storage)))

          ;; Storage grew because we have both old and new nodes
          (is (>= size-after-modify size-after-initial))

          ;; Now delete the freed addresses
          (delete-freed storage)

          (let [size-after-cleanup (storage-size storage)]
            (when *debug*
              (println "After cleanup storage size:" size-after-cleanup))

            ;; Freed set should be empty
            (is (= 0 (freed-count storage)))

            ;; Storage should be smaller than after modification
            ;; (old nodes deleted, only new nodes remain)
            (is (< size-after-cleanup size-after-modify))

            ;; Modified set should still be restorable
            (let [restored (set/restore addr2 storage)]
              (is (= (vec modified-set) (vec restored))))))))))

(deftest test-incremental-updates-with-cleanup
  (testing "Multiple updates with incremental cleanup"
    (let [storage (auto-removal-storage)
          ;; Start with initial set
          set0 (reduce conj (set/sorted-set) (range 50))
          _ (set/store set0 storage)]

      ;; Do multiple updates, cleaning up after each
      (loop [current-set set0
             iteration 0]
        (when (< iteration 5)
          (let [;; Track current addresses before modification
                old-addrs (atom #{})
                _ (set/walk-addresses current-set #(swap! old-addrs conj %))

                ;; Mark old addresses as freed
                _ (doseq [addr @old-addrs]
                    (.markFreed storage addr))

                ;; Modify set
                new-set (-> current-set
                            (disj (* iteration 10))
                            (conj (+ 100 iteration)))
                _ (set/store new-set storage)

                size-before-cleanup (storage-size storage)

                ;; Cleanup
                _ (delete-freed storage)

                size-after-cleanup (storage-size storage)]

            (when *debug*
              (println "Iteration" iteration
                       "before:" size-before-cleanup
                       "after:" size-after-cleanup))

            ;; Each cleanup should reduce storage size
            (is (<= size-after-cleanup size-before-cleanup))

            (recur new-set (inc iteration))))))))

(deftest test-automatic-marking-during-conj
  (testing "Addresses are automatically marked as freed during conj operations"
    (let [storage (auto-removal-storage)
          ;; Create set WITH storage so operations can call markFreed
          ;; Use sorted-set* with :storage option
          initial-set (reduce conj (set/sorted-set* {:storage storage}) (range 100))
          stored-addr (set/store initial-set storage)
          initial-size (storage-size storage)
          initial-freed (freed-count storage)]

      (when *debug*
        (println "After initial store - size:" initial-size "freed:" initial-freed)
        (println "Stored address:" stored-addr)
        (println "Set _address field:" (._address initial-set))
        (println "Set _storage field:" (._storage initial-set)))

      ;; Freed count should be 0 after initial store (no modifications yet)
      (is (= 0 initial-freed))

      ;; Now modify the set - this should automatically mark old leaf addresses as freed
      ;; The storage is attached to the set, so conj will call markFreed
      (let [_ (when *debug* (println "Before conj - freed:" (freed-count storage)))
            set-after-first-conj (conj initial-set 500)
            _ (when *debug*
                (println "After first conj - freed:" (freed-count storage))
                (println "First conj result _address:" (._address set-after-first-conj))
                (println "First conj result _storage:" (if (._storage set-after-first-conj) "present" "nil")))
            modified-set (conj set-after-first-conj 501)
            _ (when *debug* (println "After second conj - freed:" (freed-count storage)))
            _ (set/store modified-set storage)
            freed-after-modify (freed-count storage)]

        (when *debug*
          (println "After modify - freed:" freed-after-modify))

        ;; The old leaf node(s) that were modified should be marked as freed
        ;; (At least 1 address should be freed since we modified a leaf)
        (is (pos? freed-after-modify) "At least one address should be marked as freed after modification")

        ;; Delete the freed addresses
        (delete-freed storage)

        ;; Modified set should still be restorable
        (let [restored (set/restore (set/store modified-set storage) storage)]
          (is (contains? (set restored) 500))
          (is (contains? (set restored) 501)))))))

(deftest test-automatic-marking-during-disj
  (testing "Addresses are automatically marked as freed during disj operations"
    (let [storage (auto-removal-storage)
          ;; Create set WITH storage
          initial-set (reduce conj (set/sorted-set* {:storage storage}) (range 100))
          _ (set/store initial-set storage)]

      ;; Remove elements - should trigger automatic marking
      (let [modified-set (-> initial-set
                             (disj 50)
                             (disj 51)
                             (disj 52))
            _ (set/store modified-set storage)
            freed-after-disj (freed-count storage)]

        (when *debug*
          (println "After disj - freed:" freed-after-disj))

        ;; Disj operations should mark old addresses as freed
        (is (pos? freed-after-disj) "At least one address should be marked as freed after disj")))))

;; Run tests
(comment
  (t/run-tests 'me.tonsky.persistent-sorted-set.test.auto-removal))
