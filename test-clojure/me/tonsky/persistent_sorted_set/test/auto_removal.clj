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

  ;; Auto-removal methods
  (delete [_ addresses]
    (when (seq addresses)
      (doseq [addr addresses]
        (swap! *disk dissoc addr)
        (swap! *memory dissoc addr))))

  (markFreed [_ address]
    (when address
      (swap! *freed conj address)))

  (deleteFreed [this]
    (let [freed @*freed]
      (when (seq freed)
        (.delete this freed)
        (reset! *freed #{})))))

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
      (.deleteFreed storage)

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
      (.delete storage (take 3 addrs))

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
          (.deleteFreed storage)

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
                _ (.deleteFreed storage)

                size-after-cleanup (storage-size storage)]

            (when *debug*
              (println "Iteration" iteration
                       "before:" size-before-cleanup
                       "after:" size-after-cleanup))

            ;; Each cleanup should reduce storage size
            (is (<= size-after-cleanup size-before-cleanup))

            (recur new-set (inc iteration))))))))

;; Run tests
(comment
  (t/run-tests 'me.tonsky.persistent-sorted-set.test.auto-removal))
