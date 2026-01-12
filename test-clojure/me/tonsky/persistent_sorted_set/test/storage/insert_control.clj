(ns me.tonsky.persistent-sorted-set.test.storage.insert-control
  (:require
   [clojure.test :refer [is deftest testing]]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.test.storage :refer [*stats storage]])
  (:import
   [clojure.lang RT]
   [java.lang.ref Reference]
   [java.util ArrayList Collections Comparator Arrays List Random]
   (java.util.random RandomGenerator)
   (me.tonsky.persistent_sorted_set ANode Branch Leaf PersistentSortedSet IStorage Settings)))

(defn unwrap [^Object node]
  (if (instance? Reference node) (.get ^Reference node) node))

(defn ^ANode root [^PersistentSortedSet set]
  (unwrap (.-_root set)))

(defn addresses [node]
  (some->> (.-_addresses ^Branch (unwrap node)) (mapv unwrap) (filter some?)))

(defn children [node]
  (some->> (.-_children ^Branch (unwrap node)) (mapv unwrap) (filter some?)))

(defn ks [node]
  (some->> (.-_keys ^ANode (unwrap node)) (filterv some?)))

(defn branch? [node] (instance? Branch (unwrap node)))
(defn leaf? [node] (instance? Leaf (unwrap node)))

(defn level [^ANode node] (.level node))

#!------------------------------------------------------------------------------------------------

(deftest insert-32-control
  (and
   (testing "saturated root leaf"
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     (let [original ^PersistentSortedSet (into (set/sorted-set* {:branching-factor 32}) (range 0 32))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (leaf? (root original)))
        (is (= 32 (count (ks (root original)))))
        (is (nil? (.-_address original)))
        (let [storage (storage)
              address (set/store original storage)]
          (and
           (is (uuid? address))
           (is (= address (.-_address ^PersistentSortedSet original)))
           (is (= 1 (:writes @*stats)))
           (is (= 0 (:reads @*stats)))
           (let [restored ^PersistentSortedSet (set/restore address storage {:branching-factor 32})]
             (and
              (is (= restored original))
              (is (leaf? (root restored)))
              (is (= 1 (:reads @*stats))))))))))
   (testing "saturated root leaf + 1"
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     (let [original ^PersistentSortedSet (into (set/sorted-set* {:branching-factor 32}) (range 0 33))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (branch? (root original)))
        (is (= 2 (count (children (root original)))))
        (is (= 2 (count (ks (root original)))))
        (is (every? leaf? (children (root original))))
        (is (= [15 32] (ks (root original))))
        (is (nil? (addresses (root original))) "we have not called store() yet")
        (is (nil? (.-_address original)))
        (let [storage  (storage)
              address (set/store original storage)]
          (and
           (is (uuid? address))
           (is (= address (.-_address ^PersistentSortedSet original)))
           (is (= 3 (:writes @*stats)))
           (is (= 0 (:reads @*stats)))
           (is (= 2 (count (addresses (root original)))) "root branch stored 2 leafs at 2 address")
           (let [restored ^PersistentSortedSet (set/restore address storage {:branching-factor 32})]
             (and
              (is (= restored original))
              (is (= 3 (:reads @*stats)))
              (is (= 2 (count (addresses (root restored)))) "restored root branch has those address it just read from")
              (is (= 2 (count (children (root restored)))))
              (is (= 2 (count (ks (root restored)))))
              (is (every? leaf? (children (root restored))))
              (is (= [15 32] (ks (root restored)))))))))))
   (testing "32^4"
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     (let [expected-root-keys [65535 131071 196607 262143 327679 393215 458751 524287 589823 655359 720895 786431 851967 917503 1048575]
           original ^PersistentSortedSet (into (set/sorted-set* {:branching-factor 32}) (range 0 (Math/pow 32 4)))
           *original (atom 0)]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (= 15 (count (children (root original)))))
        (is (= 15 (count (ks (root original)))))
        (is (every? branch? (children (root original))))
        (is (= [3 3 3 3 3 3 3 3 3 3 3 3 3 3 3] (mapv level (children (root original)))))
        (is (= expected-root-keys (ks (root original))))
        (is (nil? (addresses (root original))) "we have not called store() yet")
        (is (nil? (.-_address original)))
        (is (nil? (set/walk-addresses original (fn [_] (swap! *original inc)))))
        (is (zero? @*original))
        (let [storage  (storage)
              address (set/store original storage)
              *stored (atom 0)]
          (and
           (is (uuid? address))
           (is (= address (.-_address ^PersistentSortedSet original)))
           (is (= 69901 (:writes @*stats)))
           (is (= 15 (count (addresses (root original)))) "the original holds the addresses it just wrote to")
           (is (= 0 (:reads @*stats)))
           (is (empty? (deref (:*memory storage))))
           (is (nil? (set/walk-addresses original (fn [_] (swap! *stored inc)))))
           (is (= 69901 @*stored))
           (let [restored ^PersistentSortedSet (set/restore address storage {:branching-factor 32})
                 *restored (atom 0)]
             (and
              (is (empty? (deref (:*memory storage))))
              (is (= 0 (:reads @*stats)))
              (is (= restored original))
              (is (nil? (set/walk-addresses restored (fn [_] (swap! *restored inc)))))
              (is (= 69901 @*restored))
              (is (= (int (Math/pow 32 4)) (count restored)))
              (is (= 69901 (count (deref (:*memory storage)))))
              (is (= 69901 (:reads @*stats)))
              (is (= [3 3 3 3 3 3 3 3 3 3 3 3 3 3 3] (mapv level (children (root restored)))))
              (is (= 15 (count (addresses (root restored)))) "restored root branch has those address it just read from")
              (is (= 15 (count (children (root restored)))))
              (is (= 15 (count (ks (root original)))))
              (is (every? branch? (children (root restored))))
              (is (= expected-root-keys (ks (root original)))))))))))))

(deftest insert-512-control
  (and
   (testing "saturated root leaf"
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     (let [original ^PersistentSortedSet (into (set/sorted-set) (range 0 512))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (leaf? (root original)))
        (is (= 512 (count (ks (root original)))))
        (is (nil? (.-_address original)))
        (let [storage  (storage)
              address (set/store original storage)]
          (and
           (is (uuid? address))
           (is (= address (.-_address ^PersistentSortedSet original)))
           (is (= 1 (:writes @*stats)))
           (is (= 0 (:reads @*stats)))
           (let [restored ^PersistentSortedSet (set/restore address storage)]
             (and
              (is (= restored original))
              (is (leaf? (root restored)))
              (is (= 1 (:reads @*stats))))))))))
   (testing "saturated root leaf + 1"
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     (let [original ^PersistentSortedSet (into (set/sorted-set) (range 0 513))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (branch? (root original)))
        (is (= 2 (count (children (root original)))))
        (is (= 2 (count (ks (root original)))))
        (is (every? leaf? (children (root original))))
        (is (= [255 512] (ks (root original))))
        (is (= 256 (count (ks (nth (children (root original)) 0)))))
        (is (= 257 (count (ks (nth (children (root original)) 1)))))
        (is (nil? (addresses (root original))) "we have not called store() yet")
        (is (nil? (.-_address original)))
        (let [storage  (storage)
              address (set/store original storage)]
          (and
           (is (uuid? address))
           (is (= address (.-_address ^PersistentSortedSet original)))
           (is (= 3 (:writes @*stats)))
           (is (= 0 (:reads @*stats)))
           (is (= 2 (count (addresses (root original)))) "root branch stored 2 leafs at 2 address")
           (let [restored ^PersistentSortedSet (set/restore address storage)]
             (and
              (is (= restored original))
              (is (= 3 (:reads @*stats)))
              (is (= 2 (count (addresses (root restored)))) "restored root branch has those address it just read from")
              (is (= 2 (count (children (root restored)))))
              (is (= 2 (count (ks (root restored)))))
              (is (every? leaf? (children (root restored))))
              (is (= [255 512] (ks (root restored))))
              (is (= 256 (count (ks (nth (children (root restored)) 0)))))
              (is (= 257 (count (ks (nth (children (root restored)) 1))))))))))))
   (testing "32^4"
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     (let [expected-root-keys [65535 131071 196607 262143 327679 393215 458751 524287 589823 655359 720895 786431 851967 917503 1048575]
           original ^PersistentSortedSet (into (set/sorted-set) (range 0 (Math/pow 32 4)))
           *original (atom 0)]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (= 15 (count (children (root original)))))
        (is (= 15 (count (ks (root original)))))
        (is (every? branch? (children (root original))))
        (is (= [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1] (mapv level (children (root original)))))
        (is (= expected-root-keys (ks (root original))))
        (is (nil? (addresses (root original))) "we have not called store() yet")
        (is (nil? (.-_address original)))
        (is (nil? (set/walk-addresses original (fn [_] (swap! *original inc)))))
        (is (zero? @*original))
        (let [storage (storage)
              address (set/store original storage)
              *stored (atom 0)]
          (and
           (is (uuid? address))
           (is (= address (.-_address ^PersistentSortedSet original)))
           (is (= 4111 (:writes @*stats)))
           (is (= 15 (count (addresses (root original)))) "the original holds the addresses it just wrote to")
           (is (= 0 (:reads @*stats)))
           (is (empty? (deref (:*memory storage))))
           (is (nil? (set/walk-addresses original (fn [_] (swap! *stored inc)))))
           (is (= 4111 @*stored))
           (let [restored ^PersistentSortedSet (set/restore address storage)
                 *restored (atom 0)]
             (and
              (is (empty? (deref (:*memory storage))))
              (is (= 0 (:reads @*stats)))
              (is (= restored original))
              (is (nil? (set/walk-addresses restored (fn [_] (swap! *restored inc)))))
              (is (= 4111 @*restored))
              (is (= (int (Math/pow 32 4)) (count restored)))
              (is (= 4111 (count (deref (:*memory storage)))))
              (is (= 4111 (:reads @*stats)))
              (is (= [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1] (mapv level (children (root restored)))))
              (is (= 15 (count (addresses (root restored)))) "restored root branch has those address it just read from")
              (is (= 15 (count (children (root restored)))))
              (is (= 15 (count (ks (root original)))))
              (is (every? branch? (children (root restored))))
              (is (= expected-root-keys (ks (root restored)))))))))))))

