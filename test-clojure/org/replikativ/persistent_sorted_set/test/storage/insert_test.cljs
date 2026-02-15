(ns org.replikativ.persistent-sorted-set.test.storage.insert-test
  (:require [cljs.test :as test :refer-macros [is deftest testing]]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.test.storage.util :as util
             :refer [branch? leaf? level *stats]]))

(defn root [set] (.-root set))

(defn addresses [node] (some->> (.-addresses node) (filter some?)))

(defn children [node] (some->> (.-children node) (filter some?)))

(defn ks [node] (some->> (.-keys node) (filterv some?)))

#!----------------------------------------------------------------------------------------------------------------------

(defn storage32 []
  (util/storage (atom {}) (atom {}) {:branching-factor 32}))

(defn async-storage32 []
  (util/async-storage (atom {}) (atom {}) {:branching-factor 32}))

(deftest sync-insert-32-test
  (and
   (testing "32"
     (let [_ (reset! *stats {:reads 0 :writes 0 :accessed 0})
           original (into (set/sorted-set* {:branching-factor 32}) (range 0 32))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (leaf? (root original)))
        (is (= 32 (count (ks (root original)))))
        (is (nil? (.-address original)))
        (let [storage  (storage32)
              address (set/store original storage)]
          (and
           (is (uuid? address))
           (is (= address (.-address original)))
           (is (= 1 (:writes @*stats)))
           (is (= 0 (:reads @*stats)))
           (let [restored (set/restore address storage {})]
             (and
              (is (= restored original))
              (is (leaf? (root restored)))
              (is (= 32 (count (ks (root restored)))))
              (is (= 1 (:reads @*stats))))))))))
   (testing "32 + 1"
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     (let [original (into (set/sorted-set* {:branching-factor 32}) (range 0 33))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (branch? (root original)))
        (is (= 2 (count (children (root original)))))
        (is (= 2 (count (ks (root original)))))
        (is (every? leaf? (children (root original))))
        (is (= [15 32] (ks (root original))))
        (is (nil? (addresses (root original))) "we have not called store() yet")
        (is (nil? (.-address original)))
        (let [storage (storage32)
              address (set/store original storage)]
          (and
           (is (uuid? address))
           (is (= address (.-address original)))
           (is (= 3 (:writes @*stats)))
           (is (= 0 (:reads @*stats)))
           (is (= 2 (count (addresses (root original)))) "root branch stored 2 leafs at 2 address")
           (let [restored (set/restore address storage {:branching-factor 32})]
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
           original (into (set/sorted-set* {:branching-factor 32}) (range 0 (Math/pow 32 4)))
           *original (atom 0)]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (contains? original 0))
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (contains? original (dec (Math/pow 32 4))))
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (branch? (root original)))
        (is (= 15 (count (ks (root original)))))
        (is (= 15 (count (children (root original)))))
        (is (every? branch? (children (root original))))
        (is (= expected-root-keys (ks (root original))))
        (is (nil? (addresses (root original))) "we have not called store() yet")
        (is (nil? (set/walk-addresses original (fn [_] (swap! *original inc)))))
        (is (zero? @*original))
        (is (nil? (.-address original)))
        (let [storage  (storage32)
              address (set/store original storage)
              *stored (atom 0)]
          (and
           (is (uuid? address))
           (is (= address (.-address ^PersistentSortedSet original)))
           (is (= 69901 (:writes @*stats)))
           (is (= 15 (count (addresses (root original)))) "the original holds the addresses it just wrote to")
           (is (= 0 (:reads @*stats)))
           (is (empty? (deref (:*memory storage))))
           (is (nil? (set/walk-addresses original (fn [_] (swap! *stored inc)))))
           (is (= 69901 @*stored))
           (let [restored (set/restore address storage {:branching-factor 32})
                 *restored (atom 0)]
             (and
              (is (empty? (deref (:*memory storage))))
              (is (= 0 (:reads @*stats)))
              (is (= restored original))
              (is (nil? (set/walk-addresses original (fn [_] (swap! *restored inc)))))
              (is (= 69901 @*restored))
              (is (= 15 (count (addresses (root restored)))) "restored root branch has those address it just read from")
              (is (= (int (Math/pow 32 4)) (count restored)))
              (is (= 69901 (count (deref (:*memory storage)))))
              (is (= 69901 (:reads @*stats)))
              (is (= 15 (count (ks (root original)))))
              (is (= expected-root-keys (ks (root original))))
              (is (= 15 (count (children (root restored)))))
              (is (every? branch? (children (root restored)))))))))))))

(defn do-async-insert-32-test []
  (async
   (and
    (testing "one full leaf"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [original (await (set/into (set/sorted-set* {:branching-factor 32}) (range 0 32) {:sync? false}))]
        (and
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (leaf? (root original)))
         (is (nil? (.-address original)))
         (let [storage  (async-storage32)
               address (await (set/store original storage {:sync? false}))]
           (and
            (is (uuid? address))
            (is (= address (.-address original)))
            (is (= 1 (:writes @*stats)))
            (is (= 0 (:reads @*stats)))
            (let [restored (set/restore address storage {:branching-factor 32})]
              (and
               (is (= 0 (:reads @*stats)))
               (is (true? (await (set/equiv? restored original {:sync? false}))))
               (is (= 1 (:reads @*stats)))
               (is (leaf? (root restored))))))))))
    (testing "full-leaf + 1"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [original (await (set/into (set/sorted-set* {:branching-factor 32}) (range 0 33) {:sync? false}))]
        (and
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (branch? (root original)))
         (is (nil? (.-address original)))
         (let [children (children (root original))]
           (and
            (is (= 2 (count children)))
            (is (leaf? (nth children 0)))
            (is (= 16 (count (ks (nth children 0)))))
            (is (leaf? (nth children 1)))
            (is (= 17 (count (ks (nth children 1)))))))
         (let [storage (async-storage32)
               address (await (set/store original storage {:sync? false}))]
           (and
            (is (uuid? address))
            (is (= address (.-address original)))
            (is (= 3 (:writes @*stats)))
            (is (= 0 (:reads @*stats)))
            (let [restored (set/restore address storage {:branching-factor 32})]
              (and
               (is (= 0 (:reads @*stats)))
               (is (true? (await (set/equiv? restored original {:sync? false}))))
               (is (= 3 (:reads @*stats)))
               (is (= 2 (count (children (root restored)))))
               (is (every? leaf? (children (root restored))))
               (is (= 16 (count (ks (nth (children (root restored)) 0)))))
               (is (= 17 (count (ks (nth (children (root restored)) 1))))))))))))
    (testing "32^4"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [expected-root-keys [65535 131071 196607 262143 327679 393215 458751 524287 589823 655359 720895 786431 851967 917503 1048575]
            original (await (set/into (set/sorted-set* {:branching-factor 32}) (range 0 (Math/pow 32 4)) {:sync? false}))
            *original (atom 0)]
        (and
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (true? (await (set/contains? original 0 {:sync? false}))))
         (is (true? (await (set/contains? original (dec (Math/pow 32 4)) {:sync? false}))))
         (is (false? (await (set/contains? original (Math/pow 32 4) {:sync? false}))))
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (nil? (await (set/walk-addresses original (fn [_] (swap! *original inc)) {:sync? false}))))
         (is (zero? @*original))
         (is (nil? (.-address original)))
         (is (branch? (root original)))
         (is (= 15 (count (ks (root original)))))
         (is (= expected-root-keys (ks (root original))))
         (is (= 15 (count (children (root original)))))
         (is (= [3 3 3 3 3 3 3 3 3 3 3 3 3 3 3] (mapv level (children (root original)))))
         (is (every? branch? (children (root original))))
         (let [storage (async-storage32)
               address (await (set/store original storage {:sync? false}))
               *stored (atom 0)]
           (and
            (is (uuid? address))
            (is (= address (.-address original)))
            (is (= 69901 (:writes @*stats)))
            (is (nil? (await (set/walk-addresses original (fn [_] (swap! *stored inc)) {:sync? false}))))
            (is (= 69901 @*stored))
            (is (= 0 (:reads @*stats)))
            (is (empty? (deref (:*memory storage))))
            (let [restored (set/restore address storage {:branching-factor 32})
                  *restored (atom 0)]
              (and
               (is (empty? (deref (:*memory storage))))
               (is (= 0 (:reads @*stats)))
               (is (true? (await (set/equiv? restored original {:sync? false}))))
               (is (= 69901 (:reads @*stats)))
               (is (nil? (await (set/walk-addresses restored (fn [_] (swap! *restored inc)) {:sync? false}))))
               (is (= 69901 @*restored))
               (is (= (int (Math/pow 32 4)) (count restored)) "can call sync when fully realized")
               (is (= 69901 (count (deref (:*memory storage)))))
               (is (= 69901 (:reads @*stats)))
               (is (= [3 3 3 3 3 3 3 3 3 3 3 3 3 3 3] (mapv level (children (root restored)))))
               (is (= 15 (count (children (root restored)))))
               (is (= 15 (count (ks (root original)))))
               (is (every? branch? (children (root restored))))
               (is (= expected-root-keys (ks (root original))))))))))))))

(deftest async-insert-32-test
  (test/async done
              ((do-async-insert-32-test)
               (fn [ok] (done))
               (fn [err]
                 (js/console.warn "async-ascending-insert-test failed")
                 (is (nil? err))
                 (js/console.error err)
                 (done)))))

(deftest sync-insert-512-test
  (and
   (testing "saturated root leaf"
     (let [_ (reset! *stats {:reads 0 :writes 0 :accessed 0})
           original (into (set/sorted-set) (range 0 512))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (leaf? (root original)))
        (is (= 512 (count (ks (root original)))))
        (is (nil? (.-address original)))
        (let [storage (util/storage)
              address (set/store original storage)]
          (and
           (is (uuid? address))
           (is (= address (.-address original)))
           (is (= 1 (:writes @*stats)))
           (is (= 0 (:reads @*stats)))
           (let [restored (set/restore address storage {})]
             (and
              (is (= restored original))
              (is (leaf? (root restored)))
              (is (= 512 (count (ks (root restored)))))
              (is (= 1 (:reads @*stats))))))))))
   (testing "saturated root leaf + 1"
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     (let [original (into (set/sorted-set) (range 0 513))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (branch? (root original)))
        (is (= 2 (count (children (root original)))))
        (is (= 2 (count (ks (root original)))))
        (is (every? leaf? (children (root original))))
        (is (= [255 512] (ks (root original))))
        (is (nil? (addresses (root original))) "we have not called store() yet")
        (is (nil? (.-address original)))
        (let [storage (util/storage)
              address (set/store original storage)]
          (and
           (is (uuid? address))
           (is (= address (.-address original)))
           (is (= 3 (:writes @*stats)))
           (is (= 0 (:reads @*stats)))
           (is (= 2 (count (addresses (root original)))) "root branch stored 2 leafs at 2 address")
           (let [restored (set/restore address storage {})]
             (and
              (is (= restored original))
              (is (= 3 (:reads @*stats)))
              (is (= 2 (count (addresses (root restored)))) "restored root branch has those address it just read from")
              (is (= 2 (count (children (root restored)))))
              (is (= 2 (count (ks (root restored)))))
              (is (every? leaf? (children (root restored))))
              (is (= [255 512] (ks (root restored)))))))))))
   (testing "32^4"
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     (let [expected-root-keys [65535 131071 196607 262143 327679 393215 458751 524287 589823 655359 720895 786431 851967 917503 1048575]
           original (into (set/sorted-set) (range 0 (Math/pow 32 4)))
           *original (atom 0)]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (contains? original 0))
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (contains? original (dec (Math/pow 32 4))))
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (branch? (root original)))
        (is (= 15 (count (ks (root original)))))
        (is (= 15 (count (children (root original)))))
        (is (every? branch? (children (root original))))
        (is (= expected-root-keys (ks (root original))))
        (is (= [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1] (mapv level (children (root original)))))
        (is (nil? (addresses (root original))) "we have not called store() yet")
        (is (nil? (set/walk-addresses original (fn [_] (swap! *original inc)))))
        (is (zero? @*original))
        (is (nil? (.-address original)))
        (let [storage (util/storage)
              address (set/store original storage)
              *stored (atom 0)]
          (and
           (is (uuid? address))
           (is (= address (.-address ^PersistentSortedSet original)))
           (is (= 4111 (:writes @*stats)))
           (is (= 15 (count (addresses (root original)))) "the original holds the addresses it just wrote to")
           (is (= 0 (:reads @*stats)))
           (is (empty? (deref (:*memory storage))))
           (is (nil? (set/walk-addresses original (fn [_] (swap! *stored inc)))))
           (is (= 4111 @*stored))
           (let [restored (set/restore address storage {})
                 *restored (atom 0)]
             (and
              (is (empty? (deref (:*memory storage))))
              (is (= 0 (:reads @*stats)))
              (is (= restored original))
              (is (nil? (set/walk-addresses original (fn [_] (swap! *restored inc)))))
              (is (= 4111 @*restored))
              (is (= 15 (count (addresses (root restored)))) "restored root branch has those address it just read from")
              (is (= (int (Math/pow 32 4)) (count restored)))
              (is (= 4111 (count (deref (:*memory storage)))))
              (is (= 4111 (:reads @*stats)))
              (is (= [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1] (mapv level (children (root restored)))))
              (is (= 15 (count (ks (root original)))))
              (is (= expected-root-keys (ks (root original))))
              (is (= 15 (count (children (root restored)))))
              (is (every? branch? (children (root restored)))))))))))))

(defn do-async-insert-512-test []
  (async
   (and
    (testing "saturated root leaf"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [original (await (set/into (set/sorted-set) (range 0 512) {:sync? false}))]
        (and
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (leaf? (root original)))
         (is (= 512 (count (ks (root original)))))
         (is (nil? (.-address original)))
         (let [storage (util/async-storage)
               address (await (set/store original storage {:sync? false}))]
           (and
            (is (uuid? address))
            (is (= address (.-address original)))
            (is (= 1 (:writes @*stats)))
            (is (= 0 (:reads @*stats)))
            (let [restored (set/restore address storage {})]
              (and
               (is (= 0 (:reads @*stats)))
               (is (true? (await (set/equiv? restored original {:sync? false}))))
               (is (= 1 (:reads @*stats)))
               (is (leaf? (root restored))))))))))
    (testing "saturated root leaf + 1"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [original (await (set/into (set/sorted-set* {}) (range 0 513) {:sync? false}))]
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
         (is (nil? (.-addresses (root original))))
         (is (nil? (.-address original)))
         (let [storage (util/async-storage)
               address (await (set/store original storage {:sync? false}))]
           (and
            (is (uuid? address))
            (is (= address (.-address original)))
            (is (= 3 (:writes @*stats)))
            (is (= 0 (:reads @*stats)))
            (let [restored (set/restore address storage {})]
              (and
               (is (= 0 (:reads @*stats)))
               (is (true? (await (set/equiv? restored original {:sync? false}))))
               (is (= 3 (:reads @*stats)))
               (is (= 2 (count (.-addresses (root original)))))
               (is (= 2 (count (children (root restored)))))
               (is (= 2 (count (ks (root restored)))))
               (is (every? leaf? (children (root restored))))
               (is (= 256 (count (ks (nth (children (root restored)) 0)))))
               (is (= 257 (count (ks (nth (children (root restored)) 1)))))
               (is (= [255 512] (ks (root restored)))))))))))
    (testing "32^4"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [expected-root-keys [65535 131071 196607 262143 327679 393215 458751 524287 589823 655359 720895 786431 851967 917503 1048575]
            original (await (set/into (set/sorted-set* {}) (range 0 (Math/pow 32 4)) {:sync? false}))
            *original (atom 0)]
        (and
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (true? (await (set/contains? original 0 {:sync? false}))))
         (is (true? (await (set/contains? original (dec (Math/pow 32 4)) {:sync? false}))))
         (is (false? (await (set/contains? original (Math/pow 32 4) {:sync? false}))))
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (nil? (await (set/walk-addresses original (fn [_] (swap! *original inc)) {:sync? false}))))
         (is (zero? @*original))
         (is (nil? (.-address original)))
         (is (branch? (root original)))
         (is (= expected-root-keys (ks (root original))))
         (is (= 15 (count (ks (root original)))))
         (is (= 15 (count (children (root original)))))
         (is (= [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1] (mapv level (children (root original)))))
         (is (every? branch? (children (root original))))
         (let [storage (util/async-storage)
               address (await (set/store original storage {:sync? false}))
               *stored (atom 0)]
           (and
            (is (uuid? address))
            (is (= address (.-address original)))
            (is (= 4111 (:writes @*stats)))
            (is (nil? (await (set/walk-addresses original (fn [_] (swap! *stored inc)) {:sync? false}))))
            (is (= 4111 @*stored))
            (is (= 0 (:reads @*stats)))
            (is (empty? (deref (:*memory storage))))
            (let [restored (set/restore address storage)
                  *restored (atom 0)]
              (and
               (is (empty? (deref (:*memory storage))))
               (is (= 0 (:reads @*stats)))
               (is (true? (await (set/equiv? restored original {:sync? false}))))
               (is (nil? (await (set/walk-addresses restored (fn [_] (swap! *restored inc)) {:sync? false}))))
               (is (= 4111 (:reads @*stats)))
               (is (= (int (Math/pow 32 4)) (count restored)) "can call sync when fully realized")
               (is (= 4111 @*restored))
               (is (= 4111 (count (deref (:*memory storage)))))
               (is (= [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1] (mapv level (children (root restored)))))
               (is (= 15 (count (addresses (root restored)))))
               (is (= 15 (count (children (root restored)))))
               (is (= 15 (count (ks (root restored)))))
               (is (every? branch? (children (root restored))))
               (is (= expected-root-keys (ks (root restored))))))))))))))

(deftest async-insert-512-test
  (test/async done
              ((do-async-insert-512-test)
               (fn [ok] (done))
               (fn [err]
                 (js/console.warn "async-insert-512-test failed")
                 (is (nil? err))
                 (js/console.error err)
                 (done)))))
