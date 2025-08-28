(ns me.tonsky.persistent-sorted-set.test.storage.insert-parity-tests
  (:require
   [await-cps :refer [await run-async] :refer-macros [async] :rename {run-async run}]
   ; [is.simm.lean-cps.async :refer [await run] :refer-macros [async]]
   [cljs.test :as test :refer [is are deftest testing]]
   [clojure.edn :as edn]
   [clojure.string :as str]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.impl.storage :refer [IStorage]]
   [me.tonsky.persistent-sorted-set.btset :refer [BTSet]]
   [me.tonsky.persistent-sorted-set.leaf :refer [Leaf] :as leaf]
   [me.tonsky.persistent-sorted-set.branch :refer [Branch] :as branch]))

(def ^:dynamic *debug* false)

(defn dbg [& args]
  (when *debug*
    (apply println args)))

(defn gen-addr [] (random-uuid))

(def *stats
  (atom
    {:reads 0
     :writes 0
     :accessed 0}))

(defn branch? [node] (instance? Branch node))
(defn leaf? [node] (instance? Leaf node))

(defrecord Storage [*memory *disk]
  IStorage
  (store [_ node opts]
    (assert (not (false? (:sync? opts))))
    (dbg "store<" (type node) ">")
    (swap! *stats update :writes inc)
    (let [address (gen-addr)]
      (swap! *disk assoc address
             (pr-str
              {:level     (.-shift node) ;;<------------------------------------TODO FIX ME
               :keys      (.-keys node)
               :addresses (when (branch? node) (.-addresses node))}))
      address))
  (restore [_ address opts]
    (assert (not (false? (:sync? opts))))
    (or
     (@*memory address)
     (let [{:keys [keys addresses level]} (edn/read-string (@*disk address))
           node (if addresses
                  (let [n (Branch. keys nil addresses)]
                    (set! (.-level n) level) ;;<--------------------------------TODO FIX ME
                    n)
                  (Leaf. keys))]
       (dbg "restored<" (type node) ">")
       (swap! *stats update :reads inc)
       (swap! *memory assoc address node)
       node)))
  (accessed [_ address] (swap! *stats update :accessed inc) nil))

(defn storage
  ([] (->Storage (atom {}) (atom {})))
  ([*disk] (->Storage (atom {}) *disk))
  ([*memory *disk] (->Storage *memory *disk)))

#!------------------------------------------------------------------------------

(defrecord AsyncStorage [*memory *disk]
  IStorage
  (store [_ node opts]
    (assert (false? (:sync? opts)))
    (dbg "store<" (type node) ">")
    (swap! *stats update :writes inc)
    (let [address (gen-addr)]
      (swap! *disk assoc address
            (pr-str
             {:keys      (.-keys node)
              :addresses (when (branch? node) (.-addresses node))}))
    (async address)))
  (restore [_ address opts]
    (assert (false? (:sync? opts)))
    (async
     (or
      (@*memory address)
      (let [{:keys [keys addresses level]} (edn/read-string (@*disk address))
            node (if addresses
                   (Branch. keys nil addresses)
                   (Leaf. keys))]
        (dbg "restored<" (type node) ">")
        (swap! *stats update :reads inc)
        (swap! *memory assoc address node)
        node))))
  (accessed [_ address] (swap! *stats update :accessed inc) nil))

(defn async-storage
  ([] (->AsyncStorage (atom {}) (atom {})))
  ([*disk] (->AsyncStorage (atom {}) *disk))
  ([*memory *disk] (->AsyncStorage *memory *disk)))

#!------------------------------------------------------------------------------

(defn root [set] (.-root set))

(defn addresses [node] (some->> (.-addresses node) (filter some?)))

(defn children [node] (some->> (.-children node) (filter some?)))

(defn ks [node] (some->> (.-keys node) (filterv some?)))


(deftest branch-steps-test
  "steps from full leaf to branch and back down"
  (let [og ^PersistentSortedSet (into (set/sorted-set* {}) (range 0 32))]
    (and
     (is (= 32 (count og)))
     (is (leaf? (.-root og))) ;; full leaf at root
     (is (= 32 (count (ks (.-root og)))))
     (is (= (range 0 32) (ks (.-root og))))
     (let [og' (conj og 32)] ;; split first leaf
       (and
        (is (= 33 (count og')))
        (is (branch? (.-root og')))
        (is (= 2 (count (ks (.-root og')))))
        (is (= [15 32] (ks (.-root og'))))
        (is (= 2 (count (children (.-root og')))))
        (is (= 16 (count (ks (nth (children (.-root og')) 0)))))
        (is (= (range 0 16) (ks (nth (children (.-root og')) 0))))
        (is (= 17 (count (ks (nth (children (.-root og')) 1)))))
        (is (= (range 16 33) (ks (nth (children (.-root og')) 1))))
        (let [og'' (conj og' 33)] ;; first add after split
          (and
           (is (= 34 (count og'')))
           (is (branch? (.-root og'')))
           (is (= 2 (count (ks (.-root og'')))))
           (is (= [15 33] (ks (.-root og''))))
           (is (= 2 (count (children (.-root og'')))))
           (is (= 16 (count (ks (nth (children (.-root og'')) 0)))))
           (is (= (range 0 16) (ks (nth (children (.-root og'')) 0))))
           (is (= 18 (count (ks (nth (children (.-root og'')) 1)))))
           (is (= (range 16 34) (ks (nth (children (.-root og'')) 1))))
           (let [og''- (disj og'' 33)]
             (and
              (is (= 33 (count og''-)))
              (is (branch? (.-root og''-)))
              (is (= [15 32] (ks (.-root og''-))))
              (is (= 16 (count (ks (nth (children (.-root og''-)) 0)))))
              (is (= (range 0 16) (ks (nth (children (.-root og''-)) 0))))
              (is (= 17 (count (ks (nth (children (.-root og''-)) 1)))))
              (is (= (range 16 33) (ks (nth (children (.-root og''-)) 1))))
              (let [og''-- (disj og''- 32)]
                (and
                 (is (= 32 (count og''--)))
                 ;; jvm version does not shrink here, not sure we should care
                 #_(is (branch? (.-root og''--)))
                 #_(is (= (range 0 16) (ks (nth (children (.-root og''--)) 0))))
                 #_(is (= (range 16 32) (ks (nth (children (.-root og''--)) 1)))))))))))))))

(deftest ascending-insert-storage-test
  (and
   (testing "one item"
     (let [_(reset! *stats {:reads 0 :writes 0 :accessed 0})
           original (conj (set/sorted-set* {}) 0)]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (contains? original 0))
        (is (leaf? (.-root original)))
        (is (nil? (.-address original)))
        (let [storage (storage)
              address (set/store original storage)]
          (and
           (is (uuid? address))
           (is (= address (.-address original)))
           (is (= 1 (:writes @*stats)))
           (is (= 0 (:reads @*stats)))
           (testing "restoring one item"
             (let [restored (set/restore address storage {})]
               (and
                (is (= 0 (:reads @*stats)) "nothing should have happened yet")
                (is (contains? restored 0))
                (is (= 1 (:reads @*stats)))
                (is (= 1 (count restored)) "restored has 1 item")
                (is (= restored original)  "restored is equiv")
                (is (leaf? (.-root restored)))
                (is (= 1 (:reads @*stats)))))))))))
   (testing "32"
     (let [_(reset! *stats {:reads 0 :writes 0 :accessed 0})
           original (into (set/sorted-set* {}) (range 0 32))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (leaf? (.-root original)))
        (is (= 32 (count (.-keys (.-root original)))))
        (is (nil? (.-address original)))
        (let [storage  (storage)
              address (set/store original storage)]
          (and
           (is (uuid? address))
           (is (= address (.-address original)))
           (is (= 1 (:writes @*stats)))
           (is (= 0 (:reads @*stats)))
           (let [restored (set/restore address storage {})]
             (and
              (is (= restored original))
              (is (leaf? (.-root restored)))
              (is (= 32 (count (.-keys (.-root restored)))))
              (is (= 1 (:reads @*stats))))))))))
   (testing "32 + 1"
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     (let [original (into (set/sorted-set* {}) (range 0 33))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (branch? (.-root original)))
        (is (= 2 (count (children (.-root original)))))
        (is (= 2 (count (ks (.-root original)))))
        (is (every? leaf? (children (.-root original))))
        (is (= [15 32] (ks (.-root original))))
        (is (nil? (addresses (root original))) "we have not called store() yet")
        (is (nil? (.-address original)))
        (let [storage (storage)
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
              (is (= 2 (count (children (.-root restored)))))
              (is (= 2 (count (ks (.-root restored)))))
              (is (every? leaf? (children (.-root restored))))
              (is (= [15 32] (ks (.-root restored)))))))))))
   (testing "32^2"
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     (let [original (into (set/sorted-set* {}) (range 0 1024))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (contains? original 0))
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (contains? original 1023))
        (is (branch? (.-root original)))
        (is (= 3 (count (children (.-root original)))))
        (is (every? branch? (children (.-root original))))
        (is (= [255 511 1023] (ks (.-root original))))
        (is (nil? (.-address original)))
        (let [storage (storage)
              address (set/store original storage)]
          (and
           (is (uuid? address))
           (is (= address (.-address original)))
           (is (= 67 (:writes @*stats)))
           (is (= 0 (:reads @*stats)))
           (is (empty? (deref (:*memory storage))))
           (let [restored (set/restore address storage {})]
             (and
              (is (empty? (deref (:*memory storage))))
              (is (= 0 (:reads @*stats)))
              (is (= restored original))
              (is (= 67 (:reads @*stats)))
              (is (contains? restored 0))
              (is (contains? restored 1023))
              (is (= 3 (count (children (.-root restored)))))
              (is (every? branch? (children (.-root restored))))
              (is (= 3 (count (ks (.-root original)))))
              (is (= [255 511 1023] (ks (.-root original)))))))))))
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
        (is (branch? (.-root original)))
        (is (= 15 (count (ks (.-root original)))))
        (is (= 15 (count (children (.-root original)))))
        (is (every? branch? (children (.-root original))))
        (is (= expected-root-keys (ks (.-root original))))
        (is (nil? (addresses (root original))) "we have not called store() yet")
        (is (nil? (set/walk-addresses original (fn [_] (swap! *original inc)))))
        (is (zero? @*original))
        (is (nil? (.-address original)))
        (let [storage  (storage)
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
           (let [restored (set/restore address storage {})
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
              (is (= 15 (count (ks (.-root original)))))
              (is (= expected-root-keys (ks (.-root original))))
              (is (= 15 (count (children (.-root restored)))))
              (is (every? branch? (children (.-root restored)))))))))))))

(defn do-async-ascending-insert-test []
  (async
   (and
    (testing "one full leaf"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [original (await (set/async-into (set/sorted-set* {}) (range 0 32)))]
        (and
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (leaf? (.-root original)))
         (is (nil? (.-address original)))
         (let [storage  (async-storage)
               address (await (set/store original storage {:sync? false}))]
           (and
            (is (uuid? address))
            (is (= address (.-address original)))
            (is (= 1 (:writes @*stats)))
            (is (= 0 (:reads @*stats)))
            (let [restored (set/restore address storage {})]
              (and
               (is (= 0 (:reads @*stats)))
               (is (true? (await (set/equivalent? restored original {:sync? false}))))
               (is (= 1 (:reads @*stats)))
               (is (leaf? (.-root restored))))))))))
    (testing "full-leaf + 1"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [original (await (set/async-into (set/sorted-set* {}) (range 0 33)))]
        (and
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (branch? (.-root original)))
         (is (nil? (.-address original)))
         (let [children (children (.-root original))]
           (and
            (is (= 2 (count children)))
            (is (leaf? (nth children 0)))
            (is (= 16 (count (.-keys (nth children 0)))))
            (is (leaf? (nth children 1)))
            (is (= 17 (count (.-keys (nth children 1)))))))
         (let [storage (async-storage)
               address (await (set/store original storage {:sync? false}))]
           (and
            (is (uuid? address))
            (is (= address (.-address original)))
            (is (= 3 (:writes @*stats)))
            (is (= 0 (:reads @*stats)))
            (let [restored (set/restore address storage {})]
              (and
               (is (= 0 (:reads @*stats)))
               (is (true? (await (set/equivalent? restored original {:sync? false}))))
               (is (= 3 (:reads @*stats)))
               (is (= 2 (count (children (.-root restored)))))
               (is (every? leaf? (children (.-root restored))))
               (is (= 16 (count (ks (nth (children (.-root restored)) 0)))))
               (is (= 17 (count (ks (nth (children (.-root restored)) 1))))))))))))
    (testing "32^4"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [expected-root-keys [65535 131071 196607 262143 327679 393215 458751 524287 589823 655359 720895 786431 851967 917503 1048575]
            original (await (set/async-into (set/sorted-set* {}) (range 0 (Math/pow 32 4))))
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
         (is (branch? (.-root original)))
         (is (= 15 (count (ks (.-root original)))))
         (is (= expected-root-keys (ks (.-root original))))
         (is (= 15 (count (children (.-root original)))))
         (is (every? branch? (children (.-root original))))
         (let [storage (async-storage)
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
            (let [restored (set/restore address storage)
                  *restored (atom 0)]
              (and
               (is (empty? (deref (:*memory storage))))
               (is (= 0 (:reads @*stats)))
               (is (true? (await (set/equivalent? restored original {:sync? false}))))
               (is (= 69901 (:reads @*stats)))
               (is (nil? (await (set/walk-addresses restored (fn [_] (swap! *restored inc)) {:sync? false}))))
               (is (= 69901 @*restored))
               (is (= (int (Math/pow 32 4)) (count restored)) "can call sync when fully realized")
               (is (= 69901 (count (deref (:*memory storage)))))
               (is (= 69901 (:reads @*stats)))
               (is (= 15 (count (children (.-root restored)))))
               (is (= 15 (count (ks (.-root original)))))
               (is (every? branch? (children (.-root restored))))
               (is (= expected-root-keys (ks (.-root original))))))))))))))

(deftest async-ascending-insert-test
  (test/async done
    (run (do-async-ascending-insert-test)
      (fn [ok] (done))
      (fn [err]
        (js/console.warn "async-ascending-insert-test failed")
        (is (nil? err))
        (js/console.error err)
        (done)))))
