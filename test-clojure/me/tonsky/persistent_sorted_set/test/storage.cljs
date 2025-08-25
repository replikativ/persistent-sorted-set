(ns me.tonsky.persistent-sorted-set.test.storage
  (:require
   [cljs.test :as t :refer [is are deftest testing]]
   [clojure.edn :as edn]
   [clojure.string :as str]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.protocols :refer [IStorage] :as impl]
   [me.tonsky.persistent-sorted-set.leaf :refer [Leaf] :as leaf]
   [me.tonsky.persistent-sorted-set.node :refer [Node] :as node]))

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

(defrecord Storage [*memory *disk]
  IStorage
  (store [_ node opts]
    (dbg "store<" (type node) ">")
    (swap! *stats update :writes inc)
    (let [address (gen-addr)]
      (swap! *disk assoc address
             (pr-str
              {:level     (.-shift node) ;;<------------------------------------TODO FIX ME
               :keys      (.-keys node)
               :addresses (when (instance? Node node) (.-addresses node))}))
      address))
  (restore [_ address _opts]
    (or
     (@*memory address)
     (let [{:keys [keys addresses level]} (edn/read-string (@*disk address))
           node (if addresses
                  (let [n (Node. keys nil addresses nil)]
                    (set! (.-level n) level) ;;<--------------------------------TODO FIX ME
                    n)
                  (Leaf. keys nil))]
       (dbg "restored<" (type node) ">")
       (swap! *stats update :reads inc)
       (swap! *memory assoc address node)
       node)))
  (accessed [_ address]
    (swap! *stats update :accessed inc)
    nil)
  (delete [this addresses] (throw (js/Error. "unimplemented"))))

(defn storage
  ([] (->Storage (atom {}) (atom {})))
  ([*disk] (->Storage (atom {}) *disk))
  ([*memory *disk] (->Storage *memory *disk)))

(defn roundtrip [set]
  (let [storage (storage)
        address (set/store set storage)]
    (set/restore address storage)))

(defn children [node] (some->> (.-children node) (filter some?)))

(defn ks [node] (some->> (.-keys node) (filterv some?)))

(defn node? [o] (instance? Node o))

(deftest ascending-insert-test
  (and
   (testing "one item"
     (let [_(reset! *stats {:reads 0 :writes 0 :accessed 0})
           original (conj (set/sorted-set* {}) 0)]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (contains? original 0))
        (is (instance? Leaf (.-root original)))
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
                (is (instance? Leaf (.-root restored)))
                (is (= 1 (:reads @*stats)))))))))))
   (testing "one full leaf"
     (let [_(reset! *stats {:reads 0 :writes 0 :accessed 0})
           original (into (set/sorted-set* {}) (range 0 32))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (instance? Leaf (.-root original)))
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
              (is (instance? Leaf (.-root restored)))
              (is (= 1 (:reads @*stats))))))))))
   (testing "full-leaf + 1"
     (let [_(reset! *stats {:reads 0 :writes 0 :accessed 0})
           original (into (set/sorted-set* {}) (range 0 33))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (instance? Node (.-root original)))
        (let [children (children (.-root original))]
          (and
           (is (= 2 (count children)))
           (is (instance? Leaf (nth children 0)))
           (is (= 16 (count (.-keys (nth children 0)))))
           (is (instance? Leaf (nth children 1)))
           (is (= 17 (count (.-keys (nth children 1)))))))
        (is (nil? (.-address original)))
        (let [storage (storage)
              address (set/store original storage)]
          (and
           (is (uuid? address))
           (is (= address (.-address original)))
           (is (= 3 (:writes @*stats)))
           (is (= 0 (:reads @*stats)))
           (let [restored (set/restore address storage {})]
             (and
              (is (= restored original))
              (let [children (children (.-root restored))]
                (and
                 (is (= 2 (count children)))
                 (is (instance? Leaf (nth children 0)))
                 (is (= 16 (count (.-keys (nth children 0)))))
                 (is (instance? Leaf (nth children 1)))
                 (is (= 17 (count (.-keys (nth children 1)))))))
              (is (= 3 (:reads @*stats))))))))))
   (testing "32^2"
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     (let [original  (into (set/sorted-set* {}) (range 0 1024))]
       (and
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (contains? original 0))
        (is (= 0 (:writes @*stats)))
        (is (= 0 (:reads @*stats)))
        (is (contains? original 1023))
        (is (instance? Node (.-root original)))
        (let [cs (children (.-root original))
              root-keys (ks (.-root original))]
          (and
           (is (= 3 (count cs)))
           (is (every? node? cs))
           (is (= [255 511 1023] root-keys))
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
                 (let [cs (children (.-root restored))
                       root-keys (ks (.-root original))]
                   (and
                    (is (= 3 (count cs)))
                    (is (= 3 (count root-keys)))
                    (is (every? node? cs))
                    (is (= [255 511 1023] root-keys)))))))))))))))

;;; uses ensure-root
;; slice
;; rslice
;; contains
;; conj
;; disj
;; count
;; walk-addresses


; (deftest sync-storage-test
;   (testing "Full integration test with sync storage - 10k elements"
;     (let [storage (utils/make-sync-storage)
;           s0 (set/sorted-set* {:storage storage})
;           n 10000
;           nums (shuffle (range n))
;           s-final (reduce set/conj s0 nums)]
;       (and
;        (is (= n (count s-final)))
;        (is (= 0 (first s-final)))
;        (is (= 9999 (last s-final)))
;        (testing-group "slicing on original set"
;                       (let [slice (set/slice s-final 2500 2525)]
;                         (is (= (range 2500 2526) (vec slice)))))
;        (testing-group "reverse slicing: from key-from to key-to"
;                       (let [rslice (set/rslice s-final 7525 7500)]
;                         (and
;                          (is (some? rslice))
;                          (is (= (reverse (range 7500 7526)) (vec rslice))))))
;        (testing-group "Store to storage"
;                       (let [store-info (set/store s-final {:sync? true})]
;                         (and
;                          (is (some? store-info))
;                          (is (map? store-info))
;                          (is (:root-address store-info))
;                          (and
;                           (testing  "Check storage size"
;                             (let [storage-data @(:*store storage)]
;                               (is (> 100 (count storage-data)) "Should have many nodes stored")))
;                           (testing "Restore from storage"
;                             (let [restored (set/restore store-info storage {:sync? true})]
;                               (and
;                                (is (= n (count restored)))
;                                (is (= 0 (first restored)))
;                                (is (= 9999 (last restored)))
;                                (testing "Check a sample of elements using contains?"
;                                  (loop [indices (range 0 10000 1000)]
;                                    (let [i (first indices)]
;                                      (if (nil? i)
;                                        true
;                                        (if-not (is (contains? restored i) (str "Missing element: " i))
;                                          false
;                                          (recur (rest indices)))))))
;                                (testing "Verify slices on restored set"
;                                  (and
;                                   (is (= (range 100) (vec (set/slice restored 0 99))))
;                                   (is (= (range 5000 5100) (vec (set/slice restored 5000 5099))))
;                                   (is (= (range 9900 10000) (vec (set/slice restored 9900 9999))))))
;                                (testing "conj on restored set"
;                                  (let [restored-with-new (set/conj restored 10000)]
;                                    (and
;                                     (is (= (inc n) (count restored-with-new)))
;                                     (is (contains? restored-with-new 10000)))))
;                                (testing "disj on restored set"
;                                  (let [restored-without (set/disj restored 5000)]
;                                    (and
;                                     (is (= (dec n) (count restored-without)))
;                                     (is (not (contains? restored-without 5000))))))
;                                (testing "iteration over restored set"
;                                  (and
;                                   (is (= (range 100) (take 100 restored)))
;                                   (is (= (range 9900 10000) (take 100 (drop 9900 restored)))))))))))))))))
