(ns me.tonsky.persistent-sorted-set.test.storage32
  (:require
    [clojure.edn :as edn]
    [clojure.string :as str]
    [clojure.test :as t :refer [is are deftest testing]]
    [me.tonsky.persistent-sorted-set :as set])
  (:import
    [clojure.lang RT]
    [java.lang.ref Reference]
    [java.util ArrayList Collections Comparator Arrays List Random]
    (java.util.random RandomGenerator)
    (me.tonsky.persistent_sorted_set ANode Branch Leaf PersistentSortedSet IStorage Settings)))

(set! *warn-on-reflection* true)

(def ^:dynamic *debug*
  false)

(defn dbg [& args]
  (when *debug*
    (apply println args)))

(defn gen-addr []
  (random-uuid)
  #_(str (str/join (repeatedly 10 #(rand-nth "ABCDEFGHIJKLMNOPQRSTUVWXYZ")))))

(def *stats
  (atom
    {:reads 0
     :writes 0
     :accessed 0}))

(defmacro with-stats [& body]
  `(do
     (reset! *stats {:reads 0 :writes 0 :accessed 0})
     ~@body))

(defrecord Storage [*memory *disk ^Settings settings]
  IStorage
  (store [_ node]
    (dbg "store<" (type node) ">")
    (swap! *stats update :writes inc)
    (let [node    ^ANode node
          address (gen-addr)]
      (swap! *disk assoc address
        (pr-str
          {:level     (.level node)
           :keys      (.keys node)
           :addresses (when (instance? Branch node)
                        (.addresses ^Branch node))}))
      address))
  (accessed [_ address]
    (swap! *stats update :accessed inc)
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
        (dbg "restored<" (type node) ">")
        (swap! *stats update :reads inc)
        (swap! *memory assoc address node)
        node))))

(defn storage
  (^IStorage []
   (->Storage (atom {}) (atom {}) (Settings.)))
  (^IStorage [*disk]
   (->Storage (atom {}) *disk (Settings.)))
  (^IStorage [*memory *disk]
   (->Storage *memory *disk (Settings.))))

(defn roundtrip [set]
  (let [storage (storage)
        address (set/store set storage)]
    (set/restore address storage)))

(defn loaded-ratio
  ([^PersistentSortedSet set]
   (let [storage (.-_storage set)
         address (.-_address set)
         root    (.-_root set)]
     (loaded-ratio (some-> storage :*memory deref) address root)))
  ([memory address node]
   (when *debug*
     (println address (contains? memory address) node (memory address)))
   (if (and address (not (contains? memory address)))
     0.0
     (let [node (if (instance? Reference node) (.get ^Reference node) node)
           node (or node (memory address))]
       (if (instance? Leaf node)
         1.0
         (let [node ^Branch node
               len (.len node)]
           (double
             (/ (->>
                  (mapv
                    (fn [_ child-addr child]
                      (loaded-ratio memory child-addr child))
                    (range len)
                    (or (.-_addresses node) (repeat len nil))
                    (or (.-_children node) (repeat len nil)))
                  (reduce + 0))
               len))))))))

(defn durable-ratio
  ([^PersistentSortedSet set]
   (double (durable-ratio (.-_address set) (.-_root set))))
  ([address ^ANode node]
   (cond
     (some? address)       1.0
     (instance? Leaf node) 0.0
     :else
     (let [len (.len node)]
       (/ (->>
            (map
              (fn [_ child-addr child]
                (durable-ratio child-addr child))
              (range len)
              (.-_addresses ^Branch node)
              (.-_children ^Branch node))
            (reduce + 0))
         len)))))

(deftest test-lazy-remove
  "Check that invalidating middle branch does not invalidates siblings"
  (let [size 7000 ;; 3-4 branches
        xs   (shuffle (range size))
        set  (into (set/sorted-set* {:branching-factor 32}) xs)]
    (set/store set (storage))
    (is (= 1.0 (durable-ratio set))
      (let [set' (disj set 3500)] ;; one of the middle branches
        (is (< 0.96 (durable-ratio set')))))))

(defmacro dobatches [[sym coll] & body]
  `(loop [coll# ~coll]
     (when (seq coll#)
       (let [batch# (rand-nth [1 2 3 4 5 10 20 30 40 50 100])
             [~sym tail#] (split-at batch# coll#)]
         ~@body
         (recur tail#)))))

(deftest stresstest-stable-addresses
  (let [size      10000
        adds      (shuffle (range size))
        removes   (shuffle adds)
        *set      (atom (set/sorted-set))
        *disk     (atom {})
        storage   (storage *disk)
        invariant (fn invariant
                    ([^PersistentSortedSet o]
                     (invariant (.root o) (some? (.-_address o))))
                    ([^ANode o stored?]
                     (condp instance? o
                       Branch
                       (let [node ^Branch o
                             len (.len node)]
                         (doseq [i (range len)
                                 :let [addr   (nth (.-_addresses node) i)
                                       child  (.child node storage (int i))
                                       {:keys [keys addresses]} (edn/read-string (@*disk addr))]]
                           ;; nodes inside stored? has to ALL be stored
                           (when stored?
                             (is (some? addr)))
                           (when (some? addr)
                             (is (= keys
                                   (take (.len ^ANode child) (.-_keys ^ANode child))))
                             (is (= addresses
                                   (when (instance? Branch child)
                                     (take (.len ^Branch child) (.-_addresses ^Branch child))))))
                           (invariant child (some? addr))))
                       Leaf
                       true)))]
    (testing "Persist after each"
      (dobatches [xs adds]
        (let [set' (swap! *set into xs)]
          (invariant set')
          (set/store set' storage)))

      (invariant @*set)

      (dobatches [xs removes]
        (let [set' (swap! *set #(reduce disj % xs))]
          (invariant set')
          (set/store set' storage))))

    (testing "Persist once"
      (reset! *set (into (set/sorted-set) adds))
      (set/store @*set storage)
      (dobatches [xs removes]
        (let [set' (swap! *set #(reduce disj % xs))]
          (invariant set'))))))

(deftest test-walk
  (let [size    1000
        xs      (range size)
        set     (into (set/sorted-set* {:branching-factor 32}) xs)
        *pre-store (atom 0)
        *stored (atom 0)]
    (and
     (is (nil? (set/walk-addresses set (fn [addr] (swap! *pre-store inc)))))
     (is (zero? @*pre-store))
     (is (uuid? (set/store set (storage))))
     (is (nil? (set/walk-addresses set (fn [addr] (swap! *stored inc)))))
     (is (= 66 @*stored))
     (let [set'     (conj set (* 2 size))
           *stored' (atom 0)]
       (and
        (is (nil? (set/walk-addresses set' (fn [addr] (if (some? addr) (swap! *stored' inc))))))
        (is (= 63 @*stored'))))
     (let [set'     (disj set (dec size))
           *stored' (atom 0)]
       (and
        (is (nil? (set/walk-addresses set' (fn [addr] (if (some? addr) (swap! *stored' inc))))))
        (is (= 63 @*stored')))))))

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

(deftest branch-steps-control
  (testing "32 steps"
    (let [og ^PersistentSortedSet (into (set/sorted-set* {:branching-factor 32}) (range 0 32))]
      (and
       ;; full leaf at root
       (is (= 32 (count og)))
       (is (leaf? (.-_root og)))
       (is (= 32 (count (ks (.-_root og)))))
       (is (= (range 0 32) (ks (.-_root og))))
       (let [og' ^PersistentSortedSet (conj og 32)]
         ;; split first leaf
         (and
          (is (= 33 (count og')))
          (is (branch? (.-_root og')))
          (is (= 2 (count (ks (.-_root og')))))
          (is (= [15 32] (ks (.-_root og'))))
          (is (= 2 (count (children (.-_root og')))))
          (is (= 16 (count (ks (nth (children (.-_root og')) 0)))))
          (is (= (range 0 16) (ks (nth (children (.-_root og')) 0))))
          (is (= 17 (count (ks (nth (children (.-_root og')) 1)))))
          (is (= (range 16 33) (ks (nth (children (.-_root og')) 1))))
          (let [og'' ^PersistentSortedSet (conj og' 33)]
            ;; first add after split
            (and
             (is (= 34 (count og'')))
             (is (branch? (.-_root og'')))
             (is (= 2 (count (ks (.-_root og'')))))
             (is (= [15 33] (ks (.-_root og''))))
             (is (= 2 (count (children (.-_root og'')))))
             (is (= 16 (count (ks (nth (children (.-_root og'')) 0)))))
             (is (= (range 0 16) (ks (nth (children (.-_root og'')) 0))))
             (is (= 18 (count (ks (nth (children (.-_root og'')) 1)))))
             (is (= (range 16 34) (ks (nth (children (.-_root og'')) 1))))
             (let [og''- ^PersistentSortedSet (disj og'' 33)]
               (and
                (is (= 33 (count og''-)))
                (is (branch? (.-_root og''-)))
                (is (= [15 32] (ks (.-_root og''-))))
                (is (= 16 (count (ks (nth (children (.-_root og''-)) 0)))))
                (is (= (range 0 16) (ks (nth (children (.-_root og''-)) 0))))
                (is (= 17 (count (ks (nth (children (.-_root og''-)) 1)))))
                (is (= (range 16 33) (ks (nth (children (.-_root og''-)) 1))))
                (let [og''-- ^PersistentSortedSet (disj og''- 32)]
                  (and
                   (is (branch? (.-_root og''--)))
                   (is (= 32 (count og''--)))
                   (is (= (range 0 16) (ks (nth (children (.-_root og''--)) 0))))
                   (is (= (range 16 32) (ks (nth (children (.-_root og''--)) 1))))))))))))))))

 (deftest ascending-insert-control
   (and
    (testing "one item"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [original ^PersistentSortedSet (conj (set/sorted-set* {:branching-factor 32}) 0)]
        (and
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (leaf? (root original)))
         (is (nil? (.-_address original)))
         (let [storage  (storage)
               address (set/store original storage)]
           (and
            (is (uuid? address))
            (is (= address (.-_address ^PersistentSortedSet original)))
            (is (= 1 (:writes @*stats)))
            (is (= 0 (:reads @*stats)))
            (let [restored ^PersistentSortedSet (set/restore address storage {:branching-factor 32})]
              (and
               (is (= restored original))
               (is (leaf? (unwrap (.-_root restored))))
               (is (= 1 (:reads @*stats))))))))))
    (testing "saturated root leaf"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [original ^PersistentSortedSet (into (set/sorted-set* {:branching-factor 32}) (range 0 32))
            root ^Leaf (unwrap (.-_root original))]
        (and
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (leaf? root))
         (is (= 32 (count (.-_keys root))))
         (is (nil? (.-_address original)))
         (let [storage  (storage)
               address (set/store original storage)]
           (and
            (is (uuid? address))
            (is (= address (.-_address ^PersistentSortedSet original)))
            (is (= 1 (:writes @*stats)))
            (is (= 0 (:reads @*stats)))
            (let [restored ^PersistentSortedSet (set/restore address storage {:branching-factor 32})]
              (and
               (is (= restored original))
               (is (leaf? (unwrap (.-_root restored))))
               (is (= 1 (:reads @*stats))))))))))
    (testing "saturated root leaf + 1"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [original ^PersistentSortedSet (into (set/sorted-set* {:branching-factor 32}) (range 0 33))]
        (and
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (branch? (.-_root original)))
         (is (= 2 (count (children (.-_root original)))))
         (is (= 2 (count (ks (.-_root original)))))
         (is (every? leaf? (children (.-_root original))))
         (is (= [15 32] (ks (.-_root original))))
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
               (is (= 2 (count (children (.-_root restored)))))
               (is (= 2 (count (ks (.-_root restored)))))
               (is (every? leaf? (children (.-_root restored))))
               (is (= [15 32] (ks (.-_root restored)))))))))))
    (testing "32^2"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [original ^PersistentSortedSet
            (into (set/sorted-set* {:branching-factor 32}) (range 0 1024))]
        (and
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (let [children (children (.-_root original))
               root-keys (ks (.-_root original))]
           (and
            (is (= 3 (count children)))
            (is (= 3 (count root-keys)))
            (is (every? #(instance? Branch %) children))
            (is (= [255 511 1023] root-keys))))
         (is (nil? (.-_address original)))
         (let [storage  (storage)
               address (set/store original storage)]
           (and
            (is (uuid? address))
            (is (= address (.-_address ^PersistentSortedSet original)))
            (is (= 67 (:writes @*stats)))
            (is (= 0 (:reads @*stats)))
            (is (empty? (deref (:*memory storage))))
            (let [restored ^PersistentSortedSet (set/restore address storage {:branching-factor 32})]
              (and
               (is (empty? (deref (:*memory storage))))
               (is (= 0 (:reads @*stats)))
               (is (= restored original))
               (is (= 67 (count (deref (:*memory storage)))))
               (is (= 67 (:reads @*stats)))
               (let [children (children (.-_root restored))
                     root-keys (ks (.-_root original))]
                 (and
                  (is (= 3 (count children)))
                  (is (= 3 (count root-keys)))
                  (is (every? branch? children))
                  (is (= [255 511 1023] root-keys)))))))))))
    (testing "32^4"
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (let [expected-root-keys [65535 131071 196607 262143 327679 393215 458751 524287 589823 655359 720895 786431 851967 917503 1048575]
            original ^PersistentSortedSet (into (set/sorted-set* {:branching-factor 32}) (range 0 (Math/pow 32 4)))
            *original (atom 0)]
        (and
         (is (= 0 (:writes @*stats)))
         (is (= 0 (:reads @*stats)))
         (is (= 15 (count (children (.-_root original)))))
         (is (= 15 (count (ks (.-_root original)))))
         (is (every? branch? (children (.-_root original))))
         (is (= [3 3 3 3 3 3 3 3 3 3 3 3 3 3 3] (mapv level (children (.-_root original)))))
         (is (= expected-root-keys (ks (.-_root original))))
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
               (is (= [3 3 3 3 3 3 3 3 3 3 3 3 3 3 3] (mapv level (children (.-_root restored)))))
               (is (= 15 (count (addresses (root restored)))) "restored root branch has those address it just read from")
               (is (= 15 (count (children (.-_root restored)))))
               (is (= 15 (count (ks (.-_root original)))))
               (is (every? branch? (children (.-_root restored))))
               (is (= expected-root-keys (ks (.-_root original)))))))))))))

(defn seeded-shuffle [coll ^Random rnd]
  (let [al (ArrayList. ^List coll)]
    (^[List RandomGenerator] Collections/shuffle al rnd)
    (vec al)))

(defn rand-nth-seeded [coll ^Random rnd]
  (nth coll (.nextInt rnd (count coll))))

(defn test-lazyness-impl [seed]
  (reset! *stats {:reads 0 :writes 0 :accessed 0})
  (let [rnd      (Random. seed)
        size     100000
        xs       (seeded-shuffle (range size) rnd)
        rm       (vec (repeatedly (quot size 5) #(rand-nth-seeded xs rnd)))
        original (-> (reduce disj (into (set/sorted-set* {:branching-factor 32}) xs) rm)
                     (disj (quot size 4) (quot size 2)))
        storage  (storage)
        _        (is (= 0 (:writes @*stats)))
        address  (with-stats (set/store original storage))
        _        (is (< 4000 (:writes @*stats) 4096))
        _        (is (= 0 (:reads @*stats)))
        loaded   (set/restore address storage {:branching-factor 32})
        _        (is (= 0 (:reads @*stats)))
        _        (is (= 0.0 (loaded-ratio loaded)))
        _        (is (= 1.0 (durable-ratio loaded)))

        ; touch first 100
        _       (is (= (take 100 loaded) (take 100 original)))
        _       (is (<= 5 (:reads @*stats) 10))
        l100    (loaded-ratio loaded)
        _       (is (< 0 l100 1.0))

        ; touch first 5000
        _       (is (= (take 5000 loaded) (take 5000 original)))
        l5000   (loaded-ratio loaded)
        _       (is (< l100 l5000 1.0))

        ; touch middle
        from    (- (quot size 2) (quot size 200))
        to      (+ (quot size 2) (quot size 200))
        _       (is (= (vec (set/slice loaded from to))
                       (vec (set/slice loaded from to))))
        lmiddle (loaded-ratio loaded)
        _       (is (< l5000 lmiddle 1.0))

        ; touch 100 last
        _       (is (= (take 100 (rseq loaded)) (take 100 (rseq original))))
        lrseq   (loaded-ratio loaded)
        _       (is (< lmiddle lrseq 1.0))

        ; touch 10000 last
        from    (- size (quot size 100))
        to      size
        _       (is (= (vec (set/slice loaded from to))
                       (vec (set/slice loaded from 100000))))
        ltail   (loaded-ratio loaded)
        _       (is (< lrseq ltail 1.0))

        ; conj to beginning
        loaded' (conj loaded -1)
        _       (is (= ltail (loaded-ratio loaded')))
        _       (is (< (durable-ratio loaded') 1.0))

        ; conj to middle
        loaded' (conj loaded (quot size 2))
        _       (is (= ltail (loaded-ratio loaded')))
        _       (is (< (durable-ratio loaded') 1.0))

        ; conj to end
        loaded' (conj loaded Long/MAX_VALUE)
        _       (is (= ltail (loaded-ratio loaded')))
        _       (is (< (durable-ratio loaded') 1.0))

        ; conj to untouched area
        loaded' (conj loaded (quot size 4))
        _       (is (< ltail (loaded-ratio loaded') 1.0))
        _       (is (< ltail (loaded-ratio loaded) 1.0))
        _       (is (< (durable-ratio loaded') 1.0))

        ; transients conj
        xs      (range -1000 0)
        loaded' (into loaded xs)
        _       (is (every? loaded' xs))
        _       (is (< ltail (loaded-ratio loaded')))
        _       (is (< (durable-ratio loaded') 1.0))

        ; incremental persist
        _       (with-stats
                  (set/store loaded' storage))
        _       (is (< (:writes @*stats) 80)) ;; ~ 1000 / 32 + 1000 / 32 / 32 + 1
        _       (is (= 1.0 (durable-ratio loaded')))

        ; transient disj
        xs      (take 100 loaded)
        loaded' (reduce disj loaded xs)
        _       (is (every? #(not (loaded' %)) xs))
        _       (is (< (durable-ratio loaded') 1.0))

        ; count fetches everything
        _       (is (= (count loaded) (count original)))
        l0      (loaded-ratio loaded)
        _       (is (= 1.0 l0))]))

(deftest test-lazyness
  (let [seed (System/currentTimeMillis)]
    (println (str "Running test-lazyness with seed: " seed))
    (try
      (reset! *stats {:reads 0 :writes 0 :accessed 0})
      (test-lazyness-impl seed)
      (catch Exception e
        (println (str "FAIL: test-lazyness failed with seed: " seed))
        (throw e)))))

(comment
  (dotimes [_ 20] (test-lazyness))
  )