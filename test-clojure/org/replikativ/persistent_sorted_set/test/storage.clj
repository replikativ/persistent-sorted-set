(ns org.replikativ.persistent-sorted-set.test.storage
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [clojure.test :as t :refer [is are deftest testing]]
   [org.replikativ.persistent-sorted-set :as set])
  (:import
   [clojure.lang RT]
   [java.lang.ref Reference]
   [java.util Comparator Arrays]
   [org.replikativ.persistent_sorted_set ANode ArrayUtil Branch IStorage Leaf PersistentSortedSet Settings Slot]))

(set! *warn-on-reflection* true)

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
    (swap! *stats update :writes inc)
    (let [node    ^ANode node
          address (gen-addr)
          ;; diff-buf: per-child buffered diffs, only present when diffBufSize>0 (else nil ⇒
          ;; the map is byte-identical to baseline, so diffBufSize=0 is unaffected — I0).
          slots   (when (instance? Branch node) (.slotsForStorage ^Branch node))
          m       {:level     (.level node)
                   :keys      (.keys node)
                   :addresses (when (instance? Branch node)
                                (.addresses ^Branch node))}]
      (swap! *disk assoc address (pr-str (if slots (assoc m :slots slots) m)))
      address))
  (accessed [_ address]
    (swap! *stats update :accessed inc)
    nil)
  (restore [_ address]
    (or
     (@*memory address)
     (let [{:keys [level
                   ^java.util.List keys
                   ^java.util.List addresses
                   slots]} (edn/read-string (@*disk address))
           node (if addresses
                  (Branch. (int level) ^java.util.List keys ^java.util.List addresses settings)
                  (Leaf. keys settings))]
       ;; diff-buf: reconstruct per-child buffered diffs into the Branch's slots (anchor = the
       ;; child's durable address), installed via installSlots (one volatile publish).
       ;; Branch.child projects them on descent (M5). Absent when diffBufSize=0.
       (when (and slots (instance? Branch node))
         (let [^Branch b node
               arr (object-array (alength (.-_keys b)))]
           (doseq [[idx entry] slots]
             (aset arr (int idx)
                   (Slot. (:diff entry) (long (:count entry)) (:measure entry) (nth addresses (int idx)))))
           (.installSlots b arr Branch/BUF_LAZY)))
       (swap! *stats update :reads inc)
       (swap! *memory assoc address node)
       node)))
  (markFreed [_ address]
    nil)
  (isFreed [_ address]
    false)
  (freedInfo [_ address]
    nil))

(defn storage
  (^IStorage []
   (->Storage (atom {}) (atom {}) (Settings.)))
  (^IStorage [*disk]
   (->Storage (atom {}) *disk (Settings.)))
  (^IStorage [*memory *disk]
   (->Storage *memory *disk (Settings.))))

(defn storage-with-settings
  "Create storage that uses the given Settings for node creation"
  ^IStorage [^Settings settings]
  (->Storage (atom {}) (atom {}) settings))

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
                 (or (.addressArray node) (repeat len nil))
                 (or (.childrenArray node) (repeat len nil)))
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
            (.addressArray ^Branch node)
            (.childrenArray ^Branch node))
           (reduce + 0))
          len)))))

(deftest test-lazy-remove
  "Check that invalidating middle branch does not invalidates siblings"
  (let [size 7000 ;; 3-4 branches
        xs   (shuffle (range size))
        set  (into (set/sorted-set* {:branching-factor 64}) xs)]
    (set/store set (storage))
    (is (= 1.0 (durable-ratio set))
        (let [set' (disj set 3500)] ;; one of the middle branches
          (is (< 0.98 (durable-ratio set')))))))

(defmacro dobatches [[sym coll] & body]
  `(loop [coll# ~coll]
     (when (seq coll#)
       (let [batch# (rand-nth [1 2 3 4 5 10 20 30 40 50 100])
             [~sym tail#] (split-at batch# coll#)]
         ~@body
         (recur tail#)))))

(deftest stresstest-stable-addresses
  ;; diff-buf: this asserts a BASELINE structural-sharing invariant — that the durable
  ;; node at each address byte-matches the in-memory child. Under diff-buffering a buffered
  ;; child's address re-points to its (pre-diff) ANCHOR while the diff lives in the parent's
  ;; slot, so the on-disk node intentionally differs from the post-diff child. Pin diff-buf
  ;; OFF here; the address/anchor semantics under buffering are covered by the round-trip
  ;; and generative content tests.
  (let [size      10000
        adds      (shuffle (range size))
        removes   (shuffle adds)
        *set      (atom (set/sorted-set* {:diff-buf-size 0}))
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
                                 :let [addr   (nth (.addressArray node) i)
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
                                      (take (.len ^Branch child) (.addressArray ^Branch child))))))
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
      (reset! *set (into (set/sorted-set* {:diff-buf-size 0}) adds))
      (set/store @*set storage)
      (dobatches [xs removes]
                 (let [set' (swap! *set #(reduce disj % xs))]
                   (invariant set'))))))

(deftest test-walk
  (let [size    1000000
        xs      (shuffle (range size))
        set     (into (set/sorted-set* {:branching-factor 64}) xs)
        *stored (atom 0)]
    (set/walk-addresses set
                        (fn [addr]
                          (is (nil? addr))))
    (set/store set (storage))
    (set/walk-addresses set
                        (fn [addr]
                          (is (some? addr))
                          (swap! *stored inc)))
    (let [set'     (conj set (* 2 size))
          *stored' (atom 0)]
      (set/walk-addresses set'
                          (fn [addr]
                            (if (some? addr)
                              (swap! *stored' inc))))
      (is (= (- @*stored 4) @*stored')))))

(deftest test-lazyness
  ;; diff-buf: asserts BASELINE lazy-loading invariants (loaded-ratio after partial
  ;; traversal; conj into an already-loaded area loads no extra nodes). Diff-buffering
  ;; changes which nodes are touched/written on conj (buffering + projection), so these
  ;; exact loaded-ratio equalities are a baseline property — pin diff-buf OFF here.
  (let [size       1000000
        xs         (shuffle (range size))
        rm         (vec (repeatedly (quot size 5) #(rand-nth xs)))
        original   (-> (reduce disj (into (set/sorted-set* {:branching-factor 64 :diff-buf-size 0}) xs) rm)
                       (disj (quot size 4) (quot size 2)))
        ;; storage reconstructs restored nodes with ITS settings — pin diff-buf 0 too so a
        ;; later conj on the restored set stays baseline (no ĝ child-load) for loaded-ratio.
        storage    (storage-with-settings (Settings. (int 64) nil nil nil (int 0)))
        address    (with-stats
                     (set/store original storage))
        _          (is (= 0 (:reads @*stats)))
        ; _          (is (> (:writes @*stats) (/ size PersistentSortedSet/MAX_LEN)))
        loaded     (set/restore address storage {:branching-factor 64 :diff-buf-size 0})
        _          (is (= 0 (:reads @*stats)))
        _          (is (= 0.0 (loaded-ratio loaded)))
        _          (is (= 1.0 (durable-ratio loaded)))

        ; touch first 100
        _       (is (= (take 100 loaded) (take 100 original)))
        _       (is (<= 5 (:reads @*stats) 7))
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
                       (vec (set/slice loaded from 1000000))))
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
        xs      (range -10000 0)
        loaded' (into loaded xs)
        _       (is (every? loaded' xs))
        _       (is (< ltail (loaded-ratio loaded')))
        _       (is (< (durable-ratio loaded') 1.0))

        ; incremental persist
        _       (with-stats
                  (set/store loaded' storage))
        _       (is (< (:writes @*stats) 350)) ;; ~ 10000 / 32 + 10000 / 32 / 32 + 1
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
