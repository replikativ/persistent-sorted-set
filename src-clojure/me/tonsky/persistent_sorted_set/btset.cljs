(ns me.tonsky.persistent-sorted-set.btset
  (:refer-clojure :exclude [iter sorted-set-by])
  (:require-macros [me.tonsky.persistent-sorted-set.macros :refer [async+sync]])
  (:require [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [is.simm.partial-cps.sequence :as aseq]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [me.tonsky.persistent-sorted-set.branch :as branch :refer [Branch]]
            [me.tonsky.persistent-sorted-set.leaf :as leaf :refer [Leaf]]
            [me.tonsky.persistent-sorted-set.impl.node :as node]
            [me.tonsky.persistent-sorted-set.impl.measure :as measure]
            [me.tonsky.persistent-sorted-set.impl.storage :as storage]
            [me.tonsky.persistent-sorted-set.util :refer [rotate lookup-exact splice cut-n-splice binary-search-l binary-search-r return-array merge-n-split check-n-splice]]))

(declare BTSet)

(def ^:const UNINITIALIZED_HASH nil)

(defn- $$root
  [^BTSet set {:keys [sync?] :or {sync? true} :as opts}]
  (assert (or (some? (.-address set)) (some? (.-root set))))
  (async+sync sync?
              (async
               (do
                 (when (and (nil? (.-root set)) (some? (.-address set)))
                   (assert (implements? storage/IStorage (.-storage set)))
                   (set! (.-root set) (await (storage/restore (.-storage set) (.-address set) opts)))))
               (.-root set))))

(defn $count
  [^BTSet set {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (do
                 (when (neg? (.-cnt set))
                   (let [root (await ($$root set opts))]
                     (set! (.-cnt set) (await (node/$count root (.-storage set) opts)))))
                 (.-cnt set)))))

(defn $contains?
  [^BTSet set key {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root (await ($$root set opts))]
                 (await (node/$contains? root (.-storage set) key (.-comparator set) opts))))))

(defn $conjoin
  ([^BTSet set key]
   ($conjoin set key (.-comparator set) {:sync? true}))
  ([^BTSet set key arg]
   (if (fn? arg)
     ($conjoin set key arg {:sync? true})
     ($conjoin set key (.-comparator set) arg)))
  ([^BTSet set key cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (let [root  (await ($$root set opts))
                      roots (await (node/$add root (.-storage set) key cmp opts))]
                  (if (nil? roots)
                    set
                    (do
                      ;; Mark old root address as freed if it exists
                      (when (and (.-storage set) (.-address set))
                        (storage/markFreed (.-storage set) (.-address set)))
                      ;; If count is unknown (-1), keep it unknown; will be computed lazily when needed
                      (let [current-cnt (.-cnt set)
                            new-cnt (if (neg? current-cnt) -1 (inc current-cnt))]
                        (if (== (arrays/alength roots) 1)
                          (BTSet. (arrays/aget roots 0)
                                  new-cnt
                                  (.-comparator set)
                                  (.-meta set)
                                  UNINITIALIZED_HASH
                                  (.-storage set)
                                  nil
                                  (.-settings set))
                          (let [child0 (arrays/aget roots 0)
                                lvl    (inc (node/level child0))
                                ;; Compute subtree count; propagate -1 (unknown) if any child is unknown
                                child-counts (map node/$subtree-count roots)
                                subtree-count (if (every? #(>= % 0) child-counts)
                                                (reduce + 0 child-counts)
                                                -1)
                                ;; Compute measure from children if measure-ops available
                                measure-ops (:measure (.-settings set))
                                ;; Only compute root-measure when all children have measure;
                                ;; otherwise keep nil to allow lazy recomputation
                                child-measure (when measure-ops (map node/$measure roots))
                                root-measure (when (and measure-ops (every? some? child-measure))
                                               (reduce (fn [acc cs]
                                                         (measure/merge-measure measure-ops acc cs))
                                                       (measure/identity-measure measure-ops)
                                                       child-measure))]
                            (BTSet. (Branch. lvl (arrays/amap node/max-key roots) roots nil subtree-count root-measure (.-settings set))
                                    new-cnt
                                    (.-comparator set)
                                    (.-meta set)
                                    UNINITIALIZED_HASH
                                    (.-storage set)
                                    nil
                                    (.-settings set))))))))))))

(defn $replace
  ([^BTSet set old-key new-key]
   ($replace set old-key new-key (.-comparator set) {:sync? true}))
  ([^BTSet set old-key new-key arg]
   (if (fn? arg)
     ($replace set old-key new-key arg {:sync? true})
     ($replace set old-key new-key (.-comparator set) arg)))
  ([^BTSet set old-key new-key cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (let [root  (await ($$root set opts))
                      nodes (await (node/$replace root (.-storage set) old-key new-key cmp opts))]
                  (if (nil? nodes)
                    set
                    (do
                      ;; Mark old root address as freed if it exists
                      (when (and (.-storage set) (.-address set))
                        (storage/markFreed (.-storage set) (.-address set)))
                      (BTSet. (arrays/aget nodes 0)
                              (.-cnt set)
                              (.-comparator set)
                              (.-meta set)
                              UNINITIALIZED_HASH
                              (.-storage set)
                              nil
                              (.-settings set)))))))))

(defn $disjoin
  ([^BTSet set key]
   ($disjoin set key (.-comparator set) {:sync? true}))
  ([^BTSet set key arg]
   (if (fn? arg)
     ($disjoin set key arg {:sync? true})
     ($disjoin set key (.-comparator set) arg)))
  ([^BTSet set key cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (let [root (await ($$root set opts))
                      new-roots (await (node/$remove root (.-storage set) key nil nil cmp opts))]
                  (if (nil? new-roots)
                    set
                    (do
                      ;; Mark old root address as freed if it exists
                      (when (and (.-storage set) (.-address set))
                        (storage/markFreed (.-storage set) (.-address set)))
                      (let [new-root (arrays/aget new-roots 0)
                            new-root (if (and (instance? Branch new-root)
                                              (== 1 (arrays/alength (.-children new-root))))
                                       (await (branch/$child new-root (.-storage set) 0 opts))
                                       new-root)
                            ;; If count is unknown (-1), keep it unknown; will be computed lazily when needed
                            current-cnt (.-cnt set)
                            new-cnt (if (neg? current-cnt) -1 (dec current-cnt))]
                        (BTSet. new-root
                                new-cnt
                                (.-comparator set)
                                (.-meta set)
                                UNINITIALIZED_HASH
                                (.-storage set)
                                nil
                                (.-settings set))))))))))

(defn $store
  ([^BTSet set arg]
   (if (implements? storage/IStorage arg)
     ($store set arg {:sync? true})
     ($store set (.-storage set) arg)))
  ([^BTSet set storage {:keys [sync?] :or {sync? true} :as opts}]
   (assert (instance? BTSet set))
   (assert (implements? storage/IStorage storage) "BTSet/$store requires IStorage in second argument")
   (async+sync sync?
               (async
                (do
                  (set! (.-storage set) storage)
                  (when (nil? (.-address set))
                    (set! (.-address set) (await (node/$store (.-root set) storage opts))))
                  (.-address set))))))

(defn $walk-addresses
  [^BTSet set on-address {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (some? (.-address set))
                 (when (on-address (.-address set))
                   (await (node/$walk-addresses (await ($$root set opts))
                                                (.-storage set)
                                                on-address
                                                opts)))
                 (await (node/$walk-addresses (await ($$root set opts))
                                              (.-storage set)
                                              on-address
                                              opts))))))

(defn $lookup
  [^BTSet set key not-found {:keys [sync? comparator] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root   (await ($$root set opts))
                     cmp    (or comparator (.-comparator set))
                     result (await (node/$lookup root (.-storage set) key cmp opts))]
                 (if (some? result)
                   result
                   not-found)))))

(defn $measure
  "Get the aggregated statistics for the entire set."
  [^BTSet set {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root (await ($$root set opts))
                     measure-ops (:measure (.-settings set))]
                 (if (nil? measure-ops)
                   nil
                   (or (node/$measure root)
                       (await (node/force-compute-measure root (.-storage set) measure-ops opts))))))))

(defn restore
  [root-address-or-info storage opts]
  (let [;; Handle both old format (bare UUID) and new format (map with metadata)
        address (if (map? root-address-or-info)
                  (:root-address root-address-or-info)
                  root-address-or-info)
        _       (assert (some? address))
        meta    (or (and (map? root-address-or-info) (:meta root-address-or-info))
                    (:meta opts))
        cmp     (if (map? root-address-or-info)
                  (or (:comparator root-address-or-info) compare)
                  (or (:comparator opts) compare))
        settings (select-keys (merge (when (map? root-address-or-info) root-address-or-info) opts) [:branching-factor :measure])]
    (BTSet. nil -1 cmp meta UNINITIALIZED_HASH storage address settings)))

#!------------------------------------------------------------------------------

(def ^:const EMPTY_PATH (js* "0n"))

(defn- bits-per-level [set]
  (let [bf (get (.-settings set) :branching-factor)]
    (Math/ceil (Math/log2 bf))))

(defn- max-len [set]
  (get (.-settings set) :branching-factor))

(defn- min-len [set]
  (/ (max-len set) 2))

(defn- avg-len [set]
  (arrays/half (+ (max-len set) (min-len set))))

(defn- path-inc [path]
  (+ path (js* "1n")))

(defn- path-dec [path]
  (- path (js* "1n")))

(defn- path-cmp [path1 path2]
  (- path1 path2))

(defn- path-lt [path1 path2]
  (< path1 path2))

(defn- path-lte [path1 path2]
  (<= path1 path2))

(defn- path-eq [path1 path2]
  (== path1 path2))

(defn- path-get ^number [set path ^number level]
  (let [bpl (bits-per-level set)
        shift (js/BigInt (* level bpl))
        mask (js* "~{} - 1n" (bit-shift-left (js* "1n") (js/BigInt bpl)))]
    (js/Number (bit-and (bit-shift-right path shift) mask))))

(defn- path-set [set path ^number level ^number idx]
  (let [bpl   (bits-per-level set)
        shift (js/BigInt (* level bpl))
        mask  (js* "~{} - 1n" (bit-shift-left (js* "1n") (js/BigInt bpl)))
        old   (bit-and (bit-shift-right path shift) mask)]
    (-> path
        (- (bit-shift-left old shift))
        (+ (bit-shift-left (js/BigInt idx) shift)))))

(defn- rpath
  [set node path ^number level]
  (if (pos? level)
    (let [last-idx (dec (arrays/alength (.-children node)))]
      (recur
       set
       (arrays/aget (.-children node) last-idx)
       (path-set set path level last-idx)
       (dec level)))
    (path-set set path 0 (dec (arrays/alength (.-keys node))))))

(defn- $$rpath
  [set node path ^number level {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (pos? level)
                 (let [last-idx (dec (node/len node))
                       child-node (await (branch/$child node (.-storage set) last-idx opts))]
                   (await ($$rpath set child-node (path-set set path level last-idx) (dec level) opts)))
                 (path-set set path 0 (dec (arrays/alength (.-keys node))))))))

(defn- $$_next-path
  [set node path ^number level {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (some? node) (implements? node/INode node)))
  (async+sync sync?
              (async
               (let [idx (path-get set path level)]
                 (if (pos? level)
                   (let [child-node (await (branch/$child node (.-storage set) idx opts))
                         sub-path (await ($$_next-path set child-node path (dec level) opts))]
                     (if (nil? sub-path)
                       (if (< (inc idx) (arrays/alength (.-keys node)))
                         (path-set set EMPTY_PATH level (inc idx))
                         nil)
                       (path-set set sub-path level idx)))
                   (if (< (inc idx) (arrays/alength (.-keys node)))
                     (path-set set EMPTY_PATH 0 (inc idx)) ;; advance leaf idx
                     nil))))))

(defn- $$next-path
  "Returns path representing next item after `path` in natural traversal order.
   Will overflow at leaf if at the end of the tree"
  [set path {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (< path (js* "0n"))
                 EMPTY_PATH
                 (let [root (await ($$root set opts))
                       lvl  (node/level root)]
                   (or
                    (await ($$_next-path set root path lvl opts))
                    (path-inc (if (.-storage set)
                                (await ($$rpath set root EMPTY_PATH lvl opts))
                                (rpath set root EMPTY_PATH lvl)))))))))

(defn- $$_prev-path
  [set node path ^number level {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (some? node) (implements? node/INode node)))
  (async+sync sync?
              (async
               (let [idx (path-get set path level)]
                 (if (and (== 0 level) (== 0 idx))
                   nil ;; leaf overflow
                   (if (== 0 level)
                     (path-set set EMPTY_PATH 0 (dec idx)) ;; leaf
                     (if (>= idx (node/len node))
                       (if (.-storage set) ;; branch that was overflow before
                         (await ($$rpath set node path level opts))
                         (rpath set node path level))
                       (let [child-node (await (branch/$child node (.-storage set) idx opts))
                             path' (await ($$_prev-path set child-node path (dec level) opts))]
                         (if (some? path')
                           (path-set set path' level idx) ;; no sub-overflow, keep current idx
                           (if (== 0 idx)
                             nil ;; nested overflow + this node overflow
                             (let [;; nested overflow, advance current idx, reset subsequent indexes
                                   child-node (await (branch/$child node (.-storage set) (dec idx) opts))
                                   path' (if (.-storage set)
                                           (await ($$rpath set child-node path (dec level) opts))
                                           (rpath set child-node path (dec level)))]
                               (path-set set path' level (dec idx)))))))))))))

(defn- $$prev-path
  "Returns path representing previous item before `path` in natural traversal order.
   Will overflow at leaf if at beginning of tree"
  [set path {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root (await ($$root set opts))
                     lvl  (node/level root)]
                 (if (> (path-get set path (inc lvl)) 0) ;; overflow
                   (if (.-storage set)
                     (await ($$rpath set root path lvl opts))
                     (rpath set root path lvl))
                   (or (await ($$_prev-path set root path lvl opts))
                       (path-dec EMPTY_PATH)))))))

(defn- path-same-leaf ^boolean [set path1 path2]
  (let [bpl (bits-per-level set)]
    (== (bit-shift-right path1 (js/BigInt bpl)) (bit-shift-right path2 (js/BigInt bpl)))))

(defn- path-str [set path]
  (let [ml (js/BigInt (max-len set))]
    (loop [res ()
           path path]
      (if (not= path (js* "0n"))
        (recur (conj res (js/Number (mod path ml))) (quot path ml))
        (vec res)))))

(defn- $$keys-for
  "Returns keys array for the leaf node at the given path."
  [set path {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root (await ($$root set opts))
                     lvl  (node/level root)]
                 (loop [level lvl
                        node  root]
                   (if (pos? level)
                     (recur
                      (dec level)
                      (await (branch/$child node (.-storage set) (path-get set path level) opts)))
                     (.-keys node)))))))

;;------------------------------------------------------------------------------

;; replace with cljs.core/ArrayChunk after https://dev.clojure.org/jira/browse/CLJS-2470
(deftype Chunk [arr off end]
  ICounted
  (-count [_] (- end off))

  IIndexed
  (-nth [this i] (aget arr (+ off i)))

  (-nth [this i not-found]
    (if (and (>= i 0) (< i (- end off)))
      (aget arr (+ off i))
      not-found))

  IChunk
  (-drop-first [this]
    (if (== off end)
      (throw (js/Error. "-drop-first of empty chunk"))
      (Chunk. arr (inc off) end)))

  IReduce
  (-reduce [this f]
    (if (== off end)
      (f)
      (-reduce (-drop-first this) f (aget arr off))))

  (-reduce [this f start]
    (loop [val start, n off]
      (if (< n end)
        (let [val' (f val (aget arr n))]
          (if (reduced? val')
            @val'
            (recur val' (inc n))))
        val))))

#!------------------------------------------------------------------------------

(defprotocol IIter (-copy [this left right]))

(defprotocol ISeek (-seek [this key] [this key comparator]))

(defn- $$seek
  "Returns path to first element >= key, or nil if all elements in a set < key."
  [^BTSet set key comparator {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (nil? key)
                 EMPTY_PATH
                 (loop [node  (await ($$root set opts))
                        path  EMPTY_PATH
                        level (node/level node)]
                   (let [keys-l (node/len node)]
                     (if (== 0 level)
                       (let [keys (.-keys node)
                             idx  (binary-search-l comparator keys (dec keys-l) key)]
                         (if (== keys-l idx)
                           nil
                           (path-set set path 0 idx)))
                       (let [keys (.-keys node)
                             idx  (binary-search-l comparator keys (- keys-l 2) key)
                             child-node (await (branch/$child node (.-storage set) idx opts))]
                         (recur
                          child-node
                          (path-set set path level idx)
                          (dec level))))))))))

(declare ReverseIter $seek)

(deftype Iter [^BTSet set left right keys idx]
  IIter
  (-copy [_ l r] (Iter. set l r ($$keys-for set l {:sync? true}) (path-get set l 0)))

  IEquiv
  (-equiv [this other]
    (equiv-sequential this other))

  ISequential
  ISeqable
  (-seq [this]
    (when keys this))

  ISeq
  (-first [_]
    (when keys (arrays/aget keys idx)))

  (-rest [this]
    (or (-next this) ()))

  INext
  (-next [this]
    (when keys
      (if (< (inc idx) (arrays/alength keys))
        ;; can use cached array to move forward
        (let [left' (path-inc left)]
          (when (path-lt left' right)
            (Iter. set left' right keys (inc idx))))
        (let [left' ($$next-path set left {:sync? true})]
          (when (path-lt left' right)
            (-copy this left' right))))))

  IChunkedSeq
  (-chunked-first [this]
    (let [end-idx (if (path-same-leaf set left right)
                    ;; right is in the same node
                    (path-get set right 0)
                    ;; right is in a different node
                    (arrays/alength keys))]
      (Chunk. keys idx end-idx)))

  (-chunked-rest [this]
    (or (-chunked-next this) ()))

  IChunkedNext
  (-chunked-next [this]
    (let [last  (path-set set left 0 (dec (arrays/alength keys)))
          left' ($$next-path set last {:sync? true})]
      (when (path-lt left' right)
        (-copy this left' right))))

  IReduce
  (-reduce [this f]
    (if (nil? keys)
      (f)
      (let [first (-first this)]
        (if-some [next (-next this)]
          (-reduce next f first)
          first))))

  (-reduce [this f start]
    (loop [left left
           keys keys
           idx  idx
           acc  start]
      (if (nil? keys)
        acc
        (let [new-acc (f acc (arrays/aget keys idx))]
          (cond
            (reduced? new-acc)
            @new-acc

            (< (inc idx) (arrays/alength keys)) ;; can use cached array to move forward
            (let [left' (path-inc left)]
              (if (path-lt left' right)
                (recur left' keys (inc idx) new-acc)
                new-acc))

            :else
            (let [left' ($$next-path set left {:sync? true})]
              (if (path-lt left' right)
                (recur left' ($$keys-for set left' {:sync? true}) (path-get set left' 0) new-acc)
                new-acc)))))))

  IReversible
  (-rseq [this]
    (when keys
      (let [left' ($$prev-path set left {:sync? true})
            right' ($$prev-path set right {:sync? true})]
        (ReverseIter. set left' right'
                      ($$keys-for set right' {:sync? true})
                      (path-get set right' 0)))))

  ISeek
  (-seek [this key]
    (-seek this key (.-comparator set)))

  (-seek [this key cmp]
    (cond
      (nil? key)
      (throw (js/Error. "seek can't be called with a nil key!"))

      (nat-int? (cmp (arrays/aget keys idx) key))
      this

      :else
      (when-some [left' ($$seek set key cmp {:sync? true})]
        (Iter. set left' right ($$keys-for set left' {:sync? true}) (path-get set left' 0)))))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts (seq this))))

#!------------------------------------------------------------------------------

(defn- $$rseek
  "Returns path to the first element that is > key.
   If all elements in a set are <= key, returns `(-rpath set) + 1`.
   It's a virtual path that is bigger than any path in a tree."
  [^BTSet set key comparator {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [root (await ($$root set opts))
                     lvl  (node/level root)]
                 (if (nil? key)
                   (path-inc (if (.-storage set)
                               (await ($$rpath set root EMPTY_PATH lvl opts))
                               (rpath set root EMPTY_PATH lvl)))
                   (loop [node  root
                          path  EMPTY_PATH
                          level lvl]
                     (let [keys-l (node/len node)]
                       (if (== 0 level)
                         (let [keys (.-keys node)
                               idx  (binary-search-r comparator keys (dec keys-l) key)]
                           (path-set set path 0 idx))
                         (let [keys       (.-keys node)
                               idx        (binary-search-r comparator keys (- keys-l 2) key)
                               res        (path-set set path level idx)
                               child-node (await (branch/$child node (.-storage set) idx opts))]
                           (recur
                            child-node
                            res
                            (dec level)))))))))))

;;------------------------------------------------------------------------------

(deftype AsyncSeq [^BTSet set left right ^:mutable keys ^:mutable idx]
  aseq/PAsyncSeq
  (anext [this]
    (async
     (when (and left (path-lt left right))
       (when (nil? keys)
         (set! keys (await ($$keys-for set left {:sync? false})))
         (set! idx (path-get set left 0)))
       [(arrays/aget keys idx)
        (if (< (inc idx) (arrays/alength keys))
          (AsyncSeq. set (path-inc left) right keys (inc idx))
          (let [next-path (await ($$next-path set left {:sync? false}))]
            (when (and next-path (path-lt next-path right))
              (AsyncSeq. set next-path right nil nil))))])))
  Object
  (toString [this]
    (str "AsyncSeq[" (path-str set left) " -> " (path-str set right) "]"))
  IPrintWithWriter
  (-pr-writer [this writer opts]
    (-write writer (str this))))

#!------------------------------------------------------------------------------

(declare $$iter)

(deftype ReverseIter [^BTSet set left right keys idx]
  IIter
  (-copy [_ l r] (ReverseIter. set l r ($$keys-for set r {:sync? true}) (path-get set r 0)))

  IEquiv
  (-equiv [this other] (equiv-sequential this other))

  ISequential
  ISeqable
  (-seq [this] (when keys this))

  ISeq
  (-first [this]
    (when keys
      (arrays/aget keys idx)))

  (-rest [this]
    (or (-next this) ()))

  INext
  (-next [this]
    (when keys
      (if (> idx 0)
        (let [right' (path-dec right)]
          (when (path-lt left right')
            (ReverseIter. set left right' keys (dec idx))))
        (let [right' ($$prev-path set right {:sync? true})]
          (when (path-lt left right')
            (-copy this left right'))))))

  IReversible
  (-rseq [this]
    (when keys
      ($$iter set
              ($$next-path set left {:sync? true})
              ($$next-path set right {:sync? true})
              {:sync? true})))

  ISeek
  (-seek [this key]
    (-seek this key (.-comparator set)))

  (-seek [this key cmp]
    (cond
      (nil? key)
      (throw (js/Error. "seek can't be called with a nil key!"))

      (nat-int? (cmp key (arrays/aget keys idx)))
      this

      :else
      (let [right' ($$prev-path set ($$rseek set key cmp {:sync? true}) {:sync? true})]
        (when (and right' (>= right' (js* "0n"))
                   (path-lte left right')
                   (path-lt  right' right))
          (ReverseIter. set left right' ($$keys-for set right' {:sync? true}) (path-get set right' 0))))))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts (seq this))))

#!------------------------------------------------------------------------------

(deftype AsyncReverseSeq [^BTSet set left right ^:mutable keys ^:mutable idx]
  aseq/PAsyncSeq
  (anext [this]
    (async
     [(when (and right (path-lt left right))
        (when (nil? keys)
          (set! keys (await ($$keys-for set right {:sync? false})))
          (let [i (path-get set right 0)
                n (arrays/alength keys)]
            (set! idx (if (< i n) i (dec n)))))
        (arrays/aget keys idx))
      (when keys
        (if (> idx 0)
          (let [right' (path-dec right)]
            (when (path-lt left right')
              (AsyncReverseSeq. set left right' keys (dec idx))))
          (let [right' (await ($$prev-path set right {:sync? false}))]
            (when (path-lt left right')
              (let [ks (await ($$keys-for set right' {:sync? false}))]
                (AsyncReverseSeq. set left right' ks (path-get set right' 0)))))))]))
  Object
  (toString [_] (str "AsyncReverseSeq[" (path-str set right) " <- " (path-str set left) "]"))
  IPrintWithWriter
  (-pr-writer [this w _] (-write w (str this))))

(defn $$iter
  ([^BTSet set {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (let [root (await ($$root set opts))
                      lvl  (node/level root)]
                  (when (pos? (node/len root))
                    (let [left  EMPTY_PATH
                          rpth  (if (.-storage set)
                                  (await ($$rpath set root EMPTY_PATH lvl opts))
                                  (rpath set root EMPTY_PATH lvl))
                          right (await ($$next-path set rpth opts))
                          ks (await ($$keys-for set left opts))]
                      (if sync?
                        (Iter.     set left right ks (path-get set left 0))
                        (AsyncSeq. set left right ks (path-get set left 0)))))))))
  ([^BTSet set left right {:keys [sync?] :or {sync? true} :as opts}]
   (if sync?
     (Iter. set left right ($$keys-for set left {:sync? true}) (path-get set left 0))
     (async
      (let [root (await ($$root set {:sync? false}))]
        (when (pos? (node/len root))
          (let [cmp  (.-comparator set)
                left' (await ($$seek set left cmp {:sync? false}))
                right' (await ($$rseek set right cmp {:sync? false}))]
            (when (and left' (path-lt left' right'))
              (let [ks (await ($$keys-for set left' {:sync? true}))]
                (AsyncSeq. set left' right' ks (path-get set left' 0)))))))))))

(defn $slice
  ([^BTSet set key-from key-to]
   ($slice set key-from key-to (.-comparator set) {:sync? true}))
  ([^BTSet set key-from key-to arg]
   (if (fn? arg)
     ($slice set key-from key-to arg {:sync? true})
     ($slice set key-from key-to (.-comparator set) arg)))
  ([^BTSet set key-from key-to cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (when-some [left (await ($$seek set key-from cmp opts))]
                  (let [right (await ($$rseek set key-to cmp opts))]
                    (when (path-lt left right)
                      (let [ks (await ($$keys-for set left opts))]
                        (if sync?
                          (Iter.     set left right ks (path-get set left 0))
                          (AsyncSeq. set left right ks (path-get set left 0)))))))))))

(defn $rslice
  ([^BTSet set key-from key-to]
   ($rslice set key-from key-to (.-comparator set) {:sync? true}))
  ([^BTSet set key-from key-to arg]
   (if (fn? arg)
     ($rslice set key-from key-to arg {:sync? true})
     ($rslice set key-from key-to (.-comparator set) arg)))
  ([^BTSet set key-from key-to cmp {:keys [sync?] :or {sync? true} :as opts}]
   (if sync?
     (when-some [iter ($slice set key-to key-from cmp opts)]
       (rseq iter))
     (async
      (when-some [from-path (await ($$seek set key-to cmp opts))]
        (let [to-path (await ($$rseek set key-from cmp opts))]
          (when (path-lt from-path to-path)
            (let [left-bound (await ($$prev-path set from-path opts))
                  start-path (await ($$prev-path set to-path   opts))
                  ks         (await ($$keys-for set start-path opts))
                  idx        (path-get set start-path 0)]
              (AsyncReverseSeq. set left-bound start-path ks idx)))))))))

(defn- count-slice-leaf
  "Count elements in range [from, to] within a leaf node."
  [leaf from to cmp]
  (let [keys (.-keys leaf)
        len  (arrays/alength keys)
        from-idx (if from
                   (binary-search-l cmp keys (dec len) from)
                   0)
        to-idx   (if to
                   (binary-search-r cmp keys (dec len) to)
                   len)]
    (if (or (>= from-idx len) (<= to-idx 0))
      0
      (max 0 (- (min to-idx len) (max from-idx 0))))))

(defn- $count-slice-node
  "Recursively count elements in range [from, to] within a node."
  [node storage from to cmp {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (instance? Leaf node)
                 (count-slice-leaf node from to cmp)
        ;; Branch node
                 (let [keys (.-keys node)
                       len  (arrays/alength keys)
                       from-idx (if from
                                  (let [idx (binary-search-l cmp keys (- len 2) from)]
                                    (min idx (dec len)))
                                  0)
                       to-idx   (if to
                                  (let [idx (binary-search-r cmp keys (- len 2) to)]
                                    (min idx (dec len)))
                                  (dec len))]
                   (cond
            ;; Empty range
                     (> from-idx to-idx)
                     0

            ;; Same child, recurse into it
                     (== from-idx to-idx)
                     (let [child (await (branch/$child node storage from-idx opts))]
                       (await ($count-slice-node child storage from to cmp opts)))

            ;; Spans multiple children
                     :else
                     (let [;; Count partial from first child
                           first-child (await (branch/$child node storage from-idx opts))
                           first-count (await ($count-slice-node first-child storage from nil cmp opts))
                  ;; Count partial in last child
                           last-child  (await (branch/$child node storage to-idx opts))
                           last-count  (await ($count-slice-node last-child storage nil to cmp opts))
                  ;; Count fully contained children in between
                           middle-count (loop [i (inc from-idx)
                                               acc 0]
                                          (if (>= i to-idx)
                                            acc
                                            (let [child (await (branch/$child node storage i opts))
                                                  child-count (node/$subtree-count child)
                                                  cnt (if (>= child-count 0)
                                                        child-count
                                                        (await (node/$count child storage opts)))]
                                              (recur (inc i) (+ acc cnt)))))]
                       (+ first-count middle-count last-count))))))))

(defn $count-slice
  "Count elements in the range [from, to] inclusive.
   Uses O(log n) algorithm when subtree counts are available.
   If from is nil, counts from the beginning.
   If to is nil, counts to the end."
  ([^BTSet set from to]
   ($count-slice set from to (.-comparator set) {:sync? true}))
  ([^BTSet set from to arg]
   (if (fn? arg)
     ($count-slice set from to arg {:sync? true})
     ($count-slice set from to (.-comparator set) arg)))
  ([^BTSet set from to cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (if (and from to (pos? (cmp from to)))
                  0 ;; Empty range
                  (let [root (await ($$root set opts))]
                    (if (zero? (node/len root))
                      0
                      (await ($count-slice-node root (.-storage set) from to cmp opts)))))))))

(defn- measure-slice-leaf
  "Compute measure for keys in range [from, to] within a leaf."
  [^Leaf node measure-ops from to cmp]
  (let [keys (.-keys node)
        len  (arrays/alength keys)]
    (loop [i 0
           acc (measure/identity-measure measure-ops)]
      (if (>= i len)
        acc
        (let [key (arrays/aget keys i)
              in-range? (and (or (nil? from) (>= (cmp key from) 0))
                             (or (nil? to) (<= (cmp key to) 0)))]
          (recur (inc i)
                 (if in-range?
                   (measure/merge-measure measure-ops acc (measure/extract measure-ops key))
                   acc)))))))

(defn- $measure-slice-node
  "Recursively compute measure for elements in range [from, to] within a node."
  [node storage measure-ops from to cmp {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (if (instance? Leaf node)
                 (measure-slice-leaf node measure-ops from to cmp)
        ;; Branch node
                 (let [keys (.-keys node)
                       len  (arrays/alength keys)
                       from-idx (if from
                                  (let [idx (binary-search-l cmp keys (- len 2) from)]
                                    (min idx (dec len)))
                                  0)
                       to-idx   (if to
                                  (let [idx (binary-search-r cmp keys (- len 2) to)]
                                    (min idx (dec len)))
                                  (dec len))]
                   (cond
            ;; Empty range
                     (> from-idx to-idx)
                     (measure/identity-measure measure-ops)

            ;; Same child, recurse into it
                     (== from-idx to-idx)
                     (let [child (await (branch/$child node storage from-idx opts))]
                       (await ($measure-slice-node child storage measure-ops from to cmp opts)))

            ;; Spans multiple children
                     :else
                     (let [;; Measure from partial first child
                           first-child (await (branch/$child node storage from-idx opts))
                           first-measure (await ($measure-slice-node first-child storage measure-ops from nil cmp opts))
                  ;; Measure from partial last child
                           last-child  (await (branch/$child node storage to-idx opts))
                           last-measure  (await ($measure-slice-node last-child storage measure-ops nil to cmp opts))
                  ;; Measure from fully contained children in between
                           middle-measure (loop [i (inc from-idx)
                                                 acc (measure/identity-measure measure-ops)]
                                            (if (>= i to-idx)
                                              acc
                                              (let [child (await (branch/$child node storage i opts))
                                                    child-measure (or (node/$measure child)
                                                                      (await (node/force-compute-measure child storage measure-ops opts)))]
                                                (recur (inc i)
                                                       (measure/merge-measure measure-ops acc child-measure)))))]
                       (measure/merge-measure measure-ops
                                              (measure/merge-measure measure-ops first-measure middle-measure)
                                              last-measure))))))))

(defn $measure-slice
  "Compute measure for elements in the range [from, to] inclusive.
   Uses O(log n) algorithm when subtree measure is available.
   If from is nil, computes from the beginning.
   If to is nil, computes to the end.
   Returns nil if no measure-ops configured."
  ([^BTSet set from to]
   ($measure-slice set from to (.-comparator set) {:sync? true}))
  ([^BTSet set from to arg]
   (if (fn? arg)
     ($measure-slice set from to arg {:sync? true})
     ($measure-slice set from to (.-comparator set) arg)))
  ([^BTSet set from to cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (let [measure-ops (:measure (.-settings set))]
                  (if (nil? measure-ops)
                    nil
                    (if (and from to (pos? (cmp from to)))
                      (measure/identity-measure measure-ops) ;; Empty range
                      (let [root (await ($$root set opts))]
                        (if (zero? (node/len root))
                          (measure/identity-measure measure-ops)
                          (await ($measure-slice-node root (.-storage set) measure-ops from to cmp opts)))))))))))

(defn $get-nth
  "Find the entry at weighted rank `n`.
   Navigation uses cached subtree measure and IMeasure weight for
   O(log entries) performance.

   Returns [entry local-offset] where local-offset is the rank
   within the found entry, or nil if out of bounds.

   Requires measure with weight to be configured on the set."
  ([^BTSet set n]
   ($get-nth set n {:sync? true}))
  ([^BTSet set n {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (let [root (await ($$root set opts))
                      measure-ops (:measure (.-settings set))]
                  (when (nil? measure-ops)
                    (throw (js/Error. "get-nth requires measure to be configured")))
                  (when (pos? (node/len root))
                    (let [root-measure (or (node/$measure root)
                                           (await (node/force-compute-measure root (.-storage set) measure-ops opts)))]
                      (when root-measure
                        (let [total-weight (measure/weight measure-ops root-measure)]
                          (when (and (>= n 0) (< n total-weight))
                            ;; Navigate tree
                            (loop [cur-node root
                                   rank n]
                              (if (instance? Branch cur-node)
                            ;; Branch: find child by weight
                                (let [len (node/len cur-node)
                                      result (loop [i 0
                                                    r rank]
                                               (when (< i len)
                                                 (let [child (await (branch/$child cur-node (.-storage set) i opts))
                                                       child-measure (or (node/$measure child)
                                                                         (await (node/force-compute-measure child (.-storage set) measure-ops opts)))
                                                       child-weight (measure/weight measure-ops child-measure)]
                                                   (if (< r child-weight)
                                                     [child r]
                                                     (recur (inc i) (- r child-weight))))))]
                                  (when result
                                    (recur (nth result 0) (nth result 1))))
                            ;; Leaf: iterate keys by weight
                                (let [keys (.-keys cur-node)
                                      len (arrays/alength keys)]
                                  (loop [i 0
                                         r rank]
                                    (when (< i len)
                                      (let [key (arrays/aget keys i)
                                            key-measure (measure/extract measure-ops key)
                                            key-weight (measure/weight measure-ops key-measure)]
                                        (if (< r key-weight)
                                          [key r]
                                          (recur (inc i) (- r key-weight)))))))))))))))))))

(defn $equivalent?
  [^BTSet set other {:keys [sync?] :or {sync? true} :as opts}]
  (if sync?
    (if-not (set? other)
      false
      (and (= (count set) (count other))
           (every? #($contains? set % opts) other)))
    (async
     (if-not (set? other)
       false
       (if (instance? BTSet other)
         ;; NOTE we are assuming both have async-storage (if any)!!
         (and (= (await ($count set opts)) (await ($count other opts)))
              (loop [items (await ($$iter other opts))]
                (let [item (and items (await (aseq/first items)))]
                  (if (nil? item)
                    true
                    (if-not (await ($contains? set item opts))
                      false
                      (recur (await (aseq/rest items))))))))
         (and (= (await ($count set opts)) (count other))
              (loop [items (seq other)]
                (let [item (first items)]
                  (if (nil? item)
                    true
                    (if-not (await ($contains? set item opts))
                      false
                      (recur (rest items))))))))))))

(defn $equivalent-sequential?
  [xs ys {:keys [sync?] :or {sync? true} :as opts}]
  (if sync?
    (cljs.core/equiv-sequential xs ys)
    (async
     (cond
        ;; BTSet X BTSet
       (and (instance? BTSet xs) (instance? BTSet ys))
       (let [cnt-x (await ($count xs opts))
             cnt-y (await ($count ys opts))]
         (if (not= cnt-x cnt-y)
           false
           (loop [xiter (await ($$iter xs opts))
                  yiter (await ($$iter ys opts))]
             (let [x (await (aseq/first xiter))
                   y (await (aseq/first yiter))]
               (cond
                 (nil? x) (nil? y)
                 (not= x y) false
                 :else (recur (await (aseq/rest xiter))
                              (await (aseq/rest yiter))))))))

        ;; BTSet X AsyncSeq
       (and (instance? BTSet xs) (satisfies? aseq/PAsyncSeq ys))
       (loop [xiter (await ($$iter xs opts))
              yiter ys]
         (let [x (await (aseq/first xiter))
               y (await (aseq/first yiter))]
           (cond
             (nil? x) (nil? y)
             (not= x y) false
             :else (recur (await (aseq/rest xiter))
                          (await (aseq/rest yiter))))))

        ;; AsyncSeq X BTSet
       (and (satisfies? aseq/PAsyncSeq xs) (instance? BTSet ys))
       (loop [xiter xs
              yiter (await ($$iter ys opts))]
         (let [x (await (aseq/first xiter))
               y (await (aseq/first yiter))]
           (cond
             (nil? x) (nil? y)
             (not= x y) false
             :else (recur (await (aseq/rest xiter))
                          (await (aseq/rest yiter))))))

        ;; AsyncSeq X AsyncSeq
       (and (satisfies? aseq/PAsyncSeq xs) (satisfies? aseq/PAsyncSeq ys))
       (loop [xiter xs
              yiter ys]
         (let [x (await (aseq/first xiter))
               y (await (aseq/first yiter))]
           (cond
             (nil? x) (nil? y)
             (not= x y) false
             :else (recur (await (aseq/rest xiter))
                          (await (aseq/rest yiter))))))

        ;; BTSet X Seqable
       (and (instance? BTSet xs) (or (seqable? ys) (array? ys)))
       (let [s (if (array? ys) (array-seq ys 0) (seq ys))
             cnt-x (await ($count xs opts))
             cnt-y (count s)]
         (if (not= cnt-x cnt-y)
           false
           (loop [xiter (await ($$iter xs opts))
                  z     s]
             (let [x (await (aseq/first xiter))]
               (if (nil? x)
                 (nil? z)
                 (if (= x (first z))
                   (recur (await (aseq/rest xiter)) (next z))
                   false))))))

        ;; AsyncSeq X Seqable
       (and (satisfies? aseq/PAsyncSeq xs) (or (seqable? ys) (array? ys)))
       (let [s (if (array? ys) (array-seq ys 0) (seq ys))]
         (loop [xiter xs
                z     s]
           (let [x (await (aseq/first xiter))]
             (cond
               (nil? x) (nil? z)
               (nil? z) false
               (= x (first z)) (recur (await (aseq/rest xiter)) (next z))
               :else false))))

       :else false))))

(defn async-reduce
  [arf set from]
  (assert (instance? BTSet set) "async-reduce expects a BTSet in second arg")
  (if (instance? BTSet from)
    (async
     (loop [acc set
            items (await ($$iter from {:sync? false}))]
       (if-some [x (await (aseq/first items))]
         (let [acc' (await (arf acc x))]
           (if (reduced? acc')
             (await (arf (unreduced acc')))
             (recur acc' (await (aseq/rest items)))))
         (await (arf acc)))))
    (if (satisfies? aseq/PAsyncSeq from)
      (async
       (loop [acc set
              items from]
         (if-some [x (await (aseq/first items))]
           (let [acc' (await (arf acc x))]
             (if (reduced? acc')
               (await (arf (unreduced acc')))
               (recur acc' (await (aseq/rest items)))))
           (await (arf acc)))))
      (async
       (loop [acc set
              items from]
         (if (seq items)
           (let [v (first items)]
             (if (some? v)
               (let [acc' (await (arf acc v))]
                 (if (reduced? acc')
                   (await (arf (unreduced acc')))
                   (recur acc' (rest items))))
               (await (arf acc))))
           (await (arf acc))))))))

(defn- xf-driver
  "Returns {:step (fn [x] {:out <vector> :done? <bool>})
            :complete (fn [] <vector>)}.
   Internally applies `xform` to a synchronous collecting rf, preserving
   early-termination semantics via `reduced`."
  [xform]
  (let [buf  (volatile! [])
        step (fn
               ([] nil)
               ([acc] acc)
               ([acc x] (vswap! buf conj x) acc))
        xf   (xform step)]
    {:step
     (fn [x]
       (let [start (count @buf)
             ret   (xf nil x)
             v     @buf
             out   (if (> (count v) start) (subvec v start (count v)) [])
             done? (reduced? ret)]
         (when done?
           ;; finalize the transducer and collect any final outputs
           (let [s2 (count @buf)
                 _  (xf (unreduced ret))
                 v2 @buf
                 tail (if (> (count v2) s2) (subvec v2 s2 (count v2)) [])]
             (vreset! buf [])
             {:out (if (seq tail) (into out tail) out)
              :done? true}))
         (when-not done?
           (vreset! buf [])
           {:out out :done? false})))

     :complete
     (fn []
       (let [start (count @buf)
             _     (xf nil)
             v     @buf
             out   (if (> (count v) start) (subvec v start (count v)) [])]
         (vreset! buf [])
         out))}))

(defn async-transduce
  "xform: synchronous transducer (core map/filter/comp/etc)
   arf:   MUST BE ASYNC reducing fn with arities ([acc] ...) and ([acc x] ...)
   init:  initial accumulator
   from:  BTSet | aseq/PAsyncSeq | sequential"
  [xform arf init from]
  (let [{:keys [step complete]} (xf-driver xform)
        apply-outs
        (fn [acc outs]
          (async
           (loop [a acc, i 0, n (count outs)]
             (if (< i n)
               (let [a' (await (arf a (nth outs i)))]
                 (if (reduced? a')
                   (reduced (unreduced a'))
                   (recur a' (unchecked-inc i) n)))
               a))))]

    (cond
      (instance? BTSet from)
      (async
       (loop [acc init
              items (await ($$iter from {:sync? false}))]
         (if-some [x (await (aseq/first items))]
           (let [{:keys [out done?]} (step x)
                 acc' (await (apply-outs acc out))]
             (if (reduced? acc')
               (await (arf (unreduced acc')))
               (if done?
                 (await (arf acc'))
                 (recur acc' (await (aseq/rest items))))))
           (let [tail (complete)
                 acc' (await (apply-outs acc tail))]
             (await (arf acc'))))))

      (satisfies? aseq/PAsyncSeq from)
      (async
       (loop [acc init
              items from]
         (if-some [x (await (aseq/first items))]
           (let [{:keys [out done?]} (step x)
                 acc' (await (apply-outs acc out))]
             (if (reduced? acc')
               (await (arf (unreduced acc')))
               (if done?
                 (await (arf acc'))
                 (recur acc' (await (aseq/rest items))))))
           (let [tail (complete)
                 acc' (await (apply-outs acc tail))]
             (await (arf acc'))))))

      (sequential? from)
      (async
       (loop [acc init
              xs  (seq from)]
         (if (seq xs)
           (let [{:keys [out done?]} (step (first xs))
                 acc' (await (apply-outs acc out))]
             (if (reduced? acc')
               (await (arf (unreduced acc')))
               (if done?
                 (await (arf acc'))
                 (recur acc' (next xs)))))
           (let [tail (complete)
                 acc' (await (apply-outs acc tail))]
             (await (arf acc'))))))

      :else
      (throw (js/Error. (str "async-transduce: unsupported input type " (type from)))))))

(defn $reduce
  ([rf init from]
   ($reduce rf init from {:sync? true}))
  ([rf init from {:keys [sync?] :or {sync? true}}]
   (assert (instance? BTSet init))
   (if sync?
     (reduce rf init from)
     (async-reduce rf init from))))

(defn $transduce
  ([xform arf init from]
   ($transduce xform arf init from {:sync? true}))
  ([xform arf init from {:keys [sync?] :or {sync? true}}]
   (assert (instance? BTSet init))
   (if sync?
     (transduce xform arf init from)
     (async-transduce xform arf init from))))

(defn $into
  ([to from]
   ($into to (map identity) from {:sync? true}))
  ([to arg0 arg1]
   (if (fn? arg0)
     ($into to arg0 arg1 {:sync? true})
     ($into to (map identity) arg0 arg1)))
  ([to xform from {:keys [sync?] :or {sync? true}}]
   (assert (instance? BTSet to))
   (if sync?
     (into to xform from)
     (async-transduce xform
                      (fn
                        ([acc] (async acc))
                        ([acc item]
                         (if (instance? BTSet acc)
                           ($conjoin acc item (.-comparator acc) {:sync? false})
                           (async (conj acc item)))))
                      to
                      from))))

(defn $seq
  ([set]
   ($seq set {:sync? true}))
  ([set opts]
   ($$iter set opts)))

(defn $rseq
  ([set]
   ($rseq set {:sync? true}))
  ([set {:keys [sync?] :or {sync? true} :as opts}]
   (if sync?
     (rseq ($$iter set {:sync? true}))
     (async
      (let [i (await ($$iter set opts))]
        (when (.-keys i)
          (let [l' (await ($$prev-path set (.-left i) opts))
                r' (await ($$prev-path set (.-right i) opts))
                ks (await ($$keys-for set r' opts))
                idx (path-get set r' 0)]
            (AsyncReverseSeq. set l' r' ks idx))))))))

(defn $seek
  ([seq key]
   ($seek seq key {:sync? true}))
  ([seq key arg]
   (assert (some? seq))
   (if (fn? arg)
     ($seek seq key arg {:sync? true})
     ($seek seq key (.-comparator (.-set seq)) arg)))
  ([seq key cmp {:keys [sync?] :or {sync? true} :as opts}]
   (assert (some? seq))
   (assert (fn? cmp))
   (if sync?
     (-seek seq key cmp)
     (let [set (.-set seq)]
       (assert (instance? BTSet set))
       (if (instance? AsyncSeq seq)
         (if (nat-int? (cmp (arrays/aget (.-keys seq) (.-idx seq)) key))
           (async seq)
           (async
            (when-some [left' (await ($$seek set key cmp opts))]
              (let [ks (await ($$keys-for set left' opts))]
                (AsyncSeq. set left' (.-right seq) ks (path-get set left' 0))))))
         (if (instance? AsyncReverseSeq seq)
           (if (nat-int? (cmp key (arrays/aget (.-keys seq) (.-idx seq))))
             (async seq)
             (async
              (let [k (await ($$rseek set key cmp opts))
                    right' (await ($$prev-path set k opts))]
                (when (and right' (>= right' (js* "0n"))
                           (path-lte (.-left seq) right')
                           (path-lt  right' (.-right seq)))
                  (AsyncReverseSeq. set (.-left seq) right' (await ($$keys-for set right' opts)) (path-get set right' 0))))))
           (throw (js/Error. (str "unsupported type: '" (type seq) "'")))))))))

#!------------------------------------------------------------------------------

(deftype BTSet [^:mutable root cnt comparator meta ^:mutable _hash storage ^:mutable address settings]
  Object
  (toString [this] (pr-str* this))

  ICloneable
  (-clone [_] (BTSet. root cnt comparator meta _hash storage address settings))

  IWithMeta
  (-with-meta [_ new-meta] (BTSet. root cnt comparator new-meta _hash storage address settings))

  IMeta
  (-meta [_] meta)

  IEmptyableCollection
  (-empty [_] (BTSet. (Leaf. (arrays/array) settings nil) 0 comparator meta UNINITIALIZED_HASH nil nil settings))

  IEquiv
  (-equiv [this other]
    ($equivalent? this other {:sync? true}))

  IHash
  (-hash [this] (caching-hash this hash-unordered-coll _hash))

  ICollection
  (-conj [this key] ($conjoin this key comparator {:sync? true}))

  ISet
  (-disjoin [this key] ($disjoin this key comparator {:sync? true}))

  ILookup
  (-lookup [this k] ($lookup this k nil {:sync? true}))
  (-lookup [this k not-found] ($lookup this k not-found {:sync? true}))

  ISeqable
  (-seq [this]
    ($$iter this {:sync? true}))

  IReduce
  (-reduce [this f] (if-let [i ($$iter this {:sync? true})] (-reduce i f) (f)))
  (-reduce [this f start] (if-let [i ($$iter this {:sync? true})] (-reduce i f start) start))

  IReversible
  (-rseq [this] (rseq ($$iter this {:sync? true})))

  ; ISorted
  ; (-sorted-seq [this ascending?])
  ; (-sorted-seq-from [this k ascending?])
  ; (-entry-key [this entry] entry)
  ; (-comparator [this] comparator)

  ICounted
  (-count [this] ($count this {:sync? true}))

  IEditableCollection
  (-as-transient [this] this)

  ITransientCollection
  (-conj! [this key] ($conjoin this key comparator {:sync? true}))
  (-persistent! [this] this)

  ITransientSet
  (-disjoin! [this key] ($disjoin this key comparator {:sync? true}))

  IFn
  (-invoke [this k] (-lookup this k))
  (-invoke [this k not-found] (-lookup this k not-found))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "#{" " " "}" opts (seq this))))

#!------------------------------------------------------------------------------
#! Constructors

(defn- arr-partition-approx
  "Splits `arr` into arrays of size between min-len and max-len,
   trying to stick to (min+max)/2"
  [set arr]
  (let [chunk-len (avg-len set)
        min-len   (min-len set)
        max-len   (max-len set)
        len       (arrays/alength arr)
        acc       (transient [])]
    (when (pos? len)
      (loop [pos 0]
        (let [rest (- len pos)]
          (cond
            (<= rest max-len)
            (conj! acc (.slice arr pos))
            (>= rest (+ chunk-len min-len))
            (do
              (conj! acc (.slice arr pos (+ pos chunk-len)))
              (recur (+ pos chunk-len)))
            :else
            (let [piece-len (arrays/half rest)]
              (conj! acc (.slice arr pos (+ pos piece-len)))
              (recur (+ pos piece-len)))))))
    (to-array (persistent! acc))))

(defn- sorted-arr-distinct? [arr cmp]
  (let [al (arrays/alength arr)]
    (if (<= al 1)
      true
      (loop [i 1
             p (arrays/aget arr 0)]
        (if (>= i al)
          true
          (let [e (arrays/aget arr i)]
            (if (== 0 (cmp e p))
              false
              (recur (inc i) e))))))))

(defn- sorted-arr-distinct
  "Filter out repetitive values in a sorted array.
   Optimized for no-duplicates case"
  [arr cmp]
  (if (sorted-arr-distinct? arr cmp)
    arr
    (let [al (arrays/alength arr)]
      (loop [acc (transient [(arrays/aget arr 0)])
             i   1
             p   (arrays/aget arr 0)]
        (if (>= i al)
          (into-array (persistent! acc))
          (let [e (arrays/aget arr i)]
            (if (== 0 (cmp e p))
              (recur acc (inc i) e)
              (recur (conj! acc e) (inc i) e))))))))

(defn- arr-map-inplace [f arr]
  (let [len (arrays/alength arr)]
    (loop [i 0]
      (when (< i len)
        (arrays/aset arr i (f (arrays/aget arr i)))
        (recur (inc i))))
    arr))

(defn ^BTSet from-sorted-array
  [cmp arr _len opts]
  (let [settings (select-keys opts [:branching-factor :measure])
        measure-ops (:measure settings)
        set      (BTSet. nil 0 cmp nil nil nil nil settings)
        leaves   (->> arr
                      (arr-partition-approx set)
                      (arr-map-inplace #(let [leaf (Leaf. % settings nil)]
                                          ;; Compute measure for leaf if measure-ops available
                                          (when measure-ops
                                            (node/try-compute-measure leaf nil measure-ops {:sync? true}))
                                          leaf)))
        storage  (:storage opts)]
    (loop [current-level leaves
           shift 0]
      (case (count current-level)
        0 (BTSet. (Leaf. (arrays/array) settings nil) 0 cmp nil UNINITIALIZED_HASH storage nil settings)
        1 (BTSet. (first current-level) (arrays/alength arr) cmp nil UNINITIALIZED_HASH storage nil settings)
        (recur
         (->> current-level
              (arr-partition-approx set)
              (arr-map-inplace #(let [subtree-count (reduce + 0 (map node/$subtree-count %))
                                      ;; Compute measure from children if measure-ops available
                                      measure-ops (:measure settings)
                                      child-measure (when measure-ops
                                                      (reduce (fn [acc child]
                                                                (if (nil? acc)
                                                                  (reduced nil)
                                                                  (let [cs (node/$measure child)]
                                                                    (if cs
                                                                      (measure/merge-measure measure-ops acc cs)
                                                                      (reduced nil)))))
                                                              (measure/identity-measure measure-ops)
                                                              %))]
                                  (Branch. (inc shift)
                                           (arrays/amap node/max-key %)
                                           %
                                           nil
                                           subtree-count
                                           child-measure
                                           settings))))
         (inc shift))))))

(defn ^BTSet from-sequential [cmp seq opts]
  (let [arr (-> (into-array seq) (arrays/asort cmp) (sorted-arr-distinct cmp))]
    (from-sorted-array cmp arr (alength arr) opts)))

(defn ^BTSet from-opts
  "Create a set with options map containing:
   - :storage  Storage implementation
   - :comparator  Custom comparator (defaults to compare)
   - :measure  Measure implementation (IMeasure protocol)
   - :meta     Metadata"
  [opts]
  (let [settings (select-keys opts [:branching-factor :measure])]
    (BTSet. (Leaf. (arrays/array) settings nil) 0 (or (:comparator opts) compare)
            (:meta opts) UNINITIALIZED_HASH (:storage opts) nil settings)))

(defn ^BTSet sorted-set-by
  ([cmp]
   (from-opts {:comparator cmp}))
  ([cmp & keys]
   (from-sequential cmp keys {})))

