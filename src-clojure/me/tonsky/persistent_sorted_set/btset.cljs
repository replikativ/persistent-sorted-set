(ns me.tonsky.persistent-sorted-set.btset
  (:refer-clojure :exclude [iter sorted-set-by])
  (:require-macros [me.tonsky.persistent-sorted-set.macros :refer [async+sync]])
  (:require [await-cps :refer [await] :refer-macros [async]]
            ; [is.simm.lean-cps.async :refer [await] :refer-macros [async]]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [me.tonsky.persistent-sorted-set.branch :as branch :refer [Branch]]
            [me.tonsky.persistent-sorted-set.constants :refer [MIN_LEN AVG_LEN MAX_LEN UNINITIALIZED_HASH EMPTY_PATH BITS_PER_LEVEL MAX_SAFE_PATH MAX_SAFE_LEVEL BIT_MASK]]
            [me.tonsky.persistent-sorted-set.leaf :as leaf :refer [Leaf]]
            [me.tonsky.persistent-sorted-set.impl.node :as node]
            [me.tonsky.persistent-sorted-set.impl.storage :as storage]
            [me.tonsky.persistent-sorted-set.util :refer [rotate lookup-exact splice cut-n-splice binary-search-l binary-search-r return-array merge-n-split check-n-splice]]))

(declare BTSet $$iter riter)

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
        (cond
          (nil? roots) set

          (== (arrays/alength roots) 1)
          (BTSet. (arrays/aget roots 0)
                  (inc (.-cnt set))
                  (.-comparator set)
                  (.-meta set)
                  UNINITIALIZED_HASH
                  (.-storage set)
                  nil)

          :else
          (let [child0 (arrays/aget roots 0)
                lvl    (inc (node/level child0))]
            (BTSet. (Branch. lvl (arrays/amap node/max-key roots) roots nil)
                    (inc (.-cnt set))
                    (.-comparator set)
                    (.-meta set)
                    UNINITIALIZED_HASH
                    (.-storage set)
                    nil))))))))

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
         (let [new-root (arrays/aget new-roots 0)
               new-root (if (and (instance? Branch new-root)
                                 (== 1 (arrays/alength (.-children new-root))))
                          (await (branch/$child new-root (.-storage set) 0 opts))
                          new-root)]
           (BTSet. new-root
                   (dec (.-cnt set))
                   (.-comparator set)
                   (.-meta set)
                   UNINITIALIZED_HASH
                   (.-storage set)
                   nil))))))))

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
  [^BTSet set key not-found {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
   (async
    (if (await ($contains? set key opts))
      key
      not-found))))

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
                  (or (:comparator opts) compare))]
    (BTSet. nil -1 cmp meta UNINITIALIZED_HASH storage address)))

#!------------------------------------------------------------------------------

(defn- arr-partition-approx
  "Splits `arr` into arrays of size between min-len and max-len,
   trying to stick to (min+max)/2"
  [min-len max-len arr]
  (let [chunk-len AVG_LEN
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

(defn sorted-arr-distinct
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

#!------------------------------------------------------------------------------

(defn- path-inc ^number [^number path]
  (inc path))

(defn- path-dec ^number [^number path]
  (dec path))

(defn- path-cmp ^number [^number path1 ^number path2]
  (- path1 path2))

(defn- path-lt ^boolean [^number path1 ^number path2]
  (< path1 path2))

(defn- path-lte ^boolean [^number path1 ^number path2]
  (<= path1 path2))

(defn- path-eq ^boolean [^number path1 ^number path2]
  (== path1 path2))

(def factors (into-array (map #(js/Math.pow 2 %) (range 0 52 BITS_PER_LEVEL))))

(defn- path-get ^number [^number path ^number level]
  (if (< level MAX_SAFE_LEVEL)
    (-> path
      (unsigned-bit-shift-right (* level BITS_PER_LEVEL))
      (bit-and BIT_MASK))
    (-> path
      (/ (arrays/aget factors level))
      (js/Math.floor)
      (bit-and BIT_MASK))))

(defn- path-set ^number [^number path ^number level ^number idx]
  (let [smol? (and (< path MAX_SAFE_PATH) (< level MAX_SAFE_LEVEL))
        old   (path-get path level)
        minus (if smol?
                (bit-shift-left old (* level BITS_PER_LEVEL))
                (* old (arrays/aget factors level)))
        plus  (if smol?
                (bit-shift-left idx (* level BITS_PER_LEVEL))
                (* idx (arrays/aget factors level)))]
    (-> path
      (- minus)
      (+ plus))))

(defn- rpath
  [node ^number path ^number level]
  (if (pos? level)
    (let [last-idx (dec (arrays/alength (.-children node)))]
      (recur
        (arrays/aget (.-children node) last-idx)
        (path-set path level last-idx)
        (dec level)))
    (path-set path 0 (dec (arrays/alength (.-keys node))))))

(defn- $$rpath
  [node ^number path ^number level storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
   (async
    (if (pos? level)
      (let [last-idx (dec (node/len node))
            child-node (await (branch/$child node storage last-idx opts))]
        (await ($$rpath child-node (path-set path level last-idx) (dec level) storage opts)))
      (path-set path 0 (dec (arrays/alength (.-keys node))))))))

(defn- $$_next-path
  [set node ^number path ^number level {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (some? node) (implements? node/INode node)))
  (async+sync sync?
   (async
    (let [idx (path-get path level)]
      (if (pos? level)
        (let [child-node (await (branch/$child node (.-storage set) idx opts))
              sub-path (await ($$_next-path set child-node path (dec level) opts))]
          (if (nil? sub-path)
            (if (< (inc idx) (arrays/alength (.-children node)))
              (path-set EMPTY_PATH level (inc idx))
              nil)
            (path-set sub-path level idx)))
        (if (< (inc idx) (arrays/alength (.-keys node)))
          (path-set EMPTY_PATH 0 (inc idx)) ;; advance leaf idx
          nil))))))

(defn- $$next-path
  "Returns path representing next item after `path` in natural traversal order.
   Will overflow at leaf if at the end of the tree"
  [set ^number path {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
    (async
     (if (neg? path)
       EMPTY_PATH
       (let [root (await ($$root set opts))
             lvl  (node/level root)]
         (or
          (await ($$_next-path set root path lvl opts))
          (path-inc (if (.-storage set)
                      (await ($$rpath root EMPTY_PATH lvl (.-storage set) opts))
                      (rpath root EMPTY_PATH lvl)))))))))

(defn- $$_prev-path
  [set node ^number path ^number level {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (some? node) (implements? node/INode node)))
  (async+sync sync?
    (async
      (let [idx (path-get path level)]
        (cond
          (and (== 0 level) (== 0 idx))
          nil ;; leaf overflow

          (== 0 level) ;; leaf
          (path-set EMPTY_PATH 0 (dec idx))

          (>= idx (node/len node)) ;; branch that was overflow before
          (if (.-storage set)
            (await ($$rpath node path level (.-storage set) opts))
            (rpath node path level))

          :else
          (let [child-node (await (branch/$child node (.-storage set) idx opts))
                path' (await ($$_prev-path set child-node path (dec level) opts))]
            (cond
              (some? path') ;; no sub-overflow, keep current idx
              (path-set path' level idx)

              (== 0 idx) ;; nested overflow + this node overflow
              nil

              ;; nested overflow, advance current idx, reset subsequent indexes
              :else
              (let [child-node (await (branch/$child node (.-storage set) (dec idx) opts))
                    path' (if (.-storage set)
                            (await ($$rpath child-node path (dec level) (.-storage set) opts))
                            (rpath child-node path (dec level)))]
                (path-set path' level (dec idx))))))))))

(defn- $$prev-path
  "Returns path representing previous item before `path` in natural traversal order.
   Will overflow at leaf if at beginning of tree"
  [set ^number path {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
    (async
     (let [root (await ($$root set opts))
           lvl  (node/level root)]
       (if (> (path-get path (inc lvl)) 0) ;; overflow
         (if (.-storage set)
           ($$rpath root path lvl (.-storage set) opts)
           (rpath root path lvl))
         (or
          (await ($$_prev-path set root path lvl opts))
          (path-dec EMPTY_PATH)))))))

(defn- path-same-leaf ^boolean [^number path1 ^number path2]
  (if (and
       (< path1 MAX_SAFE_PATH)
       (< path2 MAX_SAFE_PATH))
    (==
     (unsigned-bit-shift-right path1 BITS_PER_LEVEL)
     (unsigned-bit-shift-right path2 BITS_PER_LEVEL))
    (==
     (Math/floor (/ path1 MAX_LEN))
     (Math/floor (/ path2 MAX_LEN)))))

(defn- path-str [^number path]
  (loop [res ()
         path path]
    (if (not= path 0)
      (recur (conj res (mod path MAX_LEN)) (Math/floor (/ path MAX_LEN)))
      (vec res))))

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
             (await (branch/$child node (.-storage set) (path-get path level) opts)))
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
                (path-set path 0 idx)))
            (let [keys (.-keys node)
                  idx  (binary-search-l comparator keys (- keys-l 2) key)
                  child-node (await (branch/$child node (.-storage set) idx opts))]
              (recur
                child-node
                (path-set path level idx)
                (dec level))))))))))

(deftype Iter [^BTSet set left right keys idx]
  IIter
  (-copy [_ l r] (Iter. set l r ($$keys-for set l {:sync? true}) (path-get l 0)))

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
    (let [end-idx (if (path-same-leaf left right)
                    ;; right is in the same node
                    (path-get right 0)
                    ;; right is in a different node
                    (arrays/alength keys))]
      (Chunk. keys idx end-idx)))

  (-chunked-rest [this]
    (or (-chunked-next this) ()))

  IChunkedNext
  (-chunked-next [this]
    (let [last  (path-set left 0 (dec (arrays/alength keys)))
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
                (recur left' ($$keys-for set left' {:sync? true}) (path-get left' 0) new-acc)
                new-acc)))))))

  IReversible
  (-rseq [this]
    (when keys
      (riter set ($$prev-path set left {:sync? true}) ($$prev-path set right {:sync? true}))))

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
        (Iter. set left' right ($$keys-for set left' {:sync? true}) (path-get left' 0)))))

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
                    (await ($$rpath root EMPTY_PATH lvl (.-storage set) opts))
                    (rpath root EMPTY_PATH lvl)))
        (loop [node  root
               path  EMPTY_PATH
               level lvl]
          (let [keys-l (node/len node)]
            (if (== 0 level)
              (let [keys (.-keys node)
                    idx  (binary-search-r comparator keys (dec keys-l) key)]
                (path-set path 0 idx))
              (let [keys       (.-keys node)
                    idx        (binary-search-r comparator keys (- keys-l 2) key)
                    res        (path-set path level idx)
                    child-node (await (branch/$child node (.-storage set) idx opts))]
                (recur
                  child-node
                  res
                  (dec level)))))))))))

;;------------------------------------------------------------------------------

(defprotocol IAsyncSeq
  (-afirst [this] "Returns async expression yielding first element")
  (-arest [this] "Returns async expression yielding rest of sequence"))

(deftype AsyncSeq [^BTSet set path till-path ^:mutable keys ^:mutable idx]
  IAsyncSeq
  (-afirst [this]
    (async
      (when (and path (path-lt path till-path))
        ;; Load keys only if not cached
        (when (nil? keys)
          (set! keys (await ($$keys-for set path {:sync? false})))
          (set! idx (path-get path 0)))
        (arrays/aget keys idx))))
  (-arest [this]
    (async
      (when (and path (path-lt path till-path))
        ;; Load keys only if not cached
        (when (nil? keys)
          (set! keys (await ($$keys-for set path {:sync? false})))
          (set! idx (path-get path 0)))
        (if (< (inc idx) (arrays/alength keys))
          ;; Next element is in same leaf - reuse keys array!
          (AsyncSeq. set (path-inc path) till-path keys (inc idx))
          ;; Need to move to next leaf
          (let [next-path (await ($$next-path set path {:sync? false}))]
            (when (and next-path (path-lt next-path till-path))
              ;; Don't pass keys - will be loaded lazily for new leaf
              (AsyncSeq. set next-path till-path nil nil)))))))
  Object
  (toString [this]
    (str "AsyncSeq[" (path-str path) " -> " (path-str till-path) "]"))
  IPrintWithWriter
  (-pr-writer [this writer opts]
    (-write writer (str this))))

(extend-type nil
  IAsyncSeq
  (-afirst [_] (async))
  (-arest [_] (async)))

(defn async-seq
  [set path till-path]
  (when (and path (path-lt path till-path))
    (AsyncSeq. set path till-path nil nil)))

(defn afirst [s] (-afirst s))

(defn arest [s] (-arest s))

(defn async-reduce
  [arf set from]
  (assert (instance? BTSet set))
  ;; from can be (normal-seq | BTSet | AsyncSeq)
  (if (instance? BTSet from)
    (throw (ex-info "from BTSet unimplemented" {}))
    (if (instance? AsyncSeq from)
      (throw (ex-info "from AsyncSeq unimplemented" {}))
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

(defn async-transduce
  [xform arf set from]
  (throw (ex-info "async-transduce unimplemented" {})))

(defn async-into
  ([] (async nil))
  ([^BTSet set] (async set))
  ([^BTSet set from]
   (assert (instance? BTSet set))
   (async-reduce
    (fn
      ([^BTSet acc]
       (async acc))
      ([^BTSet acc item]
       ($conjoin acc item (.-comparator acc) {:sync? false})))
    set
    from))
  ([^BTSet set xform from]
   (assert (instance? BTSet set))
   (async-transduce
    xform
    (fn
      ([^BTSet acc]
       (async acc))
      ([^BTSet acc item]
       ($conjoin acc item (.-comparator acc) {:sync? false})))
    set
    from)))

#!------------------------------------------------------------------------------

(deftype ReverseIter [^BTSet set left right keys idx]
  IIter
  (-copy [_ l r] (ReverseIter. set l r ($$keys-for set r {:sync? true}) (path-get r 0)))

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
        ;; can use cached array to advance
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
        (when (and
               (nat-int? right')
               (path-lte left right')
               (path-lt  right' right))
          (ReverseIter. set left right' ($$keys-for set right' {:sync? true}) (path-get right' 0))))))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts (seq this))))

(defn riter [^BTSet set left right]
  (ReverseIter. set left right ($$keys-for set right {:sync? true}) (path-get right 0)))

#!------------------------------------------------------------------------------

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
     (when-some [from-path (await ($$seek set key-from cmp opts))]
       (let [to-path (await ($$rseek set key-to cmp opts))]
         (when (path-lt from-path to-path)
           (let [ks (await ($$keys-for set from-path opts))]
             (if sync?
               (Iter. set from-path to-path ks (path-get from-path 0))
               (AsyncSeq. set from-path to-path ks (path-get from-path 0)))))))))))

(defn $$iter
  ([^BTSet set {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
     (async
      (let [root (await ($$root set opts))
            lvl  (node/level root)]
        (when (pos? (node/len root))
          (let [left  EMPTY_PATH
                rpth  (if (.-storage set)
                        (await ($$rpath root EMPTY_PATH lvl (.-storage set) opts))
                        (rpath root EMPTY_PATH lvl))
                right (await ($$next-path set rpth opts))]
            (if sync?
              (let [ks ($$keys-for set left opts)]
                (Iter. set left right ks (path-get left 0)))
              (async-seq set left right))))))))
  ([^BTSet set left right {:keys [sync?] :or {sync? true} :as opts}]
   (if sync?
     (Iter. set left right ($$keys-for set left {:sync? true}) (path-get left 0))
     (async
      (let [root (await ($$root set {:sync? false}))]
        (when (pos? (node/len root))
          (let [cmp  (.-comparator set)
                path (await ($$seek  set left cmp {:sync? false}))
                till (await ($$rseek set right   cmp {:sync? false}))]
            (when (and path (path-lt path till))
              (async-seq set path till)))))))))

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
                (let [item (and items (await (afirst items)))]
                  (if (nil? item)
                    true
                    (if-not (await ($contains? set item opts))
                      false
                      (recur (await (arest items))))))))
         (and (= (await ($count set opts)) (count other))
              (loop [items (seq other)]
                (let [item (first items)]
                  (if (nil? item)
                    true
                    (if-not (await ($contains? set item opts))
                      false
                      (recur (rest items))))))))))))

(defn $equivalent-sequential?
  [set other {:keys [sync?] :or {sync? true} :as opts}]
  (if sync?
    (cljs.core/equiv-sequential set other)
    (async
      (if (instance? BTSet other)
        (throw (ex-info "BTSet other $equivalent-sequential? unimplemented" {:other other}))
        (if (implements? IAsyncSeq other)
          (throw (ex-info "IAsyncSeq other $equivalent-sequential? unimplemented" {:other other}))
          (if (not (sequential? other))
            false
            (if (implements? IAsyncSeq set) ;; this is typically an async slice.
              (loop [xs set
                     ys (seq other)]
                (let [x (await (afirst xs))]
                  (if (nil? x)
                    (nil? ys)
                    (if (= x (first ys))
                      (recur (await (arest xs)) (next ys))
                      false))))
              ;; count here is potentially very expensive in restored state
              ;; will realize all nodes to sum keys. otherwise its cached
              (let [cnt-x (await ($count set opts))
                    cnt-y (count other)]
                (if (not= cnt-x cnt-y)
                  false
                  (loop [xs (await ($$iter set opts))
                         ys (seq other)]
                    (let [x (await (afirst xs))]
                      (if (nil? x)
                        (nil? ys)
                        (if (= x (first ys))
                          (recur (await (arest xs)) (next ys))
                          false)))))))))))))

#!------------------------------------------------------------------------------

(deftype BTSet [root cnt comparator meta ^:mutable _hash storage address]
  Object
  (toString [this] (pr-str* this))

  ICloneable
  (-clone [_] (BTSet. root cnt comparator meta _hash storage address))

  IWithMeta
  (-with-meta [_ new-meta] (BTSet. root cnt comparator new-meta _hash storage address))

  IMeta
  (-meta [_] meta)

  IEmptyableCollection
  (-empty [_] (BTSet. (Leaf. (arrays/array)) 0 comparator meta UNINITIALIZED_HASH storage address))

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

(defn- arr-map-inplace [f arr]
  (let [len (arrays/alength arr)]
    (loop [i 0]
      (when (< i len)
        (arrays/aset arr i (f (arrays/aget arr i)))
        (recur (inc i))))
    arr))

(defn ^BTSet from-sorted-array
  [cmp arr _len opts]
  (let [leaves (->> arr
                 (arr-partition-approx MIN_LEN MAX_LEN)
                 (arr-map-inplace #(Leaf. %)))
        storage (:storage opts)]
    (loop [current-level leaves
           shift 0]
      (case (count current-level)
        0 (BTSet. (Leaf. (arrays/array)) 0 cmp nil UNINITIALIZED_HASH storage nil)
        1 (BTSet. (first current-level) (arrays/alength arr) cmp nil UNINITIALIZED_HASH storage nil)
        (recur
          (->> current-level
            (arr-partition-approx MIN_LEN MAX_LEN)
            (arr-map-inplace #(Branch. (inc shift)
                                       (arrays/amap node/max-key %)
                                       %
                                       nil)))
          (inc shift))))))

(defn ^BTSet from-sequential [cmp seq]
  (let [arr (-> (into-array seq) (arrays/asort cmp) (sorted-arr-distinct cmp))]
    (from-sorted-array cmp arr (alength arr) {})))

(defn ^BTSet sorted-set-by
  ([cmp]
   (BTSet. (Leaf. (arrays/array)) 0 cmp nil UNINITIALIZED_HASH nil nil))
  ([cmp & keys]
   (from-sequential cmp keys)))

(defn ^BTSet from-opts
  "Create a set with options map containing:
   - :storage  Storage implementation
   - :comparator  Custom comparator (defaults to compare)
   - :meta     Metadata"
  [opts]
  (BTSet. (Leaf. (arrays/array)) 0 (or (:comparator opts) compare)
          (:meta opts) UNINITIALIZED_HASH (:storage opts) nil))
