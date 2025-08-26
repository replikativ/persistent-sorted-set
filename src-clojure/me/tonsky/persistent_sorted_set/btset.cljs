(ns me.tonsky.persistent-sorted-set.btset
  (:refer-clojure :exclude [iter sorted-set-by])
  (:require-macros [me.tonsky.persistent-sorted-set.macros :refer [async+sync]])
  (:require [await-cps :refer [await] :refer-macros [async]]
            ; [is.simm.lean-cps.async :refer [await] :refer-macros [async]]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            ;;TODO capitalize and emit w/ macro
            ; [me.tonsky.persistent-sorted-set.async-transducers :as at]
            [me.tonsky.persistent-sorted-set.constants :refer [min-len avg-len max-len uninitialized-hash empty-path bits-per-level max-safe-path max-safe-level bit-mask]]
            [me.tonsky.persistent-sorted-set.leaf :as leaf :refer [Leaf]]
            [me.tonsky.persistent-sorted-set.node :as node :refer [Node]]
            [me.tonsky.persistent-sorted-set.protocols :refer [IAsyncSeq INode IStorage] :as impl]
            [me.tonsky.persistent-sorted-set.util :refer [rotate lookup-exact splice cut-n-splice binary-search-l binary-search-r return-array merge-n-split check-n-splice]]))

(declare iter riter -seek* -rseek* -rpath BTSet)

(defn alter
  ([^BTSet set root shift cnt]
   (BTSet. root shift cnt (.-comparator set) (.-meta set) uninitialized-hash (.-storage set) (.-address set)))
  ([^BTSet set root shift cnt cmp]
   (BTSet. root shift cnt cmp (.-meta set) uninitialized-hash (.-storage set) (.-address set))))

(defn- $$ensure-root
  [^BTSet set {:keys [sync?] :or {sync? true} :as opts}]
  (assert (or (some? (.-address set)) (some? (.-root set))))
  (async+sync sync?
    (async
     (do
       (when (and (nil? (.-root set)) (some? (.-address set)))
         (set! (.-root set) (await (impl/restore (.-storage set) (.-address set) opts)))))
     (.-root set))))

(defn- store-node
  [node storage {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
    (cond
      (instance? Leaf node)
      (impl/store storage node opts)

      (instance? Node node)
      (async
       (let [children (.-children node)
             addresses (arrays/make-array (arrays/alength children))]
         (dotimes [i (arrays/alength children)]
           (let [child (arrays/aget children i)
                 addr (await (store-node child storage opts))]
             (arrays/aset addresses i addr)))
         (let [node-with-addresses (Node. (.-keys node) nil addresses nil)
               final-addr (await (impl/store storage node-with-addresses opts))]
           final-addr)))

      :else
      (throw (ex-info "Unknown node type" {:node node :type (type node)})))))

#!------------------------------------------------------------------------------

(defn $count
  [^BTSet set {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
    (async
     (do
       (when (neg? (.-cnt set))
         (let [root (await ($$ensure-root set opts))]
           (set! (.-cnt set) (await (impl/node-count root (.-storage set) opts)))))
       (.-cnt set)))))

(defn $contains?
  [^BTSet set key {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
    (async
      (let [root (await ($$ensure-root set opts))]
        (await (impl/node-contains? root (.-storage set) key (.-comparator set) opts))))))

(declare async-iter afirst arest)

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
              (loop [items (await (async-iter other))]
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

(defn $conjoin
  ([^BTSet set key arg]
   (if (fn? arg)
     ($conjoin set key arg {:sync? true})
     ($conjoin set key (.-comparator set) arg)))
  ([^BTSet set key cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
    (async
      (let [root (await ($$ensure-root set opts))
            roots (await (impl/node-conj root cmp key (.-storage set) opts))]
        (cond
          (nil? roots) set

          (== (arrays/alength roots) 1)
          (alter set
                 (arrays/aget roots 0)
                 (.-shift set)
                 (inc (.-cnt set)))

          :else
          (alter set
                 (Node. (arrays/amap impl/node-lim-key roots) roots nil nil)
                 (inc (.-shift set))
                 (inc (.-cnt set)))))))))

(defn $disjoin
  ([^BTSet set key arg]
   (if (fn? arg)
     ($disjoin set key arg {:sync? true})
     ($disjoin set key (.-comparator set) arg)))
  ([^BTSet set key cmp {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
    (async
     (let [new-roots (await (impl/node-disj (.-root set) cmp key true nil nil (.-storage set) opts))]
       (if (nil? new-roots) ;; nothing changed, key wasn't in the set
         set
         (let [new-root (arrays/aget new-roots 0)]
           (if (and (instance? Node new-root)
                    (== 1 (arrays/alength (.-children new-root))))

             ;; root has one child, make him new root
             (alter set
                    (arrays/aget (.-children new-root) 0)
                    (dec (.-shift set))
                    (dec (.-cnt set)))

             ;; keeping root level
             (alter set
                    new-root
                    (.-shift set)
                    (dec (.-cnt set)))))))))))

(defn $store
  ([^BTSet set arg]
   (if (implements? IStorage arg)
     (do
       (set! (.-storage set) arg)
       ($store set arg {:sync? true}))
     ($store set (.-storage set) arg)))
  ([^BTSet set storage {:keys [sync?] :or {sync? true} :as opts}]
   (assert (instance? BTSet set))
   (assert (implements? IStorage storage))
   (async+sync sync?
     (async
      (do
        (when (nil? (.-address set))
          (set! (.-address set) (await (store-node (.-root set) storage opts))))
        (.-address set))))))

(defn $lookup
  [^BTSet set key not-found {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
    (async
     (if (await ($contains? set key opts))
       key
       not-found))))

(declare AsyncSeq)

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
  ([set] (async set))
  ([set from]
   (assert (instance? BTSet set))
   (async-reduce
    (fn
      ([acc]
       (async acc))
      ([acc item]
       ($conjoin acc item (.-comparator acc) {:sync? false})))
    set
    from))
  ([set xform from]
   (assert (instance? BTSet set))
   (async-transduce
    xform
    (fn
      ([acc]
       (async acc))
      ([acc item]
       ($conjoin acc item (.-comparator acc) {:sync? false})))
    set
    from)))

#!------------------------------------------------------------------------------

(defn restore
  [root-address-or-info storage opts]
  (let [;; Handle both old format (bare UUID) and new format (map with metadata)
        address      (if (map? root-address-or-info)
                       (:root-address root-address-or-info)
                       root-address-or-info)
        _ (assert (some? address))
        meta (or (and (map? root-address-or-info) (:meta root-address-or-info))
                 (:meta opts))
        shift        (if (map? root-address-or-info)
                       (:shift root-address-or-info)
                       (:shift opts 0))
        cmp          (if (map? root-address-or-info)
                       (or (:comparator root-address-or-info) compare)
                       (or (:comparator opts) compare))]
    (BTSet. nil shift -1 cmp meta uninitialized-hash storage address)))

#!------------------------------------------------------------------------------

(defn- arr-partition-approx
  "Splits `arr` into arrays of size between min-len and max-len,
   trying to stick to (min+max)/2"
  [min-len max-len arr]
  (let [chunk-len avg-len
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

;;------------------------------------------------------------------------------

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

(def factors (into-array (map #(js/Math.pow 2 %) (range 0 52 bits-per-level))))

(defn- path-get ^number [^number path ^number level]
  (if (< level max-safe-level)
    (-> path
      (unsigned-bit-shift-right (* level bits-per-level))
      (bit-and bit-mask))
    (-> path
      (/ (arrays/aget factors level))
      (js/Math.floor)
      (bit-and bit-mask))))

(defn- path-set ^number [^number path ^number level ^number idx]
  (let [smol? (and (< path max-safe-path) (< level max-safe-level))
        old   (path-get path level)
        minus (if smol?
                (bit-shift-left old (* level bits-per-level))
                (* old (arrays/aget factors level)))
        plus  (if smol?
                (bit-shift-left idx (* level bits-per-level))
                (* idx (arrays/aget factors level)))]
    (-> path
      (- minus)
      (+ plus))))

(defn- $$_next-path
  [set node ^number path ^number level {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (some? node) (implements? impl/INode node)))
  (async+sync sync?
    (async
     (let [idx (path-get path level)]
       (if (pos? level)
         (let [child-node (if (.-storage set)
                            (await (node/$child node (.-storage set) idx opts))
                            (node/child node idx))
               sub-path (await ($$_next-path set child-node path (dec level) opts))]
           (if (nil? sub-path)
             ;; nested node overflow
             (if (< (inc idx) (arrays/alength (.-children node)))
               ;; advance current node idx, reset subsequent indexes
               (path-set empty-path level (inc idx))
               nil) ;; current node overflow
             ;; keep current idx
             (path-set sub-path level idx)))
         ;; leaf
         (if (< (inc idx) (arrays/alength (.-keys node)))
           (path-set empty-path 0 (inc idx)) ;; advance leaf idx
           nil)))))) ;; leaf overflow

(defn- $$next-path
  "Returns path representing next item after `path` in natural traversal order.
   Will overflow at leaf if at the end of the tree"
  [set ^number path {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
    (async
     (if (neg? path)
       empty-path
       (or
        (await ($$_next-path set (.-root set) path (.-shift set) opts))
        (path-inc (if (.-storage set)
                    (await (-rpath (.-root set) empty-path (.-shift set) (.-storage set) {:sync? false}))
                    (-rpath (.-root set) empty-path (.-shift set)))))))))

(defn- $$_prev-path
  [set node ^number path ^number level {:keys [sync?] :or {sync? true} :as opts}]
  (assert (and (some? node) (implements? impl/INode node)))
  (async+sync sync?
    (async
      (let [idx (path-get path level)]
        (cond
          (and (== 0 level) (== 0 idx))
          nil ;; leaf overflow

          (== 0 level) ;; leaf
          (path-set empty-path 0 (dec idx))

          (>= idx (impl/node-len node)) ;; branch that was overflow before
          (if (.-storage set)
            (await (-rpath node path level (.-storage set) opts))
            (-rpath node path level))

          :else
          (let [child-node (if (.-storage set)
                             (await (node/$child node (.-storage set) idx opts))
                             (node/child node idx))
                path' (await ($$_prev-path set child-node path (dec level) opts))]
            (cond
              (some? path') ;; no sub-overflow, keep current idx
              (path-set path' level idx)

              (== 0 idx) ;; nested overflow + this node overflow
              nil

              ;; nested overflow, advance current idx, reset subsequent indexes
              :else
              (let [child-node (if (.-storage set)
                                  (await (node/$child node (.-storage set) (dec idx) opts))
                                  (node/child node (dec idx)))
                    path' (if (.-storage set)
                            (await (-rpath child-node path (dec level) (.-storage set) opts))
                            (-rpath child-node path (dec level)))]
                (path-set path' level (dec idx))))))))))

(defn- $$prev-path
  "Returns path representing previous item before `path` in natural traversal order.
   Will overflow at leaf if at beginning of tree"
  [set ^number path {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
    (async
     (if (> (path-get path (inc (.-shift set))) 0) ;; overflow
       (if (.-storage set)
         (-rpath (.-root set) path (.-shift set) (.-storage set))
         (-rpath (.-root set) path (.-shift set)))
       (or
        (await ($$_prev-path set (.-root set) path (.-shift set) opts))
        (path-dec empty-path))))))

(defn- path-same-leaf ^boolean [^number path1 ^number path2]
  (if (and
       (< path1 max-safe-path)
       (< path2 max-safe-path))
    (==
     (unsigned-bit-shift-right path1 bits-per-level)
     (unsigned-bit-shift-right path2 bits-per-level))
    (==
     (Math/floor (/ path1 max-len))
     (Math/floor (/ path2 max-len)))))

(defn- path-str [^number path]
  (loop [res ()
           path path]
      (if (not= path 0)
        (recur (cljs.core/conj res (mod path max-len)) (Math/floor (/ path max-len)))
        (vec res))))

;;;-----------------------------------------------------------------------------

(defn- $$keys-for
  "Returns keys array for the leaf node at the given path."
  [set path {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
    (async
     (loop [level (.-shift set)
            node  (.-root set)]
       (if (pos? level)
         (recur
           (dec level)
           (if (.-storage set)
             (await (node/$child node (.-storage set) (path-get path level) opts))
             (node/child node (path-get path level))))
         (.-keys node))))))

;;;-----------------------------------------------------------------------------


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

;;;-----------------------------------------------------------------------------
(defprotocol IIter
  (-copy [this left right]))
;;;-----------------------------------------------------------------------------
(defprotocol ISeek
  (-seek [this key] [this key comparator]))
;;;-----------------------------------------------------------------------------

(deftype Iter [^BTSet set left right keys idx]
  IIter
  (-copy [_ l r] (Iter. set l r ($$keys-for set l {:sync? true}) (path-get l 0)))

  IEquiv
  (-equiv [this other] (equiv-sequential this other))

  ISequential
  ISeqable
  (-seq [this] (when keys this))

  ISeq
  (-first [_] (when keys (arrays/aget keys idx)))

  (-rest [this] (or (-next this) ()))

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
  (-seek [this key] (-seek this key (.-comparator set)))

  (-seek [this key cmp]
    (cond
      (nil? key)
      (throw (js/Error. "seek can't be called with a nil key!"))

      (nat-int? (cmp (arrays/aget keys idx) key))
      this

      :else
      (when-some [left' (-seek* set key cmp)]
        (Iter. set left' right ($$keys-for set left' {:sync? true}) (path-get left' 0)))))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts (seq this))))

(defn iter ;;;------------------------------------------------------------------XXX should this be async?
  ([set]
   (when (pos? (impl/node-len (.-root set)))
     (let [left  empty-path
           rpath (if (.-storage set)
                   (-rpath (.-root set) empty-path (.-shift set) (.-storage set))
                   (-rpath (.-root set) empty-path (.-shift set)))
           right ($$next-path set rpath {:sync? true})]
       (Iter. set left right ($$keys-for set left {:sync? true}) (path-get left 0)))))
  ([set left right]
   (Iter. set left right ($$keys-for set left {:sync? true}) (path-get left 0))))

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
      (iter set ($$next-path set left {:sync? true}) ($$next-path set right {:sync? true}))))

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
      (let [right' ($$prev-path set (-rseek* set key cmp) {:sync? true})]
        (when (and
               (nat-int? right')
               (path-lte left right')
               (path-lt  right' right))
          (ReverseIter. set left right' ($$keys-for set right'{:sync? true}) (path-get right' 0))))))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts (seq this))))

(defn riter [^BTSet set left right]
  (ReverseIter. set left right ($$keys-for set right {:sync? true}) (path-get right 0)))

(defn- -rpath
  "Returns rightmost path possible starting from node and sping deeper.
   In sync mode returns path directly, in async mode returns channel."
  ([node ^number path ^number level]
   (if (pos? level)
     (let [last-idx (dec (arrays/alength (.-children node)))]
       (recur
         (arrays/aget (.-children node) last-idx)
         (path-set path level last-idx)
         (dec level)))
     (path-set path 0 (dec (arrays/alength (.-keys node))))))
  ([node ^number path ^number level storage]
   ;; With storage, default to sync
   (-rpath node path level storage nil))
  ([node ^number path ^number level storage {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
               (async
                (if (pos? level)
                  ;; inner node
                  (let [last-idx (dec (impl/node-len node))
                        child-node (await (node/$child node storage last-idx opts))]
                    (await (-rpath child-node
                                   (path-set path level last-idx)
                                   (dec level)
                                   storage
                                   opts)))
                  ;; leaf
                  (path-set path 0 (dec (arrays/alength (.-keys node)))))))))



#!------------------------------------------------------------------------------

(defn- -distance [^BTSet set ^Node node left right level];;---------------------BUG should be async+sync
  (let [idx-l (path-get left level)
        idx-r (path-get right level)]
    (if (pos? level)
      ;; inner node
      (if (== idx-l idx-r)
        (-distance set (if (.-storage set)
                          (node/$child node (.-storage set) idx-l {:sync? true})
                          (node/child node idx-l))
                   left
                   right
                   (dec level))
        (loop [level level
               res   (- idx-r idx-l)]
          (if (== 0 level)
            res
            (recur (dec level) (* res avg-len)))))
      (- idx-r idx-l))))

(defn- distance [^BTSet set path-l path-r];;------------------------------------BUG should be async+sync
  (cond
    (path-eq path-l path-r)
    0

    (path-eq (path-inc path-l) path-r)
    1

    (path-eq ($$next-path set path-l {:sync? true}) path-r)
    1

    :else
    (-distance set (.-root set) path-l path-r (.-shift set))))

#!------------------------------------------------------------------------------


;; Slicing

(defn- -seek*
  "Returns path to first element >= key,
   or nil if all elements in a set < key.
   In sync mode returns path directly, in async mode returns channel."
  ([^BTSet set key comparator]
   (-seek* set key comparator {:sync? true}))
  ([^BTSet set key comparator opts]
   (let [{:keys [sync?] :or {sync? true}} opts]
     (async+sync sync?
       (async
         (if (nil? key)
           empty-path
           (loop [node  (.-root set)
                  path  empty-path
                  level (.-shift set)]
             (let [keys-l (impl/node-len node)]
               (if (== 0 level)
                 (let [keys (.-keys node)
                       idx  (binary-search-l comparator keys (dec keys-l) key)]
                   (if (== keys-l idx)
                     nil
                     (path-set path 0 idx)))
                 (let [keys (.-keys node)
                       idx  (binary-search-l comparator keys (- keys-l 2) key)
                       child-node (if (.-storage set)
                                    (await (node/$child node (.-storage set) idx opts))
                                    (node/child node idx))]
                   (recur
                    child-node
                    (path-set path level idx)
                    (dec level))))))))))))

(defn- -rseek*
  "Returns path to the first element that is > key.
   If all elements in a set are <= key, returns `(-rpath set) + 1`.
   It's a virtual path that is bigger than any path in a tree.
   In sync mode returns path directly, in async mode returns channel."
  ([^BTSet set key comparator]
   (-rseek* set key comparator {:sync? true}))
  ([^BTSet set key comparator {:keys [sync?] :or {sync? true} :as opts}]
   (async+sync sync?
     (async
       (if (nil? key)
         (path-inc (if (.-storage set)
                     (await (-rpath (.-root set) empty-path (.-shift set) (.-storage set) opts))
                     (-rpath (.-root set) empty-path (.-shift set))))
         (loop [node  (.-root set)
                path  empty-path
                level (.-shift set)]
           (let [keys-l (impl/node-len node)]
             (if (== 0 level)
               (let [keys (.-keys node)
                     idx  (binary-search-r comparator keys (dec keys-l) key)
                     res  (path-set path 0 idx)]
                 res)
               (let [keys       (.-keys node)
                     idx        (binary-search-r comparator keys (- keys-l 2) key)
                     res        (path-set path level idx)
                     child-node (if (.-storage set)
                                  (await (node/$child node (.-storage set) idx opts))
                                  (node/child node idx))]
                 (recur
                   child-node
                   res
                   (dec level)))))))))))

(defn- node-requires-storage?
  "Check if a node itself needs to be loaded from storage"
  [node]
  (and (instance? Node node)
       (.-addresses node)           ; Has storage addresses
       (nil? (.-children node))))   ; But children not loaded yet

(defn- slice-path-requires-storage?
  "Check if any nodes in the slice path need storage access.
   This is the critical optimization - only check nodes that
   the slice operation will actually traverse."
  [^BTSet set node key-from key-to level]
  (let [cmp (.-comparator set)
        keys (.-keys node)]
    (when (instance? Node node)
      (let [keys-l (arrays/alength keys)
            ;; Find which children the slice bounds span
            from-idx (if key-from
                       (binary-search-l cmp keys (- keys-l 2) key-from)
                       0)
            to-idx   (if key-to
                       (binary-search-r cmp keys (- keys-l 2) key-to)
                       (dec (arrays/alength (.-addresses node))))]

        ;; Only check nodes that are actually in the slice path
        (loop [idx from-idx]
          (when (<= idx to-idx)
            (let [;; Check if this child needs loading
                  child-addr (when (.-addresses node)
                               (arrays/aget (.-addresses node) idx))
                  child-loaded? (when (.-children node)
                                  (arrays/aget (.-children node) idx))]
              (if (and child-addr (not child-loaded?))
                ;; Found unloaded node in slice path
                true
                ;; This child is loaded, recurse if needed
                (if (and child-loaded? (> level 1))
                  ;; Recursively check this child's path requirements
                  (or (slice-path-requires-storage? set child-loaded? key-from key-to (dec level))
                      ;; Move to next child in slice range
                      (recur (inc idx)))
                  ;; Move to next child in slice range
                  (recur (inc idx)))))))))))

;;;-----------------------------------------------------------------------------

(defn slice [^BTSet set key-from key-to comparator]
  (when-some [path (-seek* set key-from comparator)]
    (let [till-path (-rseek* set key-to comparator)]
      (when (path-lt path till-path)
        (Iter. set path till-path ($$keys-for set path {:sync? true}) (path-get path 0))))))

;;------------------------------------------------------------------------------

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

(defn afirst [s] (impl/-afirst s))

(defn arest [s] (impl/-arest s))

(defn async-iter [^BTSet set]
  (async
    (let [root (await ($$ensure-root set {:sync? false}))]
      (when (pos? (impl/node-len root))
        (let [left  empty-path
              rpath (await (-rpath root empty-path (.-shift set) (.-storage set) {:sync? false}))
              right (await ($$next-path set rpath {:sync? false}))]
          (async-seq set left right))))))

(defn async-slice
  "Async version of slice that returns an AsyncSeq."
  [^BTSet set key-from key-to comparator]
  (async
   (when-some [path (await (-seek* set key-from comparator {:sync? false}))]
     (let [till-path (await (-rseek* set key-to comparator {:sync? false}))]
       (async-seq set path till-path)))))

#!------------------------------------------------------------------------------

(deftype BTSet [root shift cnt comparator meta ^:mutable _hash storage address]
  Object
  (toString [this] (pr-str* this))

  ICloneable
  (-clone [_] (BTSet. root shift cnt comparator meta _hash storage address))

  IWithMeta
  (-with-meta [_ new-meta] (BTSet. root shift cnt comparator new-meta _hash storage address))

  IMeta
  (-meta [_] meta)

  IEmptyableCollection
  (-empty [_] (BTSet. (Leaf. (arrays/array) nil) 0 0 comparator meta uninitialized-hash storage address))

  IEquiv
  (-equiv [this other] ($equivalent? this other {:sync? true}))

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
  (-seq [this] (iter this))

  IReduce
  (-reduce [this f] (if-let [i (iter this)] (-reduce i f) (f)))
  (-reduce [this f start] (if-let [i (iter this)] (-reduce i f start) start))

  IReversible
  (-rseq [this] (rseq (iter this)))

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
                 (arr-partition-approx min-len max-len)
                 (arr-map-inplace #(Leaf. % nil)))
        storage (:storage opts)]
    (loop [current-level leaves
           shift 0]
      (case (count current-level)
        0 (BTSet. (Leaf. (arrays/array) nil) 0 0 cmp nil uninitialized-hash storage nil)
        1 (BTSet. (first current-level) shift (arrays/alength arr) cmp nil uninitialized-hash storage nil)
        (recur
          (->> current-level
            (arr-partition-approx min-len max-len)
            (arr-map-inplace #(Node. (arrays/amap impl/node-lim-key %) % nil nil)))
          (inc shift))))))

(defn ^BTSet from-sequential [cmp seq]
  (let [arr (-> (into-array seq) (arrays/asort cmp) (sorted-arr-distinct cmp))]
    (from-sorted-array cmp arr (alength arr) {})))

(defn ^BTSet sorted-set-by
  ([cmp]
   (BTSet. (Leaf. (arrays/array) nil) 0 0 cmp nil uninitialized-hash nil nil))
  ([cmp & keys]
   (from-sequential cmp keys)))

(defn ^BTSet from-opts
  "Create a set with options map containing:
   - :storage  Storage implementation
   - :comparator  Custom comparator (defaults to compare)
   - :meta     Metadata"
  [opts]
  (BTSet. (Leaf. (arrays/array) nil) 0 0 (or (:comparator opts) compare)
          (:meta opts) uninitialized-hash (:storage opts) nil))