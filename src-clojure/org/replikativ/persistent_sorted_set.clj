(ns ^{:author "Nikita Prokopov"
      :doc "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can’t store nil."}
 org.replikativ.persistent-sorted-set
  (:refer-clojure :exclude [conj disj sorted-set sorted-set-by replace])
  (:require
   [org.replikativ.persistent-sorted-set.arrays :as arrays])
  (:import
   [clojure.lang RT]
   [java.lang.ref SoftReference]
   [java.util Comparator Arrays]
   [java.util.function BiConsumer]
   [org.replikativ.persistent_sorted_set ANode ArrayUtil Branch IBoundary IMeasure IStorage ISubtreeCount Leaf PersistentSortedSet RefType Settings Seq]))

(set! *warn-on-reflection* true)

(defn conj
  "Analogue to [[clojure.core/conj]] but with comparator that overrides the one stored in set."
  [^PersistentSortedSet set key ^Comparator cmp]
  (.cons set key cmp))

(defn disj
  "Analogue to [[clojure.core/disj]] with comparator that overrides the one stored in set."
  [^PersistentSortedSet set key ^Comparator cmp]
  (.disjoin set key cmp))

(defn slice
  "An iterator for part of the set with provided boundaries.
   `(slice set from to)` returns iterator for all Xs where from <= X <= to.
   `(slice set from nil)` returns iterator for all Xs where X >= from.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^PersistentSortedSet set from to]
   (.slice set from to))
  ([^PersistentSortedSet set from to ^Comparator cmp]
   (.slice set from to cmp)))

(defn rslice
  "A reverse iterator for part of the set with provided boundaries.
   `(rslice set from to)` returns backwards iterator for all Xs where from <= X <= to.
   `(rslice set from nil)` returns backwards iterator for all Xs where X <= from.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^PersistentSortedSet set from to]
   (.rslice set from to))
  ([^PersistentSortedSet set from to ^Comparator cmp]
   (.rslice set from to cmp)))

(defn count-slice
  "Count elements in the range [from, to] inclusive.
   Uses O(log n) algorithm when subtree counts are available.
   If from is nil, counts from the beginning.
   If to is nil, counts to the end.
   Optionally pass in comparator that will override the one that set uses."
  ([^PersistentSortedSet set from to]
   (.countSlice set from to))
  ([^PersistentSortedSet set from to ^Comparator cmp]
   (.countSlice set from to cmp)))

(defn has-subtree-counts?
  "Check whether all nodes in this tree have precomputed subtree counts.
   When true, count-slice is guaranteed O(log n).
   When false, count-slice may degrade to O(n) for subtrees missing counts."
  [^PersistentSortedSet set]
  (.hasSubtreeCounts set))

(defn seek
  "An efficient way to seek to a specific key in a seq (either returned by [[clojure.core.seq]] or a slice.)
  `(seek (seq set) to)` returns iterator for all Xs where to <= X.
  Optionally pass in comparator that will override the one that set uses."
  ([seq to]
   (when seq (.seek ^Seq seq to)))                 ; seq is nil for an empty set ⇒ nothing to seek
  ([seq to cmp]
   (when seq (.seek ^Seq seq to ^Comparator cmp))))

(defn lookup
  "Look up a key and return the actual stored element.
  Unlike get/valAt which return the search key, this returns the
  stored element - useful when using custom comparators that only
  compare part of the key (e.g., [id value] tuples compared by id).

  O(log n) traversal with no allocations (unlike slice).

  Returns nil if not found."
  ([^PersistentSortedSet set key]
   (.lookup set key))
  ([^PersistentSortedSet set key ^Comparator cmp]
   (.lookup set key cmp)))

(defn replace
  "Replace an existing key with a new key at the same logical position.
  The comparator must return 0 for both old-key and new-key.
  This is a single-traversal update - much faster than disj + conj.

  PRECONDITION, and it is the caller's to uphold: under the comparator passed to
  THIS call, no element other than old-key itself may compare equal to old-key.
  The single-traversal update rewrites in place at the position it finds, so if a
  comparator-EQUAL sibling exists, the rewrite can order the two wrongly and leave
  the node unsorted. The sibling then still appears in iteration but is no longer
  reachable by binary search, and the disorder becomes durable the moment the set
  is stored — after which every later search on that node is arbitrary.

  This matters specifically when `cmp` is COARSER than the set's own comparator —
  the `[id value]`-compared-by-id pattern these docstrings advertise. A set built
  under a full comparator may legitimately hold two elements that a coarser `cmp`
  cannot tell apart; `replace` under that coarser `cmp` is then outside contract.

  It is checked by assertions, so it fails loudly under -ea (the :test alias) and
  is NOT checked in ordinary production runs. That is deliberate — the check costs
  a scan on a hot write path — but it means violating this silently corrupts the
  structure rather than throwing. If you cannot guarantee uniqueness under `cmp`,
  use disj + conj.

  O(log n) traversal with minimal allocations.

  Returns the updated set, or the original set if old-key not found."
  ([^PersistentSortedSet set old-key new-key]
   (when (nil? new-key)
     (throw (IllegalArgumentException. "PersistentSortedSet cannot store nil")))
   (.replace set old-key new-key))
  ([^PersistentSortedSet set old-key new-key ^Comparator cmp]
   ;; `replace` was the other way nil got in. Every other mutation path refuses it, so a set
   ;; that "can't store nil" could be made to hold one:
   ;;     (replace (sorted-set-by cmp 1) 1 nil)  =>  count 1, [nil]
   ;; O(1), so unconditional — the same treatment `conj` gives.
   (when (nil? new-key)
     (throw (IllegalArgumentException. "PersistentSortedSet cannot store nil")))
   (.replace set old-key new-key cmp)))

(defn- array-from-indexed [coll type from to]
  (cond
    (instance? clojure.lang.Indexed coll)
    (ArrayUtil/indexedToArray type coll from to)

    (arrays/array? coll)
    (Arrays/copyOfRange coll from to (arrays/array-type type))))

(defn- split
  ([coll to type avg max]
   (persistent! (split (transient []) 0 coll to type avg max)))
  ([res from coll to type avg max]
   (let [len (- to from)]
     (cond
       (== 0 len)
       res

       (>= len (* 2 avg))
       (recur (conj! res (array-from-indexed coll type from (+ from avg))) (+ from avg) coll to type avg max)

       (<= len max)
       (conj! res (array-from-indexed coll type from to))

       :else
       (-> res
           (conj! (array-from-indexed coll type from (+ from (quot len 2))))
           (conj! (array-from-indexed coll type (+ from (quot len 2)) to)))))))

(defn- elem-at [coll i]
  (if (arrays/array? coll) (aget ^objects coll i) (nth coll i)))

(defn- mst-split
  "Bulk MST partition (matches incremental conj): cut coll[0..len) AFTER index i (i<len-1)
   whose `key-of` value rises to `thresh` (= node-level+1). `key-of` maps a coll element to
   the key to hash (identity for leaf keys, .maxKey for child nodes). Produces arrays via
   array-from-indexed, exactly like the count `split`."
  [^IBoundary boundary ^Settings settings coll len type key-of thresh]
  (let [len (long len)
        thresh (long thresh)
        last-i (dec len)]
    (loop [i 0, start 0, acc (transient [])]
      (if (>= i len)
        (persistent! (if (> len (long start))
                       (conj! acc (array-from-indexed coll type start len))
                       acc))
        (if (and (< i last-i)
                 (>= (.keyLevel boundary (key-of (elem-at coll i)) settings) thresh))
          (recur (inc i) (inc i) (conj! acc (array-from-indexed coll type start (inc i))))
          (recur (inc i) start acc))))))

(defn- map->settings ^Settings [m]
  ;; A `Settings` INSTANCE is not an opts map — every keyword lookup below returns nil for
  ;; one, so it used to produce all-defaults SILENTLY. Measured: `(from-sorted-array cmp arr n
  ;; (Settings. 4 STRONG nil nil 0))` came back at branching-factor 512, :ref-type :soft, with
  ;; no error. Honour it instead of rebuilding from nils; the library's own 2-/3-arities pass
  ;; a bare `(Settings.)`, for which this is exactly equivalent to the defaults it would have
  ;; constructed. (Refusing was tried first and rejected those internal callers, because the
  ;; no-arg ctor normalises to 512/SOFT and so does not look "unconfigured".)
  (if (instance? Settings m)
    m
    (let [boundary (:boundary m)
          s (Settings.
             (int (or (:branching-factor m) 0))
             (case (:ref-type m)
               :strong RefType/STRONG
               :soft   RefType/SOFT
               :weak   RefType/WEAK
               nil)
             ^IMeasure (:measure m)
             (:leaf-processor m)
           ;; diff-buf: fall back to the shared Settings default (Settings/defaultDiffBufSize,
           ;; 0/off unless the pss.diffBufSize sysprop is set) when the caller doesn't specify.
           ;; 0 = baseline (I0). See doc/diff-buffering.md. The MST incompatibility (a buffered
           ;; spine node is addressed by hash(anchor+diff), not its canonical content hash, which
           ;; breaks the cross-peer dedup MST exists for) is enforced in ONE place — `.withBoundary`
           ;; below forces diff-buf OFF for a *content-defined* boundary (a non-content boundary is
           ;; left untouched). See .internal/SPLIT_SEAM_DESIGN.md two-hash note.
           ;; A processor with NO :diff-buf-size inherits 0, not the default — otherwise the
           ;; `pss.diffBufSize` sysprop would supply a budget the caller never asked for and
           ;; `diffBufFor` would refuse the pairing. An EXPLICIT :diff-buf-size is passed
           ;; through unchanged so that refusal fires when it should.
             (int (or (:diff-buf-size m)
                      (if (:leaf-processor m) 0 (Settings/defaultDiffBufSize)))))
        ;; split-seam: opt into a content-defined boundary (e.g. MST) per store. nil ⇒ the
        ;; default count B-tree (byte-identical baseline). See .internal/SPLIT_SEAM_DESIGN.md.
          s (if boundary (.withBoundary s ^IBoundary boundary) s)]
    ;; diff-buf: the comparator is NOT stored on Settings — it lives on the PersistentSortedSet
    ;; (_cmp) and is propagated to Branch nodes (Branch._projCmp) for leaf projection.
      s)))

(defn- settings->map [^Settings s]
  {:branching-factor (.branchingFactor s)
   :ref-type         (condp identical? (.refType s)
                       RefType/STRONG :strong
                       RefType/SOFT   :soft
                       RefType/WEAK   :weak)
   :measure          ^IMeasure (.measure s)
   :leaf-processor   (.leafProcessor s)
   :diff-buf-size      (.diffBufSize s)
   ;; The boundary was dropped here, so anything round-tripping settings through this
   ;; map silently became a count B-tree — `compact` turned an MST set into one. Note
   ;; `map->settings` re-applies it through `withBoundary`, which forces diff-buf off
   ;; for a content-defined boundary; that is the correct pairing, not a loss.
   :boundary         (.boundary s)})

(defn- assert-sorted!
  "Under `*assert*` only: verify strictly ascending order.

   Unsorted input does not fail here, it produces a tree whose invariants are
   quietly false — lookups miss, slices return the wrong range. RocksDB's
   `SstFileWriter`, the same bulk-build pattern, refuses outright with \"Keys must
   be added in strict ascending order\" rather than accepting it, and this is the
   same hazard.

   Behind `assert` because it is O(n) comparisons on a path documented as a fast
   path, so it is on in dev and test and compiled out in a production build.
   `from-sorted-seq` checks unconditionally instead: it is streaming, so the check
   is a fold it is doing anyway."
  [^Comparator cmp keys len]
  (assert (loop [i 1]
            (cond
              (>= i len) true
              (>= 0 (.compare cmp (arrays/aget keys i) (arrays/aget keys (dec i)))) false
              :else (recur (inc i))))
          "from-sorted-array requires strictly ascending, distinct input"))

(defn from-sorted-array
  "Fast path to create a set if you already have a sorted array of elements on your hands."
  ([^Comparator cmp keys]
   (from-sorted-array cmp keys (arrays/alength keys) (Settings.)))
  ([^Comparator cmp keys len]
   (from-sorted-array cmp keys len (Settings.)))
  ([^Comparator cmp keys len opts]
   ;; `len` must name a real prefix of `keys`. Unchecked, `(from-sorted-array cmp (to-array
   ;; []) 1)` built from a 1-element array whose only slot is null and returned a set
   ;; CONTAINING NIL, count 1 — the same defect 987e8e5 fixed for `from-sequential`, hidden
   ;; by the same trap: `assert-sorted!` passes VACUOUSLY at len 1, so the builder's own
   ;; precondition could not see it. Out-of-range in the other direction used to surface as
   ;; ArrayIndexOutOfBoundsException (len > alength) or IllegalArgumentException (negative),
   ;; neither of which a `.cljc` caller could catch alongside the ClojureScript half — hence
   ;; one `ex-info` on both runtimes. Unlike `assert-sorted!` this is NOT behind `assert`:
   ;; it is O(1), and the failure it prevents is a durable nil member.
   (let [alen (arrays/alength keys)]
     (when (or (neg? len) (> len alen))
       (throw (ex-info "from-sorted-array: len out of range"
                       {:len len :array-length alen}))))
   ;; NIL REJECTION, and NOT behind `assert`, unlike the ordering check below.
   ;;
   ;; The namespace docstring's one stated difference from `clojure.core/sorted-set` is that
   ;; this set "can't store nil", and `conj`, `from-sequential`, `sorted-set` and
   ;; `from-sorted-seq` all enforce it by throwing. `from-sorted-array` did not, so the
   ;; plainest possible call put a nil into a set that claims it cannot hold one:
   ;;
   ;;     (from-sorted-array compare (object-array [nil 1]) 2)  =>  count 2, [nil 1]
   ;;
   ;; and it survives a store/restore. No exotic comparator is needed — under `compare`, nil
   ;; sorts below everything, so a LEADING nil is legitimately ascending and the ordering
   ;; assert cannot see it. Under a comparator that maps nil onto a real value it can sit
   ;; anywhere and still be ascending, so the scan has to cover every element.
   ;;
   ;; O(n) pointer comparisons on a path that already makes O(n) comparator CALLS and
   ;; allocates O(n) nodes — a constant-factor addition, not a complexity change — which is
   ;; why this one is unconditional where the comparison-heavy ordering check is not.
   (dotimes [i len]
     (when (nil? (arrays/aget keys i))
       (throw (IllegalArgumentException.
               (str "PersistentSortedSet cannot store nil (index " i ")")))))
   (assert-sorted! cmp keys len)
   (let [settings             (map->settings opts)
         max-branching-factor (.branchingFactor settings)
         avg-branching-factor (-> (.minBranchingFactor settings) (+ max-branching-factor) (quot 2))
         ;; avg >= 2 or the level count never reduces and this loops forever.
         ;; `min = bf >>> 1`, so branching factors 1 and 2 both yield avg 1.
         ;; Verified: bf=2 ran to OutOfMemoryError rather than failing. The
         ;; streaming builder got this guard first; the arithmetic is shared.
         _                    (assert (>= avg-branching-factor 2)
                                      (str "branching-factor must be >= 4 (got avg fanout "
                                           avg-branching-factor "); a fanout of 1 never "
                                           "reduces the level count"))
         storage              (:storage opts)
         ^IMeasure measure-ops  (.measure settings)
         ->Leaf               (fn [keys]
                                (let [^Leaf leaf (Leaf. (count keys) ^objects keys settings)]
                                  (when measure-ops
                                    (set! (.-_measure leaf) (.tryComputeMeasure leaf nil)))
                                  leaf))
         ->Branch             (fn [level ^objects children]
                                (let [subtree-count (reduce + 0 (map #(if (instance? ISubtreeCount %)
                                                                        (.subtreeCount ^ISubtreeCount %)
                                                                        (.count ^ANode % nil))
                                                                     children))
                                      measure       (when measure-ops
                                                      (reduce (fn [acc ^ANode child]
                                                                (let [child-measure (.-_measure child)]
                                                                  (if (some? child-measure)
                                                                    (.merge measure-ops acc child-measure)
                                                                    acc)))
                                                              (.identity measure-ops)
                                                              children))]
                                  (Branch.
                                   (int level)
                                   (int (count children))
                                   ^objects (arrays/amap #(.maxKey ^ANode %) Object children)
                                   nil
                                   children
                                   (long subtree-count)
                                   measure
                                   cmp
                                   settings)))]
     (if (.contentDefined (.boundary settings))
       ;; MST bulk: chunk by boundary level so a freshly-built tree matches an incrementally
       ;; grown one for the same key set (history-independence). key-of = identity for leaf
       ;; keys, .maxKey for child nodes; thresh = node-level+1.
       (let [^IBoundary boundary (.boundary settings)]
         (loop [level 1
                nodes (mapv ->Leaf (mst-split boundary settings keys len Object identity 1))]
           (case (count nodes)
             0 (PersistentSortedSet. (:meta opts) cmp storage settings)
             1 (PersistentSortedSet. (:meta opts) cmp nil storage (first nodes) len settings 0)
             (recur (inc level)
                    (mapv #(->Branch level %)
                          (mst-split boundary settings nodes (count nodes) Object
                                     (fn [^ANode n] (.maxKey n)) (inc level)))))))
       (loop [level 1
              nodes (mapv ->Leaf (split keys len Object avg-branching-factor max-branching-factor))]
         (case (count nodes)
           0 (PersistentSortedSet. (:meta opts) cmp storage settings)
           1 (PersistentSortedSet. (:meta opts) cmp nil storage (first nodes) len settings 0)
           (recur (inc level) (mapv #(->Branch level %) (split nodes (count nodes) Object avg-branching-factor max-branching-factor)))))))))

(defn- streaming-split
  "Lazy seq of vectors, reproducing `split`'s distribution over a SEQ rather than
   an indexed collection, buffering at most `(* 2 avg)` elements.

   `split` needs the remaining count to choose a cut, which looks like it needs
   the whole collection — but its rule only ever asks whether at least `2*avg`
   remain, so a `2*avg` lookahead answers it exactly. That is what makes a
   streaming build possible at all, and it is why the output is IDENTICAL to
   `from-sorted-array`'s rather than merely similar: same cuts, same tree.

   The three cases are `split`'s, in order:
     >= 2*avg remaining  take exactly avg
     <= max remaining    take all of it as the last node
     otherwise           halve it, so the last two nodes are both >= min"
  [coll avg max]
  ;; avg >= 2 or the build never terminates: with avg = 1 every branch gets one
  ;; child, so a level with k nodes produces k nodes again and the tree grows
  ;; upward forever. avg = (min+max)/2 with min = bf>>>1, so this means bf >= 3.
  ;; Checked rather than left to hang — bf=2 spun until OOM.
  (assert (>= avg 2)
          (str "branching-factor must be >= 4 for a streaming build (got avg fanout "
               avg "); a fanout of 1 never reduces the level count"))
  (let [need (* 2 avg)
        fill (fn [buf s]
               (loop [buf buf s s]
                 (if (or (nil? s) (>= (count buf) need))
                   [buf s]
                   (recur (clojure.core/conj buf (first s)) (next s)))))]
    ((fn step [buf s]
       (lazy-seq
        (let [[buf s] (fill buf s)
              n (count buf)]
          (cond
            (zero? n) nil
            ;; `(into [] (subvec ...))` and NOT the bare subvec: a SubVector shares
            ;; its base, and `conj` on one does `base.assocN(end, o)` — so the base
            ;; grows without bound and every element ever consumed stays reachable.
            ;; That silently makes this O(n), which is the one thing the function
            ;; exists not to be. Measured: 4M elements OOM'd at -Xmx128m before
            ;; this copy, and complete comfortably after it.
            (>= n need) (cons (into [] (subvec buf 0 avg))
                              (step (into [] (subvec buf avg)) s))
            (<= n max) (list buf)
            :else (let [h (quot n 2)]
                    (list (into [] (subvec buf 0 h)) (into [] (subvec buf h))))))))
     [] (seq coll))))

(defn from-sorted-seq
  "Bulk-build a set from a SORTED, DISTINCT seq, flushing every node to `:storage`
   as it is filled. Returns the set; its root address is `(store set)`.

   ## Why this exists next to `from-sorted-array`

   `from-sorted-array` materialises everything: the element array, then every
   leaf, then every branch level, each in a Clojure vector, with storage attached
   only afterwards. That is fine for a set that fits in memory and impossible for
   a database restore — the whole point of a bulk build is the case where the
   data does not fit.

   This walks the same structure bottom-up but keeps only ONE partial node per
   level, storing each node as it is completed, which is the shape PostgreSQL's
   `nbtsort.c` uses for exactly this reason. Peak memory is
   O(depth × branching-factor), independent of the element count.

   ## The tree it builds is a RESTORED tree

   Each node is stored on completion, so its parent references it by ADDRESS and
   holds no child pointer — the same form `Branch(level, keys, addresses,
   settings)` produces on restore, and the ctor that backs both. Children load
   lazily on first access. That is a deliberate difference from
   `from-sorted-array`, whose result is fully resident.

   ## Equivalence

   For the same elements and settings the two produce the same SHAPE: identical
   cuts, identical levels (see `streaming-split`). Asserted in the test suite by
   building both ways and comparing structure, not just contents — contents alone
   would pass on a tree with the right elements and the wrong fanout.

   Requires `:storage`; without somewhere to put nodes there is nothing to stream
   to, and `from-sorted-array` is the right call instead.

   Input MUST be sorted and distinct under `cmp`. This is checked, because the
   alternative is a silently corrupt tree: RocksDB's `SstFileWriter`, the same
   pattern, refuses unsorted input with \"Keys must be added in strict ascending
   order\" rather than accepting it."
  ([^Comparator cmp xs opts]
   (let [settings (map->settings opts)
         max-bf   (.branchingFactor settings)
         avg-bf   (-> (.minBranchingFactor settings) (+ max-bf) (quot 2))
         storage  (:storage opts)
         flush-fn (:flush-fn opts)
         ^IMeasure measure-ops (.measure settings)
         _ (when (nil? storage)
             (throw (IllegalArgumentException.
                     "from-sorted-seq requires :storage — use from-sorted-array for an in-memory build")))
         _ (when (.contentDefined (.boundary settings))
             (throw (IllegalArgumentException.
                     "from-sorted-seq does not support content-defined (MST) boundaries")))
         ;; ---- element stream, order-checked as it is consumed ----
         checked (fn checked [prev s]
                   (lazy-seq
                    (when-let [s (seq s)]
                      (let [x (first s)]
                        (when (nil? x)
                          (throw (IllegalArgumentException. "PersistentSortedSet cannot store nil")))
                        (when (and (not (identical? ::none prev))
                                   (>= 0 (.compare cmp x prev)))
                          (throw (IllegalArgumentException.
                                  (str "from-sorted-seq requires strictly ascending input; "
                                       (pr-str prev) " was followed by " (pr-str x)))))
                        (cons x (checked x (rest s)))))))
         ;; ---- level 0: leaves ----
         store-node! (fn [^ANode node]
                       (let [addr (.store node ^IStorage storage)]
                         ;; Called after each node is stored, so a caller that
                         ;; buffers writes can drain instead of accumulating the
                         ;; whole tree — otherwise this function's memory bound
                         ;; is real for the TREE and nominal for the caller.
                         ;; Same seam as the ClojureScript builder, which awaits
                         ;; it; here it is an ordinary call.
                         (when flush-fn
                           ;; AWAIT a deref-able result. Discarding it meant a flush-fn
                           ;; returning a future gave neither backpressure nor error
                           ;; propagation — it was called once per node and every failure
                           ;; was dropped. The ClojureScript builder awaits its flush; this
                           ;; makes the JVM agree for the case it can express.
                           (let [r (flush-fn)]
                             (when (instance? clojure.lang.IDeref r) @r)))
                         {:key (.maxKey node)
                          :address addr
                          :count (if (instance? ISubtreeCount node)
                                   (.subtreeCount ^ISubtreeCount node)
                                   (.count node nil))
                          :measure (.-_measure node)}))
         leaves (map (fn [ks]
                       (let [^objects arr (to-array ks)
                             ^Leaf leaf (Leaf. (alength arr) arr settings)]
                         (when measure-ops
                           (set! (.-_measure ^ANode leaf) (.tryComputeMeasure leaf nil)))
                         (store-node! leaf)))
                     (streaming-split (checked ::none xs) avg-bf max-bf))
         ;; ---- level n>0: branches over the level below ----
         branch-level (fn [level entries]
                        (map (fn [es]
                               (let [n (count es)
                                     ^objects keys (to-array (map :key es))
                                     ^objects addrs (to-array (map :address es))
                                     cnt (reduce + 0 (map :count es))
                                     ^Branch b (Branch. (int level) (int n) keys addrs nil
                                                        (long cnt) settings)]
                                 (when measure-ops
                                   (set! (.-_measure ^ANode b)
                                         (reduce (fn [acc m]
                                                   (if m (.merge measure-ops acc m) acc))
                                                 (.identity measure-ops)
                                                 (map :measure es))))
                                 (store-node! b)))
                             (streaming-split entries avg-bf max-bf)))]
     ;; Climb one level at a time. `(next s)` realises only the SECOND entry, so
     ;; the check for "is this level the root" costs two nodes, not a level.
     (loop [level 1
            entries leaves]
       (let [s (seq entries)]
         (cond
           (nil? s)
           ;; `(:meta opts)`, not `{}`: `sorted-set*` and the ClojureScript
           ;; `from-sorted-seq` both honour it, and this silently dropped it —
           ;; measured, JVM `(meta (from-sorted-seq … {:meta {:x 1}}))` was `{}`
           ;; against `{:x 1}` from `sorted-set*` and from cljs.
           (PersistentSortedSet. (:meta opts) cmp storage settings)

           (nil? (next s))
           (let [{:keys [address count]} (first s)]
             ;; root is referenced by address and loaded on demand, like a restore
             (PersistentSortedSet. (:meta opts) cmp address storage nil (int count) settings 0))

           :else
           (recur (inc level) (branch-level level s))))))))

(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you don’t want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  ([^Comparator cmp keys]
   (from-sequential cmp keys (Settings.)))
  ([^Comparator cmp keys opts]
   (when (some nil? keys) (throw (IllegalArgumentException. "PersistentSortedSet cannot store nil")))
   (let [arr (to-array keys)
         _   (arrays/asort arr cmp)
         len (ArrayUtil/distinct cmp arr)]
     (from-sorted-array cmp arr len opts))))

(defn sorted-set*
  "Create a set with custom comparator, metadata and settings.
   Options:
     :comparator  Custom comparator (defaults to compare)
     :storage     IStorage implementation
     :measure     IMeasure implementation for aggregate measures
     :meta        Metadata map
     :branching-factor  B-tree branching factor (default 512)
     :ref-type    Reference type for cached nodes (:strong, :soft, :weak)"
  [opts]
  (PersistentSortedSet.
   (:meta opts)
   ^Comparator (or (:comparator opts) (:cmp opts) compare)
   (:storage opts)
   (map->settings opts)))

(defn sorted-set-by
  "Create a set with custom comparator."
  ([cmp] (PersistentSortedSet. ^Comparator cmp))
  ([cmp & keys] (from-sequential cmp keys)))

(defn sorted-set
  "Create a set with default comparator."
  ([] (PersistentSortedSet/EMPTY))
  ([& keys] (from-sequential compare keys)))

(defn restore-by
  "Constructs lazily-loaded set from storage, root address and custom comparator.
   Supports all operations that normal in-memory impl would,
   will fetch missing nodes by calling IStorage::restore when needed"
  ([cmp address ^IStorage storage]
   (restore-by cmp address storage {}))
  ([cmp address ^IStorage storage opts]
   (PersistentSortedSet. nil cmp address storage nil -1 (map->settings opts) 0)))

(defn restore
  "Constructs lazily-loaded set from storage and root address.
   Supports all operations that normal in-memory impl would,
   will fetch missing nodes by calling IStorage::restore when needed.

   Honours `:comparator` in `opts`, defaulting to the natural one. It used to
   hard-code `RT/DEFAULT_COMPARATOR` and DISCARD an explicit `:comparator`, which
   matters because `restore-by` is JVM-only: portable `.cljc` code restoring a
   custom-comparator set has no other spelling, and ClojureScript honours the key.
   So the same source gave a working set on one runtime and a broken one on the
   other. Measured on a descending-comparator set of 0..19 stored and restored:

       (vec r)          => [19 18 ... 1 0]   correct
       (count r)        => 20                correct
       (contains? r 5)  => FALSE             (restore-by desc => true)
       (disj r 5)       => a no-op
       (conj r 5)       => 21 elements, a DUPLICATE 5, durable once stored

   Every cheap oracle — seq, count, printing — looks right, because the TREE is
   fine; only the comparator the set navigates it with was wrong."
  ([address storage]
   (restore-by RT/DEFAULT_COMPARATOR address storage {}))
  ([address ^IStorage storage opts]
   ;; `:cmp` as well as `:comparator`: `btset/restore` reads `(or (:comparator opts)
   ;; (:cmp opts) compare)` and this file's own `sorted-set*` accepts `:cmp`, so a caller who
   ;; wrote `(restore addr st {:cmp cmp})` still hit the whole defect on the JVM alone after
   ;; the `:comparator` half was fixed. Measured on the fixed build before this line:
   ;;     {:cmp desc}   JVM (contains? r 5) => FALSE      cljs => true
   ;;     {:comparator} JVM (contains? r 5) => true       cljs => true
   (restore-by (or (:comparator opts) (:cmp opts) RT/DEFAULT_COMPARATOR) address storage opts)))

(defn walk-addresses
  "Visit each address used by this set. Usable for cleaning up garbage left in
   storage from previous versions of the set.

   THE RETURN VALUE OF `consume-fn` IS A CONTINUE FLAG, and a falsey one stops the
   walk. Measured on a 20000-element tree at bf 16: a fn returning `true` visits
   1819 addresses; one returning `nil` or `false` visits 1. So a side-effecting
   `#(delete! %)` whose `delete!` returns nil enumerates ONE address and reports
   nothing wrong — which matters because the usual reason to call this is GC.
   Return `true` unless you deliberately want to prune.

   The two levels disagree on what falsey means: at the root it aborts the whole
   walk (PersistentSortedSet.walkAddresses), inside a branch it prunes only that
   subtree (Branch.walkAddresses)."
  [^PersistentSortedSet set consume-fn]
  (.walkAddresses set consume-fn))

(defn store
  "Store each not-yet-stored node by calling IStorage::store and remembering
   returned address. Incremental, won’t store same node twice on subsequent calls.
   Returns root address. Remember it and use it for restore"
  ([^PersistentSortedSet set]
   (.store set))
  ([^PersistentSortedSet set ^IStorage storage]
   (.store set storage)))

(defn settings [^PersistentSortedSet set]
  (settings->map (.-_settings set)))

(defn measure
  "Get the aggregated measure for the entire set.
   Returns the measure object computed by the measure-ops provided when creating the set.
   Returns nil if no measure-ops were provided."
  [^PersistentSortedSet set]
  (let [^ANode root (.root set)
        settings (.-_settings set)
        measure-ops (.measure settings)]
    (when (and measure-ops root)
      (or (.-_measure root)
          (.forceComputeMeasure root (.-_storage set))))))

(defn- measure-slice-node
  [^ANode node ^IStorage storage ^IMeasure measure-ops from to ^java.util.Comparator cmp]
  (if (instance? Leaf node)
    ;; Leaf: iterate keys in range
    (let [^Leaf leaf node
          keys (.-_keys leaf)
          len (.-_len leaf)]
      (loop [i 0
             acc (.identity measure-ops)]
        (if (>= i len)
          acc
          (let [key (aget ^objects keys i)
                in-range? (and (or (nil? from) (>= (.compare cmp key from) 0))
                               (or (nil? to) (<= (.compare cmp key to) 0)))]
            (recur (inc i)
                   (if in-range?
                     (.merge measure-ops acc (.extract measure-ops key))
                     acc))))))
    ;; Branch: recurse
    (let [^Branch branch node
          len (int (.-_len branch))
          from-idx (int (if from
                          (let [idx (.searchFirst branch from cmp)]
                            (if (>= idx len) (dec len) idx))
                          0))
          to-idx (int (if to
                        (let [idx (inc (.searchLast branch to cmp))]
                          (min (max idx 0) (dec len)))
                        (dec len)))]
      (cond
        (> from-idx to-idx)
        (.identity measure-ops)

        (== from-idx to-idx)
        (let [child (.child branch storage from-idx)]
          (measure-slice-node child storage measure-ops from to cmp))

        :else
        (let [first-child (.child branch storage from-idx)
              first-measure (measure-slice-node first-child storage measure-ops from nil cmp)
              last-child (.child branch storage to-idx)
              last-measure (measure-slice-node last-child storage measure-ops nil to cmp)
              middle-measure (loop [i (int (inc from-idx))
                                    acc (.identity measure-ops)]
                               (if (>= i to-idx)
                                 acc
                                 (let [^ANode child (.child branch storage i)
                                       child-measure (or (.-_measure child)
                                                         (.forceComputeMeasure child storage))]
                                   (recur (inc i)
                                          (.merge measure-ops acc child-measure)))))]
          (.merge measure-ops
                  (.merge measure-ops first-measure middle-measure)
                  last-measure))))))

(defn get-nth
  "Find the entry at weighted rank `n`.
   Navigation uses cached subtree measure and IMeasure.weight() for
   O(log entries) performance.

   Returns `[entry local-offset]` where `local-offset` is the rank
   within the found entry, or nil if out of bounds.

   Requires measure with weight() to be configured on the set."
  [^PersistentSortedSet set ^long n]
  (let [offset (long-array 1)]
    (when-let [entry (.getNth set n offset)]
      [entry (aget offset 0)])))

(defn measure-slice
  "Compute measure for elements in the range [from, to] inclusive.
   Uses O(log n + k) algorithm where k is keys in boundary leaves.
   If from is nil, computes from the beginning.
   If to is nil, computes to the end.
   Returns nil if no measure-ops configured."
  [^PersistentSortedSet set from to]
  (let [^ANode root (.root set)
        settings (.-_settings set)
        measure-ops (.measure settings)
        ^Comparator cmp (.comparator set)]
    (when measure-ops
      (if (and from to (pos? (.compare cmp from to)))
        (.identity measure-ops)
        (if (zero? (.count root (.-_storage set)))
          (.identity measure-ops)
          (measure-slice-node root (.-_storage set) measure-ops from to cmp))))))

(defn compact
  "Rebuild the tree with optimal fill factors from the current elements.
   Useful after heavy insert/delete churn that may have degraded node
   fill ratios. Preserves comparator, settings, storage and metadata.
   Returns a new set with the same elements in a freshly built tree.

   It did not always preserve those last three, while claiming to. The
   boundary was dropped by `settings->map`, so compacting an MST set
   returned a count B-tree; the storage was never passed on, so a later
   `(store compacted)` threw NullPointerException; and the metadata was
   reset to `{}`, dropping the `:pss/storage-id` the wire codec resolves
   on. All three verified before the fix.

   Note: currently materializes all elements in memory. For large
   IStorage-backed sets, ensure sufficient heap space."
  [^PersistentSortedSet set]
  (let [arr   (to-array (clojure.core/seq set))
        len   (alength arr)
        opts  (assoc (settings->map (.-_settings set)) :storage (.-_storage set))
        ^PersistentSortedSet compacted (from-sorted-array (.comparator set) arr len opts)]
    ;; the builder constructs without storage and it is attached afterwards, the same
    ;; way `datahike.index.persistent-set` does after `from-sorted-seq`
    (set! (.-_storage compacted) (.-_storage set))
    (with-meta compacted (meta set))))

;; ---------------------------------------------------------------------------
;; diff — what changed between two versions of a set that share structure.
;;
;; The walk is LEVEL-SYNCHRONIZED and never loads a node it can prove it does
;; not need. A frontier entry names a node without loading it:
;;
;;     [node parent idx addr prunable?]
;;
;; `node` is nil until materialized, `addr` is the stored address (nil for a
;; node that has never been stored), and `prunable?` says whether that address
;; can be trusted to stand for the contents.
;;
;; Each round: intersect the two frontiers' addresses, drop what both sides
;; hold, and load only the remainder. The addresses come from the parents,
;; which are already loaded, so pruning itself costs no IO. That is what makes
;; the READ count proportional to the change — an earlier version collected
;; every address of both trees first, which is correct but loads both trees
;; whole, and on a 100 000-element set read all 392 nodes to report a
;; two-element delta.

(defn- child-refs
  "Frontier entries for every child of `branches` (already materialized)."
  [branches]
  (into []
        (mapcat (fn [[node]]
                  (let [^Branch b node
                        ;; ONE snapshot for the pair. `addressArray` and `slots` are two
                        ;; independent volatile reads, and this decides prunability from
                        ;; BOTH — a pre-settle address paired with post-settle slots names
                        ;; a stale address and marks it prunable, so the other side prunes
                        ;; against it and the buffered delta vanishes from the answer. See
                        ;; Branch.addressesAndSlots.
                        pair  (.addressesAndSlots b)
                        addrs (aget ^objects pair 0)
                        ;; diff-buf: a branch buffers a child's changes in its
                        ;; OWN slots and leaves the child's ADDRESS untouched,
                        ;; so for a buffered child an address match no longer
                        ;; proves the subtrees are equal. Measured before this
                        ;; guard existed: a 5000-element set with 7 additions
                        ;; reported NONE of them under -Dpss.diffBufSize=256.
                        ;;
                        ;; The guard is per CHILD, not per branch. `bufEntries`
                        ;; is a whole-branch count, and using it made one
                        ;; buffered child poison all 390 of its siblings —
                        ;; correct, but it read every node of a 100 000-element
                        ;; set for a two-element delta. `slots` is indexed by
                        ;; child and equally IO-free: a nil slot means that
                        ;; child has no buffered diff, so its address still
                        ;; stands for its contents. A non-nil slot is
                        ;; unprunable whatever its shape — for a BRANCH child
                        ;; `diff` is null and the real diff lives in the
                        ;; subtree, so nil-diff does not mean nil-change.
                        slots (aget ^objects pair 1)]
                    (map (fn [i] [nil b i (when addrs (aget ^objects addrs i))
                                  (or (nil? slots) (nil? (aget ^objects slots i)))])
                         (range (.len b))))))
        branches))

(defn- materialize
  "Load every entry that is not resident. One `restore` each, at most."
  [^IStorage storage refs]
  (mapv (fn [[node ^Branch parent idx :as ref]]
          (if (some? node) ref (assoc ref 0 (.child parent storage (int idx)))))
        refs))

(defn- prune-shared
  "Drop from each frontier the entries the other side holds at the same
   address — identical subtrees, which cannot contain a difference. No IO."
  [fa fb]
  (let [addrs   (fn [f] (into #{} (keep (fn [[_ _ _ addr prunable?]]
                                          (when (and (some? addr) prunable?) addr)))
                              f))
        sa      (addrs fa)
        sb      (addrs fb)
        shared? (fn [other] (fn [[_ _ _ addr prunable?]]
                              (and (some? addr) prunable? (contains? other addr))))]
    ;; an entry the OTHER side marked unprunable never entered its address set,
    ;; so neither side prunes against a buffered branch.
    [(into [] (remove (shared? sb)) fa)
     (into [] (remove (shared? sa)) fb)]))

(defn- descend
  "One level down. At level 0 the entries are leaves and contribute their keys
   to `cand`; above it they contribute their children to the next frontier."
  [storage refs level cand]
  (let [nodes (materialize storage refs)]
    (if (zero? (long level))
      [[] (reduce (fn [c [node]] (reduce clojure.core/conj c (.keys ^ANode node))) cand nodes)]
      [(child-refs nodes) cand])))

(defn- sorted-diff
  "Elements of `xs` absent from `ys`. Both ascending under `cmp`; O(n+m), no IO."
  [^java.util.Comparator cmp xs ys]
  (loop [xs (clojure.core/seq xs) ys (clojure.core/seq ys) out (transient [])]
    (cond
      (nil? xs) (persistent! out)
      (nil? ys) (persistent! (reduce conj! out xs))
      :else     (let [c (.compare cmp (first xs) (first ys))]
                  (cond
                    (neg? c) (recur (next xs) ys (conj! out (first xs)))
                    (pos? c) (recur xs (next ys) out)
                    :else    (recur (next xs) (next ys) out))))))

(defn diff
  "Keys added and removed between two sets that SHARE STRUCTURE.

   Returns `{:added [...] :removed [...]}`, both in the sets' sort order.

   ## Why this is not `clojure.set/difference`

   Cost is proportional to what CHANGED, not to set size — in NODES READ, which
   is the cost that matters for a set backed by storage. Two versions of a
   persistent set share every node they have in common, so a subtree whose
   address appears on both sides cannot contain a difference and is dropped
   without being loaded. Measured on sets one two-element transaction apart,
   against a storage that actually serializes:

       elements   nodes on disk   nodes read
          1 000               5          3-4
        100 000             392          3-4

   Two identical stored roots are answered without touching storage at all.

   That is the property an incremental consumer needs — replication, an audit
   trail, catching a migration target up — to be proportional to the delta
   rather than to the database.

   The answer is exact without any membership lookups: a key that lives in a
   pruned (shared) leaf is present on BOTH sides and therefore appears as a
   candidate on neither, so differencing the two candidate lists is the same
   answer differencing against the full sets would give.

   ## Requirements and limits

   Both sets must come from the same lineage (one derived from the other by
   `conj`/`disj`, or both from a common ancestor). Diffing unrelated sets is
   CORRECT but pointless: nothing is shared, so nothing prunes and it degrades
   to a full walk of both.

   Sets must be STORED for pruning to work — an in-memory set has no addresses,
   so every node is walked. Call `store` first, or diff two restored sets.

   A rebalance that repartitions keys across leaves without changing them will
   read those leaves and find no difference: pruning is an optimization on
   reads, never on the answer.

   Membership is decided by the set's comparator, so two keys that compare
   equal are treated as the same key even if they are not `=`."
  ([a b] (diff a b (.-_storage ^PersistentSortedSet b)))
  ([^PersistentSortedSet a ^PersistentSortedSet b ^IStorage storage]
   (let [addr-a (.-_address a)
         addr-b (.-_address b)]
     (if (and (some? addr-a) (= addr-a addr-b))
       {:added [] :removed []}                       ; same root: zero reads
       (let [cmp    (.comparator b)
             ;; roots go through `root()` rather than a bare restore: it stamps
             ;; the projection comparator that diff-buf needs on descent.
             root-a (.root a)
             root-b (.root b)]
         (loop [fa [[root-a nil nil addr-a true]] la (.level ^ANode root-a)
                fb [[root-b nil nil addr-b true]] lb (.level ^ANode root-b)
                ca [] cb []]
           (let [la (if (clojure.core/seq fa) (long la) -1)
                 lb (if (clojure.core/seq fb) (long lb) -1)]
             (if (and (neg? la) (neg? lb))
               {:added (sorted-diff cmp cb ca) :removed (sorted-diff cmp ca cb)}
               ;; addresses only mean the same thing at the same level, and a
               ;; shared node keeps its level, so pruning across unequal levels
               ;; would find nothing. Walk the deeper side down until they meet.
               (let [[fa fb]   (if (== la lb) (prune-shared fa fb) [fa fb])
                     la        (if (clojure.core/seq fa) la -1)
                     lb        (if (clojure.core/seq fb) lb -1)
                     down-a?   (and (>= la 0) (>= la lb))
                     down-b?   (and (>= lb 0) (>= lb la))
                     [fa' ca'] (if down-a? (descend storage fa la ca) [fa ca])
                     [fb' cb'] (if down-b? (descend storage fb lb cb) [fb cb])]
                 (recur fa' (if down-a? (dec la) la)
                        fb' (if down-b? (dec lb) lb)
                        ca' cb'))))))))))
