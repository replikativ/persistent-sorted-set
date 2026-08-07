(ns org.replikativ.persistent-sorted-set.leaf
  (:require-macros [org.replikativ.persistent-sorted-set.macros :refer [async+sync]])
  (:require [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [goog.array :as garr]
            [org.replikativ.persistent-sorted-set.arrays :as arrays]
            [org.replikativ.persistent-sorted-set.impl.node :as node :refer [INode]]
            [org.replikativ.persistent-sorted-set.impl.measure :as measure]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage]
            [org.replikativ.persistent-sorted-set.impl.boundary :as b]
            [org.replikativ.persistent-sorted-set.util :as util]))

(deftype Leaf [keys settings ^:mutable _measure]
  Object
  (toString [_] (pr-str* (vec keys)))
  INode
  (len [_] (arrays/alength keys))
  (level [_] 0)
  (max-key [_] (arrays/alast keys))
  (subtree-count [_] (arrays/alength keys))
  (measure [_] _measure)
  (try-compute-measure [this storage measure-ops {:keys [sync?] :or {sync? true}}]
    ;; For leaves, try and force are the same - just compute from keys
    (if sync?
      (when measure-ops
        (let [result (reduce (fn [acc key]
                               (measure/merge-measure measure-ops acc (measure/extract measure-ops key)))
                             (measure/identity-measure measure-ops)
                             keys)]
          (set! _measure result)
          result))
      (async
       (when measure-ops
         (let [result (reduce (fn [acc key]
                                (measure/merge-measure measure-ops acc (measure/extract measure-ops key)))
                              (measure/identity-measure measure-ops)
                              keys)]
           (set! _measure result)
           result)))))
  (force-compute-measure [this storage measure-ops opts]
    ;; For leaves, try and force are the same - just compute from keys
    (node/try-compute-measure this storage measure-ops opts))
  ;; merge / merge-split must CARRY the measure, not leave it nil.
  ;;
  ;; The old comment here said "Measure will be recomputed lazily if needed" — and
  ;; nothing recomputes it. `try-compute-measure` computes on demand for the node it
  ;; is called on; a BRANCH above a nil-measure child can only POSTPONE, so one merged
  ;; leaf erases the aggregate for its whole spine. The JVM never had this: every
  ;; merge/borrow arm of `Leaf.remove` does `join._measure = join.tryComputeMeasure(...)`
  ;; whenever the source leaf had one.
  ;;
  ;; Measured with a per-level census (test.measure-population), 200 elements built
  ;; eagerly with a measure and then removed from — the JVM held a measure on every
  ;; node at every level, ClojureScript came back
  ;;     bf  8, dropped  50: level 1: 2 of 5 without, level 0: 2 of 25 without
  ;;     bf 16, dropped 137: level 1 (the ROOT): without
  ;; A leaf computes from its own keys, so `storage` is not needed and this cannot
  ;; postpone. Guarded on the source leaf's measure, exactly as the JVM guards.
  (merge [_ next]
    (let [new-leaf (Leaf. (arrays/aconcat keys (.-keys next)) settings nil)
          measure-ops (:measure settings)]
      (when (and measure-ops _measure)
        (set! (.-_measure new-leaf)
              (node/try-compute-measure new-leaf nil measure-ops {:sync? true})))
      new-leaf))
  (merge-split [_ next]
    (let [ks (util/merge-n-split keys (.-keys next))
          measure-ops (:measure settings)
          l0 (Leaf. (arrays/aget ks 0) settings nil)
          l1 (Leaf. (arrays/aget ks 1) settings nil)]
      (when (and measure-ops _measure)
        (set! (.-_measure l0) (node/try-compute-measure l0 nil measure-ops {:sync? true}))
        (set! (.-_measure l1) (node/try-compute-measure l1 nil measure-ops {:sync? true})))
      (util/return-array l0 l1)))
  (add [this storage key cmp {:keys [sync?] :or {sync? true}}]
    (let [branching-factor (:branching-factor settings)
          measure-ops (:measure settings)
          bd               (b/content-boundary settings)
          idx              (util/binary-search-l cmp keys (dec (arrays/alength keys)) key)
          keys-l           (arrays/alength keys)
          result           (cond
                             (and (< idx keys-l) (== 0 (cmp key (arrays/aget keys idx))))
                             nil

                             ;; split-seam (MST): build the full run, then cut at boundary keys.
                             ;; Identical to the JVM Leaf.add seam path; key-level is shared cljc.
                             bd
                             (let [all-keys (util/splice keys idx idx (arrays/array key))
                                   total    (arrays/alength all-keys)
                                   lens     (b/-split-on-insert bd all-keys total idx 0)]  ; idx = insert pos
                               (if (nil? lens)
                                 (let [lf (Leaf. all-keys settings nil)]
                                   (when (and measure-ops _measure)
                                     (node/try-compute-measure lf nil measure-ops {:sync? true}))
                                   (arrays/array lf))
                                 (loop [out (transient []), pos 0, ls lens]
                                   (if (seq ls)
                                     (let [l  (first ls)
                                           lf (Leaf. (.slice all-keys pos (+ pos l)) settings nil)]
                                       (when (and measure-ops _measure)
                                         (node/try-compute-measure lf nil measure-ops {:sync? true}))
                                       (recur (conj! out lf) (+ pos l) (next ls)))
                                     (arrays/into-array (persistent! out))))))

                             (== keys-l branching-factor)
                             (let [middle (arrays/half (inc keys-l))
                                   left-leaf (if (> idx middle)
                                               (Leaf. (.slice keys 0 middle) settings nil)
                                               (Leaf. (util/cut-n-splice keys 0 middle idx idx (arrays/array key)) settings nil))
                                   right-leaf (if (> idx middle)
                                                (Leaf. (util/cut-n-splice keys middle keys-l idx idx (arrays/array key)) settings nil)
                                                (Leaf. (.slice keys middle keys-l) settings nil))]
                               ;; Compute measure for split leaves only if already computed
                               (when (and measure-ops _measure)
                                 (node/try-compute-measure left-leaf nil measure-ops {:sync? true})
                                 (node/try-compute-measure right-leaf nil measure-ops {:sync? true}))
                               (arrays/array left-leaf right-leaf))

                             :else
                             (let [new-keys (util/splice keys idx idx (arrays/array key))
                                   new-leaf (Leaf. new-keys settings nil)]
                               ;; Update measure incrementally only if we already have measure.
                               ;; If _measure is nil (e.g. from merge/merge-split), leave nil for lazy recomputation.
                               (when (and measure-ops _measure)
                                 (set! (.-_measure new-leaf)
                                       (measure/merge-measure measure-ops _measure (measure/extract measure-ops key))))
                               (arrays/array new-leaf)))]
      (if sync?
        result
        (async result))))
  ($contains? [this storage key cmp {:keys [sync?] :or {sync? true}}]
    (async+sync sync?
                (async (<= 0 ^number (garr/binarySearch keys key cmp)))))
  ($count [this storage {:keys [sync?] :or {sync? true}}]
    (if sync?
      (alength keys)
      (async (alength keys))))
  (lookup [this storage key cmp {:keys [sync?] :or {sync? true}}]
    (async+sync sync?
                (async
                 (let [idx (garr/binarySearch keys key cmp)]
                   (when (<= 0 idx)
                     (arrays/aget keys idx))))))
  ;; `removed-out`, when given, is a 1-element array this fills with the element
  ;; ACTUALLY removed — the branch above needs it for the same reason this leaf does
  ;; (see below), and cannot know it otherwise. Mirrors `$replace`'s channel.
  ($remove [this storage key left right cmp {:keys [sync? removed-out] :or {sync? true}}]
    (let [root? (and (nil? left) (nil? right))
          measure-ops (:measure settings)
          idx   (garr/binarySearch keys key cmp)]
      (async+sync sync?
                  (async
                   (when (<= 0 idx)
                     (let [;; the element the leaf ACTUALLY holds, not the caller's `key`:
                           ;; `cmp` may be coarser than the set's comparator, in which case
                           ;; `key` is merely comparator-equal to it and `remove-measure`
                           ;; would subtract the wrong contribution. Measured on the JVM twin
                           ;; (200 longs, decade comparator): an EMPTY set with a cached sum
                           ;; of 24.0 instead of 0. `node->map` serializes `:measure`, so it
                           ;; is durable and changes the node's content address.
                           ;;
                           ;; ClojureScript is hit on EVERY remove, not just a transient one:
                           ;; this is the only remove path here, and unlike the JVM's
                           ;; persistent branch it does not recompute from the new keys.
                           removed-element (arrays/aget keys idx)
                           _ (when removed-out (aset removed-out 0 removed-element))
                           new-keys (util/splice keys idx (inc idx) (arrays/array))
                           new-leaf (Leaf. new-keys settings nil)]
                       ;; Update measure only if already computed
                       (when (and measure-ops _measure)
                         (set! (.-_measure new-leaf)
                               (measure/remove-measure measure-ops _measure removed-element
                                                       #(node/try-compute-measure new-leaf storage measure-ops {:sync? true}))))
                       (util/rotate new-leaf root? left right settings)))))))
  ;; `removed-out`, when given, is a 1-element array this fills with the element
  ;; ACTUALLY removed. A diff-buf parent needs it to deposit Absent(<what the leaf
  ;; really held>), which is not the caller's `old-key` once `cmp` is coarser than
  ;; the set's comparator. Reported rather than searched for from the parent so the
  ;; comparator-bound binary search runs once, not twice (mirrors ANode's overload
  ;; on the JVM, where the second search measured +18% at bf 512).
  ($replace [this storage old-key new-key cmp {:keys [sync? removed-out] :or {sync? true}}]
    (assert (== 0 (cmp old-key new-key)) "old-key and new-key must compare as equal (cmp must return 0)")
    (async+sync sync?
                (async
                 (let [idx (garr/binarySearch keys old-key cmp)]
                   (when (<= 0 idx)
                     ;; PRECONDITION, checked only when assertions are live (release
                     ;; builds elide it): `replace` writes new-key into the OLD
                     ;; element's slot, which keeps the set sorted only while the
                     ;; replacement belongs in that same position. If the leaf holds
                     ;; another element `cmp` calls equal, binarySearch picks an
                     ;; ARBITRARY one and writing over it can move it past a sibling,
                     ;; leaving the set UNSORTED. See the Java `noEqualSibling`.
                     (assert (not (or (and (< 0 idx)
                                           (== 0 (cmp (arrays/aget keys (dec idx)) old-key)))
                                      (and (< idx (dec (arrays/alength keys)))
                                           (== 0 (cmp (arrays/aget keys (inc idx)) old-key)))))
                             (str "replace(" (pr-str old-key) " -> " (pr-str new-key) "): the leaf holds"
                                  " another element the operation comparator calls equal, so which one is"
                                  " replaced is arbitrary and the result may be UNSORTED."
                                  " `replace` requires at most one cmp-equal element per leaf;"
                                  " use disj+conj instead."))
                     (when removed-out (aset removed-out 0 (arrays/aget keys idx)))
                     (let [new-keys (arrays/aclone keys)
                           _        (aset new-keys idx new-key)
                           new-leaf (Leaf. new-keys settings nil)
                           measure-ops (:measure settings)]
                       ;; Eagerly maintain measure: compute from new leaf (which has replacement done)
                       (when (and measure-ops _measure)
                         (set! (.-_measure new-leaf)
                               ;; Compute from new-leaf which has new-key instead of old-key
                               (node/try-compute-measure new-leaf storage measure-ops {:sync? true})))
                       (arrays/array new-leaf)))))))
  (store [this storage {:keys [sync?] :or {sync? true} :as opts}]
    (async+sync sync?
                (async
                 (await (storage/store storage this opts)))))
  (walk-addresses [this storage on-address {:keys [sync?] :or {sync? true}}]
    (when-not sync? (async))))
