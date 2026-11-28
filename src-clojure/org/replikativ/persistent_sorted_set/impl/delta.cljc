(ns org.replikativ.persistent-sorted-set.impl.delta
  #?(:cljs (:require-macros [org.replikativ.persistent-sorted-set.macros :refer [async+sync]]))
  (:require #?(:clj [org.replikativ.persistent-sorted-set.macros :refer [async+sync]])
            [is.simm.partial-cps.async #?@(:cljs [:refer [await] :refer-macros [async]]
                                           :clj [:refer [async await]])]
            [org.replikativ.persistent-sorted-set.arrays :as arrays]
            [org.replikativ.persistent-sorted-set.branch :as branch #?@(:cljs [:refer [Branch]])]
            [org.replikativ.persistent-sorted-set.impl.node :as node]))

(defn child-refs
  "Frontier entries for every child of `branches` (already materialized). Pure."
  [branches]
  (into []
        (mapcat (fn [[node]]
                  (let [^Branch b node
                        addrs (.-addresses b)
                        ;; diff-buf: a branch buffers a child's changes in its OWN
                        ;; slots and leaves the child's ADDRESS untouched, so for a
                        ;; buffered child an address match no longer proves the
                        ;; subtrees are equal. The guard is per CHILD: a nil slot
                        ;; means that child has no buffered diff. A non-nil slot is
                        ;; unprunable whatever its shape — for a BRANCH child :diff
                        ;; is nil and the real diff lives in the subtree, so a nil
                        ;; :diff does not mean no change.
                        slots (.-_slots b)]
                    (map (fn [i]
                           [nil b i
                            (when addrs (arrays/aget addrs i))
                            (or (nil? slots) (nil? (arrays/aget slots i)))])
                         (range (arrays/alength (.-keys b)))))))
        branches))

(defn prune
  "Drop from each frontier the entries the other side holds at the same address —
   identical subtrees, which cannot contain a difference. Pure, no IO."
  [fa fb]
  (let [addrs   (fn [f] (into #{} (keep (fn [[_ _ _ addr prunable?]]
                                          (when (and (some? addr) prunable?) addr)))
                              f))
        sa      (addrs fa)
        sb      (addrs fb)
        shared? (fn [other] (fn [[_ _ _ addr prunable?]]
                              (and (some? addr) prunable? (contains? other addr))))]
    ;; an entry the OTHER side marked unprunable never entered its address set, so
    ;; neither side prunes against a buffered branch.
    [(into [] (remove (shared? sb)) fa)
     (into [] (remove (shared? sa)) fb)]))

(defn- walk-delta-descend
  "Materialize one frontier and return its children without retaining keys.
   `on-address` is nil on the old side and observational on the new side."
  [storage refs level on-address {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [n (count refs)
                     nodes
                     (loop [i 0 nodes []]
                       (if (< i n)
                         (let [[nd parent idx :as ref] (nth refs i)]
                           (if (some? nd)
                             (recur (inc i) (conj nodes ref))
                             (recur (inc i)
                                    (conj nodes
                                          (assoc ref 0
                                                 (await (branch/child parent storage idx opts)))))))
                         nodes))]
                 (when on-address
                   (doseq [[_ _ _ address] nodes]
                     (when (some? address)
                       (on-address address))))
                 (if (zero? level) [] (child-refs nodes))))))

(defn walk-delta
  "Restore and visit the stored nodes in `b` that cannot be pruned as subtrees
   shared with `a`.

   Uses the same paired-frontier and diff-buffer-aware pruning rules as `diff`,
   but does not retain or compare elements. `on-address` observes successfully
   materialized new-side addresses; its return value is ignored so callback return
   values cannot accidentally prune an incomplete hydration. Old-side restores
   are not reported.

   Returns nil when synchronous, or a continuation yielding nil under
   `{:sync? false}`."
  [root-of a b storage on-address {:keys [sync?] :or {sync? true} :as opts}]
  (async+sync sync?
              (async
               (let [addr-a (.-address a)
                     addr-b (.-address b)]
                 (when-not (and (some? addr-a) (= addr-a addr-b))
                   (let [root-a (await (root-of a opts))
                         root-b (await (root-of b opts))]
                     (loop [fa [[root-a nil nil addr-a true]] la (node/level root-a)
                            fb [[root-b nil nil addr-b true]] lb (node/level root-b)]
                       (let [la (if (seq fa) la -1)
                             lb (if (seq fb) lb -1)]
                         (when-not (and (neg? la) (neg? lb))
                           (let [[fa fb] (if (== la lb) (prune fa fb) [fa fb])
                                 la      (if (seq fa) la -1)
                                 lb      (if (seq fb) lb -1)
                                 down-a? (and (>= la 0) (>= la lb))
                                 down-b? (and (>= lb 0) (>= lb la))
                                 fa'     (if down-a?
                                           (await (walk-delta-descend storage fa la nil opts))
                                           fa)
                                 fb'     (if down-b?
                                           (await (walk-delta-descend storage fb lb on-address opts))
                                           fb)]
                             (recur fa' (if down-a? (dec la) la)
                                    fb' (if down-b? (dec lb) lb))))))))))))
