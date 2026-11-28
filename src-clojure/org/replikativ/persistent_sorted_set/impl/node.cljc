(ns org.replikativ.persistent-sorted-set.impl.node
  (:refer-clojure :exclude [max-key merge]))

(defprotocol INode
  (len [this])
  (level [this])
  (max-key [this])
  (merge       [_ next])
  (merge-split [_ next])
  (add             [this storage key cmp opts])
  ($contains?      [this storage key cmp opts])
  ($count          [this storage opts])
  (lookup          [this storage key cmp opts])
  (subtree-count   [this])
  (measure         [this])
  (try-compute-measure  [this storage measure-ops opts])
  (force-compute-measure [this storage measure-ops opts])
  ($remove         [this storage key left right cmp opts])
  ($replace        [this storage old-key new-key cmp opts])
  (store           [this storage opts])
  (walk-addresses  [this storage on-address opts]))

(defn new-len [len _settings]
  len)

(def ^:const MIN-BRANCHING-FACTOR 4)
(def ^:const DEFAULT-BRANCHING-FACTOR 512)

(defn check-branching-factor
  "Normalize an unset branching factor to the default and refuse an unsupported one."
  [bf]
  (let [bf (if (or (nil? bf) (not (pos? bf))) DEFAULT-BRANCHING-FACTOR bf)]
    (when (< bf MIN-BRANCHING-FACTOR)
      (throw (ex-info (str "branching-factor " bf " is not supported: the minimum is "
                           MIN-BRANCHING-FACTOR ". Below it the minimum fill (bf >>> 1) is 1, "
                           "so a branch of length 1 is considered full, its only child has no "
                           "sibling to rebalance with, and a removal leaves a length-0 leaf. "
                           "Measured: this yields nil elements and a count that disagrees with "
                           "seq in ClojureScript, and throws ArrayIndexOutOfBoundsException on "
                           "the JVM. Omit :branching-factor for the default of "
                           DEFAULT-BRANCHING-FACTOR ".")
                      {:branching-factor bf :min MIN-BRANCHING-FACTOR})))
    bf))

(defn node-settings
  "The settings a node carries, normalized and validated exactly as the JVM's
   `Settings` constructor does."
  [opts]
  (-> (select-keys opts [:branching-factor :measure :boundary :diff-buf-size])
      (update :branching-factor check-branching-factor)
      (update :diff-buf-size (fn [d] (if (or (nil? d) (neg? d)) 0 d)))))
