(ns org.replikativ.persistent-sorted-set.test.gc-leak
  "diff-buf markFreed-completeness leak probe (cljs analogue of the JVM ab.clj A/B).
   Builds a tree, then removes contiguous chunks (forcing leaf/branch merges), and measures
   leaked = (blobs ever written) - (reachable from final root) - (markFreed). With the
   structural-free fix, leaked == 0; without it, leaked is large. LIVE-FREED must be 0."
  (:require [cljs.test :refer-macros [deftest is]]
            [clojure.set]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage :refer [IStorage]]
            [org.replikativ.persistent-sorted-set.test.storage.util :as util]))

(def cmp (fn [a b] (compare (first a) (first b))))

(defn rec-storage [disk freed]
  (let [inner (util/storage (atom {}) disk {:comparator cmp :branching-factor 16 :diff-buf-size 256})]
    (reify IStorage
      (store    [_ n opts] (storage/store inner n opts))
      (restore  [_ a opts] (storage/restore inner a opts))
      (accessed [_ a] (storage/accessed inner a))
      (markFreed [_ a] (swap! freed conj a) nil)
      (isFreed  [_ a] (contains? @freed a))
      (freedInfo [_ a] nil))))

;; Reachable durable blobs = the addresses of the LIVE projected tree (matches the JVM GC oracle
;; and the corrected stress harness). An off-disk :addresses walk can diverge from the projected
;; structure under buffering — under-counting reachable, which both spuriously flags leaks AND can
;; SILENTLY miss a real over-free (freeing a still-reachable node). Walking the live tree avoids both.
(defn- reachable [disk root opts]
  (let [loaded (set/restore root (util/storage (atom {}) disk opts) opts)
        seen   (atom #{root})]                         ; the root address itself isn't a child of anything
    (set/walk-addresses loaded (fn [a] (swap! seen conj a) true))
    @seen))

(deftest gc-leak-structural
  (let [bf 16 b 256 disk (atom {}) freed (atom #{})
        opts {:comparator cmp :branching-factor bf :diff-buf-size b}
        s0 (reduce (fn [s i] (set/conj s [i 0])) (set/sorted-set* opts) (range 4000))
        a0 (set/store s0 (rec-storage disk freed) {:sync? true})
        final (loop [c 0 addr a0]
                (if (= c 40) addr
                    (let [loaded (set/restore addr (rec-storage disk freed) opts)
                          s2 (reduce (fn [s k] (set/disj s [k 0])) loaded
                                     (range (* c 40) (+ (* c 40) 40)))]   ; contiguous chunk ⇒ forces merges
                      (recur (inc c) (set/store s2 (rec-storage disk freed) {:sync? true})))))
        r       (reachable disk final opts)
        written (set (keys @disk))
        leaked  (clojure.set/difference written r @freed)
        live    (clojure.set/intersection r @freed)]
    (is (= 0 (count live)) "no live (reachable) node is ever markFreed (over-free)")
    (is (= 0 (count leaked))
        (str "every superseded blob is markFreed (markFreed completeness); leaked="
             (count leaked) " of written=" (count written)))))
