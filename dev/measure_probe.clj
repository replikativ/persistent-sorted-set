(ns measure-probe
  (:require [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
  (:import [org.replikativ.persistent_sorted_set IMeasure IStorage]))

;; stratum-like: elements are [k payload]; ordered by k; measure n/sum/min/max of payload
(deftype StatOps []
  IMeasure
  (identity [_] {:n 0 :sum 0 :min Long/MAX_VALUE :max Long/MIN_VALUE})
  (extract  [_ [_ p]] {:n 1 :sum p :min p :max p})
  (merge    [_ a b] {:n (+ (:n a) (:n b)) :sum (+ (:sum a) (:sum b))
                     :min (min (:min a) (:min b)) :max (max (:max a) (:max b))})
  (remove   [_ cur [_ p] recompute]
    (if (or (= p (:min cur)) (= p (:max cur)))
      (when recompute (.get ^java.util.function.Supplier recompute))
      {:n (dec (:n cur)) :sum (- (:sum cur) p) :min (:min cur) :max (:max cur)}))
  (weight   [_ m] (long (:n m))))
(def ops (StatOps.))
(defn cmp [a b] (compare (nth a 0) (nth b 0)))
(defrecord St [*disk *reads *leaf-reads ctx]
  IStorage
  (store [_ node] (let [a (str (java.util.UUID/randomUUID))] (swap! *disk assoc a (nodes/node->blob node)) a))
  (accessed [_ _] nil)
  (restore [_ addr]
    (swap! *reads inc)
    (let [blob (get @*disk addr)]
      (if (:addresses blob) (nodes/blob->branch ctx blob)
          (do (swap! *leaf-reads inc) (nodes/blob->leaf ctx blob)))))
  (markFreed [_ _] nil) (isFreed [_ _] false) (freedInfo [_ _] nil))

(defn census [st addr]
  (let [disk @(:*disk st) acc (volatile! {:branches 0 :nil 0})]
    ((fn walk [a] (let [b (get disk a)]
                    (when (:addresses b)
                      (vswap! acc update :branches inc)
                      (when (nil? (:measure b)) (vswap! acc update :nil inc))
                      (when (nil? (:child-measures b)) (vswap! acc update :nocm (fnil inc 0)))
                      (run! walk (:addresses b))))) addr)
    @acc))

(defn run [bf n label f]
  (let [ctx (nodes/reader-context {:measure-ops ops :default-bf bf})
        o {:comparator cmp :branching-factor bf :measure ops :ref-type :weak}
        st (->St (atom {}) (atom 0) (atom 0) ctx)
        s0 (reduce #(set/conj %1 %2 cmp) (set/sorted-set* o) (map (fn [k] [(* 2 k) (mod (* k 7919) 1000)]) (range n)))
        a0 (set/store s0 st)
        cold #(set/restore-by cmp % st (assoc o :storage st))
        before (census st a0)
        r0 @(:*reads st)
        s1 (f (cold a0))
        wr (- @(:*reads st) r0)
        a1 (set/store s1 st)
        after (census st a1)
        c (cold a1) r1 @(:*reads st) l1 @(:*leaf-reads st)
        m (set/measure c)
        mr (- @(:*reads st) r1) ml (- @(:*leaf-reads st) l1)
        c3 (cold a1) r3 @(:*reads st) _ (set/get-nth c3 (quot (count c3) 3)) nr (- @(:*reads st) r3)
        truth (reduce (fn [t [_ p]] (.merge ^IMeasure ops t {:n 1 :sum p :min p :max p})) (.identity ^IMeasure ops) (seq c))]
    (println (format "bf %3d %-22s write-reads %5d  stored branches w/o measure %4d/%4d (before %d)  cold measure: %5d reads (%d leaves)  get-nth %3d reads  no-array %s  correct %s"
                     bf label wr (:nil after) (:branches after) (:nil before) mr ml nr (:nocm after 0) (= m truth)))))

(defn -main [& _]
  (doseq [bf [16 64]]
    (let [n 100000 ks (map #(* 2 (* 487 %)) (range 1 201))]
      (run bf n "none" identity)
      (run bf n "200 conj"    (fn [s] (reduce #(set/conj %1 [(inc %2) 5] cmp) s ks)))
      (run bf n "200 replace" (fn [s] (reduce #(set/replace %1 [%2 0] [%2 500] cmp) s ks)))
      (run bf n "200 disj"    (fn [s] (reduce #(set/disj %1 [%2 0] cmp) s ks)))
      (run bf n "3000 conj (splits)" (fn [s] (reduce #(set/conj %1 [(inc (* 2 %2)) 5] cmp) s (range 20000 23000))))
      (run bf n "mixed 200 each" (fn [s] (as-> s s
                                           (reduce #(set/conj %1 [(inc %2) 5] cmp) s ks)
                                           (reduce #(set/replace %1 [(+ 2 %2) 0] [(+ 2 %2) 500] cmp) s ks)
                                           (reduce #(set/disj %1 [(+ 4 %2) 0] cmp) s ks)))))))
