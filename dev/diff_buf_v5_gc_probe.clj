(ns diff-buf-v5-gc-probe
  "Does markFreed stay correct under DIFF_BUF_V5? A buffered child re-points to its
   durable anchor, but the mutation path calls markFreed on that anchor. If the
   anchor is still reachable from the final root yet sits in the freed-set, an
   online GC would delete a LIVE node. This probe intersects the reachable set
   (walk-addresses) with everything markFreed during the build."
  (:require [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore]
            [clojure.set])
  (:import [org.replikativ.persistent_sorted_set Settings IStorage]
           [java.util Random]))

(def cmp (fn [a b] (compare (first a) (first b))))
(defn opbuf-settings ^Settings [bf b]
  (let [s (Settings. (int bf) nil nil nil (int b))]
    (set! (.-_comparator s) ^java.util.Comparator cmp) s))

;; storage that delegates store/restore to a tstore but RECORDS markFreed
(defn recording-storage [disk bf b freed]
  (let [inner (tstore/->Storage (atom {}) disk (opbuf-settings bf b))]
    (reify IStorage
      (comparator [_] cmp)
      (restore [_ a] (.restore inner a))
      (store   [_ n] (.store inner n))
      (accessed [_ a] (.accessed inner a))
      (markFreed [_ a] (swap! freed conj a))
      (isFreed [_ a] (contains? @freed a)))))

(defn run [seed bf b cycles ops keyrange]
  (let [rng (Random. seed)
        disk (atom {})
        freed (atom #{})
        mkst #(recording-storage disk bf b freed)
        s0 (reduce (fn [s i] (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor bf :diff-buf-size b}) (range 0 keyrange 2))]
    (loop [c 0 addr (ss/store s0 (mkst))]
      (if (= c cycles)
        (let [loaded (ss/restore-by cmp addr (mkst) {:branching-factor bf :diff-buf-size b :comparator cmp})
              reachable (atom #{})
              _ (ss/walk-addresses loaded (fn [a] (swap! reachable conj a)))
              live-freed (clojure.set/intersection @reachable @freed)
              all (set (keys @disk))
              ;; written objects that are neither reachable nor freed ⇒ GC can't collect ⇒ leak
              leaked (clojure.set/difference all @reachable @freed)]
          {:b b :reachable (count @reachable) :freed (count @freed) :written (count all)
           :LIVE-but-FREED (count live-freed)   ;; over-free (GC deletes live data) — must be 0
           :LEAKED (count leaked)})              ;; under-free (orphans never collected) — should be ~0
        (let [loaded (ss/restore-by cmp addr (mkst) {:branching-factor bf :diff-buf-size b :comparator cmp})
              ;; TRANSIENT batch (bulk-import style) → exercises the editable in-place
              ;; markFreed paths (Branch.java 394/574) that the persistent path skips.
              s2 (persistent! (reduce (fn [t _] (conj! t [(.nextInt rng keyrange) 0]))
                                      (transient loaded) (range ops)))]
          (recur (inc c) (ss/store s2 (mkst))))))))

(defn go []
  (doseq [b [0 64 512]]
    (println (format "diffBufSize=%-4d %s" b (pr-str (run 1 16 b 12 20 400))))))
