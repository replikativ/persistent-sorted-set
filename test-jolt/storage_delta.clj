(ns storage-delta
  (:require [clojure.test :refer [deftest is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.test.portable-storage :refer [backend]]
            [org.replikativ.persistent-sorted-set.impl.measure :as measure]))

(deftest cold-delta
  (let [disk (atom {}) reads (atom []) storage (backend disk reads)
        a (reduce s/conj (s/sorted-set* {:branching-factor 4 :storage storage}) (range 80))
        ra (s/store a)
        previous (set (keys @disk))
        b (s/conj (s/restore ra storage) 80)
        rb (s/store b)
        x (s/restore ra storage)
        y (s/restore rb storage)
        visited (atom [])
        opts {:sync? true :await-read-through? true}]
    (reset! reads [])
    (is (nil? (s/walk-delta x (s/restore ra storage) #(swap! visited conj %))))
    (is (empty? @reads))
    (is (empty? @visited))
    (is (nil? (s/walk-delta x y storage #(do (swap! visited conj %) nil) opts)))
    (is (= (set (remove previous (keys @disk))) (set @visited)))
    (is (< 1 (count @visited) (count previous)))
    (is (< (count @reads) (count previous)))
    (is (every? #(= opts (second %)) @reads))
    (is (= (range 80) (seq x)))
    (is (= (range 81) (seq y)))
    (is (= 81 (count y)))
    (let [old (s/restore ra storage)
          new (s/restore rb storage)]
      (reset! reads [])
      (is (= [79 78 77] (vec (s/rslice old nil 77))))
      (is (< 0 (count @reads) (count previous)))
      (is (= [80 79 78 77] (vec (s/rslice new nil 77)))))
    (is (= [77 78 79 80] (vec (s/slice y 77 nil))))
    (is (thrown? Throwable (s/walk-delta x y storage identity {:sync? false})))))

(deftest cold-mutations
  (let [disk (atom {}) storage (backend disk (atom []))
        cmp (fn [a b] (compare (first a) (first b)))
        items (mapv #(vector % :old) (range 64))
        original (reduce s/conj (s/sorted-set* {:branching-factor 4 :storage storage}) items)
        open #(s/restore % storage {:meta {:index :test}})
        a (open (s/store original))
        b (s/replace a [31 :probe] [31 :new] cmp)
        c (s/disj (open (s/store b)) [15 :probe] cmp {:sync? true})
        restored (open (s/store c))
        empty-set (reduce #(s/disj %1 %2 {:sync? true}) restored (seq restored))]
    (is (identical? a (s/disj a [70 :old])))
    (is (identical? a (s/replace a [70 :old] [70 :old] {:sync? true})))
    (is (thrown? Throwable (s/replace a [31 :old] nil)))
    (is (thrown? Throwable (s/disj a [31 :old] {:sync? false})))
    (is (= (meta a) (meta c)))
    (is (= items (seq (open (s/store a)))))
    (is (= (assoc items 31 [31 :new]) (seq (open (s/store b)))))
    (is (= (remove #(= 15 (first %)) (assoc items 31 [31 :new])) (seq restored)))
    (is (= 63 (count restored)))
    (is (empty? (seq (open (s/store empty-set)))))
    (is (zero? (count empty-set)))))

(deftest weighted-cold-rank
  (let [reads (atom [])
        backend (backend (atom {}) reads)
        ops (reify measure/IMeasure
              (identity-measure [_] 0)
              (extract [_ k] (mod k 4))
              (merge-measure [_ a b] (+ a b))
              (remove-measure [_ m k _] (- m (mod k 4)))
              (weight [_ m] m))
        settings {:branching-factor 4 :storage backend :measure ops}
        a (reduce s/conj (s/sorted-set* settings) (range 64))
        b (s/disj a 31)]
    (doseq [[tree keys] [[a (range 64)] [b (remove #{31} (range 64))]]]
      (let [address (s/store tree)
            open #(s/restore address backend settings)
            expected (vec (mapcat (fn [k] (map #(vector k %) (range (mod k 4)))) keys))]
        (reset! reads [])
        (is (= (nth expected 45) (s/get-nth (open) 45)))
        (is (<= 2 (count @reads) 6))
        (let [cold (open)]
          (is (= expected (mapv #(s/get-nth cold %) (range (count expected)))))
          (is (nil? (s/get-nth cold -1)))
          (is (nil? (s/get-nth cold (count expected)))))))))

(deftest retained-addresses
  (let [disk (atom {}) reads (atom []) storage (backend disk reads)
        open #(s/restore % storage)
        opts {:sync? true :await-read-through? true}
        original (reduce s/conj (s/sorted-set* {:branching-factor 4 :storage storage}) (range 80))
        old (s/store original)
        edited (reduce s/disj (open old) (range 24 52))
        walk (fn [tree descend?]
               (let [seen (atom [])]
                 (is (nil? (s/walk-addresses tree #(do (swap! seen conj %) (descend? %)) opts)))
                 @seen))
        ;; The backend records child addresses before either root is walked.
        reachable (fn [root] (tree-seq #(seq (:addresses (@disk %))) #(:addresses (@disk %)) root))
        stored-before (set (walk edited (constantly true)))
        new (s/store edited)
        before (set (reachable old))
        after (set (reachable new))
        skipped (first (:addresses (@disk old)))]
    (is (seq stored-before))
    (is (= (set (filter before after)) stored-before))
    (is (some #(not (after %)) before))
    (doseq [r [old new]]
      (is (= (frequencies (reachable r)) (frequencies (walk (open r) (constantly true))))))
    (reset! reads [])
    (is (nil? (s/walk-addresses (open old) #(do (is (= old %)) false))))
    (is (empty? @reads))
    (is (seq (:addresses (@disk skipped))))
    (let [descendants (set (rest (reachable skipped)))]
      (is (= (frequencies (remove descendants (reachable old)))
             (frequencies (walk (open old) #(not= skipped %)))))
      (is (not-any? #(= skipped (first %)) @reads))
      (is (every? #(= opts (second %)) @reads)))
    (is (thrown? Throwable (s/walk-addresses (open old) identity {:sync? false})))))

(deftest diff-of-retained-roots
  (let [disk (atom {}) reads (atom []) storage (backend disk reads)
        opts {:sync? true :await-read-through? true}
        open #(s/restore % storage)
        a (s/store (reduce s/conj (s/sorted-set* {:branching-factor 4 :storage storage})
                           (range 0 256 2)))
        b (s/store (-> (open a) (s/disj 100) (s/conj 101)))
        reachable #(tree-seq (fn [k] (seq (:addresses (@disk k))))
                             (fn [k] (:addresses (@disk k))) %)
        old (set (reachable a))
        new (set (reachable b))
        changed (concat (remove old new) (remove new old))]
    (reset! reads [])
    (is (= {:added [] :removed []} (s/diff (open a) (open a))))
    (is (empty? @reads))
    (is (= {:added [101] :removed [100]} (s/diff (open a) (open b) storage opts)))
    (is (= (frequencies changed) (frequencies (map first @reads))))
    (is (< (count @reads) (count old)))
    (is (every? #(= opts (second %)) @reads))
    (is (= {:added [100] :removed [101]} (s/diff (open b) (open a) storage)))
    (is (thrown? Throwable (s/diff (open a) (open b) storage {:sync? false})))))
