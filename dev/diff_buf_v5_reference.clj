(ns diff-buf-v5-reference
  "Executable spec for DIFF_BUF_V5 (doc/DIFF_BUF_V5.md): per-node nested diffs at the
   serialization boundary.

   Self-contained small B-tree + diff layer. The durable representation is the
   last-flushed structure (frozen) plus per-leaf logical ops (Present/Absent),
   embedded nested in branch objects; a node whose subtree op-count exceeds the
   budget B is FLUSHED (re-snapshotted into a fresh balanced subtree) on store.
   restore deserializes and materialize re-derives content (splits/merges fall
   out of re-snapshot on flush; over/under-full materialized nodes are tolerated
   at the content level here — the Java port enforces real B-tree structure).

   This validates the CORE transport soundness of the design:
   - CI   : content exact across restore->mutate->store->restore cycles
   - AGG  : per-node count snapshot exact, readable for a cold (un-restored) child
   - count: exact via present-checked incremental maintenance
   - BI   : flush keeps each stored object's embedded op-count <= B
   - recency (within a leaf): op over base, latest-wins
   Cross-level recency, peel-one-level write counts, and real split/merge sibling
   loads are validated in the Java port (they don't affect content soundness).

   Run: (diff-buf-v5-reference/run-all)")

(def ^:dynamic *bf* 4)   ; branching factor
(def ^:dynamic *B* 6)    ; per-stored-object embedded op budget

;; ---------- in-memory tree: frozen base + leaf ops; branch structure frozen between flushes ----------
;; leaf   {:t :leaf   :base <sorted vec>  :ops {k true|false}  :cnt n}   ; true=Present false=Absent
;; branch {:t :branch :children [nodes]   :cnt n}

(defn leaf [base] (let [b (vec (sort base))] {:t :leaf :base b :ops {} :cnt (count b)}))

(defn mat-keys [node]
  (case (:t node)
    :leaf   (vec (reduce (fn [s [k p]] (if p (conj s k) (disj s k)))
                         (apply sorted-set (:base node)) (:ops node)))
    :branch (vec (mapcat mat-keys (:children node)))))

(defn mx [node] (let [ks (mat-keys node)] (when (seq ks) (peek ks))))
(defn present? [node k] (contains? (set (mat-keys node)) k))

(defn- child-idx [node k]
  (let [cs (:children node) n (count cs)]
    (loop [i 0]
      (if (>= i (dec n)) i
          (let [m (mx (nth cs i))]
            (if (and m (>= (compare m k) 0)) i (recur (inc i))))))))

(defn ins [node k]
  (case (:t node)
    :leaf   (let [d (if (present? node k) 0 1)]
              [(-> node (update :ops assoc k true) (update :cnt + d)) d])
    :branch (let [i (child-idx node k)
                  [c' d] (ins (nth (:children node) i) k)]
              [(-> node (update :children assoc i c') (update :cnt + d)) d])))

(defn del [node k]
  (case (:t node)
    :leaf   (let [d (if (present? node k) 1 0)]
              [(-> node (update :ops assoc k false) (update :cnt - d)) d])
    :branch (let [i (child-idx node k)
                  [c' d] (del (nth (:children node) i) k)]
              [(-> node (update :children assoc i c') (update :cnt - d)) d])))

(defn ndiff [node]
  (case (:t node)
    :leaf (count (:ops node))
    :branch (reduce + 0 (map ndiff (:children node)))))

;; ---------- build a fresh balanced tree from sorted keys (used by flush re-snapshot) ----------
(defn- build-up [nodes]
  (if (<= (count nodes) *bf*)
    (if (= 1 (count nodes)) (first nodes)
        {:t :branch :children (vec nodes) :cnt (reduce + 0 (map :cnt nodes))})
    (recur (mapv (fn [g] {:t :branch :children (vec g) :cnt (reduce + 0 (map :cnt g))})
                 (partition-all *bf* nodes)))))

(defn build [keys]
  (let [ks (vec (sort keys))]
    (if (<= (count ks) *bf*) (leaf ks)
        (build-up (mapv leaf (partition-all *bf* ks))))))

;; ---------- durable store (content-addressed by counter) ----------
;; stored leaf   {:t :leaf :base :ops :cnt}
;; stored branch {:t :branch :cnt :children [child...]}  child = stored-obj (buffered, inline) | {:ref addr}

(defn- put! [disk obj] (let [a (swap! (:ctr disk) inc)] (swap! (:m disk) assoc a obj) a))
(defn- get* [disk a] (get @(:m disk) a))
(defn mk-disk [] {:m (atom {}) :ctr (atom 0) :writes (atom 0)})

(defn- store-clean
  "Write node + descendants as separate clean objects (ops empty). Returns addr."
  [node disk]
  (swap! (:writes disk) inc)
  (case (:t node)
    :leaf   (put! disk {:t :leaf :base (mat-keys node) :ops {} :cnt (:cnt node)})
    :branch (let [refs (mapv (fn [c] {:ref (store-clean c disk)}) (:children node))]
              (put! disk {:t :branch :cnt (:cnt node) :children refs}))))

(defn- obj-ndiff [x]
  (cond (:ref x) 0
        (= :leaf (:t x)) (count (:ops x))
        :else (reduce + 0 (map obj-ndiff (:children x)))))

(declare ser)

(defn- ser-branch [node disk]
  ;; serialize children, then peel (flush) largest buffered children until embedded <= B
  (let [child-objs (mapv #(ser % disk) (:children node))]
    (loop [objs child-objs, kids (:children node)]
      (if (<= (reduce + 0 (map obj-ndiff objs)) *B*)
        {:t :branch :cnt (:cnt node) :children objs}
        ;; find index of buffered child with max embedded ndiff
        (let [i (apply max-key #(obj-ndiff (nth objs %)) (range (count objs)))]
          (if (zero? (obj-ndiff (nth objs i)))
            {:t :branch :cnt (:cnt node) :children objs}      ; nothing left to flush
            (recur (assoc objs i {:ref (store-clean (nth kids i) disk)}) kids)))))))

(defn ser
  "Return the stored representation of node (embeds buffered descendants);
   flushes (re-snapshots + writes) any subtree whose op-count exceeds B."
  [node disk]
  (if (> (ndiff node) *B*)
    (case (:t node)
      :leaf   {:ref (store-clean (build (mat-keys node)) disk)}   ; leaf over budget -> re-snapshot (may become a subtree)
      :branch (ser-branch node disk))
    (case (:t node)
      :leaf   {:t :leaf :base (:base node) :ops (:ops node) :cnt (:cnt node)}
      :branch {:t :branch :cnt (:cnt node) :children (mapv #(ser % disk) (:children node))})))

(defn store [node disk]
  (swap! (:writes disk) inc)
  (put! disk (ser node disk)))

(defn- deref* [disk x]
  (cond
    (number? x)             (recur disk (get* disk x))
    (and (map? x) (:ref x)) (recur disk (:ref x))
    :else                   x))

(defn restore [disk x]
  (let [obj (deref* disk x)]
    (case (:t obj)
      :leaf   {:t :leaf :base (:base obj) :ops (:ops obj) :cnt (:cnt obj)}
      :branch {:t :branch :cnt (:cnt obj)
               :children (mapv #(restore disk %) (:children obj))})))

;; AGG: count of a child WITHOUT restoring it (read the snapshot)
(defn cold-child-cnt [disk branch-addr i]
  (let [obj (deref* disk branch-addr)
        c (nth (:children obj) i)
        co (deref* disk c)]
    (:cnt co)))

;; ---------- probe gate ----------
(defn- max-embedded-opcount [disk addr]
  (let [obj (deref* disk addr)]
    (letfn [(walk [o] (cond (:ref o) 0
                            (= :leaf (:t o)) (count (:ops o))
                            :else (max (reduce + 0 (map #(if (:ref %) 0 (if (= :leaf (:t (deref* disk %)))
                                                                         (count (:ops (deref* disk %))) 0))
                                                        (:children o)))
                                       (apply max 0 (map #(walk (deref* disk %)) (:children o))))))]
      (walk obj))))

(defn multi-cycle
  ([] (multi-cycle 8))
  ([cycles]
   (let [disk (mk-disk)
         seed (range 200)
         t0   (reduce (fn [t k] (first (ins t k))) (leaf []) seed)
         ref0 (apply sorted-set seed)]
     (loop [cyc 0, addr (store t0 disk), ref ref0, ok true, notes []]
       (if (= cyc cycles)
         {:ok ok :notes notes :objects (count @(:m disk))}
         (let [t      (restore disk addr)
               adds   (range (+ 1000 (* cyc 100)) (+ 1000 (* cyc 100) 60))
               rems   (range (* cyc 11) (+ (* cyc 11) 25))
               t1     (reduce (fn [x k] (first (ins x k))) t adds)
               t2     (reduce (fn [x k] (first (del x k))) t1 rems)
               ref'   (as-> ref r (reduce conj r adds) (reduce disj r rems))
               addr'  (store t2 disk)
               back   (restore disk addr')
               okc    (and (= (mat-keys t) (vec ref))
                           (= (:cnt t) (count ref))
                           (= (mat-keys t2) (vec ref'))
                           (= (:cnt t2) (count ref'))
                           (= (mat-keys back) (vec ref'))
                           (= (:cnt back) (count ref')))]
           (recur (inc cyc) addr' ref' (and ok okc)
                  (conj notes {:cyc cyc :n (count ref') :ok okc}))))))))

(defn generative
  ([] (generative (range 6) 1500))
  ([seeds nops]
   (for [seed seeds]
     (let [rng (java.util.Random. seed), disk (mk-disk)]
       (loop [i 0, t (leaf []), ref (sorted-set), fails 0]
         (if (= i nops)
           (let [addr (store t disk) back (restore disk addr)]
             {:seed seed :n (count ref)
              :content (= (mat-keys back) (vec ref))
              :cnt (= (:cnt back) (count ref))
              :fails fails})
           (let [k (.nextInt rng 80)
                 [t' ref'] (if (< (.nextInt rng 10) 6)
                             [(first (ins t k)) (conj ref k)]
                             [(first (del t k)) (disj ref k)])
                 t'' (if (zero? (mod i 25)) (restore disk (store t' disk)) t')
                 ok (and (= (mat-keys t'') (vec ref')) (= (:cnt t'') (count ref')))]
             (recur (inc i) t'' ref' (if ok fails (inc fails))))))))))

;; BI: every stored disk object's embedded (inline, non-:ref) op-count <= B
(defn- obj-embedded-ops [obj]
  (cond (:ref obj) 0
        (= :leaf (:t obj)) (count (:ops obj))
        (= :branch (:t obj)) (reduce + 0 (map obj-embedded-ops (:children obj)))
        :else 0))

(defn bi-ok? [disk] (every? #(<= (obj-embedded-ops %) *B*) (vals @(:m disk))))

(defn probe-agg
  "AGG: a parent answers a child's count from the snapshot WITHOUT restoring the child,
   and it equals the materialized child count. Also assert BI holds on the disk."
  []
  (let [disk (mk-disk)
        t    (reduce (fn [x k] (first (ins x k))) (leaf []) (shuffle (range 300)))
        addr (store t disk)
        obj  (deref* disk addr)]
    (if (not= :branch (:t obj))
      {:agg :skip-root-not-branch}
      (let [n (count (:children obj))
            checks (for [i (range n)]
                     (let [cold (cold-child-cnt disk addr i)              ; no restore of child
                           warm (count (mat-keys (restore disk (nth (:children obj) i))))]
                       (= cold warm)))]
        {:agg (every? true? checks) :children n :bi (bi-ok? disk)}))))

(defn probe-recency
  "Recency: add k -> store (flush, low B) -> remove k -> store -> restore => k absent."
  []
  (binding [*B* 2]
    (let [disk (mk-disk)
          t0   (reduce (fn [x k] (first (ins x k))) (leaf []) (range 100))
          a0   (store t0 disk)                       ; k=42 baked into a flushed leaf
          t1   (first (del (restore disk a0) 42))
          a1   (store t1 disk)
          back (restore disk a1)]
      {:recency (and (not (contains? (set (mat-keys back)) 42))
                     (= (:cnt back) 99)
                     (= (mat-keys back) (vec (disj (apply sorted-set (range 100)) 42))))
       :bi (bi-ok? disk)})))

(defn run-all []
  {:multi-cycle (multi-cycle)
   :generative  (vec (generative))
   :agg         (probe-agg)
   :recency     (probe-recency)})
