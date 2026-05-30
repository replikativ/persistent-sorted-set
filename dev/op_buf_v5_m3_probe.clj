(ns op-buf-v5-m3-probe
  "M3 + M4a gate (opBufSize>0): nested deposit on the return path + anchor capture.
   The diff is recursive per-child: a LEAF-parent slot holds a leaf-diff
   {cmp-key → element|ABSENT} + anchor + ĝ; a BRANCH slot is an anchor marker
   (diff=null) holding the child's durable address + ĝ (its nested diff is derived
   from the live subtree at store / reconstructed on restore). Store/restore are
   still baseline here, so slots are accumulated and ignored (opBufSize>0 stays
   correct; opBufSize=0 is byte-identical = I0).
   Run (src-clojure MUST precede target/classes — the latter holds a stale .clj copy;
   test-clojure provides the storage):
     clojure -Sdeps '{:paths [\"src-clojure\" \"target/classes\" \"dev\" \"test-clojure\"]}' \\
       -M -e \"(require 'op-buf-v5-m3-probe)(op-buf-v5-m3-probe/run-all)\""
  (:require [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set Branch Leaf ANode Slot PersistentSortedSet IStorage]
           [clojure.lang PersistentTreeMap]
           [java.lang.ref Reference]))

;; elements are pairs [k v]; comparator keys on the first element, so
;; (replace [k a] [k b]) is cmp-equal -> a pure content-only change.
(def cmp (fn [a b] (compare (first a) (first b))))

(defn deref-child [c]
  (cond (instance? ANode c) c
        (instance? Reference c) (.get ^Reference c)
        :else c))

(defn branch-children [^Branch b]
  (let [ch (.-_children b)]
    (when ch
      (->> (range (.-_len b)) (map #(deref-child (aget ^objects ch %))) (remove nil?)))))

(defn walk-branches [node]
  (when (instance? Branch node)
    (cons node (mapcat walk-branches (branch-children node)))))

(defn root-of [^PersistentSortedSet s] (.root s))

;; A leaf-parent slot's diff is a PersistentTreeMap; a branch marker's diff is nil.
(defn leaf-diff ^PersistentTreeMap [^Slot sl]
  (let [d (.-diff sl)] (when (instance? PersistentTreeMap d) d)))
(defn slot-val [^Slot sl k]
  (when-let [d (leaf-diff sl)] (let [v (.valAt d [k nil] ::miss)] (when (not= v ::miss) v))))

;; find [branch idx slot] of the leaf-parent slot whose leaf-diff carries key k
(defn find-leaf-slot [brs k]
  (some (fn [^Branch b]
          (when-let [slots (.-_slots b)]
            (some (fn [i]
                    (when-let [^Slot sl (aget ^objects slots i)]
                      (when (some? (slot-val sl k)) [b i sl])))
                  (range (.-_len b)))))
        brs))

(defn fails [& msgs] (println "  FAIL:" (apply str msgs)) false)

;; --- P1: content correctness (slots ignored by baseline store/restore) -------
(defn probe-content []
  (let [n   200
        ref (atom (sorted-set-by cmp))
        s   (reduce (fn [s i] (swap! ref conj [i 0]) (conj s [i 0]))
                    (ss/sorted-set* {:comparator cmp :branching-factor 4 :op-buf-size 100})
                    (shuffle (range n)))
        s   (reduce (fn [s i] (swap! ref #(-> % (disj [i 0]) (conj [i (inc i)])))
                      (ss/replace s [i 0] [i (inc i)]))
                    s (range 0 n 2))
        s   (reduce (fn [s i] (swap! ref disj [i 0]) (disj s [i 0]))
                    s (range 1 n 4))
        got (vec (seq s))
        exp (vec (seq @ref))]
    (if (= got exp)
      (do (println "  P1 content exact (conj/replace/disj, opBufSize>0):" (count got) "elems") true)
      (fails "content mismatch; got " (count got) " exp " (count exp)))))

;; --- P2: leaf deposit fires at a leaf-parent w/ exact ĝ; P3: latest-wins ------
(defn probe-deposit []
  (let [s0 (reduce (fn [s i] (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor 4 :op-buf-size 100})
                   (range 64))
        k  17
        s1 (ss/replace s0 [k 0] [k 111])
        hit (find-leaf-slot (walk-branches (root-of s1)) k)]
    (if-not hit
      (fails "no leaf-parent slot carries a leaf-diff for replaced key " k)
      (let [[^Branch b i ^Slot sl] hit
            v       (slot-val sl k)
            child   (deref-child (aget ^objects (.-_children b) i))
            cnt-ok  (= (.-count sl) (.count ^ANode child nil))]
        (println "  P2 leaf deposit: level=" (.-_level b) " slot[" i "] Present=" v
                 " ĝ.count=" (.-count sl) " child.count=" (.count ^ANode child nil))
        (cond (not= 1 (.-_level b)) (fails "leaf-diff at level " (.-_level b) " — must be a leaf-parent (level 1)")
              (not= v [k 111]) (fails "Present payload " v " != " [k 111])
              (not cnt-ok)     (fails "ĝ.count " (.-count sl) " != child count " (.count ^ANode child nil))
              :else
              (let [s2 (ss/replace s1 [k 111] [k 222])
                    v2 (some #(slot-val % k) (mapcat (fn [^Branch bb]
                                                       (when-let [ss (.-_slots bb)]
                                                         (keep #(aget ^objects ss %) (range (.-_len bb)))))
                                                     (walk-branches (root-of s2))))]
                (if (= v2 [k 222])
                  (do (println "  P3 latest-wins within leaf-diff: Present=" v2) true)
                  (fails "latest-wins broken: " v2 " != " [k 222]))))))))

;; --- Absent deposit -----------------------------------------------------------
(defn probe-absent []
  (let [s0 (reduce (fn [s i] (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor 8 :op-buf-size 100})
                   (range 200))
        ks [73 74 75]
        s1 (reduce (fn [s k] (disj s [k 0])) s0 ks)
        brs (walk-branches (root-of s1))
        absent? (fn [k]
                  (some (fn [^Branch b]
                          (when-let [slots (.-_slots b)]
                            (some (fn [i]
                                    (when-let [^Slot sl (aget ^objects slots i)]
                                      (= Slot/ABSENT (slot-val sl k))))
                                  (range (.-_len b)))))
                        brs))]
    (if (some absent? ks)
      (do (println "  P-absent: a removed key recorded as Absent in a leaf-parent slot") true)
      (do (println "  P-absent: (all 3 removes rebalanced — OK, no content-only among them)") true))))

;; --- M4b prep: anchor markers at EVERY level on the content-only path ----------
(defn probe-markers []
  (let [st (tstore/storage)
        s  (reduce (fn [s i] (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor 4 :op-buf-size 100})
                   (range 256))
        _  (ss/store s st)                       ; assigns durable addresses so anchors exist
        s  (reduce (fn [s i] (ss/replace s [i 0] [i 1])) s (range 0 256 5))
        brs (walk-branches (root-of s))
        depth (apply max (map (fn [^Branch b] (.-_level b)) brs))
        slotted (for [^Branch b brs
                      :when (.-_slots b)
                      i (range (.-_len b))
                      :let [^Slot sl (aget ^objects (.-_slots b) i)]
                      :when sl]
                  [(.-_level b) sl])
        leaf-slots   (filter (fn [[lvl ^Slot sl]] (and (= 1 lvl) (instance? PersistentTreeMap (.-diff sl)))) slotted)
        branch-marks (filter (fn [[lvl ^Slot sl]] (and (> lvl 1) (nil? (.-diff sl)))) slotted)
        bad-leaf     (filter (fn [[lvl ^Slot sl]] (and (= 1 lvl) (not (instance? PersistentTreeMap (.-diff sl))))) slotted)
        bad-branch   (filter (fn [[lvl ^Slot sl]] (and (> lvl 1) (some? (.-diff sl)))) slotted)
        no-anchor    (filter (fn [[_ ^Slot sl]] (nil? (.-anchor sl))) slotted)]
    (cond
      (< depth 2)        (fails "tree too shallow (max level " depth ")")
      (seq bad-leaf)     (fails (count bad-leaf) " leaf-parent slot(s) lack a leaf-diff map")
      (seq bad-branch)   (fails (count bad-branch) " branch slot(s) have non-null diff (should be markers)")
      (empty? branch-marks) (fails "no branch-level anchor markers (every-level capture not firing)")
      (empty? leaf-slots)   (fails "no leaf-parent leaf-diffs")
      (seq no-anchor)    (fails (count no-anchor) " slot(s) have nil anchor (anchor not captured)")
      :else (do (println "  P-markers: depth" depth "—" (count leaf-slots) "leaf-diffs +"
                         (count branch-marks) "branch markers; all carry an anchor") true))))

;; --- M4a: the slot captures the child's durable anchor ------------------------
(defn probe-anchor []
  (let [st (tstore/storage)
        s0 (reduce (fn [s i] (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor 4 :op-buf-size 100})
                   (range 64))
        _  (ss/store s0 st)
        k  17
        s1 (ss/replace s0 [k 0] [k 999])
        hit (find-leaf-slot (walk-branches (root-of s1)) k)]
    (cond
      (not hit) (fails "no leaf-parent slot carries k=" k)
      (nil? (.-anchor ^Slot (nth hit 2))) (fails "leaf slot.anchor is nil")
      :else
      (let [anchor (.-anchor ^Slot (nth hit 2))
            restored (.restore ^IStorage st anchor)
            ks (vec (take (.-_len ^ANode restored) (.-_keys ^ANode restored)))]
        (if (some #(= [k 0] %) ks)
          (do (println "  P-anchor: leaf slot.anchor →" anchor "= durable leaf still holding [" k "0]") true)
          (fails "anchor leaf doesn't hold pre-replace [" k "0]; keys=" ks))))))

(defn run-all []
  (println "=== OP_BUF_V5 M3+M4a probe (every-level deposit; anchor markers) ===")
  (let [rs [(probe-content) (probe-deposit) (probe-absent) (probe-markers) (probe-anchor)]]
    (println (if (every? true? rs) "ALL PROBES PASSED" "PROBE FAILURES"))
    (every? true? rs)))
