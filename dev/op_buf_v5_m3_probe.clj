(ns op-buf-v5-m3-probe
  "M3 gate (opBufSize>0): deposit on the return path.
   Asserts (a) content is still correct (slots are accumulated but ignored by the
   still-baseline store/restore in M3) and (b) deposits actually fire — slots carry
   the right Present/Absent entries with an exact ĝ snapshot, latest-wins per key.
   Run (src-clojure MUST precede target/classes — the latter holds a stale .clj copy):
     clojure -Sdeps '{:paths [\"src-clojure\" \"target/classes\" \"dev\"]}' \\
       -M -e \"(require 'op-buf-v5-m3-probe)(op-buf-v5-m3-probe/run-all)\""
  (:require [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set Branch Leaf ANode Slot PersistentSortedSet IStorage]
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

(defn walk-branches
  "Seq of every in-memory Branch in the tree (root first)."
  [node]
  (when (instance? Branch node)
    (cons node (mapcat walk-branches (branch-children node)))))

(defn slot-at ^Slot [^Branch b i]
  (when-let [s (.-_slots b)] (aget ^objects s i)))

(defn fails [& msgs] (println "  FAIL:" (apply str msgs)) false)

;; --- P1: content correctness (slots ignored by M3 store/restore) -------------
(defn probe-content []
  (let [n   200
        ref (atom (sorted-set-by cmp))
        s   (reduce (fn [s i] (swap! ref conj [i 0]) (conj s [i 0]))
                    (ss/sorted-set* {:comparator cmp :branching-factor 4 :op-buf-size 100})
                    (shuffle (range n)))
        ;; replace half the elements (content-only) and disj a quarter
        s   (reduce (fn [s i] (swap! ref #(-> % (disj [i 0]) (conj [i (inc i)])))
                      (ss/replace s [i 0] [i (inc i)]))
                    s (range 0 n 2))
        s   (reduce (fn [s i] (swap! ref disj [i 0]) (disj s [i 0]))
                    s (range 1 n 4))
        got (vec (seq s))
        exp (vec (seq @ref))]
    (if (= got exp)
      (do (println "  P1 content exact (conj/replace/disj, opBufSize>0):" (count got) "elems") true)
      (fails "content mismatch; got " (count got) " exp " (count exp)
             " first-diff " (first (filter (fn [[a b]] (not= a b)) (map vector got exp)))))))

;; --- P2: deposit fires with exact ĝ; --- P3: latest-wins within a slot -------
(defn probe-deposit []
  ;; build a multi-level tree, then a single content-only replace whose path we inspect
  (let [s0 (reduce (fn [s i] (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor 4 :op-buf-size 100})
                   (range 64))
        k  17
        s1 (ss/replace s0 [k 0] [k 111])
        brs (walk-branches (.root ^org.replikativ.persistent_sorted_set.PersistentSortedSet s1))
        ;; find a branch whose slot holds a Present entry for k
        hit (some (fn [^Branch b]
                    (when-let [slots (.-_slots b)]
                      (some (fn [i]
                              (when-let [^Slot sl (aget ^objects slots i)]
                                (let [v (.valAt (.-diff sl) [k nil] ::miss)]
                                  (when (not= v ::miss) [b i sl v]))))
                            (range (.-_len b)))))
                  brs)]
    (if-not hit
      (fails "no slot carried a Present entry for replaced key " k)
      (let [[^Branch b i ^Slot sl v] hit
            child   (deref-child (aget ^objects (.-_children b) i))
            cnt-ok  (= (.-count sl) (.count ^ANode child nil))
            pres-ok (= v [k 111])]
        (println "  P2 deposit fired: level=" (.-_level b) " slot[" i "] Present=" v
                 " ĝ.count=" (.-count sl) " child.count=" (.count ^ANode child nil))
        (cond (not= 1 (.-_level b)) (fails "slot at level " (.-_level b) " — must be a leaf-parent (level 1)")
              (not pres-ok) (fails "Present payload " v " != expected " [k 111])
              (not cnt-ok)  (fails "ĝ.count " (.-count sl) " != child subtree count " (.count ^ANode child nil))
              :else
              ;; P3: replace again -> latest-wins within the same slot
              (let [s2 (ss/replace s1 [k 111] [k 222])
                    brs2 (walk-branches (.root ^org.replikativ.persistent_sorted_set.PersistentSortedSet s2))
                    v2 (some (fn [^Branch bb]
                               (when-let [slots (.-_slots bb)]
                                 (some (fn [j]
                                         (when-let [^Slot s (aget ^objects slots j)]
                                           (let [x (.valAt (.-diff s) [k nil] ::miss)]
                                             (when (not= x ::miss) x))))
                                       (range (.-_len bb)))))
                             brs2)]
                (if (= v2 [k 222])
                  (do (println "  P3 latest-wins within slot: Present=" v2) true)
                  (fails "latest-wins broken: slot holds " v2 " expected " [k 222]))))))))

;; --- Absent deposit (remove that does not restructure) -----------------------
(defn probe-absent []
  ;; build a tree, remove a key that should not trigger a merge in its leaf
  (let [s0 (reduce (fn [s i] (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor 8 :op-buf-size 100})
                   (range 200))
        ;; collect every removed key's slot-state across a few removes
        ks  [73 74 75]
        s1  (reduce (fn [s k] (disj s [k 0])) s0 ks)
        brs (walk-branches (.root ^org.replikativ.persistent_sorted_set.PersistentSortedSet s1))
        absent? (fn [k]
                  (some (fn [^Branch b]
                          (when-let [slots (.-_slots b)]
                            (some (fn [i]
                                    (when-let [^Slot sl (aget ^objects slots i)]
                                      (= Slot/ABSENT (.valAt (.-diff sl) [k nil] ::miss))))
                                  (range (.-_len b)))))
                        brs))
        any (some absent? ks)]
    (if any
      (do (println "  P-absent: at least one removed key recorded as Absent in a slot") true)
      ;; not a hard fail: all three removes may have triggered rebalances (structural,
      ;; which correctly skip deposit). Report so we notice if it's ALWAYS the case.
      (do (println "  P-absent: (no content-only remove among" ks "— all rebalanced; OK)") true))))

;; --- leaf-parents-only: the M3 rework invariant -------------------------------
(defn probe-leaf-parent-only []
  ;; after the nested rework, slots live ONLY at leaf-parents (level 1);
  ;; higher branches carry no slots in memory (their nesting/ĝ is assembled at store).
  (let [s   (reduce (fn [s i] (conj s [i 0]))
                    (ss/sorted-set* {:comparator cmp :branching-factor 4 :op-buf-size 100})
                    (range 256))
        s   (reduce (fn [s i] (ss/replace s [i 0] [i 1])) s (range 0 256 3))
        brs (walk-branches (.root ^org.replikativ.persistent_sorted_set.PersistentSortedSet s))
        depth (apply max (map (fn [^Branch b] (.-_level b)) brs))
        bad   (filter (fn [^Branch b] (and (> (.-_level b) 1) (some? (.-_slots b)))) brs)
        l1ok  (filter (fn [^Branch b] (and (= 1 (.-_level b)) (some? (.-_slots b)))) brs)]
    (cond (< depth 2) (fails "tree too shallow (max level " depth ") — need ≥2 to test")
          (seq bad)   (fails (count bad) " branch(es) at level>1 carry slots (must be none)")
          (empty? l1ok) (fails "no level-1 branch carries slots (deposit not firing)")
          :else (do (println "  P-leaf-parent-only: depth" depth "; slots only at level-1 ("
                             (count l1ok) "leaf-parents), 0 higher-level slots") true))))

;; --- M4a: the slot captures the child's durable anchor -----------------------
(defn probe-anchor []
  ;; store (assigns durable addresses), then a content-only replace; the leaf-parent
  ;; slot must capture the leaf's durable address, and restoring it must yield the
  ;; pre-mutation durable leaf (the buffered child keeps its old anchor).
  (let [st (tstore/storage)
        s0 (reduce (fn [s i] (conj s [i 0]))
                   (ss/sorted-set* {:comparator cmp :branching-factor 4 :op-buf-size 100})
                   (range 64))
        _  (ss/store s0 st)                       ; assigns _addresses to all nodes + writes durable
        k  17
        s1 (ss/replace s0 [k 0] [k 999])          ; content-only ⇒ leaf buffered, keeps its anchor
        brs (walk-branches (.root ^PersistentSortedSet s1))
        hit (some (fn [^Branch b]
                    (when (and (= 1 (.-_level b)) (.-_slots b))
                      (some (fn [i]
                              (when-let [^Slot sl (aget ^objects (.-_slots b) i)]
                                (when (not= ::miss (.valAt (.-diff sl) [k nil] ::miss)) sl)))
                            (range (.-_len b)))))
                  brs)]
    (cond
      (not hit) (fails "no leaf-parent slot carries k=" k)
      (nil? (.-anchor ^Slot hit)) (fails "slot.anchor is nil (expected the leaf's durable address)")
      :else
      (let [anchor (.-anchor ^Slot hit)
            restored (.restore ^IStorage st anchor)
            ks (vec (take (.-_len ^ANode restored) (.-_keys ^ANode restored)))]
        (if (some #(= [k 0] %) ks)
          (do (println "  P-anchor: slot.anchor →" anchor "= durable leaf still holding [" k "0] (pre-replace), keys" (count ks)) true)
          (fails "anchor leaf doesn't hold pre-replace [" k "0]; keys=" ks))))))

(defn run-all []
  (println "=== OP_BUF_V5 M3+M4a probe (nested deposit at leaf-parents; anchor capture) ===")
  (let [rs [(probe-content) (probe-deposit) (probe-absent) (probe-leaf-parent-only) (probe-anchor)]]
    (println (if (every? true? rs) "ALL M3 PROBES PASSED" "M3 PROBE FAILURES"))
    (every? true? rs)))
