(ns org.replikativ.persistent-sorted-set.test.marker-slot-staleness
  "A branch-marker slot can go STALE, and that must stay harmless.

   Structural sharing means one child object can belong to several versions. Storing
   version v1 settles that child IN PLACE — a representation change, not a value change —
   so version v2's parent keeps a slot describing the PRE-settle child. `store()` walks one
   version top-down and nodes carry no parent pointers, so v2's parent is never told.

   `Branch.refreshMarkerSlots` detects this and poisons the slot. What that repair does and
   does not buy is written out at the call site; the short version is that it keeps the
   delta-maintained `bufEntries` consistent with a fresh subtree walk (the `-ea` oracle
   `assertBufEntries`), and that CONTENT is protected by the per-node budget test instead —
   measured, every stale slot was flushed rather than buffered, with the repair disabled.

   ## Why this test exists

   The suite pinned NOTHING about any of it: with the `refreshMarkerSlots` call deleted
   outright, all 320 tests still passed. So the harmlessness argument was unfalsifiable, and
   an argument nothing can falsify is not one you can safely build on.

   This test pins the two claims that make the staleness safe:

     1. a stale value is always safe-HIGH, so the budget still bounds the blob. Stale-LOW
        would let a written branch exceed B — a real correctness bug. Never observed
        (0 over 8100 trials), and argued structurally at the call site, but ARGUED is not
        PINNED, so it is asserted here.
     2. content survives regardless.

   ## Why it cannot pass vacuously

   The whole trap in this area is a fixture that never reaches the path and reports green.
   So the staleness itself is a PRECONDITION: the test reflects into the published slots,
   counts marker slots whose cached size disagrees with the live child, and FAILS if that
   count is zero. If a future change makes the condition unreachable, this test says so
   loudly instead of quietly protecting nothing.

   The shape is the one linear harnesses never produce: derive v2 from an UNSTORED v1 so
   both hold the same child objects, then store v1 FIRST, so the shared child is settled
   under v2's feet. Small budgets only — at the suite's usual B=256 nothing here fires.

   Run under `-ea` (the `:test` alias does), so `assertBufEntries` participates: removing
   the repair makes this namespace ERROR rather than merely mismeasure."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.edn :as edn]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set Settings IStorage PersistentSortedSet
            Branch ANode]
           [java.lang.ref Reference]))

(set! *warn-on-reflection* true)

(def ^:private cmp (fn [a b] (compare (first a) (first b))))

;; NEVER a bare (Settings.) — that reads diffBufSize from the `pss.diffBufSize` system
;; property, so a "diff-buf" fixture built that way silently runs with buffering OFF.
(defn- storage ^IStorage [disk bf dbs]
  (tstore/->Storage (atom {}) disk (Settings. (int bf) nil nil nil (int dbs))))

(defn- lcg [seed] (atom (+ 1 (mod seed 2147483646))))
(defn- nxt [st n] (let [v (mod (* (long @st) 48271) 2147483647)] (reset! st v) (mod v n)))

(defn- apply-ops [s st n cnt r]
  (reduce (fn [acc _]
            (let [k (nxt st (* 2 n)) op (nxt st 3)]
              (case op
                0 (ss/conj acc [k 0] cmp)
                1 (ss/disj acc [k 0] cmp)
                2 (if (contains? acc [k 0]) (ss/replace acc [k 0] [k (inc r)]) (ss/conj acc [k 0] cmp)))))
          s (range cnt)))

;; --- reflection: the published slots, and one slot's fields ------------------------------
(defn- field ^java.lang.reflect.Field [^Class c ^String n]
  (doto (.getDeclaredField c n) (.setAccessible true)))

(defn- published-slots [^Branch b]
  (let [st  (.get (field Branch "_state") b)
        buf (.get (field (class st) "buf") st)]
    (when buf (.get (field (class buf) "slots") buf))))

(defn- slot-field [sl ^String n]
  (.get (field (class sl) n) sl))

(defn- resident-children [^Branch b]
  (let [ch (.childrenArray b)]
    (mapv (fn [i] (let [x (when ch (aget ^objects ch i))]
                    (if (instance? Reference x) (.get ^Reference x) x)))
          (range (.len b)))))

(defn- stale-markers
  "Marker slots (diff == null, anchor != null) whose cached size disagrees with the live
   child. Returns {:stale n :low n} — `low` counts the DANGEROUS direction."
  [^PersistentSortedSet s]
  (let [acc (atom {:stale 0 :low 0})]
    (letfn [(w [x]
              (when (instance? Branch x)
                (let [^Branch b x sls (published-slots b) cs (resident-children b)]
                  (dotimes [i (.len b)]
                    (let [sl (when sls (aget ^objects sls i)) c (nth cs i)]
                      (when (and sl (instance? Branch c)
                                 (nil? (slot-field sl "diff"))
                                 (some? (slot-field sl "anchor")))
                        (let [cached (long (slot-field sl "bufEntries"))
                              live   (.bufEntries ^Branch c)]
                          (when (and (not= cached live) (not= cached -1))
                            (swap! acc update :stale inc)
                            (when (< cached live) (swap! acc update :low inc)))))))
                  (doseq [c cs] (w c)))))]
      (w (.root s)))
    @acc))

;; --- the budget oracle: entries embedded in a written branch blob ------------------------
(defn- nentries [diff child-level]
  (if-not (map? diff) 0
          (if (zero? child-level)
            (+ (count (:absent diff)) (count (:present diff)))
            (reduce + 0 (map (fn [e] (nentries (:diff (val e)) (dec child-level))) diff)))))

(defn- blob-embedded [blob-str]
  (let [{:keys [level slots]} (edn/read-string blob-str)]
    (reduce + 0 (map (fn [e] (nentries (:diff (val e)) (dec level))) (or slots {})))))

;; [bf B n seed nops] — small budgets; nothing here fires at the suite's default B=256.
(def ^:private cases
  [[6 1 2000 6 20] [6 1 2000 3 20] [4 1 2000 3 60] [8 2 600 2 20] [4 2 600 1 20]])

(deftest a-stale-marker-slot-is-safe-high-and-content-survives
  (testing "storing an ancestor settles a shared child under a descendant's feet"
    (let [totals (atom {:stale 0 :low 0})]
      (doseq [[bf b n seed nops] cases]
        (let [st   (lcg seed)
              disk (atom {})
              o    {:comparator cmp :branching-factor bf :diff-buf-size b}
              s0   (reduce (fn [s i] (ss/conj s [i 0] cmp)) (ss/sorted-set* o) (range n))
              a0   (ss/store s0 (storage disk bf b))
              base (ss/restore-by cmp a0 (storage disk bf b) o)
              _    (dorun (seq base))
              ;; v2 from an UNSTORED v1 ⇒ they share child objects
              v1   (apply-ops base st n nops 1)
              v2   (apply-ops v1 st n nops 2)
              c1   (vec (seq v1))
              c2   (vec (seq v2))
              lbl  (str "bf=" bf " B=" b " n=" n " seed=" seed)
              ;; store the ANCESTOR first — this is what settles the shared child
              a1   (ss/store v1 (storage disk bf b))
              seen (stale-markers v2)
              _    (swap! totals #(merge-with + % seen))
              a2   (ss/store v2 (storage disk bf b))]
          (is (zero? (:low seen))
              (str lbl ": a stale slot must never be stale-LOW (cached < live). Stale-low
                   under-counts the budget and lets a written branch exceed B. Saw "
                   (:low seen)))
          (is (= c1 (vec (seq (ss/restore-by cmp a1 (storage disk bf b) o))))
              (str lbl ": v1 round-trips"))
          (is (= c2 (vec (seq (ss/restore-by cmp a2 (storage disk bf b) o))))
              (str lbl ": v2 round-trips — the version whose slot went stale"))
          (is (every? #(<= (blob-embedded %) b)
                      (filter #(re-find #":slots" %) (vals @disk)))
              (str lbl ": every written branch still satisfies Σ embedded <= B"))))
      ;; PRECONDITION, asserted last so the per-case failures above read first.
      (is (pos? (:stale @totals))
          (str "NO stale marker slot was observed in any case, so this namespace proved
               nothing. The shape or the budgets no longer reach the condition — fix the
               fixture rather than deleting the assertion. Totals: " (pr-str @totals))))))
