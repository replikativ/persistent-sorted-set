(ns org.replikativ.persistent-sorted-set.test.diff-buf-slots
  "Three ways the diff-buf SLOT machinery can be wrong, and in all three THE BUDGET IS THE
   FIXTURE -- which is why the three sections construct storage differently and must keep
   doing so.

     * SETTINGS ADOPTION -- a set restored without `:diff-buf-size` ran at 0 over nodes at
       N. Reads were fine; the next write rebuilt through the SET's settings and dropped
       every surviving sibling's buffered elements. Needs nodes at 512 and, for the
       negative control, nodes at an EXPLICIT 0.
     * LAZY SLOT SIZING -- `store` Pass 1 read `Slot.LAZY` raw when sizing a bufferable
       child. Needs budget 256 and then 1, so the same child is buffered in one run and
       flushed in the other.
     * MARKER SLOT STALENESS -- needs budget 1 or 2. At the suite's usual 256 nothing here
       fires at all.

   NEVER a bare `(Settings.)` in this namespace. Its budget comes from the `pss.diffBufSize`
   system property -- 256 under the `:test` alias, 0 otherwise -- so a bare one silently
   substitutes the alias's budget for the one the test means to exercise. Each section below
   passes an explicit `Settings`.

   Several aliases name the same namespace (`s`/`ss`, `ts`/`tstore`). That is deliberate: it
   let the three files merge with their bodies byte-identical to the originals."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.edn :as edn]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as ts]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set
            ANode Branch Slot IStorage Settings PersistentSortedSet RefType]
           [java.lang.ref Reference]))

(set! *warn-on-reflection* true)

;; ===========================================================================
;; 1. a restored set adopts the diff-buf budget its nodes carry
;; ===========================================================================

(def ^:private bf 16)
(def ^:private budget 512)
(def ^:private n 6000)
(def ^:private scatter (vec (range 3 n 37)))

(defn- diff-buf-of [set]
  (let [f (.getDeclaredField PersistentSortedSet "_settings")]
    (.setAccessible f true)
    (.diffBufSize ^Settings (.get f set))))

(defn- scenario
  "A tree stored WITH buffered slots, then restored under `restore-opts` and
   written to once more. Returns what a cold read-back finds versus what it
   should. `nil` restore-opts means the bare documented call."
  [restore-opts]
  (let [disk  (atom {})
        ;; the NODES carry the budget, as a real self-describing blob does
        nodes (Settings. (int bf) nil nil nil (int budget))
        st    #(ts/->Storage (atom {}) disk nodes)
        full  {:branching-factor bf :diff-buf-size budget :comparator compare}
        v0    (reduce #(s/conj %1 %2 compare) (s/sorted-set* full) (range 0 n 2))
        a0    (s/store v0 (st))
        ;; buffer a scattered batch through a correctly-configured set and store it:
        ;; the blob now carries SLOTS, which is what the next write can drop
        v1    (reduce #(s/conj %1 %2 compare) (s/restore-by compare a0 (st) full) scatter)
        a1    (s/store v1 (st))
        r     (s/restore-by compare a1 (st) restore-opts)
        v2    (reduce #(s/conj %1 %2 compare) r [7777777])
        a2    (s/store v2 (st))
        got   (vec (seq (s/restore-by compare a2 (st) full)))
        want  (vec (sort (distinct (concat (range 0 n 2) scatter [7777777]))))]
    {:missing (count (remove (set got) want))
     :extra   (count (remove (set want) got))
     :adopted (diff-buf-of r)}))

(deftest a-bare-restore-adopts-the-nodes-budget
  (testing "restoring without :diff-buf-size must not lose the buffered elements
            the nodes already carry — measured at 81 lost before the adoption.

            The assertion is about the DATA, not about a particular number: what
            makes the difference between loss and no loss is whether buffering is
            ENABLED at all, not whether the two budgets match. A set at 0 over
            nodes at N rebuilds through a no-buffering path and drops their
            slots; a set at any positive budget does not. Adoption therefore only
            ever raises from 0, and this asserts exactly that — which also keeps
            the test honest under `-Dpss.diffBufSize=256`, where a bare restore
            already starts at the ambient default rather than 0."
    (let [r (scenario {:branching-factor bf})]
      (is (zero? (:missing r)) (str "elements lost: " (:missing r)))
      (is (zero? (:extra r)))
      (is (pos? (:adopted r))
          "buffering is enabled rather than left off over buffered nodes"))))

(deftest an-explicit-budget-still-wins
  (testing "adoption only ever raises from 0 — a caller who names a budget keeps it"
    (let [r (scenario {:branching-factor bf :diff-buf-size budget})]
      (is (zero? (:missing r)))
      (is (= budget (:adopted r))))))

(deftest adoption-does-not-invent-buffering
  (testing "nodes written WITHOUT a budget do not push one onto the restored set.

            The set still lands on the ambient default — `Settings/defaultDiffBufSize`,
            which the suite sets to 256 and a bare JVM leaves at 0 — because that is
            what `map->settings` has always applied. Adoption must not CHANGE that,
            so this compares against the ambient value rather than hardcoding 0,
            and it round-trips the data either way."
    (let [disk  (atom {})
          nodes (Settings. (int bf) nil nil nil (int 0))
          st    #(ts/->Storage (atom {}) disk nodes)
          base  {:branching-factor bf :diff-buf-size 0 :comparator compare}
          v0    (reduce #(s/conj %1 %2 compare) (s/sorted-set* base) (range 1000))
          a0    (s/store v0 (st))
          r     (s/restore-by compare a0 (st) {:branching-factor bf})]
      (is (= (vec (range 1000)) (vec (seq r))))
      (is (= (Settings/defaultDiffBufSize) (diff-buf-of r))
          "unchanged by adoption: the nodes had nothing to adopt"))))

;; ===========================================================================
;; 2. Pass 1 must resolve a LAZY slot before sizing it
;; ===========================================================================

(def ^:private BF 16)

(defn- seeded-disk
  "A disk holding a tree that really does carry diff-buf slots. Returns [disk addr]."
  []
  (let [st   (tstore/storage-with-settings
              (Settings. (int BF) RefType/STRONG nil nil (int 256)))
        opts {:comparator compare :branching-factor BF :ref-type :strong
              :diff-buf-size 256}
        s0   (s/from-sorted-array compare (object-array (range 0 3000 10)) 300
                                  (assoc opts :storage st))
        a0   (s/store s0 st)
        cold (s/restore-by compare a0 st opts)
        ;; the SECOND store is what deposits slots
        s1   (reduce #(s/conj %1 %2 compare) cold [5 15 25 35])]
    [(:*disk st) (s/store s1 st)]))

(deftest a-restored-slot-on-a-dirty-child-is-sized-by-its-diff
  (testing "with a budget of 1, a child carrying a real restored diff must be
            FLUSHED. Reading Slot.LAZY raw sized it as -2, which passes any
            budget test, so it was buffered instead and -2 became its recorded
            size."
    (let [[disk addr] (seeded-disk)
          budget   1
          ns1      (Settings. (int BF) RefType/STRONG nil nil (int budget))
          written  (atom [])
          ^IStorage inner (tstore/->Storage (atom {}) disk ns1)
          counting (reify IStorage
                     (store [_ node]
                       (let [a (.store inner node)]
                         (swap! written conj [(.level ^ANode node) a]) a))
                     (accessed [_ a] (.accessed inner a))
                     (restore [_ a] (.restore inner a))
                     (markFreed [_ _] nil)
                     (isFreed [_ _] false)
                     (freedInfo [_ _] nil))
          back     (s/restore-by compare addr counting
                                 {:comparator compare :branching-factor BF
                                  :ref-type :strong :diff-buf-size budget})
          ^Branch root (.root ^PersistentSortedSet back)
          slots    (aget ^objects (.addressesAndSlots root) 1)
          idx      (first (for [i (range (.len root))
                                :let [^Slot sl (when slots (aget ^objects slots i))]
                                :when (and sl (.-anchor sl) (.-diff sl))]
                            i))]
      (is (= 2 (.level ^ANode root))
          "precondition: a level-2 root, so its slots describe branch children")
      (is (some? idx)
          "precondition: some slot carries a restored diff with an anchor. Without
           one there is nothing to size and the assertions below are vacuous.")
      (let [^Slot sl (aget ^objects slots idx)]
        (is (= Slot/LAZY (.-bufEntries sl))
            "precondition: the restored slot really is LAZY — this is the value
             the raw read mistook for a size")
        ;; Build the node directly rather than calling `child(int,ANode)` on this
        ;; shared root — that writes in place and is owner-thread-only (it now
        ;; asserts `editable()`). Constructing the state is also more honest about
        ;; what is being tested: a dirty child whose restored LAZY slot survived.
        (let [len      (.len root)
              kid      (.child root ^IStorage counting (int idx))   ; resident, bare
              addrs    (object-array (seq (aget ^objects (.addressesAndSlots root) 0)))
              children (object-array len)
              slots2   (object-array (seq slots))
              _        (aset addrs idx nil)               ; dirty this index
              _        (aset children idx kid)            ; bare resident ANode
              ^Branch dirty (Branch. (int (.level ^ANode root)) (int len)
                                     (object-array (seq (.keys ^ANode root)))
                                     addrs children (long -1) nil compare ns1)
              _        (.installSlots dirty slots2 Branch/BUF_LAZY)
              _        (reset! written [])
              _        (.store dirty counting)
              addr-after (aget ^objects (aget ^objects (.addressesAndSlots dirty) 0) idx)
              child-lvl  (dec (.level ^ANode root))]
          (is (some #(= child-lvl (first %)) @written)
              (str "the child's diff exceeds the budget of " budget
                   ", so it must be flushed — a raw -2 buffers it instead"))
          (is (not= addr-after (.-anchor sl))
              "and its address must be the freshly written one, not the anchor it
               would be re-pointed to if it had been buffered"))))))

;; ===========================================================================
;; 3. a marker slot's staleness is only detectable while the child is resident
;; ===========================================================================

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
