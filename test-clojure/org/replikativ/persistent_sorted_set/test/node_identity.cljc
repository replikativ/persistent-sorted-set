(ns org.replikativ.persistent-sorted-set.test.node-identity
  "`node->identity` must depend on a node's CONTENT, never on cache warmth.

   It exists so a consumer can content-address a node: datahike's
   `branch-content-uuid` and stratum's `gen-address` both hash a projection of a
   node to decide whether two nodes are the same blob. If that projection admits
   a derived cache, two structurally identical nodes get different addresses,
   dedup silently stops working, and — because the address is what a merkle root
   is built from — two replicas holding the same data can disagree about their
   root hash.

   `node->map` is the SERIALIZATION projection and legitimately carries caches:
   `:measure` at the top level and, per diff-buf slot, `:count` and `:measure`.
   `node->identity` is the IDENTITY projection and must strip them. It stripped
   the top-level `:measure` and then re-admitted both through `:slots`.

   ## The ordering that makes it observable

   A slot captures `child.measure()` at DEPOSIT time. So warming the measure
   AFTER the deposit proves nothing — the slot already holds whatever was there
   when the deposit happened, and an earlier probe that warmed in that order came
   back green against the unfixed code. The measure has to be forced BEFORE the
   mutation that deposits. Measured on the same tree, one warm and one cold, with
   a single identical `conj`:

       slot measures     cold [[0 false]]   warm [[0 true]]
       node->map slots equal?               false
       node->identity equal?                false   (before) / true (after)

   Two separately-built trees will also differ in `:addresses`, which is real
   content, not warmth — a probe that varies both cannot distinguish the two and
   will report a defect that is there for the wrong reason. Both trees below are
   built by the same function from the same input so the addresses agree, and the
   assertion is on the whole identity rather than on a subset."
  (:require [clojure.set :as cset]
            [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes]
            #?(:clj [org.replikativ.persistent-sorted-set.test.storage :as ts]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set
                    PersistentSortedSet ANode Branch Settings RefType
                    IMeasure NumericStatsOps])))

#?(:clj
   (do
     (def ^:private BF 16)
     (def ^:private DBS 256)
     (def ^:private ops (NumericStatsOps/instance))

     (defn- node-settings ^Settings []
       (Settings. (int BF) RefType/STRONG ^IMeasure ops nil (int DBS)))

     (def ^:private opts
       {:branching-factor BF :ref-type :strong
        :diff-buf-size DBS :measure ops :comparator compare})

     (defn- fixture
       "Store ONE tree, and hand back the disk it lives on. Both trees under test
        are restored from this same disk, so their addresses agree and any
        difference in identity is warmth and nothing else."
       []
       (let [st   (ts/storage-with-settings (node-settings))
             s0   (s/from-sorted-array compare (object-array (range 0 300 10)) 30
                                       (assoc opts :storage st))]
         {:addr (s/store s0 st) :disk (:*disk st)}))

     (defn- build
       "A cold restore over that disk with an EMPTY node cache — a warm cache would
        hand both trees the same node objects and warming one would warm both."
       [{:keys [addr disk]}]
       (s/restore-by compare addr
                     (ts/->Storage (atom {}) disk (node-settings))
                     opts))

     (defn- slot-measures [m]
       (mapv (fn [[i e]] [i (some? (:measure e))]) (:slots m)))

     (deftest identity-does-not-depend-on-cache-warmth
       (testing "a slot captures child.measure() at deposit time, so the measure
                 must be forced BEFORE the mutation. Warming afterwards leaves the
                 slot as it was and the unfixed code looks correct."
         (let [fx (fixture)
               cold (build fx)
               warm (build fx)
               _    (s/measure warm)              ; force BEFORE the deposit
               s-cold (conj cold 5)
               s-warm (conj warm 5)
               r-cold (.root ^PersistentSortedSet s-cold)
               r-warm (.root ^PersistentSortedSet s-warm)
               m-cold (nodes/node->map r-cold)
               m-warm (nodes/node->map r-warm)]
           (is (= 1 (.level ^ANode r-cold))
               "precondition: a LEVEL-1 root. At level 2 the slot is a branch
                marker with no measure and nothing here can vary.")
           (is (seq (:slots m-cold))
               "precondition: the root actually carries diff-buf slots")
           (is (not= (slot-measures m-cold) (slot-measures m-warm))
               "precondition: warmth reaches the slot at all — one deposit saw a
                cold child measure and the other a populated one. If this ever
                stops holding, the rest of this test is vacuous.")
           (is (not= (:slots m-cold) (:slots m-warm))
               "and node->map, the SERIALIZATION projection, differs — that is
                allowed; it is why the identity projection has to strip.")
           (is (= (nodes/node->identity r-cold) (nodes/node->identity r-warm))
               "THE assertion: same content, same identity, regardless of warmth")
           (is (= (hash (nodes/node->identity r-cold))
                  (hash (nodes/node->identity r-warm)))
               "and therefore the same content address"))))

     (deftest identity-still-distinguishes-different-content
       (testing "stripping must not go so far that unequal nodes collide — a
                 projection that dropped enough would pass the test above by
                 making everything equal"
         (let [fx (fixture)
               a (conj (build fx) 5)
               b (conj (build fx) 7)]
           (is (not= (nodes/node->identity (.root ^PersistentSortedSet a))
                     (nodes/node->identity (.root ^PersistentSortedSet b)))
               "different buffered elements must give different identities"))))

     (deftest identity-carries-no-cache-keys-at-all
       (testing "structural, so it fails on a newly added cache field rather than
                 waiting for someone to construct the warmth that exposes it"
         (let [root (.root ^PersistentSortedSet (conj (build (fixture)) 5))
               id   (nodes/node->identity root)]
           (is (not (contains? id :measure)) "no top-level measure")
           (is (not (contains? id :count)) "no top-level count")
           (doseq [[i e] (:slots id)]
             (is (= #{} (cset/intersection (set (keys e)) #{:measure :count}))
                 (str "slot " i " must carry no cached aggregate, got "
                      (pr-str (keys e))))))))))
