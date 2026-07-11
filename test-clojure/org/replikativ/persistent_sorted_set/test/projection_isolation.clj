(ns org.replikativ.persistent-sorted-set.test.projection-isolation
  "Diff-buf projection must be VERSION-ISOLATED under a shared restore cache.

   Caching IStorage impls return restored nodes shared BY ADDRESS across tree
   versions. Consecutive commits buffer against the same durable anchor with
   different accumulated diffs — and after a store that flushes a child to
   address B, version N's exact durable child at B is the very object version
   N+1's slot {B, δ} projects onto. projectBranch used to install the
   version-specific projection (slots, separators, count, measure) INTO the
   shared cached object, so whichever version projected last leaked its content
   into every other version's reads through that object (nondeterministic under
   an evicting cache — datahike's default LRU masked it). The projection now
   lands on a fresh copy (mirroring projectLeaf); the cached object stays
   pristine durable content. JVM-only — inspects Branch internals."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as tstore])
  (:import [org.replikativ.persistent_sorted_set Branch Settings]))

(def ^:private cmp (fn [a b] (compare (first a) (first b))))
(def ^:private BF 8)
(def ^:private BUF 512)

(defn- settings ^Settings [] (Settings. (int BF) nil nil nil (int BUF)))

(defn- val-of [s k]
  (second (first (ss/slice s [k nil] [k nil] cmp))))

(defn- restore* [addr storage]
  (ss/restore-by cmp addr storage {:branching-factor BF :diff-buf-size BUF :comparator cmp}))

(defn- two-versions
  "Deep tree (bf 8, 800 elems), then two stored versions whose roots buffer
   against shared durable anchors with different diffs. Returns [disk addr1 addr2]."
  []
  (let [disk (atom {})
        st0  (tstore/->Storage (atom {}) disk (settings))
        s0   (into (ss/sorted-set* {:comparator cmp :branching-factor BF :diff-buf-size BUF})
                   (map (fn [i] [i :zero]) (range 800)))
        addr0 (ss/store s0 st0)
        st1  (tstore/->Storage (atom {}) disk (settings))
        r0   (restore* addr0 st1)
        s1   (-> r0 (disj [400 nil]) (conj [400 :v1]))
        addr1 (ss/store s1 st1)
        s2   (-> s1 (disj [400 nil]) (conj [400 :v2])
                 (disj [401 nil]) (conj [401 :v2])
                 (disj [402 nil]) (conj [402 :v2]))
        addr2 (ss/store s2 st1)]
    [disk addr1 addr2]))

(defn- expected [v1? i]
  (if v1?
    (if (= i 400) :v1 :zero)
    (if (#{400 401 402} i) :v2 :zero)))

(defn- exact? [s v1?]
  (= (set (map (fn [i] [i (expected v1? i)]) (range 800)))
     (set (seq s))))

(deftest cross-version-reads-are-isolated
  (doseq [newer-first? [true false]]
    (testing (str "shared cache, " (if newer-first? "newer" "older") " version read first")
      (let [[disk addr1 addr2] (two-versions)
            shared (tstore/->Storage (atom {}) disk (settings))
            r1 (restore* addr1 shared)
            r2 (restore* addr2 shared)
            [a b] (if newer-first? [r2 r1] [r1 r2])]
        ;; first reader projects its diffs through the shared cache
        (is (exact? a (identical? a r1)) "first reader content exact")
        ;; second reader must be unaffected by the first's projections
        (is (exact? b (identical? b r1)) "second reader content exact")
        (is (= (if newer-first? :v1 :v2) (val-of (if newer-first? r1 r2) 400)))
        ;; RE-read the first through its now-cached subtree — the other's
        ;; projection must not have leaked into it
        (is (exact? a (identical? a r1)) "first reader content exact on re-read")))))

(deftest cached-durable-objects-stay-pristine
  (testing "projection never installs version-specific state into the shared cache"
    (let [[disk addr1 addr2] (two-versions)
          memory (atom {})
          shared (tstore/->Storage memory disk (settings))
          r2 (restore* addr2 shared)
          r1 (restore* addr1 shared)]
      ;; force full projections both ways
      (doall (seq r2))
      (doall (seq r1))
      (doseq [[addr node] @memory
              :when (instance? Branch node)]
        (let [^Branch b node
              stored-slots (:slots (clojure.edn/read-string (@disk addr)))]
          ;; a cached branch may only carry the slots its OWN blob stored —
          ;; never slots a parent's projection pushed into it
          (is (= (boolean (seq stored-slots)) (boolean (.slots b)))
              (str "cached node " addr " slots differ from its durable blob")))))))
