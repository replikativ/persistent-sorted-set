(ns org.replikativ.persistent-sorted-set.test.proj-cmp-sharing
  "The ClojureScript half of `proj_cmp_sharing.clj`.

   `_projCmp` is the comparator `project-leaf` rebuilds a buffered leaf's key array with, and
   it lives as a FIELD on the node. `btset/-root` and `branch/child`'s restore arm both used
   to `set!` it unconditionally onto the object the storage returned, so two sets over one
   CACHING storage whose comparators order ties differently overwrote each other's stamp and
   whichever read last won.

   That fix landed on the JVM first and was ported here separately — the port went in without
   a test on this runtime, which is what this namespace repays. The JVM version records the
   measurement: count correct, `seq` NOT sorted under the first set's comparator, `contains?`
   false for elements present in `seq`, and one element permanently unfindable after a store
   and a cold reload.

   The storage MUST cache by address — return the same node object for the same address —
   or the two sets never share a node and this whole namespace passes while testing nothing.
   `util/storage` does cache, in the `memory` atom, which is why both sets are built over one
   `memory`."
  (:require [cljs.test :refer-macros [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.test.storage.util :as u]))

;; Same key order, ties REVERSED — they disagree only where a leaf holds several elements
;; sharing a first component, which is exactly what project-leaf reorders.
(defn- cmp1 [a b]
  (let [c (compare (first a) (first b))]
    (if-not (zero? c) c (compare (second a) (second b)))))
(defn- cmp2 [a b]
  (let [c (compare (first a) (first b))]
    (if-not (zero? c) c (compare (second b) (second a)))))

(defn- sorted-under? [cmp xs]
  (every? neg? (map (fn [[x y]] (cmp x y)) (partition 2 1 xs))))

(defn- scenario [{:keys [bf dbs n]}]
  (let [opts   {:branching-factor bf :diff-buf-size dbs :comparator cmp1}
        memory (atom {})                       ; ONE cache => the two sets share node objects
        disk   (atom {})
        st     (u/storage memory disk opts)
        base   (reduce #(s/conj %1 %2 cmp1)
                       (s/sorted-set* (assoc opts :storage st))
                       (mapcat (fn [k] [[k 0] [k 1]]) (range n)))
        _      (s/store base st)
        ;; second generation: content-only inserts, so leaf-parents carry leaf diffs
        s1     (reduce #(s/conj %1 [%2 2] cmp1) base (range 0 n 7))
        addr   (s/store s1 st)
        truth  (vec (s/seq s1))
        A      (s/restore addr st (assoc opts :comparator cmp1))
        B      (s/restore addr st (assoc opts :comparator cmp2))
        ;; INTERLEAVED, and that is the whole test. Materialising A fully and only then
        ;; reading B proves nothing: A's leaves are already projected and cached, so a later
        ;; re-stamp cannot reach them, and the first version of this namespace passed against
        ;; the unfixed build for exactly that reason. A must be MID-WALK when B stamps, so
        ;; the leaves A has not reached yet get projected under whatever the field holds
        ;; afterwards.
        it      (s/seq A)
        _       (first it)                     ; A projects its first leaf under cmp1
        _       (vec (s/seq B))                ; B reads — must not re-stamp A's nodes
        got     (vec it)]                      ; A finishes the walk
    {:truth truth :got got
     :unfindable (into [] (comp (remove #(s/contains? A %)) (take 5)) truth)}))

(def ^:private configs [{:bf 8 :dbs 64 :n 60} {:bf 8 :dbs 256 :n 200} {:bf 16 :dbs 128 :n 300}])

(deftest a-second-set-does-not-disturb-the-first
  (testing "reading through B must leave A's contents, order and lookups intact"
    (doseq [{:keys [bf dbs n] :as cfg} configs]
      (let [{:keys [truth got unfindable]} (scenario cfg)
            label (str "bf=" bf " dbs=" dbs " n=" n)]
        (is (= truth got)
            (str label ": A's interleaved walk must yield exactly what was stored"))
        (is (sorted-under? cmp1 got) (str label ": A's seq must stay sorted under cmp1"))
        (is (= [] unfindable)
            (str label ": every stored element must be findable — these were in seq but "
                 "contains? answered false: " (pr-str unfindable)))))))

(deftest empty-keeps-its-storage
  (testing "`(store (empty s))` threw here while the JVM twin returns an address — the same
            class as the compact defect, and the same failure shape: a crash on the next
            write, away from the call that caused it"
    (let [memory (atom {})
          disk   (atom {})
          opts   {:branching-factor 8 :diff-buf-size 0 :comparator compare}
          st     (u/storage memory disk opts)
          s      (reduce #(s/conj %1 %2 compare)
                         (s/sorted-set* (assoc opts :storage st :meta {:x 1}))
                         (range 50))
          e      (empty s)]
      (is (= 0 (count e)) "it is empty")
      (is (= {:x 1} (meta e)) "metadata survives, as it already did")
      (is (some? (s/store e)) "and a bare (store e) must work, i.e. the storage came along"))))
