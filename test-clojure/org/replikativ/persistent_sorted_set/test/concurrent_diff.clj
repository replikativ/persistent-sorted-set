(ns org.replikativ.persistent-sorted-set.test.concurrent-diff
  "What makes `diff`'s address/slot pairing safe against a concurrent settle.

   `diff` decides, per child, whether that child's ADDRESS still stands for its
   contents: an address held by both sides means the subtrees are identical and
   the whole subtree prunes without being read. Under diff-buf that decision
   needs TWO facts about the same child — its address, and whether the parent
   holds a buffered diff for it in `slots[i]`.

   `Branch.addressArray()` and `Branch.slots()` are two INDEPENDENT volatile
   reads of `_state`, and `addressArray`'s own comment says so: \"Pair-coherent
   only when both come from ONE snapshot.\" Pairing them across a settle would
   give a stale address wearing a safe-to-prune flag, the other side would prune
   against it, and the buffered delta would vanish from the answer — silently, as
   a smaller diff rather than an error.

   ## Why that is not reachable today, and why it is still worth fixing

   Because of a coupling in a DIFFERENT method. `store()`'s settle skips every
   child whose pre-settle address is non-null — the \"clean passthrough\" branch
   at the top of its first pass — so such a child keeps both its address and its
   slot and there is nothing to tear. A child that IS settled had a null address
   going in (the mutation moved it into the slot's anchor), and `prune-shared`
   ignores null addresses outright, so that entry never prunes whatever its slot
   says.

   Measured while trying to construct the failure, none of which produced a
   settle that re-pointed a child while clearing its slot:

       a child mutated this generation   [nil,  SLOT] -> [ADDR, SLOT]
       untouched, sibling overflows      [ADDR, SLOT] -> [ADDR, SLOT]
       untouched, structural split near  [ADDR, SLOT] -> [ADDR, SLOT]

   So `child-refs` was relying on a property of `store()` that nothing asserted.
   `Branch.addressesAndSlots()` makes the pairing correct by construction, and
   the second test below pins the coupling itself so a future settle change that
   breaks it is caught here rather than in a consumer's diff output.

   ## On the concurrency test

   It is a smoke test and is labelled as one. Two adjacent volatile reads are not
   a window Clojure can hit on purpose: run against the two-read version of
   `child-refs` it passed every time. It is kept because it exercises diff and
   settle on genuinely shared branch objects, which nothing else does — the
   readers in `concurrent_restore.clj` are `seq`/`count` and never pair an
   address with a slot."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as ss]
            [org.replikativ.persistent-sorted-set.test.storage :as ts])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet Branch]))

(set! *warn-on-reflection* true)

;; [k v t] ordered by all three; the upsert comparator keys on k alone, so
;; `(replace [k _ _] [k v' t'])` is content-only — the shape that deposits
;; Absent(old)+Present(new) into a leaf-parent's slot, and the shape datahike
;; produces. It keeps the element COUNT fixed, so a lost delta cannot hide as a
;; count difference.
(defn- full-cmp [[k1 v1 t1] [k2 v2 t2]]
  (let [c (compare k1 k2)]
    (if-not (zero? c) c
            (let [c (compare v1 v2)]
              (if-not (zero? c) c (compare t1 t2))))))

(defn- by-k [a b] (compare (nth a 0) (nth b 0)))

(def ^:private ^:const n-init 8000)
(def ^:private ^:const bf     512)   ; two levels: root -> leaves, so the slots live on the root
(def ^:private ^:const dbs    128)

(defn- upsert
  ^PersistentSortedSet [^PersistentSortedSet s ks v]
  (let [t (reduce (fn [^PersistentSortedSet t k] (.replace t [k 0 0] [k v v] by-k))
                  (.asTransient s) ks)]
    (.persistent ^PersistentSortedSet t)))

(defn- mk []
  (let [st   (ts/storage (atom {}) (atom {}))
        opts {:comparator full-cmp :branching-factor bf
              :diff-buf-size dbs :ref-type :strong}
        s0   (into (ss/sorted-set* (assoc opts :storage st))
                   (map (fn [k] [k 0 0]) (range n-init)))]
    (ss/store s0 st)
    {:st st :s0 s0}))

(defn- pairs
  "Per-child [address slot] for a branch, from ONE snapshot."
  [^Branch b]
  (let [p     (.addressesAndSlots b)
        addrs (aget ^objects p 0)
        slots (aget ^objects p 1)]
    (mapv (fn [i] [(when addrs (aget ^objects addrs i))
                   (when slots (aget ^objects slots i))])
          (range (.len b)))))

;; ---------------------------------------------------------------------------

(deftest the-address-slot-pair-comes-from-one-snapshot
  (testing "`addressesAndSlots` must return exactly what the two separate
            accessors return, or the pairing it exists to make coherent is
            coherent about the wrong thing"
    (let [{:keys [st s0]} (mk)
          s1 (upsert s0 [1 2] 1)
          ^Branch root (.root ^PersistentSortedSet s1)
          p (.addressesAndSlots root)]
      (is (identical? (.addressArray root) (aget ^objects p 0)) "same address array")
      (is (identical? (.slots root) (aget ^objects p 1)) "same slot array")
      (is (some? (aget ^objects p 1))
          "precondition: the root really carries slots, else this compares two nils")
      (ss/store s1 st))))

(deftest a-settle-never-repoints-a-child-that-already-had-an-address
  (testing "THE COUPLING that makes diff's pruning safe, and which nothing
            asserted before this test.

            store()'s settle skips every child whose pre-settle address is
            non-null — it is a clean passthrough, keeping both its address and
            its slot. That is what makes it harmless for `child-refs` to pair an
            address with a slot: a child that could be re-pointed has a NULL
            address going in, and `prune-shared` never prunes on a null address.

            If this ever fails, a settle has started re-pointing or re-slotting a
            child that already had an address, and diff's frontier can then pair
            a stale address with a cleared slot and silently drop that subtree's
            delta from the answer."
    (let [{:keys [st s0]} (mk)]
      (loop [gen 1, s s0]
        (when (<= gen 6)
          (let [;; a mix each generation: an old buffered child, a far child, and
                ;; a structural change, so passthrough children coexist with
                ;; settled ones rather than the tree being uniformly dirty
                s' (-> s
                       (upsert [1 2] gen)
                       (upsert (range (* gen 700) (+ (* gen 700) 40)) gen)
                       (as-> x (reduce (fn [acc k] (ss/conj acc [k 9 9] full-cmp))
                                       x (range (+ 20000 (* gen 1000))
                                                (+ 20400 (* gen 1000))))))
                ^Branch r (.root ^PersistentSortedSet s')
                before (pairs r)]
            (ss/store s' st)
            (let [after (pairs r)]
              (is (= (count before) (count after))
                  (str "gen " gen ": the settle changed the child count"))
              (doseq [i (range (min (count before) (count after)))]
                (let [[a-pre s-pre] (nth before i)
                      [a-post s-post] (nth after i)]
                  (when (some? a-pre)
                    (is (= a-pre a-post)
                        (str "gen " gen " child " i
                             ": a child with a pre-settle address was RE-POINTED"))
                    (is (identical? s-pre s-post)
                        (str "gen " gen " child " i
                             ": a child with a pre-settle address had its SLOT changed")))))
              (recur (inc gen) s'))))))))

(deftest diff-answers-correctly-while-a-settle-races-it
  (testing "SMOKE TEST — see the ns docstring. It does not reproduce the
            two-read defect (it passed against that version every time); a
            window of two adjacent volatile reads is not addressable from
            Clojure. It is here because nothing else runs diff and store
            concurrently over shared branch objects."
    (let [{:keys [st s0]} (mk)
          result
          (try
            (loop [round 0, prev s0]
              (if (== round 30)
                {:ok true}
                (let [next (upsert prev (range (* round 97) (+ (* round 97) 24)) (inc round))
                      ;; the model comes from the two immutable set VALUES, so it is
                      ;; exact however the threads interleaved
                      expected {:added   (vec (sort full-cmp (remove (set (seq prev)) (seq next))))
                                :removed (vec (sort full-cmp (remove (set (seq next)) (seq prev))))}
                      ds (mapv (fn [_] (future (ss/diff prev next st))) (range 4))
                      w  (future (ss/store next st))
                      answers (mapv deref ds)]
                  @w
                  (doseq [a answers]
                    (when (not= expected a)
                      (throw (ex-info "a differ lost or invented a delta"
                                      {:round round
                                       :expected-added (count (:added expected))
                                       :actual-added (count (:added a))}))))
                  (recur (inc round) next))))
            (catch java.util.concurrent.ExecutionException e {:ok false :error (.getCause e)})
            (catch Throwable e {:ok false :error e}))]
      (is (:ok result)
          (str "concurrent diff vs settle: " (:error result) " "
               (some-> ^Throwable (:error result) ex-data pr-str))))))
