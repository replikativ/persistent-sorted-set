(ns org.replikativ.persistent-sorted-set.test.tail-retention
  "A node that shrinks IN PLACE must not keep the elements it dropped alive.

   Shrinking decrements `_len` and leaves the old references sitting in the backing array
   past the new length. Reads only look at `[0, _len)`, so the tree is CORRECT — `seq`,
   `count` and `contains?` all agree — but the JVM still sees those references and the
   removed elements cannot be collected.

   Two conditions are needed, and both matter for writing a probe that is not vacuous:

     * the node must shrink IN PLACE, which happens only when it is `editable()` — i.e. on
       the TRANSIENT path. The persistent path allocates a fresh array for the successor,
       so it has no stale tail and measures 0 with or without the fix.
     * the dropped slot must not be overwritten by the shift. Deleting a contiguous PREFIX
       retains nothing, because every shift writes over the hole. Retention needs the
       `idx == _len - 1` case or the borrow/merge arms — hence the random victim set here.

   Measured before the fix (this probe, three seeds each):

       bf   8   6.00%  8.75%  6.00%   of removed elements still strongly reachable
       bf  64   7.50%  4.50%  3.75%
       bf 512   0.00%  1.00%  0.25%
       persistent path, every bf and seed:  0.00%

   The retention is bounded by the LIVE set rather than by history (roughly 1.5-7.6% of it,
   shrinking as the set shrinks) and it never clears on its own: twenty further churn rounds
   left the count unchanged. So this was a nuisance, not an unbounded leak — which is why the
   fix had to be shown free before it could land. It is: work is ~1.3 slots per delete and
   flat in branching factor, and four alternating benchmark rounds at bf 8/64/512 found no
   reliable difference.

   ## Why this test uses WeakReference and not `count`

   No content-level oracle can see this. The set's contents are already correct; what is
   wrong is the reachability of objects the set no longer contains. Only a reference queue
   can observe it.

   ## The trap that made the first probe report a false 0%

   `APersistentSortedSet.get` returns the PROBE, not the stored element, so weak-referencing
   `(get s k)` holds a key you just constructed and measures nothing. The objects must come
   out of `seq`, or — as here — be the very objects that were handed to `conj`."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set])
  (:import [java.lang.ref WeakReference]))

;; Distinct identity per element. Longs and Strings intern, which would keep them reachable
;; for reasons that have nothing to do with the tree and make the probe meaningless.
(defn- boxed [i] (into-array Object [i]))

(def ^:private cmp
  (comparator (fn [a b] (< (compare (aget ^objects a 0) (aget ^objects b 0)) 0))))

(defn- still-reachable
  "Delete a random subset and report how many of the removed elements the set still holds a
   strong reference to. Returns {:deleted n :alive n}."
  [{:keys [bf n dels transient? seed]}]
  (let [rng     (java.util.Random. seed)
        elems   (mapv boxed (range n))
        s0      (reduce #(set/conj %1 %2 cmp)
                        (set/sorted-set* {:comparator cmp :branching-factor bf}) elems)
        idxs    (take dels (distinct (repeatedly #(.nextInt rng (int n)))))
        victims (mapv #(nth elems %) idxs)
        refs    (mapv #(WeakReference. %) victims)
        s1      (if transient?
                  (persistent! (reduce disj! (clojure.core/transient s0) victims))
                  (reduce #(set/disj %1 %2 cmp) s0 victims))]
    ;; Drop every strong handle WE hold to the victims; keep the resulting SET alive, since
    ;; the question is whether IT retains them.
    (let [elems nil, victims nil, idxs nil, s0 nil]
      ;; Several passes: one System/gc is a hint, not a guarantee.
      (dotimes [_ 4] (System/gc) (Thread/sleep 50))
      {:deleted (count refs)
       :alive   (count (filter #(some? (.get ^WeakReference %)) refs))
       :count   (count s1)
       :n       n})))

(deftest a-transient-delete-does-not-retain-what-it-removed
  (testing "the in-place shrink path must clear the slots it abandons"
    (doseq [bf   [8 64 512]
            seed [1 2 3]]
      (let [{:keys [deleted alive count n]} (still-reachable
                                             {:bf bf :n 2000 :dels 400
                                              :transient? true :seed seed})]
        (is (= (- n deleted) count)
            (str "bf=" bf " seed=" seed ": PRECONDITION — the deletes must have happened, "
                 "or there is nothing to retain and this passes vacuously"))
        (is (zero? alive)
            (str "bf=" bf " seed=" seed ": " alive " of " deleted " removed elements are "
                 "still strongly reachable from the set"))))))

(deftest the-persistent-path-was-never-affected
  (testing "the control. It allocates a fresh array per successor, so it has no stale tail
            and measured 0 both before and after — a test that only ran this path would
            pass against the unfixed build."
    (doseq [bf [8 512]]
      (let [{:keys [alive]} (still-reachable {:bf bf :n 2000 :dels 400
                                              :transient? false :seed 1})]
        (is (zero? alive) (str "bf=" bf ": the persistent path retains nothing"))))))
