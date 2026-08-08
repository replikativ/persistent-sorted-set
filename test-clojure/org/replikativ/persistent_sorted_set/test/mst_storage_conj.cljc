(ns org.replikativ.persistent-sorted-set.test.mst-storage-conj
  "`conj` into a STORED, cold-restored MST tree — the combination no test covered.

   The two MST+storage tests in the suite (`cbor_handlers`, `fressian_handlers`) only ever
   `disj` after their restore, and `mst.cljc` is entirely in-memory. So the `conj` path
   through a content-defined boundary with durable addresses had never been exercised on
   either runtime, and on ClojureScript it was broken outright.

   `mst-branch-add` hard-coded `nil` for the successor's `addresses` array in both arms,
   and its caller passed nothing. The JVM's `Branch.add` MST path builds `allAddresses`
   from the snapshot with one `copyOne(null)` per new node and hands it to the successor.
   So on ClojureScript every `conj` into a stored MST branch discarded its unchanged
   siblings' durable addresses — while those siblings were still unmaterialized `nil`
   child pointers, since `ensure-children` on a cold branch allocates a nil array and
   fills only the index being descended. The node was then left with NEITHER a child NOR
   an address for those slots, which is not a representable state: a rebuild must carry an
   unchanged child's pointer, durable or in-memory.

   Measured before the fix, 9/9 configurations (bf 32, n in {200,800,3000}, lzpl in
   {2,3,4}), store -> cold restore -> one `conj`:

     JVM    {:seq-after-conj {:ok 201}, :store-after-conj {:ok true}}
     cljs   Assert failed: (some? (aget (.-children this) i))

   COLD it throws. WARM — after a full traversal has materialized everything — it does not
   throw but silently rewrites the tree: one `conj` wrote 9-46 blobs against the JVM's
   2-8, and at n=800/lzpl=4 that was 46 of 46, the entire index. That second form is the
   dangerous one, because nothing fails; it just stops sharing.

   The test asserts BOTH: the cold path works, and the write amplification stays bounded."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.boundary :as b]
            #?(:clj  [org.replikativ.persistent-sorted-set.test.storage :as tstore])
            #?(:cljs [org.replikativ.persistent-sorted-set.test.storage.util :as util]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set Settings])))

(def ^:private cmp compare)

#?(:clj
   (do
     (defn- storage [disk bf] (tstore/->Storage (atom {}) disk (Settings. (int bf) nil nil nil (int 0))))
     (defn- restore* [addr st opts] (set/restore-by cmp addr st opts)))
   :cljs
   (do
     (defn- storage [disk bf] (util/storage (atom {}) disk {:branching-factor bf :diff-buf-size 0 :comparator cmp}))
     (defn- restore* [addr st opts] (set/restore addr st opts))))

(defn- elems [s] (vec #?(:clj (seq s) :cljs (set/seq s))))

;; [branching-factor n level-probability] — lzpl is the MST boundary's level parameter.
(def ^:private cases [[32 200 2] [32 200 4] [32 800 3] [32 800 4] [32 3000 2]])

(deftest conj-into-a-cold-restored-mst-tree
  (testing "the unchanged siblings' durable addresses must survive the rebuild"
    (doseq [[bf n lzpl] cases]
      (let [opts  {:comparator cmp :branching-factor bf :boundary (b/mst-boundary lzpl)}
            disk  (atom {})
            s0    (reduce #(set/conj %1 %2 cmp) (set/sorted-set* opts) (range n))
            addr  (set/store s0 (storage disk bf))
            blobs (count @disk)
            lbl   (str "bf=" bf " n=" n " lzpl=" lzpl)

            ;; COLD: restore and conj without materializing anything first. This is where
            ;; the missing addresses surface as "neither child nor address".
            cold  (restore* addr (storage disk bf) opts)
            grown (set/conj cold n cmp)]
        (is (= (inc n) (count (elems grown)))
            (str lbl ": a cold restore + one conj must yield n+1 elements"))
        (is (= (vec (range (inc n))) (elems grown))
            (str lbl ": and the contents must be exact"))

        ;; and it must still be storable — the state above is only detectable at store time
        ;; on some shapes, because a nil address is not read until something writes.
        (let [before (count (keys @disk))
              a2     (set/store grown (storage disk bf))
              wrote  (- (count (keys @disk)) before)]
          (is (= (vec (range (inc n))) (elems (restore* a2 (storage disk bf) opts)))
              (str lbl ": round-trips after the conj"))
          ;; Write amplification: one conj touches its path, not the index. The broken
          ;; version rewrote up to 100% of the blobs; the JVM writes single digits. The
          ;; bound is generous so this pins the defect without being brittle about the
          ;; exact path length.
          (is (< wrote (max 8 (quot blobs 4)))
              (str lbl ": one conj wrote " wrote " blobs of " blobs
                   " — it must touch its path, not rewrite the index")))))))

(deftest conj-into-a-warm-restored-mst-tree
  (testing "the same, but with the tree fully materialized first — the form that does not
            throw and instead silently stops sharing structure"
    (doseq [[bf n lzpl] cases]
      (let [opts  {:comparator cmp :branching-factor bf :boundary (b/mst-boundary lzpl)}
            disk  (atom {})
            s0    (reduce #(set/conj %1 %2 cmp) (set/sorted-set* opts) (range n))
            addr  (set/store s0 (storage disk bf))
            blobs (count @disk)
            warm  (restore* addr (storage disk bf) opts)
            _     (dorun (elems warm))                    ; materialize everything
            grown (set/conj warm n cmp)
            before (count (keys @disk))
            a2    (set/store grown (storage disk bf))
            wrote (- (count (keys @disk)) before)
            lbl   (str "bf=" bf " n=" n " lzpl=" lzpl)]
        (is (= (vec (range (inc n))) (elems grown)) (str lbl ": contents"))
        (is (= (vec (range (inc n))) (elems (restore* a2 (storage disk bf) opts)))
            (str lbl ": round-trips"))
        (is (< wrote (max 8 (quot blobs 4)))
            (str lbl ": warm conj wrote " wrote " of " blobs " blobs"))))))
