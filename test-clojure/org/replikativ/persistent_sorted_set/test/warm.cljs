(ns org.replikativ.persistent-sorted-set.test.warm
  "The breadth-first warm, on ClojureScript — BOTH arms.

   The sync arm mirrors the JVM suite; the async arm is the one this platform
   exists for: `{:sync? false}` returns a partial-cps expression and every
   restore goes through the sliding window, against a storage that ASSERTS
   `{:sync? false}` on every call — so a walk that accidentally took a
   synchronous shortcut fails on the assertion, not on a hunch.

   The oracle is `count @*disk`: a `:with-leaves` warm with room in the budget
   must fetch every stored blob except the root (restored to enter the walk),
   and the storage's read counter must agree. Asserting against the blob count
   rather than a traversal keeps the oracle independent of every read path this
   library has.

   TRAP, inherited from `count-reads.cljs`: `u/storage`'s `*memory` caches by
   address and `:reads` counts only cache MISSES — every cold restore below
   gets a fresh memory cache over the same `*disk`."
  (:require [cljs.test :as test :refer-macros [is deftest testing]]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [org.replikativ.persistent-sorted-set :as s]
            [org.replikativ.persistent-sorted-set.warm :as warm]
            [org.replikativ.persistent-sorted-set.test.storage.util :as u]))

(defn- build!
  "Build 0..n-1 at branching factor `bf` through a SYNC storage, returning
   {:addr :disk :opts}. The async arm restores over the same disk with an
   async storage — the blob format is shared."
  [n bf]
  (let [opts {:branching-factor bf}
        disk (atom {})
        st   (u/storage (atom {}) disk opts)
        s0   (reduce s/conj (s/sorted-set* (assoc opts :storage st)) (range n))
        addr (s/store s0 st)]
    {:addr addr :disk disk :opts opts}))

(defn- cold-sync [{:keys [addr disk opts]}]
  (s/restore addr (u/storage (atom {}) disk opts) opts))

(defn- cold-async [{:keys [addr disk opts]}]
  (s/restore addr (u/async-storage (atom {}) disk opts) opts))

(defn- reads [] (:reads @u/*stats))

;; ---------------------------------------------------------------------------
;; sync arm

(deftest sync-full-warm-fetches-every-stored-blob
  (let [{:keys [disk] :as fx} (build! 2000 8)
        blobs (count @disk)]
    (reset! u/*stats {:reads 0 :writes 0 :accessed 0})
    (let [set (cold-sync fx)
          r   (warm/warm! set {:sync? true :depth :with-leaves :budget 100000})]
      (is (= (dec blobs) (:fetched r))
          "everything except the root, which entering the walk restored")
      (is (= blobs (reads)) "the storage agrees, root included")
      (is (false? (:budget-exhausted? r))))))

(deftest sync-budget-is-a-hard-ceiling
  (let [fx (build! 2000 8)]
    (reset! u/*stats {:reads 0 :writes 0 :accessed 0})
    (let [set (cold-sync fx)
          r   (warm/warm! set {:sync? true :depth :with-leaves :budget 7})]
      (is (= 7 (:fetched r)))
      (is (true? (:budget-exhausted? r)))
      (is (= 8 (reads)) "budget + the root restore"))))

(deftest sync-range-scoped-warm-is-proportional-to-the-range
  (let [fx (build! 4000 8)]
    (reset! u/*stats {:reads 0 :writes 0 :accessed 0})
    (let [narrow (warm/warm! (cold-sync fx)
                             {:sync? true :depth :with-leaves :budget 100000
                              :from 100 :to 140})
          full   (warm/warm! (cold-sync fx)
                             {:sync? true :depth :with-leaves :budget 100000})]
      (is (pos? (:fetched narrow)))
      (is (< (:fetched narrow) (/ (:fetched full) 10))
          (str "a 1% key range must warm a small fraction: narrow="
               (:fetched narrow) " full=" (:fetched full))))))

;; ---------------------------------------------------------------------------
;; async arm — the one this platform exists for

(defn- async-full-warm []
  (async
   (let [fx    (build! 2000 8)
         blobs (count @(:disk fx))]
     (reset! u/*stats {:reads 0 :writes 0 :accessed 0})
     (let [set (cold-async fx)
           r   (await (warm/warm! set {:sync? false :depth :with-leaves
                                       :budget 100000}))]
       (is (= (dec blobs) (:fetched r))
           "the async walk fetches everything except the root")
       (is (= blobs (reads)) "the async storage agrees")
       (is (pos? (:rounds r)))
       (is (vector? (:by-level r)))))))

(deftest async-full-warm-fetches-every-stored-blob
  (test/async done
              ((async-full-warm)
               (fn [_] (done))
               (fn [err] (is (nil? err)) (js/console.error err) (done)))))

(defn- async-budget-and-width []
  (async
   (let [fx (build! 2000 8)]
     (reset! u/*stats {:reads 0 :writes 0 :accessed 0})
     (let [set (cold-async fx)
           ;; width 2 forces the sliding window to actually slide: 7 restores
           ;; through a 2-wide window is at least four turns of starts, and a
           ;; window that started everything at once would still have to
           ;; deliver results IN ORDER for the assertions below to hold.
           r   (await (warm/warm! set {:sync? false :depth :with-leaves
                                       :budget 7 :width 2}))]
       (is (= 7 (:fetched r)))
       (is (true? (:budget-exhausted? r)))
       (is (zero? (:budget-left r)))
       (is (= 8 (reads)) "budget + the root restore")))))

(deftest async-budget-is-a-hard-ceiling
  (test/async done
              ((async-budget-and-width)
               (fn [_] (done))
               (fn [err] (is (nil? err)) (js/console.error err) (done)))))

(defn- async-two-trees []
  (async
   (let [fa (build! 2000 8)
         fb (build! 2000 8)]
     (reset! u/*stats {:reads 0 :writes 0 :accessed 0})
     (let [a (cold-async fa)
           b (cold-async fb)
           r (await (warm/warm-trees! [{:key :a :set a} {:key :b :set b}]
                                      {:sync? false :depth :with-leaves
                                       :budget 40}))]
       (is (= 40 (:fetched r)))
       (let [{:keys [a b]} (:by-index r)]
         (is (and (pos? a) (pos? b)) "both trees were warmed")
         (is (<= (abs (- a b)) 2)
             (str "the shared budget splits evenly, got a=" a " b=" b)))))))

(deftest async-trees-share-one-budget-fairly
  (test/async done
              ((async-two-trees)
               (fn [_] (done))
               (fn [err] (is (nil? err)) (js/console.error err) (done)))))

(defn- async-warm-then-sync-read []
  (async
   (let [fx (build! 500 8)]
     (reset! u/*stats {:reads 0 :writes 0 :accessed 0})
     ;; The serverless shape this exists for: datahike's cljs read path runs a
     ;; SYNC query engine over async storage, so a complete async warm is what
     ;; makes synchronous reading possible at all. Warm through the async
     ;; storage, then read the SAME disk synchronously through the memory the
     ;; warm populated.
     (let [mem (atom {})
           set (s/restore (:addr fx)
                          (u/->AsyncStorage mem (:disk fx)
                                            (merge {:branching-factor 512} (:opts fx)))
                          (:opts fx))
           _   (await (warm/warm! set {:sync? false :depth :with-leaves
                                       :budget 100000}))
           ;; every node the sync reader could need is now in `mem`
           sset (s/restore (:addr fx)
                           (u/storage mem (:disk fx) (:opts fx))
                           (:opts fx))
           before (reads)]
       (is (= (vec (range 500)) (vec sset))
           "the sync scan answers correctly over async-warmed memory")
       (is (= before (reads))
           "and performs zero further restores — the warm made sync reading free")))))

(deftest async-warm-enables-synchronous-reading
  (test/async done
              ((async-warm-then-sync-read)
               (fn [_] (done))
               (fn [err] (is (nil? err)) (js/console.error err) (done)))))
