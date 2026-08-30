(ns org.replikativ.persistent-sorted-set.test.async-chunk
  "Parity of the chunk-level async traversal (PAsyncChunkedSeq/achunk-next)
   against the sync Iter's IChunkedSeq: identical chunk boundaries and
   identical concatenated elements over identical slice bounds, including
   same-leaf right bounds and multi-leaf hops."
  (:require [cljs.test :as test :refer-macros [is deftest testing]]
            [is.simm.partial-cps.async :refer [await] :refer-macros [async]]
            [org.replikativ.persistent-sorted-set :as set]
            [org.replikativ.persistent-sorted-set.btset :as btset]
            [org.replikativ.persistent-sorted-set.test.storage.util
             :refer [async-storage]]))

(defn- sync-chunks
  "Walk an Iter's chunked seq: vector of [chunk-vec …]."
  [iter]
  (loop [it iter acc []]
    (if (nil? it)
      acc
      (let [ch (-chunked-first it)
            n (count ch)
            cv (loop [i 0 v []]
                 (if (< i n) (recur (inc i) (conj v (nth ch i))) v))]
        (recur (-chunked-next it) (conj acc cv))))))

(defn- async-chunks
  "Walk achunk-next: (async [chunk-vec …]). Cap guards runaway."
  [aseq0]
  (async
   (loop [s aseq0 acc [] n 0]
     (when (> n 1000)
       (throw (ex-info "async-chunks runaway" {:n n})))
     (if (nil? s)
       acc
       (let [chunk (await (btset/achunk-next s))]
         (if (nil? chunk)
           acc
           (let [[keys start end next] chunk
                 cv (loop [i start v []]
                      (if (< i end) (recur (inc i) (conj v (aget keys i))) v))]
             (recur next (conj acc cv) (inc n)))))))))

(defn- check-bounds
  "For [from to]: the async chunk stream over `s-actual` must equal the sync
   Iter chunk stream over `s-expected` chunk-for-chunk (same boundaries), and
   concatenation must equal the plain slice. s-actual may be a
   storage-restored copy of s-expected."
  [s-expected s-actual from to]
  (async
   (let [cmp (.-comparator s-expected)
         sync-iter (set/slice s-expected from to cmp {:sync? true})
         expected-chunks (if (nil? sync-iter) [] (sync-chunks sync-iter))
         expected-flat (vec (or (seq sync-iter) []))
         aslice (await (set/slice s-actual from to cmp {:sync? false}))
         actual-chunks (await (async-chunks aslice))]
     (and (is (= expected-chunks actual-chunks)
              (str "chunk boundaries diverge on [" from " " to "]"))
          (is (= expected-flat (vec (apply concat actual-chunks)))
              (str "flat elements diverge on [" from " " to "]"))))))

(defn- do-test []
  (async
   (let [s (reduce conj (set/sorted-set) (range 0 5000))]
     (and
      (testing "in-memory"
        (and
         (testing "full range"        (await (check-bounds s s 0 4999)))
         (testing "mid multi-leaf"    (await (check-bounds s s 123 4567)))
         (testing "same-leaf narrow"  (await (check-bounds s s 100 105)))
         (testing "single element"    (await (check-bounds s s 42 42)))
         (testing "empty range"       (await (check-bounds s s 6000 7000)))
         (testing "leaf-boundary starts"
           ;; sweep a window across several leaf boundaries
           (loop [from 0 ok true]
             (if (and ok (< from 300))
               (recur (+ from 17) (await (check-bounds s s from (+ from 130))))
               (is ok "boundary sweep"))))))
      (testing "storage-backed (restored, cold nodes load via async storage)"
        (let [storage (async-storage)
              addr (await (set/store s storage {:sync? false}))
              restored (set/restore addr storage)]
          (and
           (testing "full range"       (await (check-bounds s restored 0 4999)))
           (testing "mid multi-leaf"   (await (check-bounds s restored 123 4567)))
           (testing "same-leaf narrow" (await (check-bounds s restored 100 105)))
           (testing "boundary sweep"
             (loop [from 0 ok true]
               (if (and ok (< from 300))
                 (recur (+ from 17) (await (check-bounds s restored from (+ from 130))))
                 (is ok "storage boundary sweep")))))))))))

(deftest async-chunk-parity-test
  (test/async done
              ((do-test)
               (fn [_] (done))
               (fn [err]
                 (is (nil? err) "async-chunk-parity-test rejected")
                 (js/console.error err)
                 (done)))))
