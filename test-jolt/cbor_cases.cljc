(ns cbor-cases
  (:require [boring.core :as boring]
            [org.replikativ.persistent-sorted-set :as pss]
            [org.replikativ.persistent-sorted-set.cbor :as cbor]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes]
            #?(:jolt [org.replikativ.persistent-sorted-set.impl.storage :as storage]))
  (:import [java.io RandomAccessFile]
           #?@(:jolt [] :clj [[org.replikativ.persistent_sorted_set IStorage]])))

(defn- write-bytes [path bytes]
  (with-open [f (RandomAccessFile. path "rw")]
    (.setLength f 0)
    (.write f bytes)))

(defn- read-bytes [path]
  (with-open [f (RandomAccessFile. path "r")]
    (let [bytes (byte-array (.length f))]
      (.readFully f bytes)
      bytes)))

(defn -main [mode dir]
  (let [serial (atom 0)
        reads (atom 0)
        node-opts {:registry (cbor/registry {})}
        save (fn [node]
               (let [addr (swap! serial inc)]
                 (write-bytes (str dir "/" addr) (boring/encode node node-opts))
                 addr))
        load-node (fn [addr]
                    (swap! reads inc)
                    (let [bytes (read-bytes (str dir "/" addr))
                          node (boring/decode bytes node-opts)]
                      (assert (= (seq bytes) (seq (boring/encode node node-opts))))
                      node))
        backend #?(:jolt (reify storage/IStorage
                           (store [_ n _] (save n))
                           (restore [_ a _] (load-node a))
                           (accessed [_ _]) (markFreed [_ _]))
                   :clj (reify IStorage
                          (store [_ n] (save n))
                          (restore [_ a] (load-node a))
                          (accessed [_ _]) (markFreed [_ _])
                          (isFreed [_ _] false) (freedInfo [_ _] nil)))
        cmp #(compare %2 %1)
        m {:index :descending}
        opts {:resolve-storage (fn [metadata] (assert (= m metadata)) backend)
              :resolve-cmp (fn [metadata] (assert (= m metadata)) cmp)}
        registry {:registry (cbor/registry opts)}
        path (str dir "/root")
        values (range 60)]
    (case mode
      "bulk"
      (let [build-opts {:branching-factor 4 :diff-buf-size 0 :storage backend :meta m}
            ks (reverse values)]
        (doseq [[suffix build] [["stream" #(pss/from-sorted-seq cmp ks build-opts)]
                                ["array" #(pss/from-sorted-array cmp (to-array (concat ks [nil])) 60 build-opts)]]]
          (let [s (build)
                addr (pss/store s backend)
                reopened (pss/restore addr backend (assoc build-opts :comparator cmp))]
            (assert (= ks (seq reopened)))
            (write-bytes (str path "-" suffix) (boring/encode s registry)))))
      "write"
      (let [s (reduce #(pss/conj %1 %2 cmp)
                      (pss/sorted-set* {:branching-factor 4 :diff-buf-size 0
                                        :meta m :comparator cmp :storage backend}) values)]
        (pss/store s backend)
        (write-bytes path (boring/encode s registry))
        (let [ctx (nodes/reader-context opts)
              detached (nodes/blob->root ctx (assoc (nodes/root->blob s) :count -1))]
          (write-bytes (str path "-unknown") (boring/encode detached registry))))
      "read"
      (doseq [[suffix n] [["" 60] ["-unknown" -1]]]
        (reset! reads 0)
        (let [bytes (read-bytes (str path suffix))
              s (boring/decode bytes registry)]
          (assert (= m (meta s)))
          (assert (= n (:count (nodes/root->blob s))))
          (assert (= (seq bytes) (seq (boring/encode s registry))))
          (assert (zero? @reads))
          (assert (= (reverse values) (seq s)))
          (assert (> @reads 1))
          (assert (= 60 (count s)))
          (assert (= 32 (pss/lookup s 32 cmp))))))))
