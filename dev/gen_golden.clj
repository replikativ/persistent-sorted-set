;; Generator for test-resources/fressian-golden-0.4.137.edn — the compatibility
;; oracle that test.fressian-compat asserts against.
;;
;; MUST be run against the PRE-refactor tree (tag 0.4.137), NOT the current one.
;; Regenerating from HEAD would make the fixtures agree with whatever HEAD does,
;; which is exactly the drift the oracle exists to detect:
;;
;;   git worktree add --detach /tmp/pss-0.4.137 0.4.137
;;   cd /tmp/pss-0.4.137 && clojure -T:build java
;;   clojure -J-Dout.file=<repo>/test-resources/fressian-golden-0.4.137.edn \
;;           -Sdeps '{:deps {org.clojure/data.fressian {:mvn/version "1.1.0"} \
;;                           org.replikativ/hasch {:mvn/version "0.4.98"}}}' \
;;           -M <repo>/dev/gen_golden.clj
;;
;; Addresses are a counter, not random-uuid: a branch blob embeds its children's
;; addresses, so random ones would make the bytes uncomparable between runs.

;; Generate the golden fressian fixtures. MUST be run against origin/main (pre-refactor).
(require 'clojure.pprint)
(require '[org.replikativ.persistent-sorted-set :as set]
         '[org.replikativ.persistent-sorted-set.fressian :as pf]
         '[org.replikativ.persistent-sorted-set.boundary :as bnd]
         '[clojure.data.fressian :as fress])
(import '[org.replikativ.persistent_sorted_set IStorage]
        '[java.io ByteArrayOutputStream])

(def write-lookup (-> (merge fress/clojure-write-handlers pf/write-handlers)
                      fress/associative-lookup fress/inheritance-lookup))
(defn ser ^bytes [node lk]
  (let [out (ByteArrayOutputStream.)]
    (.writeObject (fress/create-writer out :handlers lk) node)
    (.toByteArray out)))
(defn hex [^bytes bs] (apply str (map #(format "%02x" (bit-and % 0xff)) bs)))

;; Deterministic addresses — random UUIDs would make the bytes uncomparable.
(defrecord DetStorage [ctr *blobs]
  IStorage
  (store [_ node] (let [a (str "a" (format "%04d" (swap! ctr inc)))]
                    (swap! *blobs assoc a (hex (ser node write-lookup))) a))
  (accessed [_ _] nil)
  (restore [_ _] (throw (ex-info "write-only" {})))
  (markFreed [_ _] nil) (isFreed [_ _] false) (freedInfo [_ _] nil))

(def cases
  ;; Small on purpose: these live in git. Each targets a distinct blob shape.
  [{:name "leaf-only"   :bf 64 :dbs 0 :elems (vec (range 5))}      ; single leaf, no branch
   {:name "multi-level" :bf 4  :dbs 0 :elems (vec (range 40))}     ; branches + :subtree-count
   {:name "slots"       :bf 4  :dbs 4 :elems (vec (range 40))}     ; diff-buf ⇒ :slots present
   {:name "strings"     :bf 4  :dbs 0 :elems (mapv #(str "k" %) (range 20))}])

(def fixtures
  (into {} (for [{:keys [name bf dbs elems]} cases]
             (let [*blobs (atom {})
                   st (->DetStorage (atom 0) *blobs)
                   s (reduce (fn [s e] (set/conj s e compare))
                             (set/sorted-set* {:storage st :branching-factor bf
                                               :diff-buf-size dbs})
                             elems)
                   addr (set/store s st)
                   root-lk (-> (merge fress/clojure-write-handlers
                                      pf/write-handlers pf/root-write-handlers)
                               fress/associative-lookup fress/inheritance-lookup)]
               [name {:bf bf :dbs dbs :elems (vec (sort compare elems)) :root addr
                      :nodes @*blobs :root-blob (hex (ser s root-lk))}]))))

(spit (System/getProperty "out.file")
      (str ";; GOLDEN FRESSIAN FIXTURES — generated from origin/main (PRE-refactor).\n"
           ";; Do NOT regenerate from the current tree: the point is that these bytes\n"
           ";; predate the impl.nodes extraction. See test.fressian-compat.\n"
           (with-out-str (clojure.pprint/pprint fixtures))))
(println :NODES (mapv (fn [[k v]] [k (count (:nodes v))]) fixtures))
