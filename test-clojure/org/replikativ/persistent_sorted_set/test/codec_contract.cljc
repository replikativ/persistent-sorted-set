(ns org.replikativ.persistent-sorted-set.test.codec-contract
  "Cross-codec contract tests: properties that must hold the SAME way for every node
   codec, or that are deliberately allowed to differ and must then be pinned so the
   difference cannot change silently.

   The per-codec suites (`test.fressian-handlers`, `test.cbor-handlers`,
   `test.transit-handlers`) each round-trip their own codec through itself. That
   cannot catch a codec drifting AWAY from its siblings, because every one of them
   agrees with itself by construction. This namespace is where the comparisons live."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes]
            [boring.core :as boring]
            [cognitect.transit :as transit])
  #?(:clj (:import [java.io ByteArrayOutputStream ByteArrayInputStream])))

(defn- transit-roundtrip
  "Write and read `v` with transit :json, passing `wopts` to the WRITER."
  [v wopts]
  #?(:clj  (let [out (ByteArrayOutputStream.)]
             (transit/write (transit/writer out :json wopts) v)
             (transit/read (transit/reader (ByteArrayInputStream. (.toByteArray out)) :json)))
     :cljs (transit/read (transit/reader :json)
                         (transit/write (transit/writer :json wopts) v))))

;; ---------------------------------------------------------------------------
;; Element metadata is NOT part of the node codec's contract.
;; ---------------------------------------------------------------------------

(deftest element-metadata-is-codec-dependent
  (testing "PSS cannot normalize element metadata and must not pretend it can.

            `node->map` copies the key list into a fresh vector, so nothing the
            NODE codec owns carries metadata. Whether a consumer's ELEMENT keeps
            its metadata is decided by the writer the consumer configures:

              fressian          drops it
              transit (default) drops it
              transit + :transform transit/write-meta   keeps it
              CBOR/boring       keeps it

            So a tree written as CBOR and relayed as transit loses element
            metadata, and that is a property of the two libraries, not a bug this
            codec can fix -- `:transform` and boring's encode options are set on
            the WRITER, above these handlers, and PSS never sees them.

            Pinned here so the asymmetry is a documented, tested fact rather than
            a surprise found in production. If you need metadata preserved across
            codecs, put it IN the element."
    (let [elem (with-meta [7] {:m 1})
          ;; A blob shaped like a leaf's, carrying a metadata-bearing element, run
          ;; through each library's ordinary value encoding — which is exactly what
          ;; happens to `(:keys blob)` inside a real node.
          blob {:keys [elem] :branching-factor 8 :diff-buf-size 0}
          meta-of #(meta (first (:keys %)))]
      (is (= {:m 1} (meta elem)) "precondition: the element carries metadata")

      (testing "CBOR keeps it"
        (is (= {:m 1} (meta-of (boring/decode (boring/encode blob))))))

      (testing "transit drops it by default, and keeps it with :transform write-meta"
        (is (nil?     (meta-of (transit-roundtrip blob {}))))
        (is (= {:m 1} (meta-of (transit-roundtrip blob {:transform transit/write-meta})))))

      (testing "so the two DISAGREE — the fact this test exists to pin"
        (is (not= (meta-of (boring/decode (boring/encode blob)))
                  (meta-of (transit-roundtrip blob {}))))))))

;; ---------------------------------------------------------------------------
;; Unregistered ids fail at DECODE, not at first traversal.
;; ---------------------------------------------------------------------------

(deftest unregistered-id-throws-with-a-useful-message
  (testing "an id present in the root's meta but absent from the registry is a
            misconfiguration. Returning nil deferred the failure into the lazy
            tree walk, where it surfaced as an opaque NPE arbitrarily far from the
            cause -- and for storage specifically, only once someone touched an
            unloaded node, which may be much later than the decode."
    (let [resolve-storage (nodes/registry-storage-resolver)
          resolve-cmp     (nodes/registry-cmp-resolver)
          resolve-measure (nodes/registry-measure-resolver)]

      (testing "no id at all is still nil — a lexically-scoped serializer, or a
                root with no storage, legitimately has none"
        (is (nil? (resolve-storage {})))
        (is (nil? (resolve-cmp {})))
        (is (nil? (resolve-measure {}))))

      (testing "a registered id resolves"
        (nodes/register-storage! ::s :the-storage)
        (try
          (is (= :the-storage (resolve-storage {nodes/storage-id-key ::s})))
          (finally (nodes/unregister-storage! ::s))))

      (testing "an unknown id throws, naming the id and what was registered"
        (doseq [[what f id-key]
                [["storage"    resolve-storage nodes/storage-id-key]
                 ["comparator" resolve-cmp     nodes/comparator-id-key]
                 ["measure"    resolve-measure nodes/measure-id-key]]]
          (let [e (try (f {id-key ::missing}) nil
                       (catch #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core/ExceptionInfo) e e))]
            (is (some? e) (str what ": an unknown id must throw"))
            (is (= ::nodes/unregistered (:type (ex-data e))))
            (is (= ::missing (:id (ex-data e))))
            (is (re-find #"register it before deserializing" (ex-message e))
                (str what ": the message must say what to do")))))

      (testing "unregistering makes a previously good id fail — the live-storage
                lifecycle case, where a store is closed while roots referencing it
                are still in flight"
        (nodes/register-storage! ::tmp :x)
        (is (= :x (resolve-storage {nodes/storage-id-key ::tmp})))
        (nodes/unregister-storage! ::tmp)
        (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core/ExceptionInfo)
                     (resolve-storage {nodes/storage-id-key ::tmp})))))))
