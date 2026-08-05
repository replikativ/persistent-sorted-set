(ns org.replikativ.persistent-sorted-set.test.settings-parity
  "The settings a caller ASKS for must be the settings the set HAS, on both
   runtimes and through every entry point.

   This exists because they were not. Three ClojureScript constructors —
   `sorted-set*`, `from-sorted-array` and `restore` — narrowed the options map
   with `select-keys` and left `:diff-buf-size` out, while the JVM's
   `map->settings` kept it. So `(sorted-set* {:diff-buf-size 256})` produced a
   set with diff-buf OFF on ClojureScript and ON on the JVM, silently.

   The consequence was worse than a missing option. Nodes restored from a
   slots-aware storage carry their own `:diff-buf-size` in the blob, so THOSE
   buffered while freshly created ones did not — the feature ran half-on, in a
   configuration nobody chose and no test described. It also meant the cljs test
   build's `:closure-defines {default-diff-buf-size 256}`, whose stated purpose
   is to gate the buffering path, gated nothing: the define was merged into the
   options map and then dropped one call later.

   Read the value off the SET rather than trusting the constructor's argument,
   because the argument is exactly what was being discarded."
  (:require [clojure.test :refer [deftest is testing]]
            [org.replikativ.persistent-sorted-set :as s]
            #?(:cljs [org.replikativ.persistent-sorted-set.test.storage.util :as u]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set PersistentSortedSet])))

(defn- diff-buf-size
  "The set's effective `:diff-buf-size`, read from the set itself."
  [set]
  #?(:clj  (.diffBufSize (.-_settings ^PersistentSortedSet set))
     :cljs (or (:diff-buf-size (.-settings set)) 0)))

(defn- branching-factor [set]
  #?(:clj  (.branchingFactor (.-_settings ^PersistentSortedSet set))
     :cljs (:branching-factor (.-settings set))))

;; The JVM's in-tree storage helper and the cljs one differ; each platform uses
;; the serializing storage it has.
(defn- storage []
  #?(:clj  (let [disk (atom {}) mem (atom {})]
             ((requiring-resolve 'org.replikativ.persistent-sorted-set.test.storage/storage) mem disk))
     :cljs (u/storage)))

(def ^:private opts {:branching-factor 16 :diff-buf-size 256})

(deftest sorted-set-keeps-the-settings-it-was-given
  (let [set (s/sorted-set* (assoc opts :comparator compare))]
    (is (= 256 (diff-buf-size set)))
    (is (= 16 (branching-factor set)))))

(deftest from-sorted-array-keeps-the-settings-it-was-given
  (let [arr #?(:clj (to-array (range 100)) :cljs (into-array (range 100)))
        set (s/from-sorted-array compare arr 100 opts)]
    (is (= 256 (diff-buf-size set)))
    (is (= 16 (branching-factor set)))))

(deftest restore-keeps-the-settings-it-was-given
  (testing "a restored set must agree with a fresh one — this is the case that
            regressed, and the one that matters, because a long-lived database is
            restored far more often than it is created"
    (let [st  (storage)
          src (reduce #(s/conj %1 %2 compare)
                      (s/sorted-set* (assoc opts :comparator compare))
                      (range 1000))
          addr #?(:clj (s/store src st) :cljs (s/store src st))
          set  #?(:clj  (s/restore-by compare addr st opts)
                  :cljs (s/restore addr st (assoc opts :comparator compare)))]
      (is (= 256 (diff-buf-size set)))
      (is (= 16 (branching-factor set)))
      (is (= (vec (range 1000)) (vec (seq set)))
          "and it is still the same set"))))

(deftest diff-buf-off-stays-off
  (testing "the fix must not turn buffering ON for a caller who did not ask —
            0 is the library default and the byte-identical baseline"
    (let [set (s/sorted-set* {:branching-factor 16 :diff-buf-size 0 :comparator compare})]
      (is (= 0 (diff-buf-size set))))))
