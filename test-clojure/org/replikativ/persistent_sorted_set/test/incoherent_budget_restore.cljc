(ns org.replikativ.persistent-sorted-set.test.incoherent-budget-restore
  "A storage that persists a node's diff buffer must reconstruct it with the same budget.

   Handing buffered slots to a node whose settings say buffering is OFF is an incoherent
   reconstruction. It is not a harmless mismatch: `store` takes the baseline path at budget
   0 and writes the node WITHOUT its slots, so every buffered element-change is silently
   dropped. The set reads correctly right up until the next commit, and then the data is
   gone from disk.

   The JVM has refused this at `Branch.installSlots` for a while. ClojureScript accepted it
   and lost the data — measured on cljs before the fix, a restored root carrying 7 buffered
   slots whose contents were exact beforehand: one `conj`, then store and cold restore,
   lost 13 previously committed elements. So the identical storage-contract violation was
   loud on one runtime and silent on the other, which is the worst of both worlds for
   anyone writing an `IStorage`.

   Both runtimes now refuse it at their respective single entry point for restore-time slot
   installation (`Branch.installSlots` / `branch/install-slots!`).

   ## Why this is a throw and not a repair

   The settings come from the blob the STORAGE itself wrote, so the storage is the party
   that can fix it. Guessing — say, adopting a positive budget from the slots — would make
   a wrong reconstruction look like a working one, and the failure would resurface later as
   missing elements with a matching count, which is far harder to attribute."
  (:require [clojure.test :refer [deftest testing is]]
            [org.replikativ.persistent-sorted-set :as set]
            #?(:clj  [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
            #?(:cljs [org.replikativ.persistent-sorted-set.branch :as branch]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set Branch Settings Slot])))

(def ^:private cmp compare)

;; Build a real Branch at budget 0 and hand it slots directly — the exact shape a storage
;; produces when it persisted `:slots` but rebuilt the node without the budget. Going
;; through the installation entry point rather than a full codec round-trip keeps the test
;; about the contract rather than about any one storage's blob format.
(defn- install-on-a-zero-budget-node! []
  #?(:clj
     (let [settings (Settings. (int 8) nil nil nil (int 0))
           keys     (object-array [1 2 3])
           b        (Branch. (int 1) (int 3) keys nil nil settings)
           slots    (object-array [(Slot. nil 1 nil "anchor-a") nil nil])]
       (.installSlots b slots -2))
     :cljs
     (let [b (branch/->Branch 1 (array 1 2 3) nil nil nil nil
                              {:branching-factor 8 :diff-buf-size 0 :comparator cmp}
                              nil 0 nil)]
       (branch/install-slots! b (array {:diff nil :count 1 :measure nil :anchor "anchor-a"}
                                       nil nil)))))

(deftest slots-on-a-zero-budget-node-are-refused
  (testing "the storage persisted a diff buffer and rebuilt the node declaring there is
            none — refuse it loudly rather than dropping the buffered changes at the next
            store"
    (let [e (try (install-on-a-zero-budget-node!) nil
                 (catch #?(:clj Throwable :cljs :default) t t))]
      (is (some? e)
          "installing slots on a node whose settings say diff-buf-size 0 must throw")
      (is (re-find #"diff-buf" (or (#?(:clj .getMessage :cljs ex-message) e) ""))
          (str "and the message must name the contract that was broken, got: "
               (pr-str (#?(:clj .getMessage :cljs ex-message) e)))))))

(deftest a-coherent-budget-is-accepted
  (testing "the guard must not reject the ordinary case — a node whose settings carry the
            same positive budget the slots came from. A test that only checked the throw
            would pass against a build that refused everything."
    (let [ok (try
               #?(:clj
                  (let [settings (Settings. (int 8) nil nil nil (int 64))
                        b        (Branch. (int 1) (int 3) (object-array [1 2 3]) nil nil settings)]
                    (.installSlots b (object-array [(Slot. nil 1 nil "anchor-a") nil nil]) -2)
                    true)
                  :cljs
                  (let [b (branch/->Branch 1 (array 1 2 3) nil nil nil nil
                                           {:branching-factor 8 :diff-buf-size 64 :comparator cmp}
                                           nil 0 nil)]
                    (branch/install-slots!
                     b (array {:diff nil :count 1 :measure nil :anchor "anchor-a"} nil nil))
                    true))
               (catch #?(:clj Throwable :cljs :default) t t))]
      (is (true? ok)
          (str "a matching positive budget must be accepted, got: " (pr-str ok))))))
