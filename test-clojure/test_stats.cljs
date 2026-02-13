(ns test-stats
  (:require [me.tonsky.persistent-sorted-set :as pss]
            [me.tonsky.persistent-sorted-set.impl.numeric-stats :as ns]))

(defn -main []
  (let [stats-ops ns/numeric-stats-ops

        ;; Test 1: Simple add
        _ (println "\n=== Test 1: Simple adds ===")
        s1 (pss/sorted-set* {:measure stats-ops})
        s2 (conj s1 1 2 3)
        st2 (pss/measure s2)
        _ (println "After adding 1,2,3:")
        _ (println "  Count:" (:cnt st2) "Expected: 3")
        _ (println "  Sum:" (:sum st2) "Expected: 6")

        ;; Test 2: Larger set
        _ (println "\n=== Test 2: 100 elements ===")
        s3 (into (pss/sorted-set* {:measure stats-ops}) (range 100))
        st3 (pss/measure s3)
        _ (println "Count:" (:cnt st3) "Expected: 100")
        _ (println "Sum:" (:sum st3) "Expected: 4950")

        ;; Test 3: The failing test scenario
        _ (println "\n=== Test 3: 5000 elements (failing test) ===")
        cmp-mod (fn [a b] (compare (mod a 5000) (mod b 5000)))
        s4 (into (pss/sorted-set* {:measure stats-ops :comparator cmp-mod :branching-factor 64})
                 (range 5000))
        st4 (pss/measure s4)
        _ (println "Count:" (:cnt st4) "Expected: 5000")

        ;; Test 4: After replace
        _ (println "\n=== Test 4: After replace ===")
        _ (println "Before replace, count:" (:cnt st4))
        s5 (pss/replace s4 0 5000)
        st5 (pss/measure s5)
        _ (println "After replace 0->5000, count:" (:cnt st5) "Expected: 5000")

        ;; Test 5: Simple replace on small set (single leaf)
        _ (println "\n=== Test 5: Small single-leaf set ===")
        s6 (into (pss/sorted-set* {:measure stats-ops}) (range 10))
        st6 (pss/measure s6)
        _ (println "Before replace, count:" (:cnt st6) "sum:" (:sum st6))
        s7 (pss/replace s6 0 0)
        st7 (pss/measure s7)
        _ (println "After identity replace 0->0, count:" (:cnt st7) "sum:" (:sum st7) "EXPECTED: 10, 45")

        ;; Test 6: Large set with branches
        _ (println "\n=== Test 6: Large set with branches ===")
        s9 (into (pss/sorted-set* {:measure stats-ops :branching-factor 4}) (range 20))
        st9 (pss/measure s9)
        _ (println "Before replace, count:" (:cnt st9) "sum:" (:sum st9))
        s10 (pss/replace s9 0 0)
        st10 (pss/measure s10)
        _ (println "After identity replace 0->0, count:" (:cnt st10) "sum:" (:sum st10) "EXPECTED: 20, 190")]
    nil))

(-main)
