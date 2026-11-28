(ns org.replikativ.persistent-sorted-set.impl.tree-macros)

(defmacro if-java-tree
  "Select code for JVM Java nodes or the portable CLJS/Jolt nodes."
  [java-tree portable-tree]
  #?(:jolt portable-tree
     :clj (if (:ns &env) portable-tree java-tree)))
