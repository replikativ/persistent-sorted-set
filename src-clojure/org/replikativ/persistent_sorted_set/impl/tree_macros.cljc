(ns org.replikativ.persistent-sorted-set.impl.tree-macros)

(defmacro if-java-tree
  "Select code for JVM Java nodes or the portable CLJS/Jolt nodes."
  [java-tree portable-tree]
  #?(:jolt portable-tree
     :clj (if (:ns &env) portable-tree java-tree)))

;; The JVM keeps deftype's mutable fields package-private. CLJS and Jolt can
;; access the fields directly, including in the branch traversal hot paths.
(def ^:private accessors
  '{:children children, :addresses addresses, :subtree-count subtreeCount,
    :_measure nodeMeasure, :_slots slots, :_bufEntries bufferEntries,
    :_projCmp projectionComparator, :_childMeasures childMeasures, :_cmLazy lazyMeasureRecord})

(defmacro read-field [branch field]
  (if (or (:ns &env) #?(:jolt true :clj false))
    (list (symbol (str ".-" (name field))) branch)
    (list (symbol (str "." (accessors field)))
          (with-meta branch {:tag 'org.replikativ.persistent_sorted_set.branch.BranchState}))))

(defmacro write-field! [branch field value]
  (if (or (:ns &env) #?(:jolt true :clj false))
    (list 'set! (list (symbol (str ".-" (name field))) branch) value)
    (list (symbol (str "." (accessors field)))
          (with-meta branch {:tag 'org.replikativ.persistent_sorted_set.branch.BranchState}) value)))
