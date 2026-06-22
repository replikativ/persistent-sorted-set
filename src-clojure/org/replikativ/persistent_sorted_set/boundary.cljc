(ns org.replikativ.persistent-sorted-set.boundary
  "Optional content-defined boundary policies (prolly / Merkle Search Tree mode), for BOTH
   platforms. Not required by core PSS — loaded only by consumers that opt into prolly mode
   (the default count B-tree needs no hashing). Enable per store via
   `{:boundary (mst-boundary <lzpl>)}`.

   The determinism contract: a JVM writer and a cljs reader MUST compute the IDENTICAL tree
   for the same key set, so the boundary structure is content-addressed identically. The
   whole match reduces to `key-level` returning the same integer on both platforms, which it
   does because (a) hasch.fast emits byte-identical hashes JVM↔cljs and (b) leading-zeros is
   `Integer/numberOfLeadingZeros` ≡ `js/Math.clz32`. See .internal/SPLIT_SEAM_DESIGN.md.

   `key-level` is the single shared (cljc) source of truth for that. The split RULE (cut after
   each boundary key) is trivially identical and applied per-platform over native arrays: the
   JVM impl reifies the Java IBoundary interface; the cljs impl is a PBoundary record."
  (:require [hasch.fast :as hf]
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.boundary :as b]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set IBoundary])))

;; ---- shared, determinism-critical -----------------------------------------------------

(defn- clz32
  "Leading zeros of x interpreted as a 32-bit unsigned value. JVM Integer/numberOfLeadingZeros
   and JS Math.clz32 are defined identically, which is what makes key-level cross-platform."
  ^long [x]
  #?(:clj  (Integer/numberOfLeadingZeros (unchecked-int x))
     :cljs (js/Math.clz32 x)))

(defn- hash-u32
  "Top 4 bytes of hasch.fast/edn-hash(key) assembled big-endian into a 32-bit value. hasch.fast
   is byte-identical across JVM and cljs (binary encoding, mirrored encoders)."
  [key]
  (let [h (hf/edn-hash key)
        b0 (bit-and #?(:clj (aget ^bytes h 0) :cljs (aget h 0)) 0xff)
        b1 (bit-and #?(:clj (aget ^bytes h 1) :cljs (aget h 1)) 0xff)
        b2 (bit-and #?(:clj (aget ^bytes h 2) :cljs (aget h 2)) 0xff)
        b3 (bit-and #?(:clj (aget ^bytes h 3) :cljs (aget h 3)) 0xff)]
    (bit-or (bit-shift-left b0 24)
            (bit-shift-left b1 16)
            (bit-shift-left b2 8)
            b3)))

(defn key-level
  "The level a key rises to: 0 ⇒ leaf-only, ≥1 ⇒ separator at that level. Geometric:
   P(level ≥ n) = 2^(-n·lzpl). Pure function of the key ⇒ history-independent. IDENTICAL on
   JVM and cljs (the determinism guarantee)."
  ^long [key ^long lzpl]
  (quot (clz32 (hash-u32 key)) lzpl))

;; ---- JVM: reify the Java IBoundary interface ------------------------------------------

#?(:clj
   (defn mst-boundary
     "Build an MST IBoundary. `lzpl` (leading-zeros-per-level) sets target fanout: avg node
      ≈ 2^lzpl keys. Boundary rule (leaves and branches alike): a chunk ends after a key K at
      index i<len-1 whose key-level reaches level+1; the final key is the node terminator
      (parent separator / global max), never an interior cut."
     [lzpl]
     (let [lzpl (long (max 1 lzpl))]
       (reify IBoundary
         (keyLevel [_ key _s] (key-level key lzpl))
         ;; O(1): only the inserted key (or, when appended, the displaced old max) can be a
         ;; new boundary, since an MST node has no interior boundaries except its terminator.
         (splitOnInsert [_ run len ins level _s]
           (let [thresh (inc (long level)) len (long len) ins (long ins)]
             (if (< ins (dec len))
               (when (>= (key-level (aget ^objects run ins) lzpl) thresh)
                 (int-array [(inc ins) (- len (inc ins))]))
               (when (and (>= len 2) (>= (key-level (aget ^objects run (- len 2)) lzpl) thresh))
                 (int-array [(dec len) 1])))))
         (contentDefined [_] true)))))

;; ---- CLJS: implement the PBoundary protocol -------------------------------------------

#?(:cljs
   (defrecord MstBoundary [^number lzpl]
     b/PBoundary
     (-key-level [_ key] (key-level key lzpl))
     (-split-on-insert [_ run len ins level]
       (let [thresh (inc level)]
         (if (< ins (dec len))
           (when (>= (key-level (aget run ins) lzpl) thresh)
             [(inc ins) (- len (inc ins))])
           (when (and (>= len 2) (>= (key-level (aget run (- len 2)) lzpl) thresh))
             [(dec len) 1]))))
     (-content-defined? [_] true)))

#?(:cljs
   (defn mst-boundary
     "Build an MST PBoundary (cljs). `lzpl` sets target fanout (avg node ≈ 2^lzpl keys)."
     [lzpl]
     (->MstBoundary (max 1 lzpl))))
