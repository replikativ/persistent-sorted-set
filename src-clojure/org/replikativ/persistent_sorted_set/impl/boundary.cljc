(ns org.replikativ.persistent-sorted-set.impl.boundary
  "Lightweight content-boundary protocol for the CLJS node code. Holds NO hasch dependency so
   the core cljs bundle stays hash-free in the default (count) mode — the MST boundary (which
   needs hasch) lives in the separate optional `boundary` ns and implements this protocol.

   The cljs count split differs in an irrelevant detail from the JVM count split (which side an
   inserted key lands on a midpoint tie), so — unlike the JVM, which routes count through the
   seam byte-identically — the cljs side keeps its existing inline count path untouched and
   only *gates* the MST path on `content-boundary`. MST itself is unambiguous (the cut is at the
   boundary key's position), so it is identical across platforms. See SPLIT_SEAM_DESIGN.md.")

#?(:cljs
   (defprotocol PBoundary
     "CLJS parallel of the Java IBoundary seam (MST only — count stays inline)."
     (-split-on-insert [_ run len ins level]
       "MST: O(1) split decision after a single key was inserted at `ins` (run[ins] is the new
        key). Returns [len-a len-b] if it must split, else nil. Only the inserted key — or the
        displaced old max when appended — can be a new boundary.")
     (-key-level [_ key]
       "The level a key rises to (0 ⇒ leaf-only).")
     (-content-defined? [_] "true for MST.")))

#?(:cljs
   (defn content-boundary
     "The settings' boundary iff it is content-defined (MST), else nil ⇒ use the inline count
      path. The cljs gate that mirrors the JVM `boundary().contentDefined()` branch."
     [settings]
     (when-let [b (:boundary settings)]
       (when (-content-defined? b) b))))
