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
     (-overflows? [_ run len level]
       "MST: does some key at index <len-1 rise to level+1 (a content boundary before the end)?")
     (-split-lengths [_ run len level]
       "MST: vector of successive node lengths (summing to len) — cut after each boundary key.")
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
