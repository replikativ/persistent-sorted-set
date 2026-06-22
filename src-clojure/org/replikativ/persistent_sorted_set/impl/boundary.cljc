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
     (-content-defined? [_] "true for MST.")
     (-descriptor [_] "Serializable descriptor {:type … …} for self-describing restore.")))

#?(:cljs
   (defn content-boundary
     "The settings' boundary iff it is content-defined (MST), else nil ⇒ use the inline count
      path. The cljs gate that mirrors the JVM `boundary().contentDefined()` branch."
     [settings]
     (when-let [b (:boundary settings)]
       (when (-content-defined? b) b))))

;; ---- internal descriptor → boundary resolution (NO consumer interaction) ------------------
;; The descriptor→strategy mapping lives in the optional `boundary` ns (which carries hasch). We
;; resolve it INTERNALLY so fressian restore reconstructs the strategy from the serialized data
;; alone: JVM lazy-loads that ns on demand (count blobs never trigger it ⇒ never load hasch);
;; cljs has no dynamic require, so `boundary` registers its resolver here when it loads (you
;; always load `boundary` to create an MST set in the first place).

#?(:cljs (defonce ^:private mst-resolver (atom nil)))
#?(:cljs (defn register-resolver! [f] (reset! mst-resolver f)))

(defn resolve-boundary
  "Reconstruct an IBoundary/PBoundary from its serialized descriptor (e.g. {:type :mst :lzpl 6}),
   or nil for none. Internal to PSS — never supplied by the consumer."
  [descriptor]
  (when descriptor
    #?(:clj  ((requiring-resolve 'org.replikativ.persistent-sorted-set.boundary/boundary-from-descriptor)
              descriptor)
       :cljs (if-let [f @mst-resolver]
               (f descriptor)
               (throw (ex-info "node carries a content-defined boundary but the boundary ns isn't loaded — (require 'org.replikativ.persistent-sorted-set.boundary)"
                               {:descriptor descriptor}))))))
