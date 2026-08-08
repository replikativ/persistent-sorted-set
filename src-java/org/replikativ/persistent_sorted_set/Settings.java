package org.replikativ.persistent_sorted_set;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.atomic.*;

@SuppressWarnings("rawtypes")
public class Settings {
  public final int _branchingFactor;
  public final RefType _refType;
  /** Transient ownership, in Clojure's own model: the owning Thread while editable,
   *  null once `persistent!` has been called, and the whole field null for a set that was
   *  never transient. Was an AtomicBoolean, which recorded THAT a set was editable but not
   *  BY WHOM — so `editable()` answered true on any thread and every mutating operation
   *  silently took the in-place path from anywhere. */
  public final AtomicReference<Thread> _edit;
  public final IMeasure _measure;
  public final ILeafProcessor _leafProcessor;
  // diff-buf: per-node diff budget B. 0 (default) disables the write-opt path
  // entirely, so every code path is byte-identical to baseline PSS (invariant I0).
  // Design + the IStorage slots contract: doc/diff-buffering.md.
  public final int _diffBufSize;
  // split-seam: pluggable split/merge decision. null ⇒ CountBoundary (historical count
  // B-tree, byte-identical). A content policy (MST) makes the tree history-independent.
  // See .internal/SPLIT_SEAM_DESIGN.md. Threaded like _diffBufSize (incl. editable carry).
  public final IBoundary _boundary;

  // Canonical edit-carrying constructor (does not normalize; callers pass already-normalized
  // values). Used by editable() — which threads BOTH the set's branchingFactor and diffBufSize
  // through unchanged, so a transient preserves them. (The pre-diff-buf 5-arg edit ctor was
  // removed: it was unused and, lacking a diffBufSize arg, would have silently reset it to the
  // sysprop default.)
  /**
   * Forces diff-buffering OFF whenever a `leafProcessor` is configured. Called from EVERY
   * constructor that can set both — the 5-arg one assigns its fields directly rather than
   * delegating, so a guard in the canonical constructor alone does not run (measured: the
   * repro still corrupted).
   *
   * NEUTRALIZE rather than throw, following `withBoundary`'s handling of MST + diff-buf.
   * Throwing was tried and is wrong here: the `:test` alias sets `-Dpss.diffBufSize=256`, so
   * every existing leafProcessor test inherits a budget it never asked for and 41 of them
   * failed at construction. That also shows the broken combination has been silently ACTIVE
   * across the suite all along — those tests pass only because none of them drives a
   * store/restore cycle or an eviction.
   *
   * This is a STOPGAP, not the intended end state: buffering with a processor is planned.
   * Closing it properly means `ILeafProcessor` reporting the edits it made, so the deposit
   * can record them instead of recording only the caller's element.
   */
  // A leafProcessor is INCOMPATIBLE with diff-buffering, and the combination is refused
  // here so it cannot be constructed silently — the same policy `withBoundary` applies to
  // MST + leafProcessor.
  //
  // Every diff-buf deposit records exactly the one element the CALLER named (see
  // Branch.add / .remove / .replace). A processor rewrites the WHOLE leaf, and when it
  // does not expand past the branching factor the leaf comes back as a single node, so the
  // parent classifies the change as content-only and buffers it. Every entry the processor
  // added, dropped or rewrote is then absent from the slot's diff, while `Slot.count`
  // (taken from childCount) still counts them — so the projected leaf and the cached count
  // disagree, permanently.
  //
  // Measured, compacting processor at bf 4 with diff-buf 100, elements [k v]: a set of 8
  // came back from a store/restore cycle with 9 elements, the processor-deleted [5 0]
  // resurrected. With `:ref-type :weak` it is worse than a reload artifact — the same
  // corruption appears IN PROCESS at the next GC, so a persistent value changes under the
  // caller and `seq` and `count` disagree on one object.
  //
  // Closing it properly means having ILeafProcessor report its edits so the deposit can
  // record them; until then the combination is rejected rather than silently wrong.
  // Stratum, the only in-tree consumer of ILeafProcessor, does not set :diff-buf-size, so
  // this refusal also protects it from the `pss.diffBufSize` system property switching
  // buffering on underneath it.
  //
  // REFUSE an EXPLICIT pairing; NEUTRALIZE an inherited one. The distinction is the whole
  // design here, and it exists because a budget can arrive from three places: the caller,
  // the `pss.diffBufSize` system property, or a restored node's own settings. Only the
  // first is a request.
  //
  // Silently zeroing an explicit `:diff-buf-size 256` meant a caller who asked for both got
  // neither an error nor buffering — a config that reads as enabled and is not. But a throw
  // keyed on the VALUE cannot tell the two apart: measured under `-Dpss.diffBufSize=256`,
  // a processor with no `:diff-buf-size` and a processor with an explicit 256 both arrive
  // here as 256. Throwing on that rejects stratum, which passes `:leaf-processor` and never
  // mentions diff-buf anywhere in its source, on any deployment that sets the property —
  // and rejects this suite's own `(Settings. (int bf) nil nil processor)` helpers.
  //
  // So the callers that INHERIT a budget pass 0 before they get here (the 4-arg ctor below,
  // and `map->settings` in the Clojure API), and reaching this with a positive budget means
  // someone named one. `PersistentSortedSet.root()` handles the third source separately: it
  // decides from what the subtree actually BUFFERS, not from what it declares.
  private static int diffBufFor(ILeafProcessor leafProcessor, int diffBufSize) {
    if (leafProcessor != null && diffBufSize > 0) {
      throw new IllegalArgumentException(
        "diff-buf size " + diffBufSize + " was requested together with a leafProcessor, and the "
        + "two are incompatible: a diff-buf slot records individual element edits against a "
        + "durable anchor, while a processor rewrites a whole leaf at materialization, so "
        + "replaying the diff reapplies edits the processor already folded away. Measured, a "
        + "compacting processor at bf 4 with diff-buf 100: a set of 8 came back from a "
        + "store/restore cycle with 9 elements, the processor-deleted one resurrected. Drop "
        + "one of the two — omit :diff-buf-size to run this set unbuffered.");
    }
    return diffBufSize;
  }

  public Settings(int branchingFactor, RefType refType, AtomicReference<Thread> edit, IMeasure measure, ILeafProcessor leafProcessor, int diffBufSize, IBoundary boundary) {
    diffBufSize = diffBufFor(leafProcessor, diffBufSize);
    _branchingFactor = checkBranchingFactor(branchingFactor);
    _refType = refType;
    _edit = edit;
    _measure = measure;
    _leafProcessor = leafProcessor;
    _diffBufSize = diffBufSize;
    _boundary = boundary;
  }

  public Settings() {
    this(0, null, null, null);
  }

  public Settings(int branchingFactor) {
    this(branchingFactor, null, null, null);
  }

  public Settings(int branchingFactor, RefType refType) {
    this(branchingFactor, refType, null, null);
  }

  public Settings(int branchingFactor, RefType refType, IMeasure measure) {
    this(branchingFactor, refType, measure, null);
  }

  public Settings(int branchingFactor, RefType refType, IMeasure measure, ILeafProcessor leafProcessor) {
    // No budget named ⇒ INHERIT, and a processor means the inherited one is 0. Taking
    // `defaultDiffBufSize()` here would hand `diffBufFor` a positive budget nobody asked
    // for and turn the system property into a hard failure for every processor user.
    this(branchingFactor, refType, measure, leafProcessor,
         (leafProcessor != null) ? 0 : defaultDiffBufSize());
  }

  /** The smallest branching factor this implementation supports.
   *
   *  `minBranchingFactor()` is `bf >>> 1`, so bf 2 and bf 3 both give a MINIMUM FILL OF 1.
   *  A non-root branch of length 1 is then "legally filled", its single child has no
   *  siblings, and `Leaf.remove`'s no-rebalance arm (`left == null && right == null`)
   *  returns a length-0 leaf. The parent immediately reads `maxKey()` on it, i.e.
   *  `_keys[-1]`. Measured: bf 2 and bf 3 both throw
   *  `ArrayIndexOutOfBoundsException: Index -1 out of bounds for length 0` on ordinary
   *  add/remove sequences (bf 2 on 40/40 random seeds, bf 3 on 30/40), while bf 4 and every
   *  larger factor tested — 5, 6, 7, 8, 15, 16, 17, 32, 33, 64 — pass 40/40. ClojureScript
   *  does not throw there; it retains the empty leaf and starts yielding `nil` ELEMENTS from
   *  a set whose constructor refuses nil, with `count` disagreeing with `seq`.
   *
   *  bf 4 is the first factor whose minimum fill is 2, which is what a B-tree needs for a
   *  merge to be able to restore a valid node. So this is the real floor, not a chosen one.
   *
   *  A non-positive value means UNSET and takes {@link #DEFAULT_BRANCHING_FACTOR}; that is
   *  long-standing behaviour (`Settings()` delegates with 0) and stays. Only an explicit
   *  1, 2 or 3 is refused — a caller who names one is asking for a tree that cannot work. */
  public static final int MIN_BRANCHING_FACTOR = 4;

  public static final int DEFAULT_BRANCHING_FACTOR = 512;

  private static int checkBranchingFactor(int branchingFactor) {
    if (branchingFactor <= 0) return DEFAULT_BRANCHING_FACTOR;   // unset
    if (branchingFactor < MIN_BRANCHING_FACTOR) {
      throw new IllegalArgumentException(
        "branching-factor " + branchingFactor + " is not supported: the minimum is "
        + MIN_BRANCHING_FACTOR + ". Below it the minimum fill (bf >>> 1) is 1, so a branch of "
        + "length 1 is considered full, its only child has no sibling to rebalance with, and a "
        + "removal leaves a length-0 leaf whose maxKey() reads index -1. Measured: bf 2 and 3 "
        + "throw ArrayIndexOutOfBoundsException on ordinary add/remove sequences on the JVM and "
        + "silently yield nil elements in ClojureScript. Pass 0 (or omit it) for the default of "
        + DEFAULT_BRANCHING_FACTOR + ".");
    }
    return branchingFactor;
  }

  // Normalizing constructor with explicit diffBufSize (used by the Clojure API).
  public Settings(int branchingFactor, RefType refType, IMeasure measure, ILeafProcessor leafProcessor, int diffBufSize) {
    branchingFactor = checkBranchingFactor(branchingFactor);
    if (null == refType) {
      refType = RefType.SOFT;
    }
    _branchingFactor = branchingFactor;
    _refType = refType;
    _edit = null;
    _measure = measure;
    _leafProcessor = leafProcessor;
    _diffBufSize = diffBufFor(leafProcessor, diffBufSize < 0 ? 0 : diffBufSize);
    _boundary = null; // count default; MST configured via withBoundary()
  }

  // diff-buf: diff-buffering is OFF by default (0 ⇒ byte-identical baseline, invariant
  // I0) so existing IStorage impls — which don't serialize Branch._slots — are unaffected;
  // enabling it without a slots-aware storage would silently drop buffered diffs on write.
  // Consumers that serialize :slots (e.g. datahike) opt in via Settings/the config, or set
  // the pss.diffBufSize system property (the test suite uses -Dpss.diffBufSize=256).
  // Public so the Clojure API (map->settings) shares this single default source.
  public static int defaultDiffBufSize() {
    try {
      return Integer.parseInt(System.getProperty("pss.diffBufSize", "0"));
    } catch (Exception e) {
      return 0;
    }
  }

  public int minBranchingFactor() {
    return _branchingFactor >>> 1;
  }

  public int branchingFactor() {
    return _branchingFactor;
  }

  // split-seam: the active boundary policy. Defaults to the count B-tree (byte-identical
  // baseline) when none is configured, so all split sites can route through the seam.
  public IBoundary boundary() {
    return _boundary == null ? CountBoundary.INSTANCE : _boundary;
  }

  // Returns a copy of these settings with a different boundary policy (e.g. MST). Preserves
  // edit state so it composes with transients. Used by the Clojure API to opt into prolly mode.
  //
  // A content-defined boundary is INCOMPATIBLE with two features and we reject/neutralize them
  // here so the combination can't be constructed silently:
  //  - leafProcessor: splitOnInsert assumes a single key was inserted; a processor that rewrites
  //    multiple entries per op would violate that invariant ⇒ reject.
  //  - diff-buffering: a buffered spine node is addressed by hash(anchor+diff), not its canonical
  //    content hash, which defeats the cross-peer dedup MST exists for ⇒ force off.
  /** Adopt a restored node's branching factor. See `PersistentSortedSet.root()` for why. */
  public Settings withBranchingFactor(int branchingFactor) {
    return new Settings(checkBranchingFactor(branchingFactor), _refType, _edit, _measure,
                        _leafProcessor, _diffBufSize, _boundary);
  }

  public Settings withBoundary(IBoundary boundary) {
    if (boundary != null && boundary.contentDefined()) {
      if (_leafProcessor != null)
        throw new IllegalArgumentException(
          "a content-defined boundary (MST) is incompatible with a leafProcessor");
      return new Settings(_branchingFactor, _refType, _edit, _measure, _leafProcessor, 0, boundary);
    }
    return new Settings(_branchingFactor, _refType, _edit, _measure, _leafProcessor, _diffBufSize, boundary);
  }

  /** A copy with `diffBufSize` replaced. Used by `PersistentSortedSet.root()` to adopt a
   *  restored node's own budget — nodes are self-describing, and a set running at 0 over
   *  nodes that carry slots drops their buffered elements on the next write.
   *
   *  Note this adopts over an EXPLICIT 0 as well: the value cannot be distinguished from
   *  an unset one, and must not be honoured anyway — running at 0 over nodes that carry
   *  slots drops their buffered elements on the next write, which is the very thing this
   *  adoption exists to prevent. See PersistentSortedSet.root().
   *
   *  Refuses to enable buffering under a content-defined boundary (MST), for the same
   *  reason `withBoundary` forces it off: a buffered spine node is addressed by
   *  hash(anchor+diff) rather than its canonical content hash, which breaks the
   *  cross-peer dedup MST exists for. */
  public Settings withDiffBufSize(int diffBufSize) {
    // The leafProcessor conflict is NOT decided here — it needs the ROOT NODE, which this
    // does not have. An earlier version threw whenever a processor was configured and the
    // requested budget was non-zero, keying on the node's DECLARED budget. That budget says
    // nothing about whether anything was ever buffered: under a processor `diffBufFor` has
    // already forced the set to 0, so it never buffered and never wrote a `:slots` key —
    // measured, 64 blobs with `any :slots on disk? = false` — and the throw still fired, on
    // EVERY access, because root() runs on every access. It bricked a store that had
    // previously opened fine. See PersistentSortedSet.root(), which now decides this from
    // `bufEntries()`, i.e. from whether the subtree actually carries buffered elements.
    int effective = (_boundary != null && _boundary.contentDefined()) ? 0 : diffBufSize;
    return new Settings(_branchingFactor, _refType, _edit, _measure, _leafProcessor, effective, _boundary);
  }

  public int expandLen() {
    return 8;
  }

  // diff-buf per-node diff budget; 0 disables the write-opt path (I0: baseline-identical).
  public int diffBufSize() {
    return _diffBufSize;
  }

  public RefType refType() {
    return _refType;
  }

  /** Opt-in ownership enforcement, for test suites. OFF by default, and a `static final`
   *  read from a system property so the JIT folds the branch away entirely when it is —
   *  the hot transient paths pay nothing.
   *
   *  It is off by default on purpose. The corruption below comes from CONCURRENT use;
   *  a HANDOFF (one thread finishes, publishes across a happens-before edge, another
   *  continues) is sequential and safe, and Clojure permits it — `PersistentVector`
   *  carries this very check commented out. An owner comparison sees only thread
   *  IDENTITY, so it cannot tell handoff from concurrency and would forbid the safe
   *  pattern to prevent the unsafe one. Enable it where you know no handoff occurs. */
  private static final boolean STRICT_TRANSIENTS = Boolean.getBoolean("pss.strictTransients");

  /** Is this set editable in place BY THE CALLING THREAD?
   *
   *  Throws rather than answering false for a foreign thread: answering false would send
   *  it down the persistent path, which is a silent wrong answer, not a safe one. Measured
   *  before this check existed — 4 threads x 5000 `conj!` on one transient — 19 262 of
   *  20 000 elements present, `count` disagreeing with `seq` (13 942 vs 19 262), and
   *  `sorted?` FALSE: a sorted set whose keys are not in order, so every later
   *  binarySearch is arbitrary, and durable once stored. Clojure's transients have always
   *  thrown here; this one silently corrupted.
   *
   *  KNOWN GAP: using a transient AFTER `persistent!` still degrades silently to the
   *  persistent path rather than throwing, because `persistent()` returns `this` — the
   *  stale handle and the persistent result are the same object, so they cannot be told
   *  apart. Clojure can throw because its two are different objects. Fixing that means
   *  changing what `persistent()` returns, which is a wider change than this. */
  public boolean editable() {
    if (_edit == null) return false;
    Thread owner = _edit.get();
    // Sealed by persistent!. Answering FALSE here is required, not lax: nodes share these
    // settings and `Branch.child` asks `editable()` on READ paths, so throwing would break
    // every read of a formerly-transient set. The stale-handle check belongs at the set's
    // mutation entry points — see PersistentSortedSet.ensureLiveTransient, which is where
    // Clojure puts its `ensureEditable` too.
    if (owner == null) return false;
    if (STRICT_TRANSIENTS && owner != Thread.currentThread()) {
      throw new IllegalAccessError("Transient used by non-owner thread");
    }
    return true;
  }

  /** Settings for a PERSISTENT set: same configuration, no edit reference. */
  public Settings sealed() {
    return new Settings(_branchingFactor, _refType, null, _measure, _leafProcessor, _diffBufSize, _boundary);
  }

  /** Did these settings belong to a transient that `persistent!` has since sealed?
   *  Distinguishes a STALE TRANSIENT HANDLE from a set that was never transient
   *  (`_edit == null`) and from a live one (`_edit.get() != null`). */
  public boolean sealedTransient() {
    return _edit != null && _edit.get() == null;
  }

  public Settings editable(boolean value) {
    assert !editable();
    assert value == true;
    Settings s = new Settings(_branchingFactor, _refType, new AtomicReference<>(Thread.currentThread()), _measure, _leafProcessor, _diffBufSize, _boundary);
    return s;
  }

  public IMeasure measure() {
    return _measure;
  }

  public ILeafProcessor leafProcessor() {
    return _leafProcessor;
  }

  public void persistent() {
    assert _edit != null;
    _edit.set(null);
  }

  public <T> Object makeReference(T value) {
    switch (_refType) {
    case STRONG:
      return value;
    case SOFT:
      return new SoftReference<T>(value);
    case WEAK:
      return new WeakReference<T>(value);
    default:
      throw new RuntimeException("Unexpected _refType: " + _refType);
    }
  }

  public Object readReference(Object ref) {
    return ref instanceof Reference ? ((Reference) ref).get() : ref;
  }
}
