package org.replikativ.persistent_sorted_set;

import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.atomic.*;

@SuppressWarnings("rawtypes")
public class Settings {
  public final int _branchingFactor;
  public final RefType _refType;
  public final AtomicBoolean _edit;
  public final IMeasure _measure;
  public final ILeafProcessor _leafProcessor;
  // diff-buf: per-node diff budget B. 0 (default) disables the write-opt path
  // entirely, so every code path is byte-identical to baseline PSS (invariant I0).
  // Design + the IStorage slots contract: doc/diff-buffering.md.
  public final int _diffBufSize;

  // Canonical constructor (does not normalize; callers pass already-normalized values).
  public Settings(int branchingFactor, RefType refType, AtomicBoolean edit, IMeasure measure, ILeafProcessor leafProcessor, int diffBufSize) {
    _branchingFactor = branchingFactor;
    _refType = refType;
    _edit = edit;
    _measure = measure;
    _leafProcessor = leafProcessor;
    _diffBufSize = diffBufSize;
  }

  // Back-compat: previous 5-arg canonical ctor (used by editable()).
  public Settings(int branchingFactor, RefType refType, AtomicBoolean edit, IMeasure measure, ILeafProcessor leafProcessor) {
    this(branchingFactor, refType, edit, measure, leafProcessor, defaultDiffBufSize());
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
    this(branchingFactor, refType, measure, leafProcessor, defaultDiffBufSize());
  }

  // Normalizing constructor with explicit diffBufSize (used by the Clojure API).
  public Settings(int branchingFactor, RefType refType, IMeasure measure, ILeafProcessor leafProcessor, int diffBufSize) {
    if (branchingFactor <= 0) {
      branchingFactor = 512;
    }
    if (null == refType) {
      refType = RefType.SOFT;
    }
    _branchingFactor = branchingFactor;
    _refType = refType;
    _edit = null;
    _measure = measure;
    _leafProcessor = leafProcessor;
    _diffBufSize = diffBufSize < 0 ? 0 : diffBufSize;
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

  public boolean editable() {
    return _edit != null && _edit.get();
  }

  public Settings editable(boolean value) {
    assert !editable();
    assert value == true;
    Settings s = new Settings(_branchingFactor, _refType, new AtomicBoolean(value), _measure, _leafProcessor, _diffBufSize);
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
    _edit.set(false);
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
