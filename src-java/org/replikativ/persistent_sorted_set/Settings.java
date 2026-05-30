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
  // OP_BUF_V5: per-node diff budget B. 0 (default) disables the write-opt path
  // entirely, so every code path is byte-identical to baseline PSS (invariant I0).
  public final int _opBufSize;
  // OP_BUF_V5: the set's comparator, needed to PROJECT buffered leaf-diffs onto a
  // restored leaf at the lazy-load boundary (Branch.child). Set by the Clojure API;
  // null when opBufSize==0 (projection never runs). Not final so the various ctors
  // need not all thread it; editable() preserves it.
  public Comparator _comparator;

  // Canonical constructor (does not normalize; callers pass already-normalized values).
  public Settings(int branchingFactor, RefType refType, AtomicBoolean edit, IMeasure measure, ILeafProcessor leafProcessor, int opBufSize) {
    _branchingFactor = branchingFactor;
    _refType = refType;
    _edit = edit;
    _measure = measure;
    _leafProcessor = leafProcessor;
    _opBufSize = opBufSize;
  }

  // Back-compat: previous 5-arg canonical ctor (used by editable()).
  public Settings(int branchingFactor, RefType refType, AtomicBoolean edit, IMeasure measure, ILeafProcessor leafProcessor) {
    this(branchingFactor, refType, edit, measure, leafProcessor, defaultOpBufSize());
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
    this(branchingFactor, refType, measure, leafProcessor, defaultOpBufSize());
  }

  // Normalizing constructor with explicit opBufSize (used by the Clojure API).
  public Settings(int branchingFactor, RefType refType, IMeasure measure, ILeafProcessor leafProcessor, int opBufSize) {
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
    _opBufSize = opBufSize < 0 ? 0 : opBufSize;
  }

  static int defaultOpBufSize() {
    try {
      return Integer.parseInt(System.getProperty("pss.opBufSize", "0"));
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

  // OP_BUF_V5 per-node diff budget; 0 disables the write-opt path (I0: baseline-identical).
  public int opBufSize() {
    return _opBufSize;
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
    Settings s = new Settings(_branchingFactor, _refType, new AtomicBoolean(value), _measure, _leafProcessor, _opBufSize);
    s._comparator = _comparator;  // preserve for diff projection during transient mutation
    return s;
  }

  // OP_BUF_V5: the set's comparator (for projecting buffered leaf-diffs on restore).
  public Comparator comparator() {
    return _comparator;
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
