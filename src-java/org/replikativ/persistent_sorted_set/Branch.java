package org.replikativ.persistent_sorted_set;

import java.lang.ref.*;
import java.util.*;
import java.util.function.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public class Branch<Key, Address> extends ANode<Key, Address> implements ISubtreeCount {
  // 1+ for Branches
  public final int _level;

  // Nullable, null == no addresses
  // Only valid [0 ... _len-1]
  public Address[] _addresses;

  // Nullable, null == children not populated yet
  // Only valid [0 ... _len-1]
  // Object == ANode | SoftReference<ANode> | WeakReference<ANode>
  public Object[] _children;

  // Total count of elements in this subtree. -1 means not yet computed.
  // For lazy computation when restored from old storage format.
  public long _subtreeCount;

  // diff-buf (only used when _settings.diffBufSize() > 0; null/false otherwise, so
  // diffBufSize==0 is byte-identical to baseline — invariant I0).
  //
  // Per-child diff slot: _slots[i], when non-null, is the buffered logical diff of
  // child i against its durable version (a Slot holding a PersistentTreeMap<Key,Op>
  // + a cached (count, measure) snapshot ĝ). null == child i has no buffered diff.
  public Object[] _slots;

  // diff-buf store-time bookkeeping: this node's subtree buffered-diff size, in *entries*
  // (number of buffered element-changes summed over the whole subtree — the budget-B unit).
  // Maintained by delta on the deposit return path, mirroring _subtreeCount. Three states:
  //   >= 0   : content-only — the exact buffered-entry count (what the old deep walk returned);
  //   WRITE  : must be WRITTEN, not buffered — a split/merge/borrow happened in this subtree.
  //            Set at the rebuild site; PROPAGATES UP automatically because a parent's deposit
  //            folds the child's value into this sum (a WRITE child poisons the parent), so the
  //            store-time gate is an O(1) field read instead of a recursive subtree walk.
  //   LAZY   : restored from storage, not yet derived — bufEntries() resolves it from _slots
  //            (IO-free, the diffs are already in memory). Mirrors _subtreeCount's -1/lazy.
  // Cleared to 0 at store (the node then equals its durable object). Untouched at diffBufSize==0.
  public long _bufEntries;
  public static final long BUF_WRITE = -1;   // must write (rebalanced subtree)
  public static final long BUF_LAZY  = Slot.LAZY;  // restored, derive from _slots on first read

  // diff-buf: the SET's stable comparator, used to PROJECT a buffered leaf (projectLeaf rebuilds
  // it in stored order). It is the set's comparator — NOT any per-operation/navigation comparator
  // — propagated lazily from the root down to each branch as it is materialized (see child() and
  // PersistentSortedSet.root()). A leaf-parent projects its buffered leaves with its own _projCmp,
  // so projection never depends on the comparator of whatever operation drove the descent. null
  // when diffBufSize==0 / projection never runs.
  public Comparator _projCmp;

  // For i in [0.._len):
  // 
  // 1. Not stored:       (_addresses == null || _addresses[i] == null) && _children[i] == ANode
  // 2. Stored:            _addresses[i] == Object && _children[i] == WeakReference<ANode>
  // 3. Not restored yet:  _addresses[i] == Object && (_children == null || _children[i] == null)

  public Branch(int level, int len, Key[] keys, Address[] addresses, Object[] children, Settings settings) {
    this(level, len, keys, addresses, children, -1, settings);
  }

  public Branch(int level, int len, Key[] keys, Address[] addresses, Object[] children, long subtreeCount, Settings settings) {
    super(len, keys, settings);
    assert level >= 1;
    assert addresses == null || addresses.length >= len : ("addresses = " + Arrays.toString(addresses) + ", len = " + len);
    assert children == null || children.length >= len;

    _level        = level;
    _addresses    = addresses;
    _children     = children;
    _subtreeCount = subtreeCount;
    // diff-buf: this ctor backs restore (the List ctor) and bulk build — slots, if any, are
    // attached afterwards by the storage layer, so the buffered-entry count is derived lazily
    // from _slots on first read (mirrors _subtreeCount = -1). Mutation ctors set it explicitly.
    _bufEntries   = BUF_LAZY;
  }

  // diff-buf: this ctor and the (level, len, settings) one below take the set's projection
  // comparator (projCmp) because they are only ever called by Branch's own structural ops
  // (add/remove/replace split/merge/borrow), where the new node inherits the creating node's
  // _projCmp. Passing it here — rather than stamping the field after construction — makes it
  // impossible to forge an internal branch without a comparator (the projectLeaf assert would
  // otherwise only catch it at runtime). Restored branches (no comparator at the storage layer)
  // and the root are stamped on descent instead — see child()/PersistentSortedSet.root().
  public Branch(int level, int len, Key[] keys, Address[] addresses, Object[] children, long subtreeCount, Object measure, Comparator projCmp, Settings settings) {
    super(len, keys, measure, settings);
    assert level >= 1;
    assert addresses == null || addresses.length >= len : ("addresses = " + Arrays.toString(addresses) + ", len = " + len);
    assert children == null || children.length >= len;

    _level        = level;
    _addresses    = addresses;
    _children     = children;
    _subtreeCount = subtreeCount;
    _projCmp      = projCmp;
  }

  public Branch(int level, int len, Comparator projCmp, Settings settings) {
    super(len, (Key[]) new Object[ANode.newLen(len, settings)], settings);
    assert level >= 1;

    _level        = level;
    _addresses    = null;
    _children     = null;
    _subtreeCount = -1;
    _projCmp      = projCmp;
  }

  public Branch(int level, List<Key> keys, List<Address> addresses, Settings settings) {
    this(level, keys.size(), (Key[]) keys.toArray(), (Address[]) addresses.toArray(), null, settings);
  }

  protected Address[] ensureAddresses() {
    if (_addresses == null) {
      _addresses = (Address[]) new Object[_keys.length];
    }
    return _addresses;
  }

  public List<Address> addresses() {
    if (_addresses == null) {
      return (List<Address>) Arrays.asList(new Object[_len]);
    } else if (_addresses.length == _len) {
      return Arrays.asList(_addresses);
    } else {
      return Arrays.asList(Arrays.copyOfRange(_addresses, 0, _len));
    }
  }
  
  public Address address(int idx) {
    assert 0 <= idx && idx < _len;

    if (_addresses == null) {
      return null;
    }
    return _addresses[idx];
  }

  public Address address(int idx, Address address) {
    assert 0 <= idx && idx < _len;

    if (_addresses != null || address != null) {
      ensureAddresses();
      _addresses[idx] = address;
      // if (_children != null) {
      //   _children[idx] = null;
      // }
      if (address != null && _children != null && _children[idx] instanceof ANode) {
        _children[idx] = _settings.makeReference(_children[idx]);
      }
    }
    return address;
  }

  public ANode<Key, Address> child(IStorage storage, int idx) {
    assert 0 <= idx && idx < _len;
    assert (_children != null && _children[idx] != null) || (_addresses != null && _addresses[idx] != null);

    ANode child = null;
    if (_children != null) {
      Object ref = _children[idx];
      child = (ANode) _settings.readReference(ref);
    }

    if (child == null) {
      assert _addresses[idx] != null;
      ANode base = storage.restore(_addresses[idx]);
      // diff-buf: propagate the set's projection comparator down to each restored branch, so a
      // leaf-parent projects its buffered leaves with its own _projCmp — independent of whatever
      // operation (lookup with a prefix cmp, slice, count, …) drove this descent.
      if (base instanceof Branch) ((Branch) base)._projCmp = _projCmp;
      Slot sl = (_slots != null) ? (Slot) _slots[idx] : null;
      // diff-buf push-down: project this parent's buffered diff onto the freshly loaded child —
      // leaf: batch-rebuild keys (with this leaf-parent's _projCmp); branch: install the nested
      // diff as the child's own _slots + set its aggregates from ĝ. Runs once, here, at
      // materialization (reads stay baseline). Parent's slot supersedes any diff in the child.
      if (_settings.diffBufSize() > 0 && sl != null && sl.diff != null) {
        child = (base instanceof Leaf) ? (ANode) projectLeaf((Leaf) base, sl.diff, _projCmp)
                                       : (ANode) projectBranch((Branch) base, sl);
      } else {
        child = base;
      }
      ensureChildren()[idx] = _settings.makeReference(child);
    } else {
      if (_addresses != null && _addresses[idx] != null) {
        storage.accessed(_addresses[idx]);
      }
    }
    return child;
  }

  public ANode<Key, Address> child(int idx, ANode<Key, Address> child) {
    address(idx, null);
    if (_children != null || child != null) {
      ensureChildren();
      _children[idx] = child;
    }
    return child;
  }

  @Override
  public int count(IStorage storage) {
    if (_subtreeCount < 0) {
      long computed = computeSubtreeCount(storage);
      // Cache the computed count. Safe for persistent (immutable) nodes since
      // the count never changes. For transient nodes it's also safe since
      // add/remove maintain the count incrementally.
      _subtreeCount = computed;
      return (int) computed;
    }
    return (int) _subtreeCount;
  }

  public int level() {
    return _level;
  }

  @Override
  public long subtreeCount() {
    return _subtreeCount;
  }

  // diff-buf: this node's buffered-diff size in entries (see _bufEntries). Resolves a restored
  // node's LAZY value from its slots once (IO-free — the slot diffs are already in memory),
  // then caches; passes BUF_WRITE (must-write) through. O(1) once resolved. Mirrors count().
  public long bufEntries() {
    if (_bufEntries == BUF_LAZY) {
      long s = 0;
      if (_slots != null)
        for (int j = 0; j < _len; j++) {
          Slot sl = (Slot) _slots[j];
          if (sl != null) s += slotBE(sl);
        }
      _bufEntries = s;
    }
    return _bufEntries;
  }

  // Entry count contributed by one slot of THIS node: its cached value, or — for a slot
  // reconstructed from storage (LAZY) — derived from the diff blob (IO-free). A slot's child
  // is at _level-1, so its diff is summed with that childLevel.
  private int slotBE(Slot sl) {
    return (sl.bufEntries == Slot.LAZY) ? diffSize(sl.diff, _level - 1) : (int) sl.bufEntries;
  }

  // diff-buf oracle (assert-only, -ea): recompute a content-only node's buffered-entry total by
  // walking the live subtree (the work the old store-time deep walk did), to cross-check the
  // delta-maintained _bufEntries. Skipped for BUF_WRITE/BUF_LAZY (negative ⇒ not a settled size).
  // A >=0 node can't have a rebalanced descendant (the poison would have made it negative), so the
  // walk never hits a must-write child. Runs only under -ea (e.g. the :dev alias), never in :test.
  private boolean assertBufEntries(IStorage storage) {
    if (_bufEntries < 0) return true;
    // Post-order: check dirty (resident, marker) children first so the DEEPEST mismatch throws.
    if (_slots != null)
      for (int j = 0; j < _len; j++) {
        Slot s = (Slot) _slots[j];
        if (s != null && s.diff == null) {
          ANode gc = child(storage, j);
          if (gc instanceof Branch) ((Branch) gc).assertBufEntries(storage);
        }
      }
    long slow = bufEntriesSlow(storage);
    if (_bufEntries != slow)
      throw new AssertionError("diff-buf: _bufEntries " + _bufEntries + " != recomputed " + slow + " (level " + _level + ")");
    return true;
  }

  private long bufEntriesSlow(IStorage storage) {
    if (_slots == null) return 0;
    long total = 0;
    for (int j = 0; j < _len; j++) {
      Slot sl = (Slot) _slots[j];
      if (sl == null) continue;
      if (sl.anchor == null) continue;                        // anchorless child ⇒ written wholesale at
                                                              // store ⇒ embeds nothing here (deposit set newBE=0)
      if (sl.diff == null) {                                   // live branch marker → recurse resident child
        ANode gc = child(storage, j);
        total += (gc instanceof Branch) ? ((Branch) gc).bufEntriesSlow(storage) : 0;
      } else {
        total += diffSize(sl.diff, _level - 1);                // leaf-diff or restored-nested
      }
    }
    return total;
  }

  /**
   * Computes subtree count by summing children's counts.
   * Used for lazy computation when restored from old storage without counts.
   */
  public long computeSubtreeCount(IStorage storage) {
    long count = 0;
    for (int i = 0; i < _len; ++i) {
      ANode child = child(storage, i);
      if (child instanceof ISubtreeCount) {
        long childCount = ((ISubtreeCount) child).subtreeCount();
        if (childCount < 0) {
          // Child is a Branch with unknown count, compute recursively
          childCount = ((Branch) child).computeSubtreeCount(storage);
          // Cache in child — safe for persistent (immutable count) and transient
          // (add/remove maintain counts; this only fires for unknown/-1 values)
          ((Branch) child)._subtreeCount = childCount;
        }
        count += childCount;
      } else {
        count += child.count(storage);
      }
    }
    return count;
  }

  @Override
  public Object tryComputeMeasure(IStorage storage) {
    // Try to compute measure from in-memory children only; postpone if any child not loaded
    IMeasure measureOps = _settings.measure();
    if (measureOps == null) return null;
    if (_children == null) return null; // no children loaded

    Object result = measureOps.identity();
    for (int i = 0; i < _len; i++) {
      Object raw = _children[i];
      ANode child = null;
      if (raw instanceof ANode) {
        child = (ANode) raw;
      } else if (raw instanceof java.lang.ref.Reference) {
        child = (ANode) ((java.lang.ref.Reference<?>) raw).get();
      }
      if (child == null) return null; // child not in memory, postpone
      Object childMeasure = child.measure();
      if (childMeasure == null) return null; // child measure unavailable, postpone
      result = measureOps.merge(result, childMeasure);
    }
    return result;
  }

  @Override
  public Object forceComputeMeasure(IStorage storage) {
    // Force compute measure, recursively descending if needed
    IMeasure measureOps = _settings.measure();
    if (measureOps == null) return null;

    Object result = measureOps.identity();
    for (int i = 0; i < _len; i++) {
      ANode child = child(storage, i);
      Object childMeasure = child.measure();
      if (childMeasure == null) {
        // Force computation by recursively computing child measure
        childMeasure = child.forceComputeMeasure(storage);
      }
      if (childMeasure == null) {
        // Child cannot compute measure - cannot produce accurate branch measure
        return null;
      }
      result = measureOps.merge(result, childMeasure);
    }
    _measure = result;
    return result;
  }

  /**
   * Try to compute subtree count from in-memory children.
   * Returns -1 if any child is not available in memory or has unknown count.
   */
  private static long tryComputeSubtreeCountFromChildren(Object[] children, int len, IStorage storage) {
    if (children == null) return -1;
    long count = 0;
    for (int i = 0; i < len; i++) {
      Object raw = children[i];
      ANode child = null;
      if (raw instanceof ANode) child = (ANode) raw;
      else if (raw instanceof java.lang.ref.Reference)
        child = (ANode) ((java.lang.ref.Reference<?>) raw).get();
      if (child == null) return -1; // not in memory, propagate unknown
      long childCount = getSubtreeCount(child, storage);
      if (childCount < 0) return -1;
      count += childCount;
    }
    return count;
  }

  /**
   * Helper to get subtree count from an ANode.
   */
  private static long getSubtreeCount(ANode node, IStorage storage) {
    if (node instanceof ISubtreeCount) {
      long count = ((ISubtreeCount) node).subtreeCount();
      if (count >= 0) return count;
      // Branch with unknown count - compute it
      if (node instanceof Branch) {
        return ((Branch) node).computeSubtreeCount(storage);
      }
    }
    return node.count(storage);
  }


  /**
   * Helper to try computing measure from children array (postpone if any child unavailable).
   */
  private static Object tryComputeMeasureFromChildren(Object[] children, int len, IStorage storage, IMeasure measureOps) {
    if (measureOps == null) return null;
    if (children == null) return null;
    Object result = measureOps.identity();
    for (int i = 0; i < len; ++i) {
      Object raw = children[i];
      ANode child = null;
      if (raw instanceof ANode) {
        child = (ANode) raw;
      } else if (raw instanceof java.lang.ref.Reference) {
        child = (ANode) ((java.lang.ref.Reference<?>) raw).get();
      }
      if (child == null) return null; // child not available, postpone
      Object childMeasure = child.measure();
      if (childMeasure == null) {
        // Child measure unavailable - postpone
        return null;
      }
      result = measureOps.merge(result, childMeasure);
    }
    return result;
  }

  protected Object[] ensureChildren() {
    if (_children == null) {
      _children = new Object[_keys.length];
    }
    return _children;
  }

  @Override
  public boolean contains(IStorage storage, Key key, Comparator<Key> cmp) {
    int idx = search(key, cmp);
    if (idx >= 0) return true;
    int ins = -idx - 1;
    if (ins == _len) return false;
    assert 0 <= ins && ins < _len;
    return child(storage, ins).contains(storage, key, cmp);
  }

  @Override
  public ANode[] add(IStorage storage, Key key, Comparator<Key> cmp, Settings settings) {
    int idx = search(key, cmp);
    if (idx >= 0) { // already in set
      return PersistentSortedSet.UNCHANGED;
    }
    
    int ins = -idx - 1;
    if (ins == _len) ins = _len - 1;
    assert 0 <= ins && ins < _len;
    ANode oldChild = child(storage, ins);
    long oldChildCount = ((ISubtreeCount) oldChild).subtreeCount();
    // diff-buf: capture child ins's durable address BEFORE the mutation nulls it,
    // so a deposit at this level can record it as the buffer anchor.
    Object anchor0 = (_settings.diffBufSize() > 0 && _addresses != null) ? _addresses[ins] : null;
    ANode[] nodes = oldChild.add(storage, key, cmp, settings);

    if (PersistentSortedSet.UNCHANGED == nodes) { // child signalling already in set
      return PersistentSortedSet.UNCHANGED;
    }

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
      // Editable in-place path: processor didn't fire, exactly one element added
      if (_subtreeCount >= 0) _subtreeCount += 1;
      // Update measure: recompute from children (child's stats were updated in place)
      IMeasure measureOps = _settings.measure();
      if (measureOps != null && _measure != null) {
        _measure = tryComputeMeasure(storage);
      }
      if (_settings.diffBufSize() > 0) depositInto(storage, ins, key, key, anchor0); // content-only: Present(key) / branch marker
      return PersistentSortedSet.EARLY_EXIT;
    }

    // Compute new children's total count (accounts for processor expanding/compacting).
    // A child with UNKNOWN count (-1, e.g. a lazily-restored subtree under diff-buf whose count
    // hasn't been materialized) makes the total unknown: blindly summing -1 silently corrupts
    // the parent's cached _subtreeCount (count-drift after restore+mutate). Signal unknown as -1
    // so the delta updates below fall back to lazy recompute. (Baseline dodges this because its
    // unprojected oldChild has count -1, tripping the oldChildCount guard; diff-buf projection
    // can populate oldChildCount, so the newChildrenCount guard is the one that must hold.)
    long newChildrenCount = 0;
    for (ANode n : nodes) {
      long c = ((ISubtreeCount) n).subtreeCount();
      if (c < 0) { newChildrenCount = -1; break; }
      newChildrenCount += c;
    }

    IMeasure measureOps = _settings.measure();

    // same len, editable
    if (1 == nodes.length && editable()) {
      ANode<Key, Address> node = nodes[0];
      _keys[ins] = node.maxKey();
      // Mark old child address as freed before clearing. diff-buf: under diff-buf the old
      // address may be re-pointed as a buffered anchor at store, so freeing is DEFERRED to
      // store (which frees the old root + any flushed anchors) — else GC frees a live node.
      if (_settings.diffBufSize() <= 0 && storage != null && _addresses != null && _addresses[ins] != null) {
        storage.markFreed(_addresses[ins]);
      }
      child(ins, node);
      // Update subtree count using exact delta from old vs new child
      if (_subtreeCount >= 0 && oldChildCount >= 0 && newChildrenCount >= 0)
        _subtreeCount = _subtreeCount - oldChildCount + newChildrenCount;
      else
        _subtreeCount = -1;
      // Update measure: recompute from children
      if (measureOps != null && _measure != null) {
        _measure = tryComputeMeasure(storage);
      }
      if (_settings.diffBufSize() > 0) depositInto(storage, ins, key, key, anchor0); // content-only: Present(key) / branch marker
      if (ins == _len - 1)
        return new ANode[]{ this }; // last child changed, propagate maxKey update
      else
        return PersistentSortedSet.EARLY_EXIT;
    }

    // same len, not editable
    if (1 == nodes.length) {
      ANode<Key, Address> node = nodes[0];
      // Always copy arrays — sharing them would allow a later transient
      // editable() path to mutate the original persistent branch's arrays
      Key[] newKeys = Arrays.copyOfRange(_keys, 0, _len);
      newKeys[ins] = node.maxKey();

      Address[] newAddresses = null;
      if (_addresses != null) {
        newAddresses = Arrays.copyOfRange(_addresses, 0, _len);
        newAddresses[ins] = null;
      }

      Object[] newChildren = _children == null ? new Object[_keys.length] : Arrays.copyOfRange(_children, 0, _len);
      newChildren[ins] = node;

      // Exact subtree count using delta from old vs new child
      long newCount = (_subtreeCount >= 0 && oldChildCount >= 0 && newChildrenCount >= 0)
          ? _subtreeCount - oldChildCount + newChildrenCount : -1;
      Object newMeasure = tryComputeMeasureFromChildren(newChildren, _len, storage, measureOps);
      Branch<Key, Address> nb = new Branch(_level, _len, newKeys, newAddresses, newChildren, newCount, newMeasure, _projCmp, settings);
      if (settings.diffBufSize() > 0) { nb._bufEntries = bufEntries(); // carry this node's running total (or BUF_WRITE) onto the successor
                                      nb.carryAndDeposit(storage, _slots, ins, key, key, anchor0); } // content-only: Present(key) / branch marker
      return new ANode[]{ nb };
    }

    // nodes.length >= 2: replace 1 child with N children
    int extra = nodes.length - 1;
    int newLen = _len + extra;

    // Build full merged arrays
    Key[] allKeys = (Key[]) new Object[newLen];
    Stitch ks = new Stitch(allKeys, 0);
    ks.copyAll(_keys, 0, ins);
    for (int i = 0; i < nodes.length; i++) ks.copyOne(nodes[i].maxKey());
    ks.copyAll(_keys, ins + 1, _len);

    Object[] allChildren = new Object[newLen];
    Stitch cs = new Stitch(allChildren, 0);
    Object[] existingChildren = _children != null ? _children : ensureChildren();
    cs.copyAll(existingChildren, 0, ins);
    for (int i = 0; i < nodes.length; i++) cs.copyOne(nodes[i]);
    cs.copyAll(existingChildren, ins + 1, _len);

    Address[] allAddresses = null;
    if (_addresses != null) {
      allAddresses = (Address[]) new Object[newLen];
      Stitch as = new Stitch(allAddresses, 0);
      as.copyAll(_addresses, 0, ins);
      for (int i = 0; i < nodes.length; i++) as.copyOne(null);
      as.copyAll(_addresses, ins + 1, _len);
    }

    // split-seam: boundary policy decides overflow + cut. Count ⇒ newLen>bf, midpoint
    // (byte-identical). MST ⇒ O(1): the one promoted separator at `ins` (the split child's new
    // max) is the only possible new boundary; cut there if it rises to level+1. null ⇒ absorb.
    int[] lengths = settings.boundary().splitOnInsert(allKeys, newLen, ins, _level, settings);

    // Absorb: fits in single branch
    if (lengths == null) {
      // Use delta formula: exact and O(1), avoids scanning all children
      long count = (_subtreeCount >= 0 && oldChildCount >= 0 && newChildrenCount >= 0)
          ? _subtreeCount - oldChildCount + newChildrenCount : -1;
      Object measure = tryComputeMeasureFromChildren(allChildren, newLen, storage, measureOps);
      Branch<Key, Address> nb = new Branch(_level, newLen, allKeys, allAddresses, allChildren, count, measure, _projCmp, settings);
      if (settings.diffBufSize() > 0) {
        nb._bufEntries = BUF_WRITE; // absorbed a child split: structural → written, but it still buffers surviving siblings
        nb._slots = stitchSlots(ins, nodes.length, newLen); // carry buffered siblings' slots through the rebuild
        freeDroppedChild(storage, ins); // diff-buf: free the split child's old blob (replaced by N new nodes)
      }
      return new ANode[]{ nb };
    }

    // Split into two branches (incremental ⇒ ≤2-way; lengths[0] is the first cut)
    int half1 = lengths[0], half2 = newLen - half1;

    Key[] keys1 = Arrays.copyOfRange(allKeys, 0, half1);
    Key[] keys2 = Arrays.copyOfRange(allKeys, half1, newLen);

    Object[] children1 = Arrays.copyOfRange(allChildren, 0, half1);
    Object[] children2 = Arrays.copyOfRange(allChildren, half1, newLen);

    Address[] addresses1 = null, addresses2 = null;
    if (allAddresses != null) {
      addresses1 = Arrays.copyOfRange(allAddresses, 0, half1);
      addresses2 = Arrays.copyOfRange(allAddresses, half1, newLen);
    }

    long count1 = tryComputeSubtreeCountFromChildren(children1, half1, storage);
    long count2 = tryComputeSubtreeCountFromChildren(children2, half2, storage);
    Object measure1 = tryComputeMeasureFromChildren(children1, half1, storage, measureOps);
    Object measure2 = tryComputeMeasureFromChildren(children2, half2, storage, measureOps);
    Branch<Key, Address> sb1 = new Branch(_level, half1, keys1, addresses1, children1, count1, measure1, _projCmp, settings);
    Branch<Key, Address> sb2 = new Branch(_level, half2, keys2, addresses2, children2, count2, measure2, _projCmp, settings);
    if (settings.diffBufSize() > 0) {
      sb1._bufEntries = BUF_WRITE; sb2._bufEntries = BUF_WRITE; // split: structural → written, still buffer surviving siblings
      freeDroppedChild(storage, ins); // diff-buf: free the split child's old blob (replaced by N new nodes)
      Object[] all = stitchSlots(ins, nodes.length, newLen); // carry buffered siblings' slots through the split
      if (all != null) {
        sb1._slots = Arrays.copyOfRange(all, 0, half1);
        sb2._slots = Arrays.copyOfRange(all, half1, newLen);
      }
    }
    return new ANode[]{ sb1, sb2 };
  }

  @Override
  public ANode[] remove(IStorage storage, Key key, ANode _left, ANode _right, Comparator<Key> cmp, Settings settings) {
    Branch left = (Branch) _left;
    Branch right = (Branch) _right;

    int idx = search(key, cmp);
    if (idx < 0) idx = -idx - 1;

    if (idx == _len) // not in set
      return PersistentSortedSet.UNCHANGED;

    assert 0 <= idx && idx < _len;
    
    // diff-buf: capture child idx's durable address before the mutation nulls it.
    Object anchor0 = (_settings.diffBufSize() > 0 && _addresses != null) ? _addresses[idx] : null;
    ANode leftChild  = idx > 0      ? child(storage, idx - 1) : null,
          rightChild = idx < _len-1 ? child(storage, idx + 1) : null;
    int leftChildLen = safeLen(leftChild);
    int rightChildLen = safeLen(rightChild);
    ANode[] nodes = child(storage, idx).remove(storage, key, leftChild, rightChild, cmp, settings);

    if (PersistentSortedSet.UNCHANGED == nodes) // child signalling element not in set
      return PersistentSortedSet.UNCHANGED;

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
      // Editable in-place path: processor didn't fire, exactly one element removed
      if (_subtreeCount >= 0) _subtreeCount -= 1;
      // Update measure: recompute from children (child's stats were updated in place)
      IMeasure measureOps = _settings.measure();
      if (measureOps != null && _measure != null) {
        _measure = tryComputeMeasure(storage);
      }
      if (_settings.diffBufSize() > 0) depositInto(storage, idx, key, Slot.ABSENT, anchor0); // content-only: Absent(key) / branch marker
      return PersistentSortedSet.EARLY_EXIT;
    }

    // Child.remove() always returns exactly 3 elements: [left, center, right].
    // The processor cannot expand during remove (asserted in Leaf.remove), so
    // the center is always a single node and this convention is safe.
    assert nodes.length == 3 : "child.remove() must return exactly 3 elements, got " + nodes.length;
    boolean leftChanged = leftChild != nodes[0] || leftChildLen != safeLen(nodes[0]);
    boolean rightChanged = rightChild != nodes[2] || rightChildLen != safeLen(nodes[2]);

    IMeasure measureOps = _settings.measure();

    // nodes[1] always not nil
    int newLen = _len - 1
                 - (leftChild  != null ? 1 : 0)
                 - (rightChild != null ? 1 : 0)
                 + (nodes[0] != null ? 1 : 0)
                 + 1
                 + (nodes[2] != null ? 1 : 0);

    // no rebalance needed
    if (newLen >= _settings.minBranchingFactor() || (left == null && right == null)) {
      // can update in place
      if (editable() && idx < _len-2) {
        // Mark freed addresses before clearing them.
        //  - baseline (diffBufSize<=0): free the replaced child + changed siblings immediately.
        //  - diff-buf: the CONTENT-ONLY sub-case re-points _addresses[idx] to the child's anchor
        //    (deferred to store); a STRUCTURAL sub-case (merge/borrow) consumes idx / changed
        //    siblings — never re-pointed — so free them now (store has no slot/anchor for the
        //    materialized structural child, so it can't free them later → they would leak).
        if (storage != null && _addresses != null) {
          if (_settings.diffBufSize() <= 0) {
            if (_addresses[idx] != null) storage.markFreed(_addresses[idx]);
            if (leftChanged && idx > 0 && _addresses[idx - 1] != null) storage.markFreed(_addresses[idx - 1]);
            if (rightChanged && _addresses[idx + 1] != null) storage.markFreed(_addresses[idx + 1]);
          } else if (leftChanged || rightChanged || newLen != _len) { // diff-buf, structural
            freeDroppedChild(storage, idx);
            if (leftChanged && idx > 0) freeDroppedChild(storage, idx - 1);
            if (rightChanged) freeDroppedChild(storage, idx + 1);
          }
        }

        Stitch ks = new Stitch(_keys, Math.max(idx-1, 0));
        if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                              ks.copyOne(nodes[1].maxKey());
        if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
        if (newLen != _len)
          ks.copyAll(_keys, idx+2, _len);

        if (_addresses != null) {
          Stitch as = new Stitch(_addresses, Math.max(idx - 1, 0));
          if (nodes[0] != null) as.copyOne(leftChanged ? null : address(idx - 1));
                                as.copyOne(null);
          if (nodes[2] != null) as.copyOne(rightChanged ? null : address(idx + 1));
          if (newLen != _len)
            as.copyAll(_addresses, idx+2, _len);
        }

        ensureChildren();
        Stitch cs = new Stitch(_children, Math.max(idx - 1, 0));
        if (nodes[0] != null) cs.copyOne(nodes[0]);
                              cs.copyOne(nodes[1]);
        if (nodes[2] != null) cs.copyOne(nodes[2]);
        if (newLen != _len)
          cs.copyAll(_children, idx+2, _len);

        if (_settings.diffBufSize() > 0 && _slots != null
            && (leftChanged || rightChanged || newLen != _len)) { // structural only: mirror the address Stitch
          Stitch ss = new Stitch(_slots, Math.max(idx - 1, 0));   // (content-only keeps _slots[idx] so the deposit below accumulates)
          if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(idx - 1));
                                ss.copyOne(null);
          if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(idx + 1));
          if (newLen != _len)
            ss.copyAll(_slots, idx+2, _len);
        }

        _len = newLen;
        // Compute exact subtree count from children (accounts for processor changes)
        _subtreeCount = tryComputeSubtreeCountFromChildren(_children, newLen, storage);
        // Update measure: recompute from children
        if (measureOps != null && _measure != null) {
          _measure = tryComputeMeasure(storage);
        }
        if (_settings.diffBufSize() > 0) {
          if (!leftChanged && !rightChanged && newLen == _len) {
            depositInto(storage, idx, key, Slot.ABSENT, anchor0); // content-only: Absent(key) / branch marker
          } else {
            _bufEntries = BUF_WRITE; // a child merged/borrowed with a sibling: structural → write in full
          }
        }
        return PersistentSortedSet.EARLY_EXIT;
      }

      Branch newCenter = new Branch(_level, newLen, _projCmp, settings);

      Stitch ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys, idx + 2, _len);

      if (_addresses != null) {
        Stitch as = new Stitch(newCenter.ensureAddresses(), 0);
        as.copyAll(_addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : address(idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : address(idx + 1));
        as.copyAll(_addresses, idx + 2, _len);
      }

      newCenter.ensureChildren();
      Stitch cs = new Stitch(newCenter._children, 0);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);

      // Compute exact subtree count from children (accounts for processor changes)
      newCenter._subtreeCount = tryComputeSubtreeCountFromChildren(newCenter._children, newLen, storage);
      newCenter._measure = tryComputeMeasureFromChildren(newCenter._children, newLen, storage, measureOps);
      if (settings.diffBufSize() > 0) {
        if (!leftChanged && !rightChanged && newLen == _len) {
          // content-only: carry slots aligned and ACCUMULATE Absent onto the center's
          // existing diff (it may already hold buffered Present/Absent for this leaf).
          newCenter._bufEntries = bufEntries(); // carry this node's running total (or BUF_WRITE) onto the successor
          newCenter.carryAndDeposit(storage, _slots, idx, key, Slot.ABSENT, anchor0);
        } else {
          newCenter._bufEntries = BUF_WRITE;                   // structural: mirror the address Stitch
          // diff-buf: free this node's dropped children (consumed into the new structure,
          // never re-pointed) so they don't leak — store has no slot/anchor for them.
          freeDroppedChild(storage, idx);
          if (leftChanged && idx > 0) freeDroppedChild(storage, idx - 1);
          if (rightChanged) freeDroppedChild(storage, idx + 1);
          Object[] ns = new Object[newCenter._keys.length];    // (center/changed siblings materialized → null slot)
          Stitch ss = new Stitch(ns, 0);
          slotCopyAll(ss, _slots, 0, idx - 1);
          if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(idx - 1));
                                ss.copyOne(null);
          if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(idx + 1));
          slotCopyAll(ss, _slots, idx + 2, _len);
          newCenter._slots = ns;
        }
      }
      return new ANode[] { left, newCenter, right };
    }

    // can join with left
    if (left != null && left._len + newLen <= _settings.branchingFactor()) {
      Branch join = new Branch(_level, left._len + newLen, _projCmp, settings);

      Stitch ks = new Stitch(join._keys, 0);
      ks.copyAll(left._keys, 0, left._len);
      ks.copyAll(_keys,      0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys,     idx + 2, _len);

      if (left._addresses != null || _addresses != null) {
        Stitch as = new Stitch(join.ensureAddresses(), 0);
        as.copyAll(left._addresses, 0, left._len);
        as.copyAll(_addresses,      0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : address(idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : address(idx + 1));
        as.copyAll(_addresses, idx + 2, _len);
      }

      join.ensureChildren();
      Stitch cs = new Stitch(join._children, 0);
      cs.copyAll(left._children, 0, left._len);
      cs.copyAll(_children,      0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);

      // Compute exact subtree count from children (accounts for processor changes)
      join._subtreeCount = tryComputeSubtreeCountFromChildren(join._children, left._len + newLen, storage);
      join._measure = tryComputeMeasureFromChildren(join._children, left._len + newLen, storage, measureOps);
      if (settings.diffBufSize() > 0) {
        join._bufEntries = BUF_WRITE; // merged with left: structural → written, still buffers surviving siblings
        freeDroppedChild(storage, idx);                        // diff-buf: free dropped (merged) children
        if (leftChanged && idx > 0) freeDroppedChild(storage, idx - 1);
        if (rightChanged) freeDroppedChild(storage, idx + 1);
        Object[] ns = new Object[join._keys.length];           // mirror the address Stitch above
        Stitch ss = new Stitch(ns, 0);
        slotCopyAll(ss, left._slots, 0, left._len);
        slotCopyAll(ss, _slots,      0, idx - 1);
        if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(idx - 1));
                              ss.copyOne(null);
        if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(idx + 1));
        slotCopyAll(ss, _slots, idx + 2, _len);
        join._slots = ns;
      }
      return new ANode[] { null, join, right };
    }

    // can join with right
    if (right != null && newLen + right._len <= _settings.branchingFactor()) {
      Branch join = new Branch(_level, newLen + right._len, _projCmp, settings);

      Stitch ks = new Stitch(join._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys,       idx + 2, _len);
      ks.copyAll(right._keys, 0, right._len);

      if (_addresses != null || right._addresses != null) {
        Stitch as = new Stitch(join.ensureAddresses(), 0);
        as.copyAll(_addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : address(idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : address(idx + 1));
        as.copyAll(_addresses, idx + 2, _len);
        as.copyAll(right._addresses, 0, right._len);
      }

      join.ensureChildren();
      Stitch cs = new Stitch(join._children, 0);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children,     idx + 2, _len);
      cs.copyAll(right._children, 0, right._len);

      // Compute exact subtree count from children (accounts for processor changes)
      join._subtreeCount = tryComputeSubtreeCountFromChildren(join._children, newLen + right._len, storage);
      join._measure = tryComputeMeasureFromChildren(join._children, newLen + right._len, storage, measureOps);
      if (settings.diffBufSize() > 0) {
        join._bufEntries = BUF_WRITE; // merged with right: structural → written, still buffers surviving siblings
        freeDroppedChild(storage, idx);                        // diff-buf: free dropped (merged) children
        if (leftChanged && idx > 0) freeDroppedChild(storage, idx - 1);
        if (rightChanged) freeDroppedChild(storage, idx + 1);
        Object[] ns = new Object[join._keys.length];           // mirror the address Stitch above
        Stitch ss = new Stitch(ns, 0);
        slotCopyAll(ss, _slots, 0, idx - 1);
        if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(idx - 1));
                              ss.copyOne(null);
        if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(idx + 1));
        slotCopyAll(ss, _slots, idx + 2, _len);
        slotCopyAll(ss, right._slots, 0, right._len);
        join._slots = ns;
      }
      return new ANode[] { left, join, null };
    }

    // borrow from left
    if (left != null && (right == null || left._len >= right._len)) {
      int totalLen     = left._len + newLen;
      int newLeftLen   = totalLen >>> 1;
      int newCenterLen = totalLen - newLeftLen;

      Branch newLeft   = new Branch(_level, newLeftLen, _projCmp, settings);
      Branch newCenter = new Branch(_level, newCenterLen, _projCmp, settings);

      ArrayUtil.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);

      Stitch ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(left._keys, newLeftLen, left._len);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys, idx + 2, _len);

      if (left._addresses != null) {
        ArrayUtil.copy(left._addresses, 0, newLeftLen, newLeft.ensureAddresses(), 0);
      }
      if (left._children != null) {
        ArrayUtil.copy(left._children, 0, newLeftLen, newLeft.ensureChildren(), 0);
      }

      if (left._addresses != null || _addresses != null) {
        Stitch as = new Stitch(newCenter.ensureAddresses(), 0);
        as.copyAll(left._addresses, newLeftLen, left._len);
        as.copyAll(_addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : address(idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : address(idx + 1));
        as.copyAll(_addresses, idx + 2, _len);
      }

      newCenter.ensureChildren();
      Stitch cs = new Stitch(newCenter._children, 0);
      cs.copyAll(left._children, newLeftLen, left._len);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);

      if (newLeft._children != null) {
        newLeft._subtreeCount = tryComputeSubtreeCountFromChildren(newLeft._children, newLeftLen, storage);
        newLeft._measure = tryComputeMeasureFromChildren(newLeft._children, newLeftLen, storage, measureOps);
      }
      newCenter._subtreeCount = tryComputeSubtreeCountFromChildren(newCenter._children, newCenterLen, storage);
      newCenter._measure = tryComputeMeasureFromChildren(newCenter._children, newCenterLen, storage, measureOps);
      if (settings.diffBufSize() > 0) {
        newLeft._bufEntries = BUF_WRITE; newCenter._bufEntries = BUF_WRITE; // borrowed from left: structural
        freeDroppedChild(storage, idx);                          // diff-buf: free dropped (rebalanced) children
        if (leftChanged && idx > 0) freeDroppedChild(storage, idx - 1);
        if (rightChanged) freeDroppedChild(storage, idx + 1);
        if (left._slots != null) {                               // newLeft keeps left's first newLeftLen slots
          Object[] nl = new Object[newLeft._keys.length];
          ArrayUtil.copy(left._slots, 0, newLeftLen, nl, 0);
          newLeft._slots = nl;
        }
        Object[] nc = new Object[newCenter._keys.length];        // mirror the newCenter address Stitch above
        Stitch ss = new Stitch(nc, 0);
        slotCopyAll(ss, left._slots, newLeftLen, left._len);
        slotCopyAll(ss, _slots, 0, idx - 1);
        if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(idx - 1));
                              ss.copyOne(null);
        if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(idx + 1));
        slotCopyAll(ss, _slots, idx + 2, _len);
        newCenter._slots = nc;
      }
      return new ANode[] { newLeft, newCenter, right };
    }

    // borrow from right
    if (right != null) {
      int totalLen     = newLen + right._len,
          newCenterLen = totalLen >>> 1,
          newRightLen  = totalLen - newCenterLen,
          rightHead    = right._len - newRightLen;

      Branch newCenter = new Branch(_level, newCenterLen, _projCmp, settings),
             newRight  = new Branch(_level, newRightLen, _projCmp, settings);

      Stitch ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys, idx + 2, _len);
      ks.copyAll(right._keys, 0, rightHead);

      ArrayUtil.copy(right._keys, rightHead, right._len, newRight._keys, 0);

      if (_addresses != null || right._addresses != null) {
        Stitch as = new Stitch(newCenter.ensureAddresses(), 0);
        as.copyAll(_addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : address(idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : address(idx + 1));
        as.copyAll(_addresses, idx + 2, _len);
        as.copyAll(right._addresses, 0, rightHead);
      }

      newCenter.ensureChildren();
      Stitch cs = new Stitch(newCenter._children, 0);
      cs.copyAll(_children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(_children, idx + 2, _len);
      cs.copyAll(right._children, 0, rightHead);

      if (right._addresses != null) {
        ArrayUtil.copy(right._addresses, rightHead, right._len, newRight.ensureAddresses(), 0);
      }
      if (right._children != null) {
        ArrayUtil.copy(right._children, rightHead, right._len, newRight.ensureChildren(), 0);
      }

      newCenter._subtreeCount = tryComputeSubtreeCountFromChildren(newCenter._children, newCenterLen, storage);
      newCenter._measure = tryComputeMeasureFromChildren(newCenter._children, newCenterLen, storage, measureOps);
      if (newRight._children != null) {
        newRight._subtreeCount = tryComputeSubtreeCountFromChildren(newRight._children, newRightLen, storage);
        newRight._measure = tryComputeMeasureFromChildren(newRight._children, newRightLen, storage, measureOps);
      }
      if (settings.diffBufSize() > 0) {
        newCenter._bufEntries = BUF_WRITE; newRight._bufEntries = BUF_WRITE; // borrowed from right: structural
        freeDroppedChild(storage, idx);                          // diff-buf: free dropped (rebalanced) children
        if (leftChanged && idx > 0) freeDroppedChild(storage, idx - 1);
        if (rightChanged) freeDroppedChild(storage, idx + 1);
        Object[] nc = new Object[newCenter._keys.length];        // mirror the newCenter address Stitch above
        Stitch ss = new Stitch(nc, 0);
        slotCopyAll(ss, _slots, 0, idx - 1);
        if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(idx - 1));
                              ss.copyOne(null);
        if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(idx + 1));
        slotCopyAll(ss, _slots, idx + 2, _len);
        slotCopyAll(ss, right._slots, 0, rightHead);
        newCenter._slots = nc;
        if (right._slots != null) {                              // newRight keeps right's tail slots
          Object[] nr = new Object[newRight._keys.length];
          ArrayUtil.copy(right._slots, rightHead, right._len, nr, 0);
          newRight._slots = nr;
        }
      }
      return new ANode[] { left, newCenter, newRight };
    }

    throw new RuntimeException("Unreachable");
  }

  @Override
  public ANode removeContent(IStorage storage, Key key, Comparator<Key> cmp, Settings settings) {
    int idx = search(key, cmp);
    if (idx < 0) idx = -idx - 1;
    if (idx == _len) return null; // key greater than everything → not present

    ANode oldChild = child(storage, idx);
    ANode newChild = oldChild.removeContent(storage, key, cmp, settings);
    if (newChild == null) return null; // not present below

    IBoundary boundary = settings.boundary();
    IMeasure measureOps = settings.measure();

    // The removed key was a boundary between child idx and idx+1 (at the children's level)
    // iff keyLevel(key) >= this branch's level. Then those two children merge.
    boolean mergeRight = idx < _len - 1 && boundary.keyLevel(key, settings) >= _level;

    Object[] newChildren;
    if (newChild instanceof Leaf && newChild._len == 0) {
      // Child's whole subtree is gone (it collapsed to an empty Leaf — note its level no
      // longer matches its siblings, so it can never participate in a merge): drop it. This
      // subsumes the mergeRight case, since merging an empty node with its sibling yields
      // exactly that sibling.
      newChildren = new Object[_len - 1];
      for (int i = 0; i < idx; i++)        newChildren[i] = child(storage, i);
      for (int i = idx + 1; i < _len; i++) newChildren[i - 1] = child(storage, i);
    } else if (mergeRight) {
      // merge newChild with the right sibling child (junction merges recursively down).
      ANode merged = newChild.mstMergeWith(child(storage, idx + 1), storage, settings);
      newChildren = new Object[_len - 1];
      for (int i = 0; i < idx; i++)            newChildren[i] = child(storage, i);
      newChildren[idx] = merged;
      for (int i = idx + 2; i < _len; i++)     newChildren[i - 1] = child(storage, i);
    } else {
      // no boundary removed at this level: just replace child idx
      newChildren = new Object[_len];
      for (int i = 0; i < _len; i++) newChildren[i] = child(storage, i);
      newChildren[idx] = newChild;
    }

    int n = newChildren.length;
    if (n == 0) {
      // everything under this branch is gone
      return new Leaf(0, (Key[]) new Object[0], settings);
    }

    Key[] keys = (Key[]) new Object[n];
    for (int i = 0; i < n; i++) keys[i] = ((ANode<Key, Address>) newChildren[i]).maxKey();
    long count = tryComputeSubtreeCountFromChildren(newChildren, n, storage);
    Object measure = tryComputeMeasureFromChildren(newChildren, n, storage, measureOps);
    return new Branch(_level, n, keys, null, newChildren, count, measure, _projCmp, settings);
  }

  @Override
  public ANode mstMergeWith(ANode right, IStorage storage, Settings settings) {
    Branch r = (Branch) right;
    IBoundary boundary = settings.boundary();
    IMeasure measureOps = settings.measure();

    Object[] children;
    int n;
    // The junction — this branch's last child with right's first child — fuses ONLY when this
    // branch's last child ended at the *removed* boundary (its maxKey is no longer a boundary
    // at its level). If this branch's last child is still terminated by a LIVE boundary (e.g.
    // its deepest boundary leaf was dropped, leaving a properly-terminated tail), the two must
    // NOT fuse — plain concatenation preserves the live boundary. keyLevel(maxKey) >= _level
    // ⇔ the last child (level _level-1) is boundary-terminated.
    if (boundary.keyLevel(maxKey(), settings) >= _level) {
      n = _len + r._len;
      children = new Object[n];
      for (int i = 0; i < _len; i++)   children[i] = child(storage, i);
      for (int i = 0; i < r._len; i++) children[_len + i] = r.child(storage, i);
    } else {
      ANode junction = child(storage, _len - 1).mstMergeWith(r.child(storage, 0), storage, settings);
      n = _len + r._len - 1;
      children = new Object[n];
      for (int i = 0; i < _len - 1; i++)  children[i] = child(storage, i);
      children[_len - 1] = junction;
      for (int i = 1; i < r._len; i++)    children[_len - 1 + i] = r.child(storage, i);
    }

    Key[] keys = (Key[]) new Object[n];
    for (int i = 0; i < n; i++) keys[i] = ((ANode<Key, Address>) children[i]).maxKey();
    long count = tryComputeSubtreeCountFromChildren(children, n, storage);
    Object measure = tryComputeMeasureFromChildren(children, n, storage, measureOps);
    return new Branch(_level, n, keys, null, children, count, measure, _projCmp, settings);
  }

  @Override
  public ANode[] replace(IStorage storage, Key oldKey, Key newKey, Comparator<Key> cmp, Settings settings) {
    assert 0 == cmp.compare(oldKey, newKey) : "oldKey and newKey must compare as equal (cmp.compare must return 0)";

    // Find which child contains the key
    int idx = search(oldKey, cmp);
    if (idx < 0) idx = -idx - 1;
    if (idx == _len) idx = _len - 1; // key might be in last child
    assert 0 <= idx && idx < _len;

    // diff-buf: capture child idx's durable address before the mutation nulls it.
    Object anchor0 = (_settings.diffBufSize() > 0 && _addresses != null) ? _addresses[idx] : null;
    // Recursively replace in child
    ANode[] nodes = child(storage, idx).replace(storage, oldKey, newKey, cmp, settings);

    if (PersistentSortedSet.UNCHANGED == nodes) // key not found
      return PersistentSortedSet.UNCHANGED;

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // replaced in transient child, no updates needed
      // Try to recompute measure from final state (after child replacement)
      IMeasure measureOps = settings.measure();
      if (measureOps != null && _measure != null) {
        _measure = tryComputeMeasure(storage);
      }
      if (_settings.diffBufSize() > 0) depositReplace(storage, idx, oldKey, newKey, anchor0); // content-only: Absent(oldKey)+Present(newKey) / branch marker
      return PersistentSortedSet.EARLY_EXIT;
    }

    // Child was replaced (nodes.length == 1)
    ANode<Key, Address> newChild = nodes[0];
    Key newMaxKey = newChild.maxKey();
    boolean maxKeyChanged = (idx == _len - 1) && (0 != cmp.compare(newMaxKey, _keys[idx]));
    IMeasure measureOps = settings.measure();

    // Transient: can modify in place
    if (editable()) {
      _keys[idx] = newMaxKey;
      // Mark old child address as freed before clearing. diff-buf: deferred to store
      // under diff-buf (old address may be re-pointed as a buffered anchor — see add()).
      if (_settings.diffBufSize() <= 0 && storage != null && _addresses != null && _addresses[idx] != null) {
        storage.markFreed(_addresses[idx]);
      }
      child(idx, newChild);
      // Note: child() already clears _addresses[idx] via address(idx, null)
      // Try to recompute measure from final state (after child replacement)
      if (measureOps != null && _measure != null) {
        _measure = tryComputeMeasure(storage);
      }
      if (_settings.diffBufSize() > 0) depositReplace(storage, idx, oldKey, newKey, anchor0); // content-only: Absent(oldKey)+Present(newKey) / branch marker
      if (maxKeyChanged)
        return new ANode[]{this};
      else
        return PersistentSortedSet.EARLY_EXIT;
    }

    // Persistent: create new branch with updated child
    // Always copy — sharing arrays would allow a later transient
    // editable() path to mutate the original persistent branch's arrays
    Key[] newKeys = Arrays.copyOfRange(_keys, 0, _len);
    newKeys[idx] = newMaxKey;

    final Address[] newAddresses = _addresses != null ? Arrays.copyOfRange(_addresses, 0, _len) : null;
    if (newAddresses != null) {
      newAddresses[idx] = null; // child changed, address invalid
    }

    final Object[] newChildren = _children == null ? new Object[_keys.length] : Arrays.copyOfRange(_children, 0, _len);
    newChildren[idx] = newChild;

    // Try to recompute measure from final state (after child replacement) for new branch
    Branch<Key, Address> newBranch = new Branch(_level, _len, newKeys, newAddresses, newChildren, _subtreeCount, null, _projCmp, settings);
    if (measureOps != null && _measure != null) {
      newBranch._measure = newBranch.tryComputeMeasure(storage);
    }
    if (settings.diffBufSize() > 0) { newBranch._bufEntries = bufEntries(); // carry this node's running total (or BUF_WRITE) onto the successor
                                    newBranch.carryAndDepositReplace(storage, _slots, idx, oldKey, newKey, anchor0); } // content-only: Present(newKey)

    return new ANode[]{newBranch};
  }

  @Override
  public void walkAddresses(IStorage storage, IFn onAddress) {
    for (int i = 0; i < _len; ++i) {
      Address address = address(i);
      if (address != null) {
        if (!RT.booleanCast(onAddress.invoke(address))) {
          continue;
        }
      }
      if (_level > 1) {
        child(storage, i).walkAddresses(storage, onAddress);
      }
    }
  }

  // ---- diff-buf deposit (active only when _settings.diffBufSize() > 0) ----
  //
  // On the mutation return path, a content-only change to child i is recorded
  // into _slots[i] as Present(element) | Absent, with ĝ refreshed from the
  // (already-mutated, in-memory) child's count/measure. Structural returns
  // (split/merge/borrow) instead set _bufEntries = BUF_WRITE and never deposit —
  // that node will be written in full, materializing the new structure (see store()).

  // Exact subtree count of the in-memory child i (cheap: maintained by add/remove).
  private long childCount(IStorage storage, int i) {
    ANode child = child(storage, i);
    if (child instanceof ISubtreeCount) {
      long c = ((ISubtreeCount) child).subtreeCount();
      if (c >= 0) return c;
    }
    return child.count(storage);
  }

  // In-place deposit into this branch's slot for child i. anchor0 is child i's
  // pre-mutation durable address (captured before the mutation nulled it); the
  // slot keeps a previously-captured anchor if it already has one (first capture
  // of the txn wins; subsequent ops accumulate against the same anchor).
  private void depositInto(IStorage storage, int i, Object mapKey, Object val, Object anchor0) {
    depositKV(storage, i, new Object[]{ mapKey, val }, anchor0);
  }

  // diff-buf: a replace removes the element matching oldKey and inserts newKey. The leaf-diff
  // is replayed later under the SET's comparator (_projCmp), NOT the operation comparator that
  // located the element — so when oldKey and newKey differ under _projCmp (a replace with a
  // COARSER op comparator, e.g. datahike's value-changing upsert), Present(newKey) alone would
  // add newKey without removing oldKey on projection. Record both Absent(oldKey) + Present(newKey)
  // so projectLeaf reproduces the net effect with no comparator logic. When oldKey == newKey under
  // _projCmp (an in-place replace), the Present(newKey) assoc overwrites the Absent(oldKey) at the
  // same key ⇒ net Present(newKey), identical to a plain deposit. See doc/diff-buffering.md.
  private void depositReplace(IStorage storage, int i, Object oldKey, Object newKey, Object anchor0) {
    depositKV(storage, i, new Object[]{ oldKey, Slot.ABSENT, newKey, newKey }, anchor0);
  }

  // Core leaf-diff deposit: accumulate the given (key,val) pairs onto this branch's slot i,
  // keyed by the SET's comparator (_projCmp) — the comparator the durable leaf is sorted by and
  // the only one projectLeaf uses. Keying by _projCmp (not the operation comparator) is what makes
  // the diff a self-contained "remove/upsert these elements" language that replays without logic.
  private void depositKV(IStorage storage, int i, Object[] kv, Object anchor0) {
    if (_slots == null) {
      _slots = new Object[_keys.length];
    }
    Slot prev = (Slot) _slots[i];
    Object anchor = (prev != null && prev.anchor != null) ? prev.anchor : anchor0;
    ANode child = child(storage, i);
    Object diff;
    long newBE;          // this slot's new buffered-entry count (subtree total for a branch child)
    if (anchor == null) {
      // diff-buf: this child has NO durable base to diff against (a never-stored tree, or a
      // child created/split this txn). At store() such a slot takes the write-wholesale branch
      // (Pass 1: sl.anchor == null ⇒ writeList) and its diff is NEVER projected — so building a
      // leaf-diff here is pure waste (the dominant per-op cost: a PersistentTreeMap.assoc per
      // insert on a bulk load / fresh split). Skip it. The slot still carries count/measure
      // (unchanged) for op-buf-aware aggregation; diff=null + anchor=null is store-identical to
      // today's wasted-diff slot — the child is written in full either way. See store() Pass 1.
      diff = null;
      newBE = 0;                  // written wholesale ⇒ consumes none of the parent's buffer budget
    } else if (_level == 1) {
      assert _projCmp != null : "diff-buf: leaf-parent has no _projCmp at deposit (see Branch.child / root)";
      // leaf child: accumulate the leaf-op(s) (net latest-wins) onto the existing diff.
      PersistentTreeMap d;
      if (prev != null && prev.diff instanceof PersistentTreeMap) {
        d = (PersistentTreeMap) prev.diff;                         // already sorted under _projCmp
      } else if (prev != null && prev.diff instanceof java.util.Map) {
        // restored leaf-diff in storage form {:absent [el…] :present [el…]}: rebuild a _projCmp-sorted
        // map so accumulation (net latest-wins) and projection stay correct vs the anchor.
        d = Slot.emptyDiff(_projCmp);
        IPersistentMap dm = (IPersistentMap) prev.diff;
        for (ISeq s = RT.seq(dm.valAt(KW_ABSENT));  s != null; s = s.next()) d = (PersistentTreeMap) d.assoc(s.first(), Slot.ABSENT);
        for (ISeq s = RT.seq(dm.valAt(KW_PRESENT)); s != null; s = s.next()) d = (PersistentTreeMap) d.assoc(s.first(), s.first());
      } else {
        d = Slot.emptyDiff(_projCmp);
      }
      for (int k = 0; k < kv.length; k += 2) d = (PersistentTreeMap) d.assoc(kv[k], kv[k + 1]);
      diff = d;
      newBE = d.count();          // leaf-diff entry count (net latest-wins ⇒ one entry per cmp-key)
    } else {
      // branch child: anchor marker; its nested diff is derived from the live subtree at store
      diff = null;
      newBE = ((Branch) child).bufEntries();   // child's whole-subtree total (BUF_WRITE if it rebalanced)
    }
    // diff-buf: maintain this node's running buffered-entry total by delta (mirrors _subtreeCount).
    // A BUF_WRITE child poisons us → the must-write signal climbs to the nearest written ancestor
    // with no extra propagation code (this is the deposit sum doing the work the old deep walk did).
    long cur = bufEntries();                    // resolve our own LAZY (restored) value first
    if (cur != BUF_WRITE) {
      if (newBE == BUF_WRITE) _bufEntries = BUF_WRITE;
      else                    _bufEntries = cur - (prev != null ? slotBE(prev) : 0) + newBE;
    }
    _slots[i] = new Slot(diff, childCount(storage, i), child.measure(), anchor, newBE);
  }

  // diff-buf: a child's slot travels with its address. These mirror the per-element /
  // bulk copies of the address Stitch so a structural REMOVE rebuild carries surviving
  // siblings' buffered slots (null-source tolerant: a sibling branch may have no _slots).
  private Object slotAt(int i) { return (_slots != null) ? _slots[i] : null; }
  private static void slotCopyAll(Stitch ss, Object[] src, int from, int to) {
    if (src == null) { for (int i = from; i < to; ++i) ss.copyOne(null); }
    else ss.copyAll(src, from, to);
  }

  // diff-buf: free child i's durable blob when a STRUCTURAL rebuild (merge/borrow/split)
  // drops it. Such a child is materialized into a new node and its old blob is never
  // re-pointed as a buffered anchor (unlike the content-only case, which IS deferred to
  // store), so it is dead now. The live durable address is _addresses[i] if set, else the
  // anchor parked in slot i (the child was content-buffered earlier this txn, so its address
  // was nulled but its anchor lives in the slot). Read i from THIS node before the rebuild
  // overwrites it. No-op when diff-buf is off (callers gate on diffBufSize() > 0).
  private void freeDroppedChild(IStorage storage, int i) {
    if (storage == null) return;
    if (_addresses != null && _addresses[i] != null) { storage.markFreed(_addresses[i]); return; }
    if (_slots != null && _slots[i] instanceof Slot) {
      Object a = ((Slot) _slots[i]).anchor;
      if (a != null) storage.markFreed((Address) a);
    }
  }

  // Carry _slots through a structural rebuild where child `ins` was replaced by `nNodes`
  // new nodes (split/absorb), producing a slots array of length newLen laid out exactly
  // like the rebuilt _children: surviving siblings keep their slot; the new nodes get none
  // (they are materialized/written, no durable anchor). Null if this node has no slots.
  private Object[] stitchSlots(int ins, int nNodes, int newLen) {
    if (_slots == null) return null;
    Object[] out = new Object[newLen];
    Stitch s = new Stitch(out, 0);
    s.copyAll(_slots, 0, ins);
    for (int k = 0; k < nNodes; k++) s.copyOne(null);
    s.copyAll(_slots, ins + 1, _len);
    return out;
  }

  // For persistent (non-editable) returns: carry the source branch's slots into
  // this freshly-built branch, then deposit at i. (1-for-1 child replacement, so
  // indices are aligned with the source.)
  private void carrySlots(Object[] srcSlots) {
    if (srcSlots != null) {
      _slots = Arrays.copyOf(srcSlots, _keys.length);
    }
  }

  private void carryAndDeposit(IStorage storage, Object[] srcSlots, int i, Object mapKey, Object val, Object anchor0) {
    carrySlots(srcSlots);
    depositInto(storage, i, mapKey, val, anchor0);
  }

  private void carryAndDepositReplace(IStorage storage, Object[] srcSlots, int i, Object oldKey, Object newKey, Object anchor0) {
    carrySlots(srcSlots);
    depositReplace(storage, i, oldKey, newKey, anchor0);
  }

  // ---- diff-buf store-side helpers ----
  private static final Keyword KW_COUNT   = Keyword.intern(null, "count");
  private static final Keyword KW_MEASURE = Keyword.intern(null, "measure");
  private static final Keyword KW_DIFF    = Keyword.intern(null, "diff");
  private static final Keyword KW_MAXKEY  = Keyword.intern(null, "max-key");
  private static final Keyword KW_ABSENT  = Keyword.intern(null, "absent");
  private static final Keyword KW_PRESENT = Keyword.intern(null, "present");

  // diff-buf: convert a live leaf-diff (PersistentTreeMap {element -> element|ABSENT}, keyed by the
  // set's comparator _projCmp) into the COMPARATOR-AGNOSTIC storage form {:absent [el…] :present [el…]}.
  // Storage carries no comparator, so the wire form must not be a map keyed by the element: the
  // element's own .equals/.hashCode (e.g. datahike Datom = e,a,v) can be COARSER than _projCmp
  // (e,a,v,tx), which would collapse two entries the diff legitimately distinguishes (a tx-only
  // replace's Absent(old)+Present(new) → one entry, losing the removal). Two element vectors are
  // lossless. Pass-through if `diff` is already in storage form (a restored leaf-diff re-emitted on
  // a later store) or null. See doc/diff-buffering.md.
  static Object leafDiffForStorage(Object diff) {
    if (!(diff instanceof PersistentTreeMap)) return diff;   // already storage form (restored) or null
    Object absent = PersistentVector.EMPTY, present = PersistentVector.EMPTY;
    for (ISeq s = RT.seq(diff); s != null; s = s.next()) {
      IMapEntry e = (IMapEntry) s.first();
      if (Slot.ABSENT.equals(e.val())) absent  = ((IPersistentVector) absent).cons(e.key());
      else                             present = ((IPersistentVector) present).cons(e.val());
    }
    return PersistentHashMap.EMPTY.assoc(KW_ABSENT, absent).assoc(KW_PRESENT, present);
  }

  // Entry count (number of buffered element-changes) in a slot's diff blob.
  //
  // CONTRACT: `childLevel` is the level of the NODE whose diff this is — i.e. for a slot in a node
  // at level L describing its child (level L-1), call diffSize(slot.diff, L-1). At childLevel 0 the
  // diff is a LEAF-diff (live PersistentTreeMap or storage form {:absent :present}) ⇒ element count;
  // at childLevel >= 1 it is a BRANCH-diff {idx -> {:diff <child-diff>}} ⇒ sum of diffSize over each
  // entry's child diff at childLevel-1, recursing down to the leaf-diffs. So the result counts only
  // leaf entries; interior branch-diff levels contribute nothing (the size is linear in buffered ops,
  // not exponential in depth). callers: slotBE (resolving a restored LAZY slot) and bufEntriesSlow.
  //
  // We discriminate by `childLevel` rather than by probing values, because a leaf-diff's values are
  // the set's ELEMENTS — which may themselves be Associative (e.g. datahike Datoms), so a
  // value.containsKey(:diff) probe is unsafe.
  private static int diffSize(Object diff, int childLevel) {
    if (!(diff instanceof java.util.Map)) return 0;
    java.util.Map m = (java.util.Map) diff;
    if (m.isEmpty()) return 0;
    if (childLevel == 0) {                                       // leaf-diff entry count
      if (diff instanceof PersistentTreeMap) return m.size();    // live form: one entry per element
      IPersistentMap sm = (IPersistentMap) diff;                 // storage form {:absent :present}
      return RT.count(sm.valAt(KW_ABSENT)) + RT.count(sm.valAt(KW_PRESENT));
    }
    int t = 0;                                                  // nested branch-diff (Long-keyed)
    for (Object v : m.values()) t += diffSize(((IPersistentMap) v).valAt(KW_DIFF), childLevel - 1);
    return t;
  }

  // (The recursive content-only-diff-size walk that classified a dirty branch child here was
  // replaced by the O(1) _bufEntries aggregate: a child's subtree size is read from its slot
  // (slotBE) and its must-write status is the BUF_WRITE poison that already climbed the deposit
  // sum — see store() Pass 1 and depositKV. No store-time subtree walk.)

  // Assemble c's serializable nested diff {Long idx -> {:count :measure :diff}}, recursing
  // markers into the (resident) live subtree. Called once when c is first buffered, so the
  // result can be written back into the parent's slot (survives eviction / passthrough).
  private Object assembleNested(IStorage storage, Branch c) {
    IPersistentMap m = PersistentHashMap.EMPTY;
    if (c._slots != null) {
      for (int j = 0; j < c._len; j++) {
        Slot sl = (Slot) c._slots[j];
        if (sl == null) continue;
        Object d = (sl.diff != null)
            ? (c._level == 1 ? leafDiffForStorage(sl.diff) : sl.diff)   // leaf child ⇒ comparator-agnostic storage form
            : assembleNested(storage, (Branch) c.child(storage, j));
        // c._keys[j] = grandchild j's CURRENT (post-diff) separator; carry it so a reconstructed
        // (buffered) c restores its separators instead of keeping the anchor's stale ones.
        IPersistentMap entry = (IPersistentMap) PersistentHashMap.EMPTY
            .assoc(KW_COUNT, sl.count).assoc(KW_MEASURE, sl.measure).assoc(KW_DIFF, d)
            .assoc(KW_MAXKEY, c._keys[j]);
        m = (IPersistentMap) m.assoc((long) j, entry);
      }
    }
    return m;
  }

  // Serializable slots for THIS node (diffs already assembled during store); null if none.
  // Storage backends call this to persist the per-child buffered diffs alongside addresses.
  public Object slotsForStorage() {
    if (_slots == null) return null;
    IPersistentMap m = PersistentHashMap.EMPTY;
    for (int i = 0; i < _len; ++i) {
      Slot sl = (Slot) _slots[i];
      if (sl == null) continue;
      // _level==1 ⇒ this slot's child is a leaf ⇒ sl.diff is a leaf-diff: emit the comparator-agnostic
      // storage form {:absent :present}. _level>1 ⇒ sl.diff is null or a (restored) nested map ⇒ as-is.
      Object diffOut = (_level == 1) ? leafDiffForStorage(sl.diff) : sl.diff;
      IPersistentMap entry = (IPersistentMap) PersistentHashMap.EMPTY
          .assoc(KW_COUNT, sl.count).assoc(KW_MEASURE, sl.measure).assoc(KW_DIFF, diffOut)
          .assoc(KW_MAXKEY, _keys[i]);   // child i's current (post-diff) separator
      m = (IPersistentMap) m.assoc((long) i, entry);
    }
    return m.count() == 0 ? null : m;
  }

  // ---- diff-buf restore-side projection ----

  // Apply a leaf-diff to a durable leaf in ONE pass (no split/merge): merge the durable
  // keys with the diff (Present upserts the element, Absent removes) under the set's
  // comparator, emitting the result elements in key order. The net diff keeps the leaf
  // within [min, BF] (else the writer would have rebalanced and written it), so this is IO-free.
  private Leaf<Key, Address> projectLeaf(Leaf<Key, Address> base, Object diff, Comparator cmp) {
    assert cmp != null : "diff-buf: leaf-parent has no _projCmp — the set's comparator wasn't propagated to this branch (see PersistentSortedSet.root / Branch.child)";
    PersistentTreeMap m = (PersistentTreeMap) PersistentTreeMap.create(cmp, (ISeq) null);
    for (int i = 0; i < base._len; ++i) m = (PersistentTreeMap) m.assoc(base._keys[i], base._keys[i]);
    if (diff instanceof PersistentTreeMap) {                     // live form {element -> element|ABSENT}
      for (ISeq s = RT.seq(diff); s != null; s = s.next()) {
        IMapEntry e = (IMapEntry) s.first();
        if (Slot.ABSENT.equals(e.val())) m = (PersistentTreeMap) m.without(e.key());
        else                             m = (PersistentTreeMap) m.assoc(e.val(), e.val()); // upsert: value carries current element
      }
    } else {                                                     // storage form {:absent [el…] :present [el…]}
      IPersistentMap dm = (IPersistentMap) diff;
      for (ISeq s = RT.seq(dm.valAt(KW_ABSENT));  s != null; s = s.next()) m = (PersistentTreeMap) m.without(s.first());
      for (ISeq s = RT.seq(dm.valAt(KW_PRESENT)); s != null; s = s.next()) m = (PersistentTreeMap) m.assoc(s.first(), s.first());
    }
    int n = m.count();
    Object[] keys = new Object[n];
    int i = 0;
    for (ISeq s = RT.seq(m); s != null; s = s.next()) keys[i++] = ((IMapEntry) s.first()).val();
    return new Leaf(n, (Key[]) keys, _settings);
  }

  // Push one level down: install the nested diff as base's own _slots (each grandchild's
  // diff + ĝ, anchored at base's durable child address) and set base's aggregates from ĝ.
  // Grandchildren project lazily on their own descent.
  private Branch<Key, Address> projectBranch(Branch<Key, Address> base, Slot sl) {
    Object[] slots = new Object[base._keys.length];
    for (ISeq s = RT.seq(sl.diff); s != null; s = s.next()) {
      IMapEntry e = (IMapEntry) s.first();
      int i = ((Number) e.key()).intValue();
      IPersistentMap entry = (IPersistentMap) e.val();
      long cnt = ((Number) entry.valAt(KW_COUNT)).longValue();
      Object measure = entry.valAt(KW_MEASURE);
      Object d = entry.valAt(KW_DIFF);
      Object mk = entry.valAt(KW_MAXKEY);
      slots[i] = new Slot(d, cnt, measure, base._addresses[i]);   // anchor = grandchild's durable address
      // Restore the separator: base came from the anchor (old durable object) whose _keys[i] is
      // the PRE-diff max. The diff changed child i's max, so fix the separator here — otherwise
      // search/contains route against a phantom max-key (the verified diff-buf-v5 read bug).
      if (mk != null) base._keys[i] = (Key) mk;
    }
    base._slots = slots;
    base._subtreeCount = sl.count;        // ĝ.count — no child summing
    base._measure = sl.measure;           // ĝ.measure
    return base;
  }

  @Override
  public Address store(IStorage<Key, Address> storage) {
    if (_settings.diffBufSize() <= 0) {                           // baseline ⇒ byte-identical (I0)
      ensureAddresses();
      for (int i = 0; i < _len; ++i) {
        if (_addresses[i] == null) {
          assert _children != null && _children[i] != null && _children[i] instanceof ANode;
          address(i, ((ANode<Key, Address>) _children[i]).store(storage));
        }
      }
      return storage.store(this);
    }

    // diff-buf: buffer content-only dirty children (record their diff in THIS object,
    // re-point the address to the child's durable anchor) up to the budget B; write the rest.
    //
    // Eviction is BIGGEST-FIRST: when the dirty children don't all fit, we flush the ones
    // with the LARGEST diffs and keep the small ones buffered. This makes a slot that
    // regularly consumes a big share of the budget get written proportionally often (rather
    // than jamming the buffer while small diffs are flushed under a naive index-order fill),
    // and reclaims the most budget per PUT. Only dirty (resident) children are ever flushed,
    // so a flush never triggers a read; clean buffered-passthrough children consume budget
    // but are left untouched (flushing them would require loading their anchor). The running
    // total stays <= B strictly. See doc/diff-buffering.md (Store / eviction policy).
    ensureAddresses();
    final int budget = _settings.diffBufSize();
    assert assertBufEntries(storage);  // -ea oracle: delta-maintained total == fresh subtree walk

    // Pass 1: account clean passthrough diffs, and classify each dirty child as bufferable
    // (content-only) or must-write — both now O(1) per child: the size is the slot's cached
    // bufEntries (resolved from the diff for a restored slot), and the must-write gate is the
    // BUF_WRITE poison the deposit sum already lifted from the rebalance point (no subtree walk).
    int passthrough = 0;
    int[] csz = new int[_len];                                  // dirty child's diff size
    Object[] cnested = new Object[_len];                        // its assembled nested diff
    java.util.ArrayList<Integer> bufferable = new java.util.ArrayList<>();
    java.util.ArrayList<Integer> writeList = new java.util.ArrayList<>();
    for (int i = 0; i < _len; ++i) {
      Slot sl = (_slots != null) ? (Slot) _slots[i] : null;
      if (_addresses[i] != null) {
        if (sl != null) passthrough += slotBE(sl);             // clean buffered-passthrough subtree total
        continue;
      }
      // _addresses[i] == null: dirty this commit ⇒ child is resident; its slot is live (deposited).
      if (sl == null || sl.anchor == null) {                    // no durable anchor ⇒ must write
        writeList.add(i);
      } else if (sl.bufEntries == BUF_WRITE) {                  // subtree rebalanced (poison) ⇒ must write
        writeList.add(i);
      } else {                                                  // content-only ⇒ bufferable
        csz[i] = (int) sl.bufEntries;
        cnested[i] = (sl.diff != null) ? sl.diff                // leaf-diff, or restored-nested branch-diff
                   : assembleNested(storage, (Branch)(ANode) _settings.readReference(_children[i])); // live branch marker
        bufferable.add(i);
      }
    }

    // Pass 2: buffer the SMALLEST bufferable children while the running total (passthrough +
    // buffered) stays within budget; the rest — the largest — are flushed (biggest-first).
    bufferable.sort((x, y) -> Integer.compare(csz[x], csz[y]));
    int embedded = passthrough;
    for (int i : bufferable) {
      Slot sl = (Slot) _slots[i];
      if (embedded + csz[i] <= budget) {
        _addresses[i] = (Address) sl.anchor;                    // re-point to durable anchor (no write)
        _slots[i] = new Slot(cnested[i], sl.count, sl.measure, sl.anchor, csz[i]); // write back assembled diff + its size
        embedded += csz[i];
      } else {
        writeList.add(i);                                       // doesn't fit ⇒ flush
      }
    }

    // Pass 3: write the flushed/structural children (all resident ⇒ no read).
    for (int i : writeList) {
      ANode child = (ANode) _settings.readReference(_children[i]);
      Slot sl = (_slots != null) ? (Slot) _slots[i] : null;
      if (sl != null && sl.anchor != null) storage.markFreed((Address) sl.anchor);
      _addresses[i] = ((ANode<Key, Address>) child).store(storage);
      if (_slots != null) _slots[i] = null;
    }
    Address a = storage.store(this);
    // Written ⇒ this node now equals its durable object, whose remaining slots are exactly the
    // children we BUFFERED (passthrough + newly buffered) — the flushed ones were nulled. So the
    // settled buffered-entry total is `embedded`, not 0 (a later commit deltas from here). This
    // also clears any BUF_WRITE poison: the new structure is now materialized on disk.
    _bufEntries = embedded;
    return a;
  }

  public String str(IStorage storage, int lvl) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < _len; ++i) {
      sb.append("\n");
      for (int j = 0; j < lvl; ++j)
        sb.append("| ");
      sb.append(_keys[i] + ": " + child(storage, i).str(storage, lvl+1));
    }
    return sb.toString();
  }

  @Override
  public void toString(StringBuilder sb, Address address, String indent) {
    sb.append(indent);
    sb.append("Branch addr: " + address + " len: " + _len + " ");
    for (int i = 0; i < _len; ++i) {
      sb.append("\n");
      ANode child = null;
      if (_children != null) {
        Object ref = _children[i];
        if (ref != null) {
          child = (ANode) _settings.readReference(ref);
        }
      }
      if (child != null)
        child.toString(sb, address(i), indent + "  ");
      else
        sb.append(indent + "  " + address(i) + ": <lazy> ");
    }
  }
}