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

  // OP_BUF_V5 (only used when _settings.opBufSize() > 0; null/false otherwise, so
  // opBufSize==0 is byte-identical to baseline — invariant I0).
  //
  // Per-child diff slot: _slots[i], when non-null, is the buffered logical diff of
  // child i against its durable version (a Slot holding a PersistentTreeMap<Key,Op>
  // + a cached (count, measure) snapshot ĝ). null == child i has no buffered diff.
  public Object[] _slots;

  // Store-time bookkeeping (set on the mutation return path, read by store):
  //   _rebalanced   — a split/merge/borrow happened AT this node this txn.
  //   _childWritten — set during store when a child of this node was written,
  //                   making this node structural (its pointers changed).
  public boolean _rebalanced;
  public boolean _childWritten;

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
  }

  public Branch(int level, int len, Key[] keys, Address[] addresses, Object[] children, long subtreeCount, Object measure, Settings settings) {
    super(len, keys, measure, settings);
    assert level >= 1;
    assert addresses == null || addresses.length >= len : ("addresses = " + Arrays.toString(addresses) + ", len = " + len);
    assert children == null || children.length >= len;

    _level        = level;
    _addresses    = addresses;
    _children     = children;
    _subtreeCount = subtreeCount;
  }

  public Branch(int level, int len, Settings settings) {
    super(len, (Key[]) new Object[ANode.newLen(len, settings)], settings);
    assert level >= 1;

    _level        = level;
    _addresses    = null;
    _children     = null;
    _subtreeCount = -1;
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
      Slot sl = (_slots != null) ? (Slot) _slots[idx] : null;
      // OP_BUF_V5 push-down (M5): project this parent's buffered diff onto the freshly
      // loaded child — leaf: batch-rebuild keys; branch: install the nested diff as the
      // child's own _slots + set its aggregates from ĝ. Runs once, here, at materialization
      // (reads stay baseline). Parent's slot supersedes any diff baked in the child's object.
      if (_settings.opBufSize() > 0 && sl != null && sl.diff != null) {
        child = (base instanceof Leaf) ? (ANode) projectLeaf((Leaf) base, sl.diff)
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
    // OP_BUF_V5: capture child ins's durable address BEFORE the mutation nulls it,
    // so a leaf-parent deposit can record it as the buffer anchor (M4a).
    Object anchor0 = (_settings.opBufSize() > 0 && _addresses != null) ? _addresses[ins] : null;
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
      if (_settings.opBufSize() > 0) depositInto(storage, ins, key, key, cmp, anchor0); // content-only: Present(key) / branch marker
      return PersistentSortedSet.EARLY_EXIT;
    }

    // Compute new children's total count (accounts for processor expanding/compacting)
    long newChildrenCount = 0;
    for (ANode n : nodes) newChildrenCount += ((ISubtreeCount) n).subtreeCount();

    IMeasure measureOps = _settings.measure();

    // same len, editable
    if (1 == nodes.length && editable()) {
      ANode<Key, Address> node = nodes[0];
      _keys[ins] = node.maxKey();
      // Mark old child address as freed before clearing
      if (storage != null && _addresses != null && _addresses[ins] != null) {
        storage.markFreed(_addresses[ins]);
      }
      child(ins, node);
      // Update subtree count using exact delta from old vs new child
      if (_subtreeCount >= 0 && oldChildCount >= 0)
        _subtreeCount = _subtreeCount - oldChildCount + newChildrenCount;
      else
        _subtreeCount = -1;
      // Update measure: recompute from children
      if (measureOps != null && _measure != null) {
        _measure = tryComputeMeasure(storage);
      }
      if (_settings.opBufSize() > 0) depositInto(storage, ins, key, key, cmp, anchor0); // content-only: Present(key) / branch marker
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
      long newCount = (_subtreeCount >= 0 && oldChildCount >= 0)
          ? _subtreeCount - oldChildCount + newChildrenCount : -1;
      Object newMeasure = tryComputeMeasureFromChildren(newChildren, _len, storage, measureOps);
      Branch<Key, Address> nb = new Branch(_level, _len, newKeys, newAddresses, newChildren, newCount, newMeasure, settings);
      if (settings.opBufSize() > 0) nb.carryAndDeposit(storage, _slots, ins, key, key, cmp, anchor0); // content-only: Present(key) / branch marker
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

    // Absorb: fits in single branch
    if (newLen <= settings.branchingFactor()) {
      // Use delta formula: exact and O(1), avoids scanning all children
      long count = (_subtreeCount >= 0 && oldChildCount >= 0)
          ? _subtreeCount - oldChildCount + newChildrenCount : -1;
      Object measure = tryComputeMeasureFromChildren(allChildren, newLen, storage, measureOps);
      Branch<Key, Address> nb = new Branch(_level, newLen, allKeys, allAddresses, allChildren, count, measure, settings);
      if (settings.opBufSize() > 0) {
        nb._rebalanced = true; // absorbed a child split: structural → written, but it still buffers surviving siblings
        nb._slots = stitchSlots(ins, nodes.length, newLen); // carry buffered siblings' slots through the rebuild
      }
      return new ANode[]{ nb };
    }

    // Split into two branches
    int half1 = newLen >>> 1, half2 = newLen - half1;

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
    Branch<Key, Address> sb1 = new Branch(_level, half1, keys1, addresses1, children1, count1, measure1, settings);
    Branch<Key, Address> sb2 = new Branch(_level, half2, keys2, addresses2, children2, count2, measure2, settings);
    if (settings.opBufSize() > 0) {
      sb1._rebalanced = true; sb2._rebalanced = true; // split: structural → written, still buffer surviving siblings
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
    
    // OP_BUF_V5: capture child idx's durable address before the mutation nulls it (M4a).
    Object anchor0 = (_settings.opBufSize() > 0 && _addresses != null) ? _addresses[idx] : null;
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
      if (_settings.opBufSize() > 0) depositInto(storage, idx, key, Slot.ABSENT, cmp, anchor0); // content-only: Absent(key) / branch marker
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
        // Mark freed addresses before clearing them
        if (storage != null && _addresses != null) {
          // The child at idx is always being replaced
          if (_addresses[idx] != null) {
            storage.markFreed(_addresses[idx]);
          }
          // Left child if changed
          if (leftChanged && idx > 0 && _addresses[idx - 1] != null) {
            storage.markFreed(_addresses[idx - 1]);
          }
          // Right child if changed
          if (rightChanged && _addresses[idx + 1] != null) {
            storage.markFreed(_addresses[idx + 1]);
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

        if (_settings.opBufSize() > 0 && _slots != null) {       // mirror the address Stitch in place
          Stitch ss = new Stitch(_slots, Math.max(idx - 1, 0));
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
        if (_settings.opBufSize() > 0) {
          if (!leftChanged && !rightChanged && newLen == _len) {
            depositInto(storage, idx, key, Slot.ABSENT, cmp, anchor0); // content-only: Absent(key) / branch marker
          } else {
            _rebalanced = true; // a child merged/borrowed with a sibling: structural → write in full
          }
        }
        return PersistentSortedSet.EARLY_EXIT;
      }

      Branch newCenter = new Branch(_level, newLen, settings);

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
      if (settings.opBufSize() > 0) {
        Object[] ns = new Object[newCenter._keys.length];      // mirror the address Stitch above
        Stitch ss = new Stitch(ns, 0);
        slotCopyAll(ss, _slots, 0, idx - 1);
        if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(idx - 1));
                              ss.copyOne(null);
        if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(idx + 1));
        slotCopyAll(ss, _slots, idx + 2, _len);
        newCenter._slots = ns;
        if (!leftChanged && !rightChanged && newLen == _len)
          newCenter.depositInto(storage, idx, key, Slot.ABSENT, cmp, anchor0); // content-only center
        else
          newCenter._rebalanced = true; // a child merged/borrowed with a sibling: structural → write in full
      }
      return new ANode[] { left, newCenter, right };
    }

    // can join with left
    if (left != null && left._len + newLen <= _settings.branchingFactor()) {
      Branch join = new Branch(_level, left._len + newLen, settings);

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
      if (settings.opBufSize() > 0) {
        join._rebalanced = true; // merged with left: structural → written, still buffers surviving siblings
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
      Branch join = new Branch(_level, newLen + right._len, settings);

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
      if (settings.opBufSize() > 0) {
        join._rebalanced = true; // merged with right: structural → written, still buffers surviving siblings
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

      Branch newLeft   = new Branch(_level, newLeftLen, settings);
      Branch newCenter = new Branch(_level, newCenterLen, settings);

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
      if (settings.opBufSize() > 0) {
        newLeft._rebalanced = true; newCenter._rebalanced = true; // borrowed from left: structural
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

      Branch newCenter = new Branch(_level, newCenterLen, settings),
             newRight  = new Branch(_level, newRightLen, settings);

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
      if (settings.opBufSize() > 0) {
        newCenter._rebalanced = true; newRight._rebalanced = true; // borrowed from right: structural
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
  public ANode[] replace(IStorage storage, Key oldKey, Key newKey, Comparator<Key> cmp, Settings settings) {
    assert 0 == cmp.compare(oldKey, newKey) : "oldKey and newKey must compare as equal (cmp.compare must return 0)";

    // Find which child contains the key
    int idx = search(oldKey, cmp);
    if (idx < 0) idx = -idx - 1;
    if (idx == _len) idx = _len - 1; // key might be in last child
    assert 0 <= idx && idx < _len;

    // OP_BUF_V5: capture child idx's durable address before the mutation nulls it (M4a).
    Object anchor0 = (_settings.opBufSize() > 0 && _addresses != null) ? _addresses[idx] : null;
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
      if (_settings.opBufSize() > 0) depositInto(storage, idx, newKey, newKey, cmp, anchor0); // content-only: Present(newKey) / branch marker
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
      // Mark old child address as freed before clearing
      if (storage != null && _addresses != null && _addresses[idx] != null) {
        storage.markFreed(_addresses[idx]);
      }
      child(idx, newChild);
      // Note: child() already clears _addresses[idx] via address(idx, null)
      // Try to recompute measure from final state (after child replacement)
      if (measureOps != null && _measure != null) {
        _measure = tryComputeMeasure(storage);
      }
      if (_settings.opBufSize() > 0) depositInto(storage, idx, newKey, newKey, cmp, anchor0); // content-only: Present(newKey) / branch marker
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
    Branch<Key, Address> newBranch = new Branch(_level, _len, newKeys, newAddresses, newChildren, _subtreeCount, null, settings);
    if (measureOps != null && _measure != null) {
      newBranch._measure = newBranch.tryComputeMeasure(storage);
    }
    if (settings.opBufSize() > 0) newBranch.carryAndDeposit(storage, _slots, idx, newKey, newKey, cmp, anchor0); // content-only: Present(newKey) / branch marker

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

  // ---- OP_BUF_V5 deposit (active only when _settings.opBufSize() > 0) ----
  //
  // On the mutation return path, a content-only change to child i is recorded
  // into _slots[i] as Present(element) | Absent, with ĝ refreshed from the
  // (already-mutated, in-memory) child's count/measure. Structural returns
  // (split/merge/borrow) instead set _rebalanced and never deposit — that node
  // will be written in full, materializing the new structure (see store(), M4).

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
  private void depositInto(IStorage storage, int i, Object mapKey, Object val, Comparator cmp, Object anchor0) {
    if (_slots == null) {
      _slots = new Object[_keys.length];
    }
    Slot prev = (Slot) _slots[i];
    Object anchor = (prev != null && prev.anchor != null) ? prev.anchor : anchor0;
    ANode child = child(storage, i);
    Object diff;
    if (_level == 1) {
      // leaf child: accumulate the leaf-op (net latest-wins) onto the existing diff.
      PersistentTreeMap d;
      if (prev != null && prev.diff instanceof PersistentTreeMap) {
        d = (PersistentTreeMap) prev.diff;                         // already sorted under cmp
      } else if (prev != null && prev.diff instanceof java.util.Map) {
        // restored leaf-diff (plain edn map): rebuild a cmp-sorted map so accumulation
        // (net latest-wins on cmp-equal keys) and projection stay correct vs the anchor.
        d = Slot.emptyDiff(cmp);
        for (ISeq s = RT.seq(prev.diff); s != null; s = s.next()) {
          IMapEntry e = (IMapEntry) s.first();
          d = (PersistentTreeMap) d.assoc(e.key(), e.val());
        }
      } else {
        d = Slot.emptyDiff(cmp);
      }
      diff = d.assoc(mapKey, val);
    } else {
      // branch child: anchor marker; its nested diff is derived from the live subtree at store
      diff = null;
    }
    _slots[i] = new Slot(diff, childCount(storage, i), child.measure(), anchor);
  }

  // OP_BUF_V5: a child's slot travels with its address. These mirror the per-element /
  // bulk copies of the address Stitch so a structural REMOVE rebuild carries surviving
  // siblings' buffered slots (null-source tolerant: a sibling branch may have no _slots).
  private Object slotAt(int i) { return (_slots != null) ? _slots[i] : null; }
  private static void slotCopyAll(Stitch ss, Object[] src, int from, int to) {
    if (src == null) { for (int i = from; i < to; ++i) ss.copyOne(null); }
    else ss.copyAll(src, from, to);
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
  private void carryAndDeposit(IStorage storage, Object[] srcSlots, int i, Object mapKey, Object val, Comparator cmp, Object anchor0) {
    if (srcSlots != null) {
      _slots = Arrays.copyOf(srcSlots, _keys.length);
    }
    depositInto(storage, i, mapKey, val, cmp, anchor0);
  }

  // ---- OP_BUF_V5 store-side helpers (M4b) ----
  private static final Keyword KW_COUNT   = Keyword.intern(null, "count");
  private static final Keyword KW_MEASURE = Keyword.intern(null, "measure");
  private static final Keyword KW_DIFF    = Keyword.intern(null, "diff");

  // Entry count of an already-assembled slot diff: a leaf-diff's size, or the
  // recursive sum over a nested {idx -> {:count :measure :diff}} map.
  private static int diffSize(Object diff) {
    if (!(diff instanceof java.util.Map)) return 0;
    java.util.Map m = (java.util.Map) diff;
    if (m.isEmpty()) return 0;
    // Distinguish a nested branch-diff (values are {:count :measure :diff} maps) from a
    // leaf-diff (values are elements / ABSENT) — needed because a RESTORED leaf-diff is a
    // plain edn map, not a PersistentTreeMap, so we can't tell by type alone.
    Object firstVal = m.values().iterator().next();
    boolean branch = (firstVal instanceof Associative) && ((Associative) firstVal).containsKey(KW_DIFF);
    if (branch) {
      int t = 0;
      for (Object v : m.values()) t += diffSize(((IPersistentMap) v).valAt(KW_DIFF));
      return t;
    }
    return m.size();                                            // leaf-diff entry count
  }

  // Total content-only diff size of c's dirty subtree, or -1 if any dirty descendant
  // REBALANCED (⇒ c is not bufferable — the affected path must be written; W3). The
  // recursive check is needed because a deep rebalance leaves intermediate nodes
  // content-only (a single-{node} return up), so _rebalanced alone at c is insufficient.
  private int contentOnlyDiffSize(IStorage storage, Branch c) {
    if (c._rebalanced) return -1;
    if (c._slots == null) return 0;
    int total = 0;
    for (int j = 0; j < c._len; j++) {
      Slot sl = (Slot) c._slots[j];
      if (sl == null) continue;
      if (sl.diff == null) {                                    // branch marker -> recurse live child
        ANode gc = c.child(storage, j);
        if (!(gc instanceof Branch)) return -1;
        int s = contentOnlyDiffSize(storage, (Branch) gc);
        if (s < 0) return -1;
        total += s;
      } else {
        total += diffSize(sl.diff);                             // leaf-diff or restored-nested
      }
    }
    return total;
  }

  // Assemble c's serializable nested diff {Long idx -> {:count :measure :diff}}, recursing
  // markers into the (resident) live subtree. Called once when c is first buffered, so the
  // result can be written back into the parent's slot (survives eviction / passthrough).
  private Object assembleNested(IStorage storage, Branch c) {
    IPersistentMap m = PersistentHashMap.EMPTY;
    if (c._slots != null) {
      for (int j = 0; j < c._len; j++) {
        Slot sl = (Slot) c._slots[j];
        if (sl == null) continue;
        Object d = (sl.diff != null) ? sl.diff : assembleNested(storage, (Branch) c.child(storage, j));
        IPersistentMap entry = (IPersistentMap) PersistentHashMap.EMPTY
            .assoc(KW_COUNT, sl.count).assoc(KW_MEASURE, sl.measure).assoc(KW_DIFF, d);
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
      IPersistentMap entry = (IPersistentMap) PersistentHashMap.EMPTY
          .assoc(KW_COUNT, sl.count).assoc(KW_MEASURE, sl.measure).assoc(KW_DIFF, sl.diff);
      m = (IPersistentMap) m.assoc((long) i, entry);
    }
    return m.count() == 0 ? null : m;
  }

  // ---- OP_BUF_V5 restore-side projection (M5) ----

  // Apply a leaf-diff to a durable leaf in ONE pass (no split/merge): merge the durable
  // keys with the diff (Present upserts the element, Absent removes) under the set's
  // comparator, emitting the result elements in key order. The net diff keeps the leaf
  // within [min, BF] (else the writer would have rebalanced and written it), so this is IO-free.
  private Leaf<Key, Address> projectLeaf(Leaf<Key, Address> base, Object diff) {
    Comparator cmp = _settings.comparator();
    PersistentTreeMap m = (PersistentTreeMap) PersistentTreeMap.create(cmp, (ISeq) null);
    for (int i = 0; i < base._len; ++i) m = (PersistentTreeMap) m.assoc(base._keys[i], base._keys[i]);
    for (ISeq s = RT.seq(diff); s != null; s = s.next()) {
      IMapEntry e = (IMapEntry) s.first();
      if (Slot.ABSENT.equals(e.val())) m = (PersistentTreeMap) m.without(e.key());
      else                             m = (PersistentTreeMap) m.assoc(e.val(), e.val()); // upsert: value carries current element
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
      slots[i] = new Slot(d, cnt, measure, base._addresses[i]);   // anchor = grandchild's durable address
    }
    base._slots = slots;
    base._subtreeCount = sl.count;        // ĝ.count — no child summing
    base._measure = sl.measure;           // ĝ.measure
    return base;
  }

  @Override
  public Address store(IStorage<Key, Address> storage) {
    if (_settings.opBufSize() <= 0) {                           // baseline ⇒ byte-identical (I0)
      ensureAddresses();
      for (int i = 0; i < _len; ++i) {
        if (_addresses[i] == null) {
          assert _children != null && _children[i] != null && _children[i] instanceof ANode;
          address(i, ((ANode<Key, Address>) _children[i]).store(storage));
        }
      }
      return storage.store(this);
    }

    // OP_BUF_V5: buffer content-only dirty children (record their diff in THIS object,
    // re-point the address to the child's durable anchor) up to the budget B; write the rest.
    ensureAddresses();
    final int budget = _settings.opBufSize();
    int embedded = 0;
    for (int i = 0; i < _len; ++i) {
      Slot sl = (_slots != null) ? (Slot) _slots[i] : null;
      if (_addresses[i] != null) {
        if (sl != null) embedded += diffSize(sl.diff);          // passthrough (do NOT touch child)
        continue;                                                // clean or buffered-passthrough
      }
      // _addresses[i] == null: dirty this commit ⇒ child is resident
      ANode child = (ANode) _settings.readReference(_children[i]);
      boolean canBuffer;
      int sz;
      Object nested;
      if (sl == null || sl.anchor == null) {                    // no durable anchor ⇒ must write
        canBuffer = false; sz = 0; nested = null;
      } else if (child instanceof Leaf) {
        canBuffer = true; sz = diffSize(sl.diff); nested = sl.diff;
      } else {
        int s = contentOnlyDiffSize(storage, (Branch) child);   // -1 if subtree rebalanced
        canBuffer = (s >= 0);
        sz = (s >= 0) ? s : 0;
        nested = (s >= 0) ? ((sl.diff != null) ? sl.diff : assembleNested(storage, (Branch) child)) : null;
      }
      if (canBuffer && embedded + sz <= budget) {
        _addresses[i] = (Address) sl.anchor;                    // re-point to durable anchor (no write)
        _slots[i] = new Slot(nested, sl.count, sl.measure, sl.anchor); // write back assembled diff
        embedded += sz;
      } else {
        if (sl != null && sl.anchor != null) storage.markFreed((Address) sl.anchor);
        _addresses[i] = ((ANode<Key, Address>) child).store(storage);
        if (_slots != null) _slots[i] = null;
        _childWritten = true;
      }
    }
    return storage.store(this);
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