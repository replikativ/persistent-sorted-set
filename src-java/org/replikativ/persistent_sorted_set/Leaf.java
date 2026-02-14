package org.replikativ.persistent_sorted_set;

import java.util.*;
import java.util.function.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public class Leaf<Key, Address> extends ANode<Key, Address> implements ISubtreeCount {
  public Leaf(int len, Key[] keys, Settings settings) {
    super(len, keys, settings);
  }

  public Leaf(int len, Key[] keys, Object measure, Settings settings) {
    super(len, keys, measure, settings);
  }

  public Leaf(int len, Settings settings) {
    super(len, (Key[]) new Object[ANode.newLen(len, settings)], settings);
  }

  public Leaf(List<Key> keys, Settings settings) {
    this(keys.size(), (Key[]) keys.toArray(), settings);
  }

  private static <K> boolean isSorted(List<K> list, Comparator<K> cmp) {
    for (int i = 1; i < list.size(); i++) {
      if (cmp.compare(list.get(i - 1), list.get(i)) >= 0) return false;
    }
    return true;
  }

  @Override
  public int level() {
    return 0;
  }

  @Override
  public Object tryComputeMeasure(IStorage storage) {
    // For leaves, try and force are the same - just compute from keys
    IMeasure measureOps = _settings.measure();
    if (measureOps == null) return null;

    Object result = measureOps.identity();
    for (int i = 0; i < _len; i++) {
      result = measureOps.merge(result, measureOps.extract(_keys[i]));
    }
    return result;
  }

  @Override
  public Object forceComputeMeasure(IStorage storage) {
    // For leaves, try and force are the same - just compute from keys
    _measure = tryComputeMeasure(storage);
    return _measure;
  }

  @Override
  public int count(IStorage storage) {
    return _len;
  }

  @Override
  public long subtreeCount() {
    return _len;
  }

  @Override
  public boolean contains(IStorage storage, Key key, Comparator<Key> cmp) {
    return search(key, cmp) >= 0;
  }

  @Override
  public ANode[] add(IStorage storage, Key key, Comparator<Key> cmp, Settings settings) {
    int idx = search(key, cmp);
    if (idx >= 0) // already in set
      return PersistentSortedSet.UNCHANGED;

    int ins = -idx - 1;
    assert 0 <= ins && ins <= _len;

    IMeasure measureOps = _settings.measure();

    // Skip transient shortcut when processor would fire — processor needs the
    // persistent-style path to create new arrays, run processing, and handle splits
    ILeafProcessor processor = settings.leafProcessor();
    boolean processorWillFire = processor != null && processor.shouldProcess(_len + 1, settings);

    // can modify array in place (only if processor won't fire)
    if (editable() && _len < _keys.length && !processorWillFire) {
      if (ins == _len) {
        _keys[_len] = key;
        _len += 1;
        // Update stats incrementally only if already computed
        if (measureOps != null && _measure != null) {
          _measure = measureOps.merge(_measure, measureOps.extract(key));
        }
        return new ANode[]{this}; // maxKey needs updating
      } else {
        ArrayUtil.copy(_keys, ins, _len, _keys, ins+1);
        _keys[ins] = key;
        _len += 1;
        // Update stats incrementally only if already computed
        if (measureOps != null && _measure != null) {
          _measure = measureOps.merge(_measure, measureOps.extract(key));
        }
        return PersistentSortedSet.EARLY_EXIT;
      }
    }

    // Build complete entries with new key inserted
    Key[] allKeys = (Key[]) new Object[_len + 1];
    new Stitch(allKeys, 0)
      .copyAll(_keys, 0, ins)
      .copyOne(key)
      .copyAll(_keys, ins, _len);
    int totalLen = _len + 1;

    // Run processor if configured
    if (processor != null && processor.shouldProcess(totalLen, settings)) {
      List<Key> processed = processor.processLeaf(Arrays.asList(allKeys), storage, settings);
      assert processed.size() > 0 : "ILeafProcessor.processLeaf must return at least one entry";
      assert isSorted(processed, cmp) : "ILeafProcessor.processLeaf must return entries in sorted order";
      totalLen = processed.size();
      allKeys = (Key[]) new Object[totalLen];
      for (int i = 0; i < totalLen; i++) {
        allKeys[i] = processed.get(i);
      }
    }

    // Fits in single leaf
    if (totalLen <= settings.branchingFactor()) {
      Leaf n = new Leaf(totalLen, allKeys, settings);
      if (_measure != null) {
        n._measure = n.tryComputeMeasure(storage);
      }
      return new ANode[]{n};
    }

    // Split into ceil(totalLen / BF) leaves (remainder goes to later leaves)
    int bf = settings.branchingFactor();
    int numLeaves = (totalLen + bf - 1) / bf;
    ANode[] result = new ANode[numLeaves];
    int pos = 0;
    int baseLen = totalLen / numLeaves;
    int remainder = totalLen % numLeaves;
    for (int i = 0; i < numLeaves; i++) {
      // Remainder distributed to later leaves (matches old half1 = totalLen >>> 1 behavior)
      int leafLen = baseLen + (i >= numLeaves - remainder ? 1 : 0);
      Leaf n = new Leaf(leafLen, settings);
      ArrayUtil.copy(allKeys, pos, pos + leafLen, n._keys, 0);
      if (_measure != null) {
        n._measure = n.tryComputeMeasure(storage);
      }
      result[i] = n;
      pos += leafLen;
    }
    return result;
  }

  @Override
  public ANode[] remove(IStorage storage, Key key, ANode _left, ANode _right, Comparator<Key> cmp, Settings settings) {
    Leaf left = (Leaf) _left;
    Leaf right = (Leaf) _right;

    int idx = search(key, cmp);
    if (idx < 0) // not in set
      return PersistentSortedSet.UNCHANGED;

    int newLen = _len - 1;
    IMeasure measureOps = _settings.measure();
    final Leaf thisLeaf = this;

    // Skip transient shortcut when processor would fire
    ILeafProcessor processor = settings.leafProcessor();
    boolean processorWillFire = processor != null && processor.shouldProcess(newLen, settings);

    // nothing to merge — transient, can edit in place (only if processor won't fire)
    if (editable() && !processorWillFire && (newLen >= _settings.minBranchingFactor() || (left == null && right == null))) {
      ArrayUtil.copy(_keys, idx + 1, _len, _keys, idx);
      _len = newLen;
      if (measureOps != null && _measure != null) {
        _measure = measureOps.remove(_measure, key, () -> thisLeaf.tryComputeMeasure(storage));
      }
      if (idx == newLen) // removed last, need to signal new maxKey
        return new ANode[]{left, this, right};
      return PersistentSortedSet.EARLY_EXIT;
    }

    // Build center entries (this leaf minus removed key)
    Key[] centerKeys = (Key[]) new Object[newLen];
    new Stitch(centerKeys, 0)
      .copyAll(_keys, 0, idx)
      .copyAll(_keys, idx + 1, _len);
    int centerLen = newLen;

    // Run processor if configured
    if (processorWillFire) {
      List<Key> processed = processor.processLeaf(Arrays.asList(centerKeys), storage, settings);
      assert processed.size() > 0 : "ILeafProcessor.processLeaf must return at least one entry";
      assert isSorted(processed, cmp) : "ILeafProcessor.processLeaf must return entries in sorted order";
      assert processed.size() <= settings.branchingFactor()
          : "ILeafProcessor.processLeaf must not expand beyond branchingFactor during remove (got "
            + processed.size() + ", max " + settings.branchingFactor() + ")";
      centerLen = processed.size();
      centerKeys = (Key[]) new Object[centerLen];
      for (int i = 0; i < centerLen; i++) {
        centerKeys[i] = processed.get(i);
      }
    }

    // nothing to merge
    if (centerLen >= _settings.minBranchingFactor() || (left == null && right == null)) {
      Leaf center = new Leaf(Math.max(centerLen, 0), centerKeys, settings);
      if (_measure != null) {
        center._measure = center.tryComputeMeasure(storage);
      }
      return new ANode[] { left, center, right };
    }

    // can join with left
    if (left != null && left._len + centerLen <= _settings.branchingFactor()) {
      Leaf join = new Leaf(left._len + centerLen, settings);
      new Stitch(join._keys, 0)
        .copyAll(left._keys,  0, left._len)
        .copyAll(centerKeys,  0, centerLen);
      if (_measure != null) {
        join._measure = join.tryComputeMeasure(storage);
      }
      return new ANode[] { null, join, right };
    }

    // can join with right
    if (right != null && centerLen + right.len() <= _settings.branchingFactor()) {
      Leaf join = new Leaf(centerLen + right._len, settings);
      new Stitch(join._keys, 0)
        .copyAll(centerKeys,  0, centerLen)
        .copyAll(right._keys, 0, right._len);
      if (_measure != null) {
        join._measure = join.tryComputeMeasure(storage);
      }
      return new ANode[]{ left, join, null };
    }

    // borrow from left
    if (left != null && (left.editable() || right == null || left._len >= right._len)) {
      int totalLen     = left._len + centerLen,
          newLeftLen   = totalLen >>> 1,
          newCenterLen = totalLen - newLeftLen;

      Leaf newLeft, newCenter;

      // prepend left tail to center
      if (editable() && newCenterLen <= _keys.length) {
        int leftTail = left._len - newLeftLen;
        newCenter = this;
        ArrayUtil.copy(centerKeys,  0, centerLen, _keys, leftTail);
        ArrayUtil.copy(left._keys, newLeftLen, left._len, _keys, 0);
        _len = newCenterLen;
        if (measureOps != null && _measure != null) {
          newCenter._measure = newCenter.tryComputeMeasure(storage);
        }
      } else {
        newCenter = new Leaf(newCenterLen, settings);
        new Stitch(newCenter._keys, 0)
          .copyAll(left._keys,  newLeftLen, left._len)
          .copyAll(centerKeys,  0,          centerLen);
        if (_measure != null) {
          newCenter._measure = newCenter.tryComputeMeasure(storage);
        }
      }

      // shrink left
      if (left.editable()) {
        newLeft  = left;
        left._len = newLeftLen;
        if (measureOps != null && _measure != null) {
          newLeft._measure = newLeft.tryComputeMeasure(storage);
        }
      } else {
        newLeft = new Leaf(newLeftLen, settings);
        ArrayUtil.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);
        if (_measure != null) {
          newLeft._measure = newLeft.tryComputeMeasure(storage);
        }
      }

      return new ANode[]{ newLeft, newCenter, right };
    }

    // borrow from right
    if (right != null) {
      int totalLen     = centerLen + right._len,
          newCenterLen = totalLen >>> 1,
          newRightLen  = totalLen - newCenterLen,
          rightHead    = right._len - newRightLen;

      Leaf newCenter, newRight;

      // append right head to center
      if (editable() && newCenterLen <= _keys.length) {
        newCenter = this;
        ArrayUtil.copy(centerKeys,  0, centerLen, _keys, 0);
        ArrayUtil.copy(right._keys, 0, rightHead, _keys, centerLen);
        _len = newCenterLen;
        if (measureOps != null && _measure != null) {
          newCenter._measure = newCenter.tryComputeMeasure(storage);
        }
      } else {
        newCenter = new Leaf(newCenterLen, settings);
        new Stitch(newCenter._keys, 0)
          .copyAll(centerKeys,  0, centerLen)
          .copyAll(right._keys, 0, rightHead);
        if (_measure != null) {
          newCenter._measure = newCenter.tryComputeMeasure(storage);
        }
      }

      // cut head from right
      if (right.editable()) {
        newRight = right;
        ArrayUtil.copy(right._keys, rightHead, right._len, right._keys, 0);
        right._len = newRightLen;
        if (measureOps != null && _measure != null) {
          newRight._measure = newRight.tryComputeMeasure(storage);
        }
      } else {
        newRight = new Leaf(newRightLen, settings);
        ArrayUtil.copy(right._keys, rightHead, right._len, newRight._keys, 0);
        if (_measure != null) {
          newRight._measure = newRight.tryComputeMeasure(storage);
        }
      }

      return new ANode[]{ left, newCenter, newRight };
    }
    throw new RuntimeException("Unreachable");
  }

  @Override
  public ANode[] replace(IStorage storage, Key oldKey, Key newKey, Comparator<Key> cmp, Settings settings) {
    assert 0 == cmp.compare(oldKey, newKey) : "oldKey and newKey must compare as equal (cmp.compare must return 0)";

    int idx = search(oldKey, cmp);
    if (idx < 0) // not in set
      return PersistentSortedSet.UNCHANGED;

    IMeasure measureOps = settings.measure();

    // Transient: can modify in place
    if (editable()) {
      _keys[idx] = newKey;
      // Recompute stats from final state (after replacement)
      if (measureOps != null && _measure != null) {
        _measure = tryComputeMeasure(storage);
      }
      // If we replaced the last element, maxKey changed
      if (idx == _len - 1) {
        return new ANode[]{this}; // signal maxKey update needed
      }
      return PersistentSortedSet.EARLY_EXIT;
    }

    // Persistent: create new leaf with replaced key
    Leaf n = new Leaf(_len, settings);
    ArrayUtil.copy(_keys, 0, _len, n._keys, 0);
    n._keys[idx] = newKey;
    // Recompute stats from final state (after replacement)
    if (measureOps != null && _measure != null) {
      n._measure = n.tryComputeMeasure(storage);
    }

    // Always return the new node - parent needs to update its child reference
    return new ANode[]{n};
  }

  @Override
  public void walkAddresses(IStorage storage, IFn onAddress) {
    // noop
  }

  @Override
  public Address store(IStorage<Key, Address> storage) {
    return storage.store(this);
  }

  @Override
  public String str(IStorage storage, int lvl) {
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < _len; ++i) {
      if (i > 0) sb.append(" ");
      sb.append(_keys[i].toString());
    }
    return sb.append("}").toString();
  }

  @Override
  public void toString(StringBuilder sb, Address address, String indent) {
    sb.append(indent);
    sb.append("Leaf   addr: " + address + " len: " + _len + " ");
  }

}
