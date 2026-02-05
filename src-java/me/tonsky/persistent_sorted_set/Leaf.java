package me.tonsky.persistent_sorted_set;

import java.util.*;
import java.util.function.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public class Leaf<Key, Address> extends ANode<Key, Address> implements ISubtreeCount {
  public Leaf(int len, Key[] keys, Settings settings) {
    super(len, keys, settings);
  }

  public Leaf(int len, Key[] keys, Object stats, Settings settings) {
    super(len, keys, stats, settings);
  }

  public Leaf(int len, Settings settings) {
    super(len, (Key[]) new Object[ANode.newLen(len, settings)], settings);
  }

  public Leaf(List<Key> keys, Settings settings) {
    this(keys.size(), (Key[]) keys.toArray(), settings);
  }

  @Override
  public int level() {
    return 0;
  }

  @Override
  public Object computeStats(IStorage storage) {
    IStats statsOps = _settings.stats();
    if (statsOps == null) return null;

    Object result = statsOps.identity();
    for (int i = 0; i < _len; i++) {
      result = statsOps.merge(result, statsOps.extract(_keys[i]));
    }
    return result;
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

    IStats statsOps = _settings.stats();

    // can modify array in place
    if (editable() && _len < _keys.length) {
      if (ins == _len) {
        _keys[_len] = key;
        _len += 1;
        // Update stats incrementally
        if (statsOps != null) {
          Object prevStats = _stats != null ? _stats : statsOps.identity();
          _stats = statsOps.merge(prevStats, statsOps.extract(key));
        }
        return new ANode[]{this}; // maxKey needs updating
      } else {
        ArrayUtil.copy(_keys, ins, _len, _keys, ins+1);
        _keys[ins] = key;
        _len += 1;
        // Update stats incrementally
        if (statsOps != null) {
          Object prevStats = _stats != null ? _stats : statsOps.identity();
          _stats = statsOps.merge(prevStats, statsOps.extract(key));
        }
        return PersistentSortedSet.EARLY_EXIT;
      }
    }

    // simply adding to array
    if (_len < _settings.branchingFactor()) {
      Leaf n = new Leaf(_len + 1, settings);
      new Stitch(n._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(key)
        .copyAll(_keys, ins, _len);
      n._stats = n.computeStats(storage);
      return new ANode[]{n};
    }

    // splitting
    int half1 = (_len + 1) >>> 1,
        half2 = _len + 1 - half1;

    // goes to first half
    if (ins < half1) {
      Leaf n1 = new Leaf(half1, settings),
           n2 = new Leaf(half2, settings);
      new Stitch(n1._keys, 0)
        .copyAll(_keys, 0, ins)
        .copyOne(key)
        .copyAll(_keys, ins, half1 - 1);
      ArrayUtil.copy(_keys, half1 - 1, _len, n2._keys, 0);
      n1._stats = n1.computeStats(storage);
      n2._stats = n2.computeStats(storage);
      return new ANode[]{n1, n2};
    }

    // copy first, insert to second
    Leaf n1 = new Leaf(half1, settings),
         n2 = new Leaf(half2, settings);
    ArrayUtil.copy(_keys, 0, half1, n1._keys, 0);
    new Stitch(n2._keys, 0)
      .copyAll(_keys, half1, ins)
      .copyOne(key)
      .copyAll(_keys, ins, _len);
    n1._stats = n1.computeStats(storage);
    n2._stats = n2.computeStats(storage);
    return new ANode[]{n1, n2};
  }

  @Override
  public ANode[] remove(IStorage storage, Key key, ANode _left, ANode _right, Comparator<Key> cmp, Settings settings) {
    Leaf left = (Leaf) _left;
    Leaf right = (Leaf) _right;

    int idx = search(key, cmp);
    if (idx < 0) // not in set
      return PersistentSortedSet.UNCHANGED;

    int newLen = _len - 1;
    IStats statsOps = _settings.stats();
    final Leaf thisLeaf = this;

    // nothing to merge
    if (newLen >= _settings.minBranchingFactor() || (left == null && right == null)) {

      // transient, can edit in place
      if (editable()) {
        ArrayUtil.copy(_keys, idx + 1, _len, _keys, idx);
        _len = newLen;
        // Update stats using remove operation
        if (statsOps != null) {
          if (_stats != null) {
            _stats = statsOps.remove(_stats, key, () -> thisLeaf.computeStats(storage));
          } else {
            // Stats were never initialized, compute from scratch
            _stats = thisLeaf.computeStats(storage);
          }
        }
        if (idx == newLen) // removed last, need to signal new maxKey
          return new ANode[]{left, this, right};
        return PersistentSortedSet.EARLY_EXIT;
      }

      // persistent
      Leaf center = new Leaf(newLen, settings);
      new Stitch(center._keys, 0)
        .copyAll(_keys, 0, idx)
        .copyAll(_keys, idx + 1, _len);
      center._stats = center.computeStats(storage);
      return new ANode[] { left, center, right };
    }

    // can join with left
    if (left != null && left._len + newLen <= _settings.branchingFactor()) {
      Leaf join = new Leaf(left._len + newLen, settings);
      new Stitch(join._keys, 0)
        .copyAll(left._keys, 0,       left._len)
        .copyAll(_keys,      0,       idx)
        .copyAll(_keys,      idx + 1, _len);
      join._stats = join.computeStats(storage);
      return new ANode[] { null, join, right };
    }

    // can join with right
    if (right != null && newLen + right.len() <= _settings.branchingFactor()) {
      Leaf join = new Leaf(newLen + right._len, settings);
      new Stitch(join._keys, 0)
        .copyAll(_keys,       0,       idx)
        .copyAll(_keys,       idx + 1, _len)
        .copyAll(right._keys, 0,       right._len);
      join._stats = join.computeStats(storage);
      return new ANode[]{ left, join, null };
    }

    // borrow from left
    if (left != null && (left.editable() || right == null || left._len >= right._len)) {
      int totalLen     = left._len + newLen,
          newLeftLen   = totalLen >>> 1,
          newCenterLen = totalLen - newLeftLen,
          leftTail     = left._len - newLeftLen;

      Leaf newLeft, newCenter;

      // prepend to center
      if (editable() && newCenterLen <= _keys.length) {
        newCenter = this;
        ArrayUtil.copy(_keys,      idx + 1,    _len,     _keys, leftTail + idx);
        ArrayUtil.copy(_keys,      0,          idx,      _keys, leftTail);
        ArrayUtil.copy(left._keys, newLeftLen, left._len, _keys, 0);
        _len = newCenterLen;
        // Recompute stats since we borrowed from left
        if (statsOps != null) {
          newCenter._stats = newCenter.computeStats(storage);
        }
      } else {
        newCenter = new Leaf(newCenterLen, settings);
        new Stitch(newCenter._keys, 0)
          .copyAll(left._keys, newLeftLen, left._len)
          .copyAll(_keys,      0,          idx)
          .copyAll(_keys,      idx+1,      _len);
        newCenter._stats = newCenter.computeStats(storage);
      }

      // shrink left
      if (left.editable()) {
        newLeft  = left;
        left._len = newLeftLen;
        // Recompute stats for shrunk left
        if (statsOps != null) {
          newLeft._stats = newLeft.computeStats(storage);
        }
      } else {
        newLeft = new Leaf(newLeftLen, settings);
        ArrayUtil.copy(left._keys, 0, newLeftLen, newLeft._keys, 0);
        newLeft._stats = newLeft.computeStats(storage);
      }

      return new ANode[]{ newLeft, newCenter, right };
    }

    // borrow from right
    if (right != null) {
      int totalLen     = newLen + right._len,
          newCenterLen = totalLen >>> 1,
          newRightLen  = totalLen - newCenterLen,
          rightHead    = right._len - newRightLen;

      Leaf newCenter, newRight;

      // append to center
      if (editable() && newCenterLen <= _keys.length) {
        newCenter = this;
        new Stitch(_keys, idx)
          .copyAll(_keys,       idx + 1, _len)
          .copyAll(right._keys, 0,       rightHead);
        _len = newCenterLen;
        // Recompute stats since we borrowed from right
        if (statsOps != null) {
          newCenter._stats = newCenter.computeStats(storage);
        }
      } else {
        newCenter = new Leaf(newCenterLen, settings);
        new Stitch(newCenter._keys, 0)
          .copyAll(_keys,       0,       idx)
          .copyAll(_keys,       idx + 1, _len)
          .copyAll(right._keys, 0,       rightHead);
        newCenter._stats = newCenter.computeStats(storage);
      }

      // cut head from right
      if (right.editable()) {
        newRight = right;
        ArrayUtil.copy(right._keys, rightHead, right._len, right._keys, 0);
        right._len = newRightLen;
        // Recompute stats for shrunk right
        if (statsOps != null) {
          newRight._stats = newRight.computeStats(storage);
        }
      } else {
        newRight = new Leaf(newRightLen, settings);
        ArrayUtil.copy(right._keys, rightHead, right._len, newRight._keys, 0);
        newRight._stats = newRight.computeStats(storage);
      }

      return new ANode[]{ left, newCenter, newRight };
    }
    throw new RuntimeException("Unreachable");
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
