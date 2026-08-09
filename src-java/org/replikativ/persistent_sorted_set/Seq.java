package org.replikativ.persistent_sorted_set;

import java.util.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public class Seq extends ASeq implements IReduce, Reversible, IChunkedSeq, ISeek{
  final PersistentSortedSet _set;
  Seq   _parent;
  ANode _node;
  int   _idx;
  final Object _keyTo;
  final Comparator _cmp;
  final boolean _asc;
  final int _version;
  // True when the entire current leaf is within bounds — skip over() check
  boolean _leafSafe;

  Seq(IPersistentMap meta, PersistentSortedSet set, Seq parent, ANode node, int idx, Object keyTo, Comparator cmp, boolean asc, int version) {
    super(meta);
    _set = set;
    _parent = parent;
    _node   = node;
    _idx    = idx;
    _keyTo  = keyTo;
    _cmp    = cmp;
    _asc    = asc;
    _version = version;
    _leafSafe = computeLeafSafe();
  }

  // Check if all remaining elements in the current leaf are within bounds.
  // Avoids per-element comparator calls during within-leaf iteration.
  boolean computeLeafSafe() {
    if (_keyTo == null || _cmp == null) return true;
    if (_asc) {
      // All elements up to leaf end are valid if maxKey <= _keyTo
      return _cmp.compare(_node.maxKey(), _keyTo) <= 0;
    } else {
      // All elements down to leaf start are valid if minKey >= _keyTo
      return _cmp.compare(_node.minKey(), _keyTo) >= 0;
    }
  }

  void checkVersion() {
    if (_version != _set._version)
      throw new RuntimeException("Tovarisch, you are iterating and mutating a transient set at the same time!");
  }

  ANode child() {
    assert _node instanceof Branch : _node;
    return ((Branch) _node).child(_set._storage, _idx);
  }

  boolean over() {
    if (_keyTo == null) return false;
    int d = _cmp.compare(first(), _keyTo);
    return _asc ? d > 0 : d < 0;
  }

  boolean advance() {
    checkVersion();
    if (_asc) {
      if (_idx < _node._len - 1) {
        _idx++;
        return _leafSafe || !over();
      } else if (_parent != null) {
        _parent = _parent.next();
        if (_parent != null) {
          _node = _parent.child();
          _idx = 0;
          _leafSafe = computeLeafSafe();
          return _leafSafe || !over();
        }
      }
    } else { // !_asc
      if (_idx > 0) {
        _idx--;
        return _leafSafe || !over();
      } else if (_parent != null) {
        _parent = _parent.next();
        if (_parent != null) {
          _node = _parent.child();
          _idx = _node._len - 1;
          _leafSafe = computeLeafSafe();
          return _leafSafe || !over();
        }
      }
    }
    return false;
  }

  protected Seq clone() {
    return new Seq(meta(), _set, _parent, _node, _idx, _keyTo, _cmp, _asc, _version);
  }

  // ASeq
  public Object first() {
    checkVersion();
    // assert _node.leaf();
    return _node._keys[_idx];
  }

  public Seq next() {
    Seq next = clone();
    return next.advance() ? next : null;
  }

  public Obj withMeta(IPersistentMap meta) {
    if (meta() == meta) return this;
    return new Seq(meta, _set, _parent, _node, _idx, _keyTo, _cmp, _asc, _version);
  }

  // IReduce
  public Object reduce(IFn f) {
    checkVersion();
    Seq clone = clone();
    Object ret = clone.first();
    while (clone.advance()) {
      ret = f.invoke(ret, clone.first());
      if (ret instanceof Reduced)
        return ((Reduced) ret).deref();
    }
    return ret;
  }

  public Object reduce(IFn f, Object start) {
    checkVersion();
    Seq clone = clone();
    Object ret = start;
    do {
      ret = f.invoke(ret, clone.first());
      if (ret instanceof Reduced)
        return ((Reduced) ret).deref();
    } while (clone.advance());
    return ret;
  }

  // Iterable
  public Iterator iterator() { checkVersion(); return new JavaIter(clone()); }

  // IChunkedSeq
  public Chunk chunkedFirst() { checkVersion(); return new Chunk(this); }

  public Seq chunkedNext() {
    checkVersion();
    if (_parent == null) return null;
    Seq nextParent = _parent.next();
    if (nextParent == null) return null;
    ANode node = nextParent.child();
    Seq seq = new Seq(meta(), _set, nextParent, node, _asc ? 0 : node._len - 1, _keyTo, _cmp, _asc, _version);
    return seq.over() ? null : seq;
  }

  public ISeq chunkedMore() {
    Seq seq = chunkedNext();
    if (seq == null) return PersistentList.EMPTY;
    return seq;
  }

  // Reversible
  boolean atBeginning() {
    return _idx == 0 && (_parent == null || _parent.atBeginning());
  }

  boolean atEnd() {
    return _idx == _node._len-1 && (_parent == null || _parent.atEnd());
  }

  public Seq rseq() {
    checkVersion();
    if (_asc)
      return _set.rslice(_keyTo, atBeginning() ? null : first(), _cmp);
    else
      return _set.slice(_keyTo, atEnd() ? null : first(), _cmp);
  }

  public Seq seek(Object to) { return seek(to, _cmp); }
  public Seq seek(Object to, Comparator cmp) {
    if (to == null) throw new RuntimeException("seek can't be called with a nil key!");

    Seq seq = this._parent;
    ANode node = this._node;

    if (_asc) {

      // Climb while `to` lies outside this subtree — on EITHER side.
      //
      // This used to test only `maxKey(node) < to`, i.e. only the forward direction. A `to`
      // BELOW the current position therefore never climbed, and `searchFirst` below re-ran on
      // the current LEAF, which answered with that leaf's first element. The result was wrong
      // in two directions at once — measured on `(apply sorted-set (range 10000))`, seeking to
      // 5000 and then back to 2500:
      //
      //     5008 elements, first 4992      (documented: 7500 elements, first 2500)
      //
      // so 2500..4991 were silently missing AND 4992..4999 were re-emitted below the position
      // already consumed. The rewind distance is the leaf boundary, hence shape-dependent,
      // which is why the (all-forward) seek tests never saw it.
      //
      // Forward seeks are unaffected: `to` is then above the current position, so it is never
      // below the current node's minKey and the added disjunct is false. `||` short-circuits,
      // so a forward climb evaluates the second comparison exactly once, at the level where it
      // stops.
      while (node != null && seq != null
             && (cmp.compare(node.maxKey(), to) < 0 || cmp.compare(to, node.minKey()) < 0)) {
        node = seq._node;
        seq = seq._parent;
      }
      // Past the end of the whole tree: nothing is at or after `to`.
      if (node == null || cmp.compare(node.maxKey(), to) < 0) return null;

      while (true) {
        int idx = node.searchFirst(to, cmp);
        if (idx < 0)
          idx = -idx - 1;
        if (idx == node._len)
          return null;
        if (node instanceof Branch) {
          seq = new Seq(null, this._set, seq, node, idx, null, null, true, _version);
          node = seq.child();
        } else { // Leaf
          seq = new Seq(null, this._set, seq, node, idx, this._keyTo, cmp, true, _version);
          return seq.over() ? null : seq;
        }
      }

    } else {

      // Mirror of the ascending climb above: leave the subtree when `to` is outside it on
      // EITHER side. Testing only `to < minKey(node)` covered only the descending direction of
      // travel, so seeking BACK UP a descending iterator clamped inside the current subtree.
      // Measured on `(rslice (apply sorted-set (range 10000)) 9999 nil)`, seek 5000 then 7500:
      //
      //     first 5375, 5376 elements      (documented: first 7500, 7501 elements)
      //
      // A `to` above everything is still handled by the `clamp to last` line below, and a `to`
      // below everything by the leaf arm's `advance()`, which returns null when it cannot.
      while (node != null && seq != null
             && (cmp.compare(to, node.minKey()) < 0 || cmp.compare(node.maxKey(), to) < 0)) {
        node = seq._node;
        seq = seq._parent;
      }

      while (true) {
        if (node instanceof Branch) {
          int idx = node.searchLast(to, cmp) + 1;
          if (idx == node._len) --idx; // last or beyond, clamp to last
          seq = new Seq(null, this._set, seq, node, idx, null, null, false, _version);
          node = seq.child();
        } else { // Leaf
          int idx = node.searchLast(to, cmp);
          if (idx == -1) { // not in this, so definitely in prev
            seq = new Seq(null, this._set, seq, node, 0, this._keyTo, cmp, false, _version);
            return seq.advance() ? seq : null;
          } else { // exact match
            seq = new Seq(null, this._set, seq, node, idx, this._keyTo, cmp, false, _version);
            return seq.over() ? null : seq;
          }
        }
      }
    }
  }
}
