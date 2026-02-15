package org.replikativ.persistent_sorted_set;                                                                                                                                                                                                   

import clojure.lang.*;
import java.util.*;
import java.util.function.*;

@SuppressWarnings("unchecked")
public class PersistentSortedSet<Key, Address> extends APersistentSortedSet<Key, Address>
    implements IEditableCollection,
        ITransientSet,
        Reversible,
        Sorted,
        IReduce,
        IPersistentSortedSet<Key, Address> {

  public static ANode[] EARLY_EXIT = new ANode[0];
  public static ANode[] UNCHANGED = new ANode[0];

  public static final PersistentSortedSet EMPTY = new PersistentSortedSet();

  public Address _address;
  public Object _root; // Object == ANode | SoftReference<ANode> | WeakReference<ANode>
  public int _count;
  public int _version;
  public final Settings _settings;
  public IStorage<Key, Address> _storage;

  public PersistentSortedSet() {
    this(null, RT.DEFAULT_COMPARATOR);
  }

  public PersistentSortedSet(Comparator<Key> cmp) {
    this(null, cmp);
  }

  public PersistentSortedSet(IPersistentMap meta, Comparator<Key> cmp) {
    this(meta, cmp, null, new Settings());
  }

  public PersistentSortedSet(IPersistentMap meta, Comparator<Key> cmp, IStorage<Key, Address> storage, Settings settings) {
    this(meta, cmp, null, storage, new Leaf<Key, Address>(0, settings), 0, settings, 0);
  }

  public PersistentSortedSet(IPersistentMap meta, Comparator<Key> cmp, Address address, IStorage<Key, Address> storage, Object root, int count, Settings settings, int version) {
    super(meta, cmp);
    _address  = address;
    _root     = root;
    _count    = count;
    _version  = version;
    _settings = settings;
    _storage  = storage;
  }

  public ANode<Key, Address> root() {
    assert _address != null || _root != null;
    ANode root = (ANode<Key, Address>) _settings.readReference(_root);
    if (root == null && _address != null) {
      root = _storage.restore(_address);
      _root = _settings.makeReference(root);
    }
    return root;
  }

  private int alterCount(int delta) {
    return _count < 0 ? _count : _count + delta;
  }

  /**
   * Helper to get subtree count from an ANode.
   */
  private static long getSubtreeCount(ANode node) {
    if (node instanceof ISubtreeCount) {
      return ((ISubtreeCount) node).subtreeCount();
    }
    // Fallback - shouldn't happen with proper implementation
    return -1;
  }

  /**
   * Create a Branch node from an array of child nodes (N >= 2).
   * Used when root splits into multiple children.
   */
  private ANode makeBranchFromChildren(ANode[] nodes) {
    int n = nodes.length;
    Object[] keys = new Object[n];
    Object[] children = new Object[n];
    long subtreeCount = 0;
    boolean countKnown = true;
    for (int i = 0; i < n; i++) {
      keys[i] = nodes[i].maxKey();
      children[i] = nodes[i];
      if (countKnown) {
        long c = getSubtreeCount(nodes[i]);
        if (c >= 0) subtreeCount += c;
        else countKnown = false;
      }
    }
    Object measure = computeMeasureFromChildren(nodes);
    return new Branch(nodes[0].level() + 1, n, keys, null, children,
                      countKnown ? subtreeCount : -1, measure, _settings);
  }

  public boolean editable() {
    return _settings.editable();
  }

  public Address address(Address address) {
    _address = address;
    return address;
  }

  // IPersistentSortedSet
  @Override
  public Seq slice(Key from, Key to) {
    return slice(from, to, _cmp);
  }

  @Override
  public Seq slice(Key from, Key to, Comparator<Key> cmp) {
    assert from == null || to == null || cmp.compare(from, to) <= 0 : "From " + from + " after to " + to;
    Seq seq = null;
    ANode node = root();

    if (node.len() == 0) {
      return null;
    }

    if (from == null) {
      while (true) {
        if (node instanceof Branch) {
          seq = new Seq(null, this, seq, node, 0, null, null, true, _version);
          node = seq.child();
        } else {
          seq = new Seq(null, this, seq, node, 0, to, cmp, true, _version);
          return seq.over() ? null : seq;
        }
      }
    }

    while (true) {
      int idx = node.searchFirst(from, cmp);
      if (idx < 0) idx = -idx - 1;
      if (idx == node._len) return null;
      if (node instanceof Branch) {
        seq = new Seq(null, this, seq, node, idx, null, null, true, _version);
        node = seq.child();
      } else {
        seq = new Seq(null, this, seq, node, idx, to, cmp, true, _version);
        return seq.over() ? null : seq;
      }
    }
  }

  public Seq rslice(Key from, Key to) {
    return rslice(from, to, _cmp);
  }

  public Seq rslice(Key from, Key to, Comparator<Key> cmp) {
    assert from == null || to == null || cmp.compare(from, to) >= 0 : "From " + from + " before to " + to;
    Seq seq = null;
    ANode node = root();

    if (node.len() == 0) return null;

    if (from == null) {
      while (true) {
        int idx = node._len - 1;
        if (node instanceof Branch) {
          seq = new Seq(null, this, seq, node, idx, null, null, false, _version);
          node = seq.child();
        } else {
          seq = new Seq(null, this, seq, node, idx, to, cmp, false, _version);
          return seq.over() ? null : seq;
        }
      }
    }

    while (true) {
      if (node instanceof Branch) {
        int idx = node.searchLast(from, cmp) + 1;
        if (idx == node._len) --idx; // last or beyond, clamp to last
        seq = new Seq(null, this, seq, node, idx, null, null, false, _version);
        node = seq.child();
      } else {
        int idx = node.searchLast(from, cmp);
        if (idx == -1) { // not in this, so definitely in prev
          seq = new Seq(null, this, seq, node, 0, to, cmp, false, _version);
          return seq.advance() ? seq : null;
        } else { // exact match
          seq = new Seq(null, this, seq, node, idx, to, cmp, false, _version);
          return seq.over() ? null : seq;
        }
      }
    }
  }

  /**
   * Count elements in the range [from, to] inclusive.
   * Uses O(log n) algorithm when subtree counts are available.
   * If from is null, counts from the beginning.
   * If to is null, counts to the end.
   */
  public long countSlice(Key from, Key to) {
    return countSlice(from, to, _cmp);
  }

  public long countSlice(Key from, Key to, Comparator<Key> cmp) {
    if (from != null && to != null && cmp.compare(from, to) > 0) {
      return 0; // Empty range
    }
    ANode<Key, Address> node = root();
    if (node.len() == 0) {
      return 0;
    }
    return countSliceNode(node, from, to, cmp);
  }

  /**
   * Recursive helper for countSlice.
   */
  private long countSliceNode(ANode<Key, Address> node, Key from, Key to, Comparator<Key> cmp) {
    if (node instanceof Leaf) {
      return countSliceLeaf((Leaf<Key, Address>) node, from, to, cmp);
    }
    return countSliceBranch((Branch<Key, Address>) node, from, to, cmp);
  }

  private long countSliceLeaf(Leaf<Key, Address> leaf, Key from, Key to, Comparator<Key> cmp) {
    int fromIdx = 0;
    int toIdx = leaf._len - 1;

    if (from != null) {
      fromIdx = leaf.searchFirst(from, cmp);
      if (fromIdx >= leaf._len) return 0;
    }

    if (to != null) {
      toIdx = leaf.searchLast(to, cmp);
      if (toIdx < 0) return 0;
    }

    return Math.max(0, toIdx - fromIdx + 1);
  }

  private long countSliceBranch(Branch<Key, Address> branch, Key from, Key to, Comparator<Key> cmp) {
    int fromIdx = 0;
    int toIdx = branch._len - 1;

    // Find the first child that could contain 'from'
    if (from != null) {
      fromIdx = branch.searchFirst(from, cmp);
      if (fromIdx >= branch._len) fromIdx = branch._len - 1;
    }

    // Find the last child that could contain 'to'
    if (to != null) {
      toIdx = branch.searchLast(to, cmp) + 1;
      if (toIdx >= branch._len) toIdx = branch._len - 1;
      if (toIdx < 0) toIdx = 0;
    }

    // If same child, recurse into it
    if (fromIdx == toIdx) {
      return countSliceNode(branch.child(_storage, fromIdx), from, to, cmp);
    }

    long count = 0;

    // Count partial from the first child
    count += countSliceNode(branch.child(_storage, fromIdx), from, null, cmp);

    // Count fully contained children in between
    for (int i = fromIdx + 1; i < toIdx; i++) {
      ANode<Key, Address> child = branch.child(_storage, i);
      if (child instanceof ISubtreeCount) {
        long childCount = ((ISubtreeCount) child).subtreeCount();
        if (childCount >= 0) {
          count += childCount;
          continue;
        }
      }
      count += child.count(_storage);
    }

    // Count partial in the last child
    count += countSliceNode(branch.child(_storage, toIdx), null, to, cmp);

    return count;
  }

  /**
   * Find the key at a given weighted rank.
   * Each key has a weight determined by measureOps.weight(measure).
   * Interior nodes use cached subtree measure for O(log n) navigation.
   *
   * Requires measure with weight() to be configured on the set.
   *
   * @param rank       The target rank (0-based, in terms of total weight)
   * @param outOffset  If non-null, outOffset[0] is set to the local offset
   *                   within the found key (rank remainder after subtraction)
   * @return The key at the given rank, or null if out of bounds
   */
  @SuppressWarnings("unchecked")
  public Key getNth(long rank, long[] outOffset) {
    ANode<Key, Address> node = root();
    if (node.len() == 0) return null;

    IMeasure measureOps = _settings.measure();
    if (measureOps == null) {
      throw new IllegalStateException("getNth requires measure to be configured");
    }

    // Check bounds using root measure
    Object rootMeasure = node._measure;
    if (rootMeasure == null) rootMeasure = node.forceComputeMeasure(_storage);
    long totalWeight = measureOps.weight(rootMeasure);
    if (rank < 0 || rank >= totalWeight) return null;

    // Navigate tree
    while (node instanceof Branch) {
      Branch<Key, Address> branch = (Branch<Key, Address>) node;
      boolean found = false;
      for (int i = 0; i < branch._len; i++) {
        ANode<Key, Address> child = branch.child(_storage, i);
        Object childMeasure = child._measure;
        if (childMeasure == null) childMeasure = child.forceComputeMeasure(_storage);
        long childWeight = measureOps.weight(childMeasure);
        if (rank < childWeight) {
          node = child;
          found = true;
          break;
        }
        rank -= childWeight;
      }
      if (!found) return null; // shouldn't happen if bounds check passed
    }

    // At leaf level — iterate keys
    Leaf<Key, Address> leaf = (Leaf<Key, Address>) node;
    for (int i = 0; i < leaf._len; i++) {
      Object keyMeasure = measureOps.extract(leaf._keys[i]);
      long keyWeight = measureOps.weight(keyMeasure);
      if (rank < keyWeight) {
        if (outOffset != null) outOffset[0] = rank;
        return leaf._keys[i];
      }
      rank -= keyWeight;
    }
    return null; // shouldn't happen
  }

  public void walkAddresses(IFn onAddress) {
    if (_address != null) {
      if (!RT.booleanCast(onAddress.invoke(_address))) {
        return;
      }
    }
    root().walkAddresses(_storage, onAddress);
  }

  public Address store() {
    assert _storage != null;

    if (_address == null) {
      ANode<Key, Address> root = (ANode) _settings.readReference(_root);
      address(root.store(_storage));
      _root = _settings.makeReference(root);
    }

    return _address;
  }

  public Address store(IStorage<Key, Address> storage) {
    _storage = storage;
    return store();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("#{");
    for (Object o: this) {
      sb.append(o).append(" ");
    }
    if (sb.charAt(sb.length() - 1) == " ".charAt(0)) {
      sb.delete(sb.length() - 1, sb.length());
    }
    sb.append("}");
    return sb.toString();
  }

  public String str() {
    return root().str(_storage, 0);
  }

  // IObj
  public PersistentSortedSet withMeta(IPersistentMap meta) {
    if (_meta == meta) {
      return this;
    }
    return new PersistentSortedSet(meta, _cmp, _address, _storage, _root, _count, _settings, _version);
  }

  // Counted
  public int count() {
    if (_count < 0) _count = root().count(_storage);
    // assert _count == _root.count(_storage) : _count + " != " + _root.count(_storage);
    return _count;
  }

  // Sorted
  public Comparator comparator() {
    return _cmp;
  }

  public Object entryKey(Object entry) {
    return entry;
  }

  // IReduce
  public Object reduce(IFn f) {
    Seq seq = (Seq) seq();
    return seq == null ? f.invoke() : seq.reduce(f);
  }

  public Object reduce(IFn f, Object start) {
    Seq seq = (Seq) seq();
    return seq == null ? start : seq.reduce(f, start);
  }

  // IPersistentCollection
  public PersistentSortedSet empty() {
    return new PersistentSortedSet(_meta, _cmp, _storage, _settings);
  }

  public PersistentSortedSet cons(Object key) {
    return cons(key, _cmp);
  }

  /**
   * Helper to compute measure from an array of child nodes.
   */
  private Object computeMeasureFromChildren(ANode[] children) {
    IMeasure measureOps = _settings.measure();
    if (measureOps == null) return null;
    Object result = measureOps.identity();
    for (ANode child : children) {
      Object childMeasure = child.measure();
      if (childMeasure == null) {
        childMeasure = child.forceComputeMeasure(_storage);
      }
      if (childMeasure != null) {
        result = measureOps.merge(result, childMeasure);
      }
    }
    return result;
  }

  public PersistentSortedSet cons(Object key, Comparator cmp) {
    ANode[] nodes = root().add(_storage, (Key) key, cmp, _settings);

    if (UNCHANGED == nodes) return this;

    // Mark old root address as freed if it exists (works in both persistent and transient modes)
    if (_storage != null && _address != null) {
      _storage.markFreed(_address);
    }

    if (editable()) {
      // Clear address - must always clear when tree is modified (including EARLY_EXIT case)
      _address = null;

      if (1 == nodes.length) {
        _root = nodes[0];
      } else if (nodes.length >= 2) {
        _root = makeBranchFromChildren(nodes);
      }
      // EARLY_EXIT case (nodes.length == 0): tree was modified in place, _address already cleared above
      // When processor is configured, count may differ from +1
      if (_settings.leafProcessor() != null) {
        long rootCount = getSubtreeCount(root());
        _count = (rootCount >= 0) ? (int) rootCount : -1;
      } else {
        _count = alterCount(1);
      }
      _version += 1;
      return this;
    }

    ANode newRoot;
    if (1 == nodes.length) {
      newRoot = nodes[0];
    } else {
      newRoot = makeBranchFromChildren(nodes);
    }

    // Use root's subtreeCount for exact count (works correctly even with processor)
    long rootCount = getSubtreeCount(newRoot);
    int newCount = (rootCount >= 0) ? (int) rootCount : -1;
    return new PersistentSortedSet(_meta, _cmp, null, _storage, newRoot, newCount, _settings, _version + 1);
  }

  // IPersistentSet
  public PersistentSortedSet disjoin(Object key) {
    return disjoin(key, _cmp);
  }

  public PersistentSortedSet disjoin(Object key, Comparator cmp) {
    ANode[] nodes = root().remove(_storage, (Key) key, null, null, cmp, _settings);

    // not in set
    if (UNCHANGED == nodes) return this;

    // Mark old root address as freed if it exists (works in both persistent and transient modes)
    if (_storage != null && _address != null) {
      _storage.markFreed(_address);
    }

    // in place update
    if (nodes == EARLY_EXIT) {
      // Clear address
      _address = null;
      _count = alterCount(-1);
      _version += 1;
      return this;
    }

    ANode newRoot = nodes[1];
    if (editable()) {
      if (newRoot instanceof Branch && newRoot._len == 1)
        newRoot = ((Branch) newRoot).child(_storage, 0);
      // Clear address
      _address = null;
      _root = newRoot;
      // When processor is configured, count may differ from -1
      if (_settings.leafProcessor() != null) {
        long rootCount = getSubtreeCount(newRoot);
        _count = (rootCount >= 0) ? (int) rootCount : -1;
      } else {
        _count = alterCount(-1);
      }
      _version += 1;
      return this;
    }
    if (newRoot instanceof Branch && newRoot._len == 1) {
      newRoot = ((Branch) newRoot).child(_storage, 0);
    }
    // Use root's subtreeCount for exact count (works correctly even with processor)
    long rootCount = getSubtreeCount(newRoot);
    int newCount = (rootCount >= 0) ? (int) rootCount : -1;
    return new PersistentSortedSet(_meta, _cmp, null, _storage, newRoot, newCount, _settings, _version + 1);
  }

  /**
   * Replace an existing key with a new key at the same logical position.
   * The comparator must return 0 for both oldKey and newKey.
   * This is a single-traversal update - much faster than disjoin + cons.
   *
   * @param oldKey The key to find and replace
   * @param newKey The replacement key (must compare equal to oldKey)
   * @return Updated set, or this if oldKey not found
   */
  public PersistentSortedSet replace(Object oldKey, Object newKey) {
    return replace(oldKey, newKey, _cmp);
  }

  public PersistentSortedSet replace(Object oldKey, Object newKey, Comparator cmp) {
    ANode[] nodes = root().replace(_storage, (Key) oldKey, (Key) newKey, cmp, _settings);

    // Not in set
    if (UNCHANGED == nodes) return this;

    // Mark old root address as freed if it exists (works in both persistent and transient modes)
    if (_storage != null && _address != null) {
      _storage.markFreed(_address);
    }

    // In-place update (transient)
    if (EARLY_EXIT == nodes) {
      // Clear address
      _address = null;
      _version += 1;
      return this;
    }

    // New root node (persistent case or maxKey changed in transient)
    ANode newRoot = nodes[0];
    if (editable()) {
      // Clear address
      _address = null;
      _root = newRoot;
      _version += 1;
      return this;
    }

    return new PersistentSortedSet(_meta, _cmp, null, _storage, newRoot, _count, _settings, _version + 1);
  }

  public boolean contains(Object key) {
    return root().contains(_storage, (Key) key, _cmp);
  }

  /**
   * Look up a key and return the actual stored element.
   * Unlike get/valAt which return the search key, this returns the
   * stored element - useful when using custom comparators that only
   * compare part of the key (e.g., [id value] tuples compared by id).
   *
   * O(log n) traversal with no allocations (unlike slice).
   *
   * @param key The key to search for
   * @return The stored element, or null if not found
   */
  public Key lookup(Object key) {
    return lookup(key, _cmp);
  }

  /**
   * Look up a key with custom comparator and return the actual stored element.
   */
  public Key lookup(Object key, Comparator<Key> cmp) {
    ANode<Key, Address> node = root();

    if (node.len() == 0) {
      return null;
    }

    while (true) {
      int idx = node.searchFirst((Key) key, cmp);
      if (idx >= node._len) {
        return null;
      }

      if (node instanceof Branch) {
        // For Branch nodes: _keys[idx] is the max key of child subtree.
        // We descend if max key >= search key (which searchFirst guarantees).
        // Don't check for exact match here - the actual key is in a descendant.
        node = ((Branch<Key, Address>) node).child(_storage, idx);
      } else {
        // Leaf node - check for exact match and return
        if (cmp.compare(node._keys[idx], (Key) key) != 0) {
          return null;
        }
        return node._keys[idx];
      }
    }
  }

  // IEditableCollection
  public PersistentSortedSet asTransient() {
    if (editable()) {
      throw new IllegalStateException("Expected persistent set");
    }
    return new PersistentSortedSet(_meta, _cmp, _address, _storage, _root, _count, _settings.editable(true), _version);
  }

  // ITransientCollection
  public PersistentSortedSet conj(Object key) {
    return cons(key, _cmp);
  }

  public PersistentSortedSet persistent() {
    if (!editable()) {
      throw new IllegalStateException("Expected transient set");
    }
    _settings.persistent();
    return this;
  }

  // Iterable
  public Iterator iterator() {
    return new JavaIter((Seq) seq());
  }
}