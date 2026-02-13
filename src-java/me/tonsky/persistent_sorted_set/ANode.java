package me.tonsky.persistent_sorted_set;

import java.util.*;
import java.util.function.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public abstract class ANode<Key, Address> {
  // >= 0
  public int _len;

  // NotNull
  // Only valid [0 ... _len-1]
  public final Key[] _keys;

  public final Settings _settings;

  // Optional measure for the subtree rooted at this node.
  // Null if measure is not configured in settings.
  public Object _measure;

  public ANode(int len, Key[] keys, Settings settings) {
    assert keys.length >= len;

    _len   = len;
    _keys  = keys;
    _settings  = settings;
    _measure = null;
  }

  public ANode(int len, Key[] keys, Object measure, Settings settings) {
    assert keys.length >= len;

    _len   = len;
    _keys  = keys;
    _settings  = settings;
    _measure = measure;
  }

  /**
   * Get the measure for this node. May be null if measure not configured.
   */
  public Object measure() {
    return _measure;
  }

  /**
   * Try to compute measure from this node's keys (for Leaf) or children's measures (for Branch).
   * Returns null if measure not configured in settings OR if any child has unavailable measure.
   * Used during operations (add/remove/replace) to avoid O(n) traversals.
   */
  public abstract Object tryComputeMeasure(IStorage storage);

  /**
   * Force compute measure from this node, recursively descending into children if needed.
   * Returns null if measure not configured in settings.
   * Used for explicit user queries where O(n) traversal is acceptable.
   */
  public abstract Object forceComputeMeasure(IStorage storage);

  public int len() {
    return _len;
  }

  public Key minKey() {
    return _keys[0];
  }

  public Key maxKey() {
    return _keys[_len - 1];
  }

  public List<Key> keys() {
    if (_keys.length == _len) {
      return Arrays.asList(_keys);
    } else {
      return Arrays.asList(Arrays.copyOfRange(_keys, 0, _len));
    }
  }

  public boolean editable() {
    return _settings.editable();
  }

  public int search(Key key, Comparator<Key> cmp) {
    return Arrays.binarySearch(_keys, 0, _len, key, cmp);

    // int low = 0, high = _len;
    // while (high - low > 16) {
    //   int mid = (high + low) >>> 1;
    //   int d = cmp.compare(_keys[mid], key);
    //   if (d == 0) return mid;
    //   else if (d > 0) high = mid;
    //   else low = mid;
    // }

    // // linear search
    // for (int i = low; i < high; ++i) {
    //   int d = cmp.compare(_keys[i], key);
    //   if (d == 0) return i;
    //   else if (d > 0) return -i - 1; // i
    // }

    // return -high - 1; // high
  }

  public int searchFirst(Key key, Comparator<Key> cmp) {
    int low = 0, high = _len;
    while (low < high) {
      int mid = (high + low) >>> 1;
      int d = cmp.compare(_keys[mid], key);
      if (d < 0)
        low = mid + 1;
      else
        high = mid;
    }
    return low;
  }

  public int searchLast(Key key, Comparator<Key> cmp) {
    int low = 0, high = _len;
    while (low < high) {
      int mid = (high + low) >>> 1;
      int d = cmp.compare(_keys[mid], key);
      if (d <= 0)
        low = mid + 1;
      else
        high = mid;
    }
    return low - 1;
  }

  public static <Key, Address> ANode restore(int level, List<Key> keys, List<Address> addresses, Settings settings) {
    if (level == 0 || addresses == null) {
      return new Leaf(keys, settings);
    } else {
      return new Branch(level, keys, addresses, settings);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    toString(sb, null, "");
    return sb.toString();
  }

  public abstract int count(IStorage storage);
  // 0 for Leafs, 1+ for Branches
  public abstract int level();
  public abstract boolean contains(IStorage storage, Key key, Comparator<Key> cmp);
  public abstract ANode[] add(IStorage storage, Key key, Comparator<Key> cmp, Settings settings);
  public abstract ANode[] remove(IStorage storage, Key key, ANode left, ANode right, Comparator<Key> cmp, Settings settings);

  /**
   * Replace an existing key with a new key at the same position.
   * The comparator must return 0 for both oldKey and newKey (same logical position).
   * This is a single-traversal update - much faster than remove + add.
   *
   * @return UNCHANGED if oldKey not found, EARLY_EXIT if replaced without maxKey change,
   *         or array with updated node(s) if maxKey changed
   */
  public abstract ANode[] replace(IStorage storage, Key oldKey, Key newKey, Comparator<Key> cmp, Settings settings);
  public abstract String str(IStorage storage, int lvl);
  public abstract void walkAddresses(IStorage storage, IFn onAddress);
  public abstract Address store(IStorage<Key, Address> storage);
  public abstract void toString(StringBuilder sb, Address address, String indent);

  protected static int newLen(int len, Settings settings) {
    if (settings.editable())
        return Math.min(settings.branchingFactor(), len + settings.expandLen());
    else
        return len;
  }

  protected static int safeLen(ANode node) {
    return node == null ? -1 : node._len;
  }
}
