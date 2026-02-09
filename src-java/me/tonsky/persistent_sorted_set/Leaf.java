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
      return processLeafNodes(new ANode[]{n}, storage, settings);
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
      return processLeafNodes(new ANode[]{n1, n2}, storage, settings);
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
    return processLeafNodes(new ANode[]{n1, n2}, storage, settings);
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

      // Process the center leaf (may split into multiple)
      Leaf[] processed = processSingleLeaf(center, storage, settings);
      if (processed.length == 1) {
        return new ANode[] { left, processed[0], right };
      } else if (processed.length > 1) {
        // Split into multiple - return all (parent will handle re-organization)
        return (ANode[]) processed;
      } else {
        // Empty - removed all entries
        return new ANode[] { left, null, right };
      }
    }

    // can join with left
    if (left != null && left._len + newLen <= _settings.branchingFactor()) {
      Leaf join = new Leaf(left._len + newLen, settings);
      new Stitch(join._keys, 0)
        .copyAll(left._keys, 0,       left._len)
        .copyAll(_keys,      0,       idx)
        .copyAll(_keys,      idx + 1, _len);
      join._stats = join.computeStats(storage);

      // Process the joined leaf
      Leaf[] processed = processSingleLeaf(join, storage, settings);
      if (processed.length == 1) {
        return new ANode[] { null, processed[0], right };
      } else if (processed.length > 1) {
        // Split into multiple
        return (ANode[]) processed;
      } else {
        return new ANode[] { null, null, right };
      }
    }

    // can join with right
    if (right != null && newLen + right.len() <= _settings.branchingFactor()) {
      Leaf join = new Leaf(newLen + right._len, settings);
      new Stitch(join._keys, 0)
        .copyAll(_keys,       0,       idx)
        .copyAll(_keys,       idx + 1, _len)
        .copyAll(right._keys, 0,       right._len);
      join._stats = join.computeStats(storage);

      // Process the joined leaf
      Leaf[] processed = processSingleLeaf(join, storage, settings);
      if (processed.length == 1) {
        return new ANode[]{ left, processed[0], null };
      } else if (processed.length > 1) {
        // Split into multiple
        return (ANode[]) processed;
      } else {
        return new ANode[]{ left, null, null };
      }
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
  public ANode[] replace(IStorage storage, Key oldKey, Key newKey, Comparator<Key> cmp, Settings settings) {
    assert 0 == cmp.compare(oldKey, newKey) : "oldKey and newKey must compare as equal (cmp.compare must return 0)";

    int idx = search(oldKey, cmp);
    if (idx < 0) // not in set
      return PersistentSortedSet.UNCHANGED;

    IStats statsOps = settings.stats();

    // Transient: can modify in place
    if (editable()) {
      _keys[idx] = newKey;
      // Eagerly update stats: remove old key, add new key
      if (statsOps != null && _stats != null) {
        Object removed = statsOps.remove(_stats, oldKey, () -> computeStats(storage));
        _stats = statsOps.merge(removed, statsOps.extract(newKey));
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
    // Eagerly compute stats for new leaf
    if (statsOps != null && _stats != null) {
      Object removed = statsOps.remove(_stats, oldKey, () -> n.computeStats(storage));
      n._stats = statsOps.merge(removed, statsOps.extract(newKey));
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

  /**
   * Post-process a single leaf through ILeafProcessor if configured.
   *
   * Used for remove operations where we only want to process the center node.
   * Zero overhead if no processor is configured.
   *
   * @param leaf The leaf to process
   * @param storage The storage backend
   * @param settings The settings (contains processor)
   * @return Processed leaf (may be split into multiple)
   */
  @SuppressWarnings("unchecked")
  private static <Key, Address> Leaf[] processSingleLeaf(
      Leaf<Key, Address> leaf,
      IStorage storage,
      Settings settings) {

    ILeafProcessor processor = settings.leafProcessor();

    // Zero-cost early exit if no processor configured
    if (processor == null) {
      return new Leaf[]{ leaf };
    }

    // Check if processing is needed (avoid allocation if not)
    if (!processor.shouldProcess(leaf._len, settings)) {
      return new Leaf[]{ leaf };
    }

    // Convert leaf keys to list for processing
    List<Key> entries = Arrays.asList(Arrays.copyOfRange(leaf._keys, 0, leaf._len));

    // Call processor
    List<Key> processed = processor.processLeaf(entries, storage, settings);

    // If same size and same content, no changes needed
    if (processed.size() == leaf._len && processed.equals(entries)) {
      return new Leaf[]{ leaf };
    }

    // Rebuild leaf(s) from processed entries
    int processedSize = processed.size();

    if (processedSize == 0) {
      // All entries removed
      return new Leaf[0];
    } else if (processedSize <= settings.branchingFactor()) {
      // Fits in single leaf
      Leaf<Key, Address> newLeaf = new Leaf<>(processedSize, settings);
      for (int i = 0; i < processedSize; i++) {
        newLeaf._keys[i] = processed.get(i);
      }
      newLeaf._stats = newLeaf.computeStats(storage);
      return new Leaf[]{ newLeaf };
    } else {
      // Needs to split into multiple leaves
      int numLeaves = (processedSize + settings.branchingFactor() - 1) / settings.branchingFactor();
      int baseSize = processedSize / numLeaves;
      int remainder = processedSize % numLeaves;

      Leaf[] result = new Leaf[numLeaves];
      int offset = 0;
      for (int i = 0; i < numLeaves; i++) {
        int leafSize = baseSize + (i < remainder ? 1 : 0);
        Leaf<Key, Address> newLeaf = new Leaf<>(leafSize, settings);
        for (int j = 0; j < leafSize; j++) {
          newLeaf._keys[j] = processed.get(offset + j);
        }
        newLeaf._stats = newLeaf.computeStats(storage);
        result[i] = newLeaf;
        offset += leafSize;
      }
      return result;
    }
  }

  /**
   * Post-process leaf nodes through ILeafProcessor if configured.
   *
   * This is called AFTER add/remove logic completes but BEFORE returning
   * to the parent branch. Allows custom compaction/splitting of entries.
   *
   * Zero overhead if no processor is configured (_leafProcessor == null).
   *
   * @param nodes The result from add/remove (array of 1-3 nodes)
   * @param storage The storage backend
   * @param settings The settings (contains processor)
   * @return Processed nodes (may be split/merged differently)
   */
  @SuppressWarnings("unchecked")
  private static <Key, Address> ANode[] processLeafNodes(
      ANode[] nodes,
      IStorage storage,
      Settings settings) {

    ILeafProcessor processor = settings.leafProcessor();

    // Zero-cost early exit if no processor configured
    if (processor == null) {
      return nodes;
    }

    // Handle special return values (unchanged, early exit)
    if (nodes == PersistentSortedSet.UNCHANGED || nodes == PersistentSortedSet.EARLY_EXIT) {
      return nodes;
    }

    // Process each leaf node
    List<Leaf<Key, Address>> processedLeaves = new ArrayList<>();

    for (ANode node : nodes) {
      if (node == null) {
        // Preserve nulls (used in remove for signaling merges)
        processedLeaves.add(null);
        continue;
      }

      // Only process Leaf nodes (branches are handled separately)
      if (!(node instanceof Leaf)) {
        return nodes;  // Shouldn't happen in leaf operations, but be safe
      }

      Leaf<Key, Address> leaf = (Leaf<Key, Address>) node;

      // Check if processing is needed (avoid allocation if not)
      if (!processor.shouldProcess(leaf._len, settings)) {
        processedLeaves.add(leaf);
        continue;
      }

      // Convert leaf keys to list for processing
      List<Key> entries = Arrays.asList(Arrays.copyOfRange(leaf._keys, 0, leaf._len));

      // Call processor
      List<Key> processed = processor.processLeaf(entries, storage, settings);

      // If same size and same content, no changes needed
      if (processed.size() == leaf._len && processed.equals(entries)) {
        processedLeaves.add(leaf);
        continue;
      }

      // Rebuild leaf(s) from processed entries
      int processedSize = processed.size();

      if (processedSize == 0) {
        // All entries removed - signal by adding null (will be handled by parent)
        processedLeaves.add(null);
      } else if (processedSize <= settings.branchingFactor()) {
        // Fits in single leaf
        Leaf<Key, Address> newLeaf = new Leaf<>(processedSize, settings);
        for (int i = 0; i < processedSize; i++) {
          newLeaf._keys[i] = processed.get(i);
        }
        newLeaf._stats = newLeaf.computeStats(storage);
        processedLeaves.add(newLeaf);
      } else {
        // Needs to split into multiple leaves
        // Split as evenly as possible
        int numLeaves = (processedSize + settings.branchingFactor() - 1) / settings.branchingFactor();
        int baseSize = processedSize / numLeaves;
        int remainder = processedSize % numLeaves;

        int offset = 0;
        for (int i = 0; i < numLeaves; i++) {
          int leafSize = baseSize + (i < remainder ? 1 : 0);
          Leaf<Key, Address> newLeaf = new Leaf<>(leafSize, settings);
          for (int j = 0; j < leafSize; j++) {
            newLeaf._keys[j] = processed.get(offset + j);
          }
          newLeaf._stats = newLeaf.computeStats(storage);
          processedLeaves.add(newLeaf);
          offset += leafSize;
        }
      }
    }

    // Convert back to array
    // Handle case where processing caused splits (more leaves than input)
    int resultSize = processedLeaves.size();

    if (resultSize == nodes.length) {
      // Same number of nodes, return directly
      return processedLeaves.toArray(new ANode[resultSize]);
    } else if (resultSize == 1 && nodes.length == 1) {
      // Still one node
      return new ANode[]{ processedLeaves.get(0) };
    } else if (resultSize > nodes.length) {
      // Split into more leaves - need to signal this
      // Return array with all new leaves (parent will handle splits)
      return processedLeaves.toArray(new ANode[resultSize]);
    } else {
      // Merged into fewer leaves
      // For remove operations, preserve the 3-element [left, center, right] structure
      if (nodes.length == 3) {
        // Map back to 3-element structure
        // This is complex - for now, just return processed nodes
        // Parent will need to handle this
        return processedLeaves.toArray(new ANode[resultSize]);
      }
      return processedLeaves.toArray(new ANode[resultSize]);
    }
  }
}
