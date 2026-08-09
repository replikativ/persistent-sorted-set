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
  // Not final: a lazily-restored root self-describes its split strategy (the boundary); root()
  // adopts it on first materialization so conj/disj use the right splitter even when the restore
  // opts didn't specify one (the restore-by path). Like the _root lazy cache, a one-time write.
  public Settings _settings;
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
      // NOTE the publish of `_root` is at the END of this block, not here. Everything
      // below adjusts `_settings` from what the restored node turns out to carry, and one
      // arm of it THROWS. Publishing first made both of those skippable, because the
      // `root == null` guard above means a second entry never re-runs them:
      //
      //   * the leafProcessor refusal degraded into silent data loss. Measured, single
      //     threaded: 1st root() -> THREW, 2nd root() -> NO-THROW with the set left at
      //     diffBufSize 0 over nodes carrying slots — precisely the state the refusal
      //     exists to prevent. Every read and every write calls root(), so one `count`
      //     was enough to arm it.
      //   * the diff-buf and boundary adoptions became racy: another thread observing the
      //     published `_root` before the `_settings` write proceeds at budget 0 over
      //     buffered nodes, and its next write drops their buffered elements.
      //
      // Publishing last makes the adopted settings visible before anything can use the
      // root, and makes the refusal fire on every call rather than only the first.
      // `makeReference` reads only `_refType`, which none of the adjustments below
      // change, so deferring it is behaviour-preserving for the reference kind.

      // self-describing BRANCHING FACTOR, and it is not optional. The rebalance arms decide
      // whether a merge FITS from the NODE's settings (`Leaf.java:250` `left._len + centerLen <=
      // _settings.branchingFactor()`, and its three siblings, plus the Branch twins) while the
      // resulting array is allocated from the SET's (`ANode.java:259` caps `newLen` at
      // `settings.branchingFactor()` when editable). Nothing made the two agree, so a node bf
      // LARGER than the set's overruns the array it was just given.
      //
      // Reachable through the shipped codec, which is what makes this urgent rather than
      // theoretical: `impl.nodes/blob->leaf|blob->branch` rebuild every node with the
      // `:branching-factor` recorded in its OWN blob, so simply reopening a store with a
      // different `:branching-factor` than it was written with is enough. Measured, 2000
      // elements written at bf 64 and reopened at bf 8, one transient `disj`:
      //
      //     AssertionError at ANode.<init>:23   (-ea)
      //     ArrayIndexOutOfBoundsException: last destination index 31 out of bounds for
      //     object array[8]  in Stitch.copyAll   (-da)
      //
      // The persistent path does not throw — it silently builds leaves of up to NODE-bf keys
      // inside a set that believes it is bf 8, which is worse.
      //
      // Adopting is the same principle already applied to the boundary and the diff-buf budget
      // just below: the data knows the number and the caller cannot be expected to. Unlike
      // those two this adopts UNCONDITIONALLY rather than only upward from a default, because
      // there is no safe way to run at a smaller bf over larger nodes.
      if (root._settings.branchingFactor() != _settings.branchingFactor()) {
        _settings = _settings.withBranchingFactor(root._settings.branchingFactor());
      }

      // self-describing boundary: a restored node carries its split strategy; adopt it so this
      // set's own conj/disj use the right splitter even when restore opts didn't specify one.
      // Idempotent for the root-handler restore path (settings already carry the boundary).
      IBoundary nodeBoundary = root._settings.boundary();
      if (nodeBoundary.contentDefined() && !_settings.boundary().contentDefined()) {
        _settings = _settings.withBoundary(nodeBoundary);
      }
      // diff-buf: self-describing in exactly the same way, and adopted for a stronger
      // reason. A node carries its own budget in its blob, so a set restored WITHOUT
      // `:diff-buf-size` ran at 0 over nodes at N: reads were fine (projection is driven
      // by the NODE's settings through child()), but the next write rebuilt through the
      // set's settings and dropped every surviving sibling's buffered elements. Measured
      // before this adoption, bf 16 / budget 512 / 6000 elements, a tree stored WITH
      // slots and then restored bare: 81 elements silently gone; zero when the caller
      // passed the budget. The caller cannot be expected to remember a number the data
      // already knows.
      //
      // Adopts upward from 0 — including over an EXPLICIT 0, which the previous wording
      // ("an explicit budget wins") got backwards. `Settings` cannot distinguish an
      // explicit 0 from an unset one, but more importantly it must not: honouring a
      // request for 0 over nodes that carry slots is exactly the state described above,
      // where the next write drops their buffered elements. A caller asking for baseline
      // over buffered data is asking to lose it, so the data wins.
      //
      // Consequence for tests, which is how this was found: pinning `:diff-buf-size 0`
      // in the SET's opts does NOT give baseline once the storage hands back nodes that
      // carry a budget — and the `:test` alias sets `-Dpss.diffBufSize=256`, so every
      // node a default test storage reconstructs carries 256. A baseline test must give
      // the STORAGE 0 as well; see test/diff_buf_restore_cycle.clj's node-settings.
      //
      // `withDiffBufSize` still refuses to enable buffering under a content-defined
      // boundary.
      if (_settings.diffBufSize() <= 0 && root._settings.diffBufSize() > 0) {
        if (_settings.leafProcessor() != null) {
          // A leafProcessor and diff-buf corrupt together (a deposit records one element, a
          // processor rewrites the whole leaf), so adopting is unsafe — but refusing is only
          // right when there is something to lose. Decide on what the subtree ACTUALLY
          // carries, not on the budget it declares: `bufEntries()` is 0 when there is no
          // buffer at all, and otherwise the subtree total, resolved from slots already in
          // memory with no IO.
          long buffered = (root instanceof Branch) ? ((Branch) root).bufEntries() : 0;
          if (buffered > 0) {
            throw new IllegalStateException(
              "cannot open this store with a leafProcessor: its nodes declare a diff-buf budget of "
              + root._settings.diffBufSize() + " and carry " + buffered + " buffered element(s), but a "
              + "leafProcessor forces buffering OFF (the two corrupt together), so those elements "
              + "would be dropped on the next write. Open it without a leafProcessor, or rewrite it "
              + "with diff-buf off.");
          }
          // Nothing buffered ⇒ nothing to preserve ⇒ stay at 0 and carry on.
        } else {
          _settings = _settings.withDiffBufSize(root._settings.diffBufSize());
        }
      }
      _root = _settings.makeReference(root);   // PUBLISH LAST — see the note above
    }
    // A DIRTY root (no address) must be held strongly — see markDirty. If it is gone
    // there is no durable copy to fall back on, so say so rather than dereference null
    // three frames later.
    if (root == null) {
      throw new IllegalStateException(
          "PersistentSortedSet has neither a resident root nor an address: a dirty root's "
          + "reference was cleared. `_address == null` must imply a strongly-held root.");
    }
    // diff-buf: seed the projection comparator at the root; Branch.child propagates it down
    // as nodes materialize, so a leaf-parent can project buffered leaves with the set's
    // comparator. (Idempotent; no-op for a Leaf root, which has no buffered children.)
    // Seed it when the root carries none; COPY when it already carries a DIFFERENT one. The
    // object in `_root` is whatever the IStorage returned, and a caching storage hands the
    // same object to every set opened at that address — so an unconditional write here let
    // the last set to call root() decide how every other set's buffered leaves are ordered.
    // The copy is published into `_root`, so a conflicting pair costs one copy per set, not
    // one per read; the single-comparator path is the same plain write it always was.
    if (root instanceof Branch) {
      Branch rb = (Branch) root;
      ANode stamped = rb.stampOrCopy(_cmp);
      if (stamped != rb) {
        root = stamped;
        // A DIRTY root (no address) must be held STRONGLY — there is no durable copy to fall
        // back on, and the guard above throws IllegalStateException if its reference is ever
        // cleared. Wrapping unconditionally demoted it: measured, `:soft` and `:weak` both
        // turned the Branch into a Soft/WeakReference whose clear() then made the very next
        // root() throw "a dirty root's reference was cleared". Same rule markDirty follows.
        _root = (_address == null) ? root : _settings.makeReference(root);
      }
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
    // diff-buf: this ctor leaves Branch._buf at its default null (settled-empty: no slots, zero
    // buffered entries). That is correct here precisely because the result is ALWAYS the root
    // (both callers assign it to _root): the root is always written, so its bufEntries() is never
    // read by a parent to decide buffering, and it has no slots (nothing buffered). If it is
    // mutated further this txn the first deposit folds its children's values in (poisoning it to
    // BUF_WRITE if a child rebalanced). Do not reuse this for a non-root branch without
    // publishing a diff-buf snapshot.
    return new Branch(nodes[0].level() + 1, n, keys, null, children,
                      countKnown ? subtreeCount : -1, measure, _cmp, _settings);
  }

  /**
   * Build the new root from the children the root's add() returned. Count ⇒ a single wrap
   * (byte-identical). MST ⇒ promote by boundary level until one node remains: a separator
   * whose key rises to level+1 ends a parent chunk, so one high-level key can grow several
   * levels at once (rare; may leave degenerate single-child branches — history-independent,
   * collapsing them is a follow-up). The rightmost child is never a cut (global max).
   */
  private ANode growRoot(ANode[] nodes) {
    IBoundary boundary = _settings.boundary();
    if (!boundary.contentDefined()) {
      return makeBranchFromChildren(nodes);
    }
    while (nodes.length >= 2) {
      int level = nodes[0].level() + 1;
      int thresh = level + 1;
      List<ANode> branches = new ArrayList<>();
      int start = 0;
      for (int i = 0; i < nodes.length; i++) {
        boolean cut = i < nodes.length - 1
                      && boundary.keyLevel(nodes[i].maxKey(), _settings) >= thresh;
        if (cut || i == nodes.length - 1) {
          branches.add(makeBranchFromChildren(Arrays.copyOfRange(nodes, start, i + 1)));
          start = i + 1;
        }
      }
      if (branches.size() == 1) return branches.get(0);
      nodes = branches.toArray(new ANode[0]);
    }
    return nodes[0];
  }

  /**
   * Check whether all nodes in this tree have precomputed subtree counts.
   * When true, countSlice is guaranteed O(log n).
   * When false, countSlice may degrade to O(n) for subtrees missing counts.
   */
  public boolean hasSubtreeCounts() {
    ANode root = root();
    return getSubtreeCount(root) >= 0;
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
      // UNWEIGHTED: the nth ELEMENT by position, navigating the subtree counts the tree
      // already maintains for countSlice. No measure needed, and none should be invented:
      // requiring one here is what tempted `IMeasure.weight`'s old default to claim every
      // entry weighed 1 — which is not a monoid homomorphism and made getNth answer null for
      // every index above 0.
      //
      // Every element weighs exactly one, so the offset within the element is always 0.
      long n = count();
      if (rank < 0 || rank >= n) return null;
      while (node instanceof Branch) {
        Branch<Key, Address> branch = (Branch<Key, Address>) node;
        boolean found = false;
        for (int i = 0; i < branch._len; i++) {
          ANode<Key, Address> child = branch.child(_storage, i);
          long childCount = child.count(_storage);
          if (rank < childCount) { node = child; found = true; break; }
          rank -= childCount;
        }
        if (!found) return null;
      }
      if (outOffset != null) outOffset[0] = 0;
      return ((Leaf<Key, Address>) node)._keys[(int) rank];
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
      if (root == null) {
        throw new IllegalStateException(
            "PersistentSortedSet cannot be stored: it has no address and its root reference "
            + "was cleared. `_address == null` must imply a strongly-held root (see markDirty).");
      }
      address(root.store(_storage));
      _root = _settings.makeReference(root);
    }

    return _address;
  }

  /** Attach `storage` and store whatever is not yet stored.
   *
   *  This does NOT copy an already-stored tree to a different backend, though the
   *  signature invites that reading. `store()` returns early when `_address != null`,
   *  so calling this with a second storage writes ZERO blobs and returns the FIRST
   *  storage's address — a later restore from the second backend then fails on a
   *  missing node. Verified.
   *
   *  It is left as-is deliberately: the address alone cannot say which backend it
   *  belongs to, and two handles onto the same underlying store are a normal thing to
   *  pass here (datahike constructs a CachedStorage per connection), so refusing on
   *  identity would reject correct calls. To copy a tree to another backend, build a
   *  fresh set there. */
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

  /** INVARIANT: `_address == null` implies `_root` holds a STRONG reference.
   *
   *  A dirty root has no durable copy. If its Soft/WeakReference were cleared the
   *  tree would be unrecoverable, and `root()`/`store()` would dereference null.
   *  Every site that clears `_address` therefore publishes the root strongly, in
   *  one place, rather than each remembering to.
   *
   *  This matters most for the EARLY_EXIT paths, where a node is mutated IN PLACE
   *  and so no new root is produced to assign: those used to clear `_address` and
   *  leave `_root` as whatever the last `store()` wrapped it in. That state was
   *  not reachable through the public API when this was written — a just-stored
   *  tree is not mutable in place, so the first mutation after a store always
   *  returns a node — but nothing enforced it, and the cost of holding the
   *  invariant is one assignment.
   *
   *  Note the reference type deliberately changes here: a dirty root is held
   *  STRONGLY even when the set is configured `:soft`/`:weak`. That is the point.
   *  A dirty root is the only copy in existence, so allowing the collector to take
   *  it is never correct. */
  /** Refuse a STALE TRANSIENT HANDLE — a set that was made transient and has since been
   *  sealed by `persistent!`. Clojure throws here (`ensureEditable`, identical in
   *  PersistentVector/HashMap/ArrayMap); this used to answer `editable() == false` and
   *  quietly take the persistent path instead, returning a NEW set from what looked like
   *  an in-place `conj!` — so the caller's mutation went somewhere they were not looking.
   *
   *  A set that was never transient has no edit reference and passes straight through. */
  private void ensureLiveTransient() {
    if (_settings.sealedTransient()) {
      throw new IllegalAccessError("Transient used after persistent! call");
    }
  }

  private void markDirty(ANode<Key, Address> root) {
    _address = null;
    _root = root;                      // bare node, never a Reference
  }

  public PersistentSortedSet cons(Object key, Comparator cmp) {
    // nil is not a storable value (matches upstream persistent-sorted-set; nil would also be
    // ambiguous against the null "not found"/sentinel returns and comparator-dependent ordering).
    if (key == null) throw new IllegalArgumentException("PersistentSortedSet cannot store nil");
    ensureLiveTransient();
    final ANode<Key, Address> r = root();
    ANode[] nodes = r.add(_storage, (Key) key, cmp, _settings);

    if (UNCHANGED == nodes) return this;

    // Mark old root address as freed if it exists (works in both persistent and transient modes)
    if (_storage != null && _address != null) {
      _storage.markFreed(_address);
    }

    if (editable()) {
      if (1 == nodes.length) {
        markDirty(nodes[0]);
      } else if (nodes.length >= 2) {
        markDirty(growRoot(nodes));
      } else {
        // EARLY_EXIT (nodes.length == 0): `r` was modified IN PLACE, so it is the
        // new root and must be published strongly — see markDirty.
        markDirty(r);
      }
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
      newRoot = growRoot(nodes);
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
    // BEFORE the mode split: a stale transient handle must be refused in BOTH modes. This used
    // to sit below the content-defined arm, so `disj!` on a sealed handle took the MST path and
    // silently degraded to a persistent remove — it returned a NEW set and left the handle the
    // caller kept unchanged. Since discarding the return value is the whole point of a
    // transient, the delete was lost with no signal. Measured, bf 8, 100 elements, a handle
    // sealed by `persistent!`: count mode threw IllegalAccessError, MST returned a new set of
    // count 99 while the stale handle still had 100 and still contained the key.
    // `cons` and `replace` always guarded both modes; only this one did not.
    ensureLiveTransient();

    // split-seam (MST/content mode): sibling-free removeContent recursion, then collapse a
    // single-child root (count path below is untouched / byte-identical).
    if (_settings.boundary().contentDefined()) {
      ANode newRoot = root().removeContent(_storage, (Key) key, cmp, _settings);
      if (newRoot == null) return this; // not in set
      if (_storage != null && _address != null) _storage.markFreed(_address);
      while (newRoot instanceof Branch && newRoot._len == 1) {
        newRoot = ((Branch) newRoot).child(_storage, 0);
      }
      if (editable()) {
        _address = null;
        _root = newRoot;
        long rc = getSubtreeCount(newRoot);
        _count = (rc >= 0) ? (int) rc : -1;
        _version += 1;
        return this;
      }
      long rc = getSubtreeCount(newRoot);
      int newCount = (rc >= 0) ? (int) rc : -1;
      return new PersistentSortedSet(_meta, _cmp, null, _storage, newRoot, newCount, _settings, _version + 1);
    }

    final ANode<Key, Address> r = root();
    ANode[] nodes = r.remove(_storage, (Key) key, null, null, cmp, _settings);

    // not in set
    if (UNCHANGED == nodes) return this;

    // Mark old root address as freed if it exists (works in both persistent and transient modes)
    if (_storage != null && _address != null) {
      _storage.markFreed(_address);
    }

    // in place update
    if (nodes == EARLY_EXIT) {
      // `r` was modified IN PLACE, so it is the new root and must be published
      // strongly — see markDirty.
      markDirty(r);
      // When a processor is configured, count may differ from -1 — the same distinction
      // `cons` makes above and the rebuild arms below. This arm returned early and so
      // skipped the guard entirely; measured before the fix, count outran the elements by
      // one per op (bf 8 / n 200: count 179, seq 178).
      if (_settings.leafProcessor() != null) {
        long rootCount = getSubtreeCount(root());
        _count = (rootCount >= 0) ? (int) rootCount : -1;
      } else {
        _count = alterCount(-1);
      }
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
    // split-seam (MST): an in-place replace keeps the tree's boundary structure, which is canonical
    // ONLY when oldKey and newKey rise to the same level. When a partial comparator shifts the key
    // hash across a level boundary the node would have to re-split/merge, so fall back to
    // disjoin+cons (the history-independent rebuild). Count mode is position-only ⇒ always in-place.
    // See doc/merkle-search-tree.md.
    IBoundary boundary = _settings.boundary();
    if (boundary.contentDefined()
        && boundary.keyLevel(oldKey, _settings) != boundary.keyLevel(newKey, _settings)) {
      if (!root().contains(_storage, (Key) oldKey, cmp)) return this; // not found ⇒ no-op
      return disjoin(oldKey, cmp).cons(newKey, cmp);
    }

    ensureLiveTransient();
    final ANode<Key, Address> r = root();
    ANode[] nodes = r.replace(_storage, (Key) oldKey, (Key) newKey, cmp, _settings);

    // Not in set
    if (UNCHANGED == nodes) return this;

    // Mark old root address as freed if it exists (works in both persistent and transient modes)
    if (_storage != null && _address != null) {
      _storage.markFreed(_address);
    }

    // In-place update (transient)
    if (EARLY_EXIT == nodes) {
      // `r` was modified IN PLACE, so it is the new root and must be published
      // strongly — see markDirty.
      markDirty(r);
      _version += 1;
      return this;
    }

    // New root node (persistent case or maxKey changed in transient)
    ANode newRoot = nodes[0];
    if (editable()) {
      markDirty(newRoot);
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

  /**
   * Look up the first element >= key (ceiling/GE lookup).
   * O(log n) with zero allocations — no Seq chain created.
   * Returns null if no element >= key exists.
   *
   * <p><b>Internal API</b> — subject to change without notice.
   * Not exposed in the public Clojure namespace.
   */
  public Key lookupGE(Object key) {
    return lookupGE(key, _cmp);
  }

  public Key lookupGE(Object key, Comparator<Key> cmp) {
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
        node = ((Branch<Key, Address>) node).child(_storage, idx);
      } else {
        return node._keys[idx];
      }
    }
  }

  /**
   * Mutable forward-only cursor for efficient sequential lookupGE.
   * For sorted lookup keys, amortized O(1) per lookup instead of O(log n).
   * Not thread-safe. Must only be used for forward (ascending) seeks.
   *
   * <p><b>Internal API</b> — this class is subject to change or removal
   * without notice. It is not exposed in the public Clojure namespace.
   * External consumers should use {@link #lookupGE} or {@code slice} instead.
   *
   * <p>Usage:
   * <pre>
   *   ForwardCursor c = pss.forwardCursor();
   *   Key result1 = c.seekGE(key1);  // O(log n) — first seek
   *   Key result2 = c.seekGE(key2);  // O(1) if in same leaf, else O(siblings skipped)
   * </pre>
   */
  public class ForwardCursor {
    private final Comparator<Key> _cursorCmp;
    private ANode<Key, Address> _leaf;     // current leaf node
    private int _leafIdx;                   // current index within leaf
    // Stack for tree traversal (height levels)
    // For a tree of height H, we need H-1 branch levels above the leaf.
    private Branch<Key, Address>[] _branches;
    private int[] _branchIdxs;
    private int _depth;                     // number of branch levels (0 for leaf-only)

    @SuppressWarnings("unchecked")
    ForwardCursor(Comparator<Key> cmp) {
      _cursorCmp = cmp;
      ANode<Key, Address> root = root();
      if (root.len() == 0) {
        _leaf = null;
        _depth = 0;
        return;
      }
      // Compute tree height
      int height = 0;
      ANode<Key, Address> n = root;
      while (n instanceof Branch) {
        height++;
        n = ((Branch<Key, Address>) n).child(_storage, 0);
      }
      _depth = height;
      _branches = new Branch[height];
      _branchIdxs = new int[height];
      // Position at start (leftmost leaf)
      n = root;
      for (int level = 0; level < height; level++) {
        _branches[level] = (Branch<Key, Address>) n;
        _branchIdxs[level] = 0;
        n = ((Branch<Key, Address>) n).child(_storage, 0);
      }
      _leaf = n;
      _leafIdx = 0;
    }

    /**
     * Seek forward to first element >= key.
     * Keys MUST be passed in ascending order across calls.
     * Returns null if no element >= key exists.
     */
    public Key seekGE(Key key) {
      if (_leaf == null) return null;

      // Fast path: key is within current leaf
      if (_cursorCmp.compare(key, _leaf.maxKey()) <= 0) {
        // Search from current position — target is always >= _leafIdx since keys are ascending
        int idx = _leaf.searchFirstFrom(key, _cursorCmp, _leafIdx);
        if (idx < _leaf._len) {
          _leafIdx = idx;
          return _leaf._keys[idx];
        }
      }

      // Need to advance to a later leaf.
      // Walk up the branch stack to find a branch that contains our key,
      // then walk back down.
      int level = _depth - 1; // start from immediate parent of leaf
      while (level >= 0) {
        Branch<Key, Address> branch = _branches[level];
        int bi = _branchIdxs[level] + 1; // advance past current child
        // Linear scan forward through siblings (amortized O(1))
        while (bi < branch._len) {
          if (_cursorCmp.compare(key, branch._keys[bi]) <= 0) {
            // key <= this child's maxKey, so answer is in this subtree
            _branchIdxs[level] = bi;
            // Walk down to leaf
            ANode<Key, Address> node = branch.child(_storage, bi);
            for (int d = level + 1; d < _depth; d++) {
              _branches[d] = (Branch<Key, Address>) node;
              int childIdx = node.searchFirst(key, _cursorCmp);
              if (childIdx >= node._len) childIdx = node._len - 1;
              _branchIdxs[d] = childIdx;
              node = ((Branch<Key, Address>) node).child(_storage, childIdx);
            }
            _leaf = node;
            int idx = _leaf.searchFirst(key, _cursorCmp);
            if (idx < _leaf._len) {
              _leafIdx = idx;
              return _leaf._keys[idx];
            }
            // Key exceeds this leaf — continue to next sibling at this level
            bi++;
            continue;
          }
          bi++;
        }
        level--; // go up one level
      }

      // Exhausted all branches
      _leaf = null;
      return null;
    }

    /**
     * Advance cursor to the next element and return it.
     * Returns null if no more elements exist.
     * O(1) within a leaf, amortized O(1) across leaves.
     */
    public Key next() {
      if (_leaf == null) return null;
      _leafIdx++;
      if (_leafIdx < _leaf._len) {
        return _leaf._keys[_leafIdx];
      }
      // Need to advance to next leaf via branch stack
      for (int level = _depth - 1; level >= 0; level--) {
        int bi = _branchIdxs[level] + 1;
        if (bi < _branches[level]._len) {
          _branchIdxs[level] = bi;
          ANode<Key, Address> node = _branches[level].child(_storage, bi);
          for (int d = level + 1; d < _depth; d++) {
            _branches[d] = (Branch<Key, Address>) node;
            _branchIdxs[d] = 0;
            node = ((Branch<Key, Address>) node).child(_storage, 0);
          }
          _leaf = node;
          _leafIdx = 0;
          return _leaf._keys[0];
        }
      }
      _leaf = null;
      return null;
    }

    /**
     * Return the current element without advancing.
     * Returns null if the cursor is exhausted or not yet positioned.
     */
    public Key current() {
      if (_leaf == null || _leafIdx < 0 || _leafIdx >= _leaf._len) return null;
      return _leaf._keys[_leafIdx];
    }

  }

  /**
   * Create a forward cursor positioned at the start of the set.
   * Use for efficient sequential lookupGE with ascending keys.
   *
   * <p><b>Internal API</b> — subject to change without notice.
   */
  public ForwardCursor forwardCursor() {
    return new ForwardCursor(_cmp);
  }

  public ForwardCursor forwardCursor(Comparator<Key> cmp) {
    return new ForwardCursor(cmp);
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

  /** Seal the transient and return the persistent set.
   *
   *  Returns a NEW PersistentSortedSet rather than `this`, as every Clojure transient
   *  does. That is what lets a stale handle be DETECTED: while the two were the same
   *  object, "the transient after persistent!" and "the persistent result" were
   *  indistinguishable, so `conj!` on the stale handle could only degrade silently.
   *  The result carries sealed settings (no edit reference); `this` keeps the sealed
   *  edit reference and so answers `ensureLiveTransient` by throwing.
   *
   *  Shared nodes keep the old settings object, whose `editable()` is now false — which
   *  is exactly the committed-node state they should be in. */
  public PersistentSortedSet persistent() {
    if (!editable()) {
      throw new IllegalStateException("Expected transient set");
    }
    Settings sealed = _settings.sealed();
    _settings.persistent();                 // null the shared owner: this handle is now stale
    return new PersistentSortedSet(_meta, _cmp, _address, _storage, _root, _count, sealed, _version);
  }

  // Iterable
  public Iterator iterator() {
    return new JavaIter((Seq) seq());
  }
}