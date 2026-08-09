package org.replikativ.persistent_sorted_set;

import java.lang.ref.*;
import java.util.*;
import java.util.function.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public class Branch<Key, Address> extends ANode<Key, Address> implements ISubtreeCount {
  // 1+ for Branches
  public final int _level;

  // Total count of elements in this subtree. -1 means not yet computed.
  // For lazy computation when restored from old storage format.
  public long _subtreeCount;

  // diff-buf (only used when _settings.diffBufSize() > 0; null/false otherwise, so
  // diffBufSize==0 is byte-identical to baseline — invariant I0).
  //
  // diff-buf state snapshot: {slots, entries} always mutually consistent. Kept as the
  // inner type of NodeState below (one volatile publish covers slots, entries, addresses
  // and children together).
  //
  // slots[i], when non-null, is the buffered logical diff of child i against its
  // durable version (a Slot holding a PersistentTreeMap<Key,Op> + a cached
  // (count, measure) snapshot ĝ). null == child i has no buffered diff.
  //
  // entries is this node's subtree buffered-diff size, in *entries* (number of buffered
  // element-changes summed over the whole subtree — the budget-B unit), maintained by
  // delta on the deposit return path, mirroring _subtreeCount. Three states:
  //   >= 0   : content-only — the exact buffered-entry count (== sum of slot entry sizes);
  //   WRITE  : must be WRITTEN, not buffered — a split/merge/borrow happened in this subtree.
  //            Set at the rebuild site; PROPAGATES UP automatically because a parent's deposit
  //            folds the child's value into this sum (a WRITE child poisons the parent), so the
  //            store-time gate is an O(1) field read instead of a recursive subtree walk.
  //   LAZY   : restored from storage, not yet derived — bufEntries() resolves it from slots
  //            (IO-free, the diffs are already in memory). Mirrors _subtreeCount's -1/lazy.
  // Settled to the embedded total at store (the node then equals its durable object).
  static final class BufState {
    final Object[] slots;   // may be null (no slots)
    final long entries;     // settled total, or BUF_WRITE / BUF_LAZY sentinel
    BufState(Object[] slots, long entries) { this.slots = slots; this.entries = entries; }
  }

  public static final long BUF_WRITE = -1;   // must write (rebalanced subtree)
  public static final long BUF_LAZY  = Slot.LAZY;  // restored, derive from slots on first read

  // All settle-visible per-child state as ONE immutable snapshot. A shared node is assumed
  // to have a single writer (a settle) and many readers (apply-thread copies, query
  // traversals): one volatile reference makes every observable state internally consistent —
  // a reader/copier can never see addresses and children from different generations.
  //
  // "Single writer" is a CONTRACT ON THE CALLER, not a property this class provides. Nothing
  // here serialises store(), and structural sharing means two versions can share dirty nodes:
  // measured on a pipelining-writer shape at bf 8 / n 1000, 3 Branch objects were reachable
  // from both roots and dirty in both, and two concurrent stores settle those same objects.
  // The required exclusion is therefore per LINEAGE (in practice per storage), not per tree —
  // see doc/CONCURRENCY.md, which also records what does and does not go wrong when it is
  // violated. The snapshot below defends readers; it does not defend writers from each other. Replaces the plain (_addresses, _children) pair whose two-step settle
  // (write _addresses[i], THEN wrap _children[i] in a Reference) tore under a
  // pipelining writer: a concurrent copy could mix a pre-settle (null) address with a
  // post-settle wrapped child — the forbidden "dirty child behind a SoftReference"
  // state that crashed baseline commits (#17) — or, worse, an old address with the
  // wrapper of a NEWER child, silently resurrecting stale data after a soft-ref
  // clearance. Generalizes the BufState single-publish discipline from #15 (BufState
  // is folded in: settle publishes {addresses, children, buf} with one write).
  // See doc/CONCURRENCY.md for the full memory model.
  //
  // Per i in [0.._len), the (addresses[i], children[i]) pair is in exactly one state:
  //   1. Not stored:       addresses == null || addresses[i] == null, children[i] is a bare ANode
  //   2. Stored:           addresses[i] != null, children[i] is a Soft/WeakReference<ANode> (or a
  //                        bare ANode under :strong ref-type / on paths that keep it resident)
  //   3. Not restored yet: addresses[i] != null, children == null || children[i] == null
  // The forbidden state — addresses[i] == null with children[i] a Reference — is now
  // impossible within any single snapshot (asserted permanently in store()).
  //
  // Mutability contract for the ARRAYS inside a snapshot:
  //   - EDITABLE (owner-thread transient) and not-yet-published nodes mutate them in
  //     place (unshared by contract — the transient ownership rule); replacing an array
  //     (null -> allocated) republishes a NodeState carrying the other components.
  //   - On possibly-SHARED nodes the arrays are read-only; the only writers are the
  //     settle (single publish in store()) and reader cache-fills (copy-on-write + CAS:
  //     child()'s restore-fill, bufEntries()'s LAZY resolve — a settle overwriting a
  //     fill is fine, a fill must never clobber a settle).
  public static final class NodeState<Address> {
    public final Address[] addresses;  // may be null (no addresses)
    public final Object[]  children;   // ANode | Soft/WeakReference<ANode> | null per slot; may be null
    final BufState buf;                // diff-buf snapshot; null == settled-empty (no slots, 0 entries)
    NodeState(Address[] addresses, Object[] children, BufState buf) {
      this.addresses = addresses;
      this.children  = children;
      this.buf       = buf;
    }
  }

  // The ONE mutable reference to this node's per-child state. Never null (every ctor
  // installs a snapshot). Readers take ONE snapshot per method and use only its fields.
  public volatile NodeState<Address> _state;

  // CAS access to _state for the publishers that can race the store()-time settle on a
  // SHARED node: child()'s restore cache-fill and bufEntries()'s LAZY resolution (a
  // transaction-thread read of a shared node while the commit thread settles it). All
  // other publishers write to unshared nodes (fresh copies, editable transients,
  // restore-time installs) or are the settle itself. Java-8-compatible (no VarHandle).
  @SuppressWarnings("rawtypes")
  private static final java.util.concurrent.atomic.AtomicReferenceFieldUpdater<Branch, NodeState> STATE_UPDATER =
      java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater(Branch.class, NodeState.class, "_state");

  // diff-buf: the SET's stable comparator, used to PROJECT a buffered leaf (projectLeaf rebuilds
  // it in stored order). It is the set's comparator — NOT any per-operation/navigation comparator
  // — propagated lazily from the root down to each branch as it is materialized (see child() and
  // PersistentSortedSet.root()). A leaf-parent projects its buffered leaves with its own _projCmp,
  // so projection never depends on the comparator of whatever operation drove the descent. null
  // when diffBufSize==0 / projection never runs.
  // VOLATILE, and stamped by CAS through PROJCMP_UPDATER rather than by a plain write. Two
  // sets with different comparators can concurrently first-touch the SAME node a caching
  // IStorage handed both of them; with a plain read-then-write both observe null, both stamp,
  // and both proceed on the uncopied node — reinstating the very defect copy-on-conflict
  // exists to prevent, since that arm only sees a conflict that is already visible.
  //
  // Two concurrent READERS over one storage is legal (the contract is one WRITER per lineage),
  // so this is inside the supported region. Measured over 300 000 two-thread rounds: 9 rounds
  // where both threads reached the same freshly-restored child, and 6 where set A's elements
  // came back in set B's descending tie order — A expected [[0 0] [0 1] [0 2]] and got
  // [[0 2] [0 1] [0 0]].
  //
  // Cost: a volatile READ on x86 is a plain load, and this field is read once per child()
  // descent and written at most once per node.
  public volatile Comparator _projCmp;

  @SuppressWarnings("rawtypes")
  private static final java.util.concurrent.atomic.AtomicReferenceFieldUpdater<Branch, Comparator>
      PROJCMP_UPDATER = java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater(
          Branch.class, Comparator.class, "_projCmp");

  public Branch(int level, int len, Key[] keys, Address[] addresses, Object[] children, Settings settings) {
    this(level, len, keys, addresses, children, -1, settings);
  }

  public Branch(int level, int len, Key[] keys, Address[] addresses, Object[] children, long subtreeCount, Settings settings) {
    super(len, keys, settings);
    assert level >= 1;
    assert addresses == null || addresses.length >= len : ("addresses = " + Arrays.toString(addresses) + ", len = " + len);
    assert children == null || children.length >= len;

    _level        = level;
    _subtreeCount = subtreeCount;
    // diff-buf: this ctor backs restore (the List ctor) and bulk build — slots, if any, are
    // attached afterwards by the storage layer (installSlots), so the buffered-entry count is
    // derived lazily from the slots on first read (mirrors _subtreeCount = -1). Mutation ctors
    // publish an explicit snapshot instead.
    _state        = new NodeState<>(addresses, children, new BufState(null, BUF_LAZY));
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
    _subtreeCount = subtreeCount;
    _projCmp      = projCmp;
    // buf == null: settled-empty. Mutation callers that carry diff-buf state publish
    // their own snapshot right after construction (the node is still unpublished).
    _state        = new NodeState<>(addresses, children, null);
  }

  public Branch(int level, int len, Comparator projCmp, Settings settings) {
    super(len, (Key[]) new Object[ANode.newLen(len, settings)], settings);
    assert level >= 1;

    _level        = level;
    _subtreeCount = -1;
    _projCmp      = projCmp;
    _state        = new NodeState<>(null, null, null);
  }

  public Branch(int level, List<Key> keys, List<Address> addresses, Settings settings) {
    this(level, keys.size(), (Key[]) keys.toArray(), (Address[]) addresses.toArray(), null, settings);
  }

  // OWNER-THREAD ONLY (editable transient or not-yet-published node): make sure the
  // current snapshot carries an addresses array, allocating + republishing if absent.
  // Callers must use the RETURN VALUE (the republish invalidates earlier local
  // references to the snapshot). Never call on a possibly-shared node.
  protected Address[] ensureAddresses() {
    NodeState<Address> s = _state;
    if (s.addresses == null) {
      Address[] addresses = (Address[]) new Object[_keys.length];
      _state = new NodeState<>(addresses, s.children, s.buf);
      return addresses;
    }
    return s.addresses;
  }

  public List<Address> addresses() {
    // ONE snapshot; the returned List is a read-only view by contract — callers must
    // never mutate it (it may be backed by a published snapshot's array).
    Address[] addresses = _state.addresses;
    if (addresses == null) {
      return (List<Address>) Arrays.asList(new Object[_len]);
    } else if (addresses.length == _len) {
      return Arrays.asList(addresses);
    } else {
      return Arrays.asList(Arrays.copyOfRange(addresses, 0, _len));
    }
  }

  // Raw snapshot arrays for diagnostics/tests. Callers must NOT mutate. Pair-coherent
  // only when both come from ONE snapshot — read _state yourself if you need the pair.
  public Object[] addressArray() { return _state.addresses; }
  public Object[] childrenArray() { return _state.children; }

  /**
   * ONE snapshot's {addresses, slots} pair: `[0]` the address array (may be null),
   * `[1]` the diff-buf slot array (may be null). Callers must NOT mutate either.
   *
   * Exists because `addressArray()` and `slots()` are two independent volatile reads,
   * and a caller deciding something from BOTH must not straddle a settle. `diff`'s
   * frontier does exactly that: it pairs each child's address with that child's slot to
   * decide whether the address still stands for the subtree's contents. A pre-settle
   * address paired with post-settle slots would name a stale address while reporting the
   * child as unbuffered -- prunable -- and the other side of the diff, holding that same
   * old address, would prune with it. The delta buffered in that slot would simply be
   * missing from the answer.
   *
   * NOT reachable today, and the reason is worth writing down because it is not local.
   * store()'s settle skips every child whose pre-settle address is NON-null (the
   * "clean passthrough" branch): such a child keeps both its address and its slot, so
   * there is nothing to tear. A child that IS settled had a null address going in --
   * the mutation moved it into the slot's anchor -- and `prune-shared` ignores a null
   * address outright (`(some? addr)`), so that entry never prunes whatever the slot
   * says. Measured: a child mutated in the current generation reads [nil, SLOT]
   * pre-settle and [ADDR, SLOT] after; an untouched one stays [ADDR, SLOT] across a
   * sibling's overflow and across a structural split elsewhere.
   *
   * So this is a latent contract violation rather than a live defect. It is fixed
   * anyway: the pairing is correct here by construction instead of by a coupling to
   * which children the settle happens to touch, which is a property of a DIFFERENT
   * method that no test pinned and a future change could quietly drop.
   *
   * `_len` is a plain field, not part of the snapshot, so it needs no pairing here.
   */
  public Object[] addressesAndSlots() {
    final NodeState<Address> s = _state;
    final BufState b = s.buf;
    return new Object[]{ s.addresses, (b != null) ? b.slots : null };
  }

  public Address address(int idx) {
    assert 0 <= idx && idx < _len;

    Address[] addresses = _state.addresses;
    if (addresses == null) {
      return null;
    }
    return addresses[idx];
  }

  // (The old address(idx, address) setter is gone: its store()-time caller was the
  // two-step settle — write the address, then wrap the child — that tore under a
  // concurrent copier. The settle now stages both arrays locally and publishes ONE
  // NodeState (see store()); the only remaining mutation is the owner-thread clear
  // inlined in child(idx, node) below.)

  public ANode<Key, Address> child(IStorage storage, int idx) {
    assert 0 <= idx && idx < _len;
    final NodeState<Address> s = _state;                       // ONE snapshot: pair-coherent
    assert (s.children != null && s.children[idx] != null) || (s.addresses != null && s.addresses[idx] != null);

    ANode child = null;
    if (s.children != null) {
      Object ref = s.children[idx];
      child = (ANode) _settings.readReference(ref);
    }

    if (child != null) {
      if (s.addresses != null && s.addresses[idx] != null) {
        storage.accessed(s.addresses[idx]);
      }
      return child;
    }

    assert s.addresses[idx] != null;
    ANode base = storage.restore(s.addresses[idx]);
    // diff-buf: propagate the set's projection comparator down to each restored branch, so a
    // leaf-parent projects its buffered leaves with its own _projCmp — independent of whatever
    // operation (lookup with a prefix cmp, slice, count, …) drove this descent.
    // Seed it when the restored node has none; COPY when it already carries a different one,
    // because `base` is the object the IStorage returned and a caching storage shares it by
    // address across sets. Overwriting it there is what let one set's comparator decide
    // another set's leaf order — see withProjCmp for the measurement.
    if (base instanceof Branch) base = ((Branch) base).stampOrCopy(_projCmp);
    // slots from the SAME snapshot as the address we restored from — the pair can't mix
    // a pre-settle address with post-settle slots (or vice versa).
    Object[] slots = (s.buf != null) ? s.buf.slots : null;
    Slot sl = (slots != null) ? (Slot) slots[idx] : null;
    // diff-buf push-down: project this parent's buffered diff onto the freshly loaded child —
    // leaf: batch-rebuild keys (with this leaf-parent's _projCmp); branch: install the nested
    // diff as the child's own slots + set its aggregates from ĝ. Runs once, here, at
    // materialization (reads stay baseline). Parent's slot supersedes any diff in the child.
    //
    // Gated on the SLOT, not on `_settings.diffBufSize()`. The budget is a WRITE-side
    // policy — how much this node may buffer before it must flush. Projection is a
    // READ-side obligation: a slot carrying a diff describes the difference between the
    // durable child and the current one, so skipping it does not "disable buffering", it
    // returns the wrong data.
    //
    // The two came apart whenever a node's settings disagreed with the set's, which a
    // storage decides — nodes are reconstructed with whatever `Settings` the IStorage
    // hands them. A set at `:diff-buf-size 128` over a storage rebuilding nodes at 0
    // silently DROPPED every buffered diff on read here, and then crashed at the next
    // store in `assembleNested`, casting a Leaf to a Branch.
    //
    // Invariant I0 is untouched: at diffBufSize 0 no slot is ever created, so `sl` is
    // always null and this is the same `child = base` it always was.
    if (sl != null && sl.diff != null) {
      child = (base instanceof Leaf) ? (ANode) projectLeaf((Leaf) base, sl.diff, _projCmp)
                                     : (ANode) projectBranch((Branch) base, sl);
    } else {
      child = base;
    }

    // Cache-fill: write the restored child back into children[idx].
    Object wrapped = _settings.makeReference(child);
    if (editable()) {
      // owner-thread, unshared by contract: fill in place within the current snapshot
      ensureChildren()[idx] = wrapped;
    } else {
      // possibly SHARED: copy-on-write + CAS retry. A concurrent settle (or another
      // fill) advancing _state makes the CAS fail and we re-read — the fill must never
      // clobber a settle; a settle clobbering our fill just loses a cache entry.
      for (;;) {
        NodeState<Address> cur = _state;
        ANode existing = (cur.children != null) ? (ANode) _settings.readReference(cur.children[idx]) : null;
        if (existing != null) return existing;                 // someone else filled — use theirs
        Object[] children = (cur.children != null)
            ? Arrays.copyOf(cur.children, cur.children.length)
            : new Object[_keys.length];
        children[idx] = wrapped;
        if (STATE_UPDATER.compareAndSet(this, cur, new NodeState<>(cur.addresses, children, cur.buf))) break;
      }
    }
    return child;
  }

  // OWNER-THREAD ONLY (editable transient): replace child idx with a freshly-mutated
  // node — clears the slot's address and installs the bare child, both in place within
  // the current snapshot (unshared by contract; see NodeState).
  public ANode<Key, Address> child(int idx, ANode<Key, Address> child) {
    // The "unshared by contract" in the comment above is the whole safety argument for
    // writing into the snapshot's arrays in place, and nothing checked it. Under -ea (the
    // :test alias) this turns the contract into something the suite enforces. Measured
    // before adding it: 0 violations over 38,813,363 calls across the diff-buf namespaces,
    // so it costs nothing today and fails loudly the moment a non-owner path appears.
    assert editable() : "child(int,ANode) writes in place — it may only be called on an "
                      + "editable (owner-thread, unshared) node, not a shared one";
    NodeState<Address> s = _state;
    if (s.addresses != null) {
      s.addresses[idx] = null;
    }
    if (s.children != null || child != null) {
      ensureChildren()[idx] = child;
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

  // diff-buf: this node's current slots array — ONE volatile snapshot; may be null.
  // READ-ONLY: callers must never mutate the returned array (it may be shared via a
  // published snapshot); writers publish a fresh array through _state instead.
  public Object[] slots() {
    BufState b = _state.buf;
    return (b != null) ? b.slots : null;
  }

  // diff-buf restore: install reconstructed slots (fressian read side / storage impls)
  // as ONE atomically-published snapshot carrying the node's current {addresses,
  // children} (restore-time: the node is not yet published, so the plain
  // read-modify-write publish is single-threaded by contract). entries is usually
  // BUF_LAZY (derived from the slots on first read), unless the caller knows the
  // settled total.
  public void installSlots(Object[] slots, long entries) {
    // A node carrying buffered diffs whose SETTINGS say buffering is off is an
    // incoherent reconstruction: the storage persisted `:slots` and is now
    // rebuilding the node declaring there is no buffer. Every read of it is
    // then subtly wrong rather than loudly broken — measured on the diff-buf
    // stress sweep (bf 8, set budget 64, node budget 0): content mismatch with
    // a MATCHING count, i.e. substituted elements, on 5 of 5 seeds.
    //
    // Refused rather than accommodated. The settings are the storage's to get
    // right — they come from the blob the storage itself wrote — and a library
    // that silently returns the wrong set is worse than one that says which
    // half of the contract was broken.
    if (slots != null && _settings.diffBufSize() <= 0) {
      throw new IllegalStateException(
          "diff-buf: a node reconstructed with diffBufSize=" + _settings.diffBufSize()
          + " was handed buffered slots. The storage persisted this node's diff buffer "
          + "and must reconstruct it with the same budget — see IStorage.restore and "
          + "Settings.diffBufSize().");
    }
    NodeState<Address> s = _state;
    _state = new NodeState<>(s.addresses, s.children, new BufState(slots, entries));
  }

  // diff-buf: this node's buffered-diff size in entries (see BufState.entries). Resolves a
  // restored node's LAZY value from its slots once (IO-free — the slot diffs are already in
  // memory), then caches; passes BUF_WRITE (must-write) through. O(1) once resolved. Mirrors
  // count(). The cache write publishes a NEW snapshot carrying the same slots — racing
  // resolvers compute identical values from the same immutable snapshot, so the race is benign.
  public long bufEntries() {
    for (;;) {
      NodeState<Address> s = _state;
      BufState b = s.buf;
      if (b == null) return 0;
      if (b.entries != BUF_LAZY) return b.entries;
      long v = sumSlotBE(b.slots);
      // CAS, not a plain publish: a concurrent store() settle may have replaced the state
      // since we read s — a plain write would CLOBBER the settled {addresses, children,
      // slots, entries} with this pre-settle snapshot (re-buffering already-flushed
      // children: double projection on read, phantom totals; resurrecting pre-settle
      // addresses). The reverse direction is safe — a settle overwriting our resolution
      // is strictly newer and its per-slot arithmetic never depended on the node-level
      // LAZY. On CAS failure re-read: the winner's value is typically already settled
      // (non-LAZY) and we just return it.
      if (STATE_UPDATER.compareAndSet(this, s, new NodeState<>(s.addresses, s.children, new BufState(b.slots, v)))) return v;
    }
  }

  // Pure snapshot variant of bufEntries(): resolve LAZY from the GIVEN snapshot without
  // publishing — used where the caller must pair the entries with that snapshot's slots
  // (the carry sites), so the pair can never mix two generations of the state.
  private long bufEntriesOf(BufState b) {
    if (b == null) return 0;
    return (b.entries == BUF_LAZY) ? sumSlotBE(b.slots) : b.entries;
  }

  private long sumSlotBE(Object[] slots) {
    long s = 0;
    if (slots != null)
      for (int j = 0; j < _len; j++) {
        Slot sl = (Slot) slots[j];
        if (sl != null) s += slotBE(sl);
      }
    return s;
  }

  // Entry count contributed by one slot of THIS node: its cached value, or — for a slot
  // reconstructed from storage (LAZY) — derived from the diff blob (IO-free). A slot's child
  // is at _level-1, so its diff is summed with that childLevel.
  private int slotBE(Slot sl) {
    return (sl.bufEntries == Slot.LAZY) ? diffSize(sl.diff, _level - 1) : (int) sl.bufEntries;
  }

  // diff-buf oracle (assert-only, -ea): recompute a content-only node's buffered-entry total by
  // walking the live subtree (the work the old store-time deep walk did), to cross-check the
  // delta-maintained entries. Skipped for BUF_WRITE/BUF_LAZY (negative ⇒ not a settled size).
  // A >=0 node can't have a rebalanced descendant (the poison would have made it negative), so the
  // walk never hits a must-write child. Runs only under -ea (e.g. the :dev alias), never in :test.
  private boolean assertBufEntries(IStorage storage) {
    BufState b = _state.buf;                                    // one snapshot
    long e = (b != null) ? b.entries : 0;
    if (e < 0) return true;
    Object[] slots = (b != null) ? b.slots : null;
    // Post-order: check dirty (resident, marker) children first so the DEEPEST mismatch throws.
    if (slots != null)
      for (int j = 0; j < _len; j++) {
        Slot s = (Slot) slots[j];
        if (s != null && s.diff == null) {
          ANode gc = child(storage, j);
          if (gc instanceof Branch) ((Branch) gc).assertBufEntries(storage);
        }
      }
    long slow = bufEntriesSlow(storage);
    if (e != slow)
      throw new AssertionError("diff-buf: bufEntries " + e + " != recomputed " + slow + " (level " + _level + ")");
    return true;
  }

  // A branch-marker slot caches the child's whole-subtree buffered total as of DEPOSIT time.
  // store() settles that child IN PLACE, so every OTHER version whose parent slot points at the
  // same child object keeps the pre-settle total. Detect it post-order and poison the slot so
  // Pass 1 writes the child wholesale.
  //
  // WHAT THIS DOES AND DOES NOT PROTECT — measured, so it is not re-litigated:
  //
  //   * It does NOT protect CONTENT. Of ~1175 stale slots observed with this repair DISABLED,
  //     not one was ever actually buffered: the per-node budget test in Pass 2 flushed every
  //     one. 0 content mismatches over 13164 trials (bf 4-16, B 1-256, four sharing orders
  //     including forks and descendant-first). The budget test is what stands between a stale
  //     slot and a wrong blob, not this walk.
  //   * It does NOT protect the ANCHOR. The same settle calls markFreed on the anchor that the
  //     other version's slot still names; with this repair ON or OFF the resulting dangling
  //     anchors are byte-identical (25 read failures / 768 trials against a storage that
  //     reclaims). See .internal/NEXT_SESSION.md — that is a separate, open defect, and it also
  //     originates from LEAF-diff slots, which this walk never inspects.
  //   * It DOES keep the delta-maintained bufEntries consistent with a fresh subtree walk, i.e.
  //     it is what makes the -ea oracle assertBufEntries hold. Dropping the recursion below
  //     costs 9 oracle failures / 1080 trials; dropping the repair entirely costs 21. Every one
  //     is the oracle, never a content mismatch.
  //
  // WHY A STALE VALUE IS ALWAYS SAFE-HIGH (never stale-LOW, which WOULD be a correctness bug,
  // since an under-count would let a blob exceed the budget B):
  //
  //   A published node's `entries` never increases. The only in-place writers are installSlots
  //   (pre-publish, or on a node owned by the current transient), the BUF_LAZY->sum CAS
  //   (resolution, not a change), this method (-> BUF_WRITE), and the settle. At a settle,
  //   entries - embedded = the sum of csz[i] over bufferable children that were FLUSHED, and a
  //   child is flushed with slotBE > 0 only when embedded + csz[i] > B (Pass 2 non-fit) or
  //   embedded > B (the D3 overflow arm). So `entries` strictly decreases only if the
  //   pre-settle value already exceeded B — which is exactly the value the other version's slot
  //   cached. Hence stale implies over budget implies flushed. Measured: 0 stale-LOW and 0
  //   over-budget blobs across 8100 trials with this repair disabled.
  //
  // Kept because the cost is inside the noise floor (-2.1% to +2.4% on a store-heavy workload,
  // against a +-5-10% run-to-run spread), so there is nothing to buy by removing it. An earlier
  // measurement of +4-12% was an artifact: that build incremented an AtomicLong per slot
  // scanned, and the comparison arm skipped the counters along with the walk.
  private void refreshMarkerSlots(IStorage storage) {
    for (;;) {
      NodeState<Address> s = _state;
      BufState b = s.buf;
      Object[] slots = (b != null) ? b.slots : null;
      if (slots == null || s.children == null) return;
      Object[] ns = null;
      boolean poisoned = false;
      for (int i = 0; i < _len; i++) {
        Slot sl = (Slot) slots[i];
        if (sl == null || sl.diff != null || sl.anchor == null) continue;   // marker slots only
        if (sl.bufEntries == BUF_WRITE) continue;                            // already poisoned
        Object ref = s.children[i];
        if (ref == null) continue;                                           // not resident
        ANode c = (ANode) _settings.readReference(ref);
        if (!(c instanceof Branch)) continue;
        ((Branch) c).refreshMarkerSlots(storage);                            // post-order
        if (((Branch) c).bufEntries() == slotBE(sl)) continue;
        if (ns == null) ns = Arrays.copyOf(slots, slots.length);
        poisoned = true;
        ns[i] = new Slot(sl.diff, sl.count, sl.measure, sl.anchor, BUF_WRITE);
      }
      if (ns == null) return;
      long entries = poisoned ? BUF_WRITE : bufEntriesOf(b);
      if (STATE_UPDATER.compareAndSet(this, s, new NodeState<>(s.addresses, s.children, new BufState(ns, entries)))) return;
    }
  }

  private long bufEntriesSlow(IStorage storage) {
    Object[] slots = slots();                                   // one snapshot
    if (slots == null) return 0;
    long total = 0;
    for (int j = 0; j < _len; j++) {
      Slot sl = (Slot) slots[j];
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
    Object[] children = _state.children;                        // one snapshot
    if (children == null) return null; // no children loaded

    Object result = measureOps.identity();
    for (int i = 0; i < _len; i++) {
      Object raw = children[i];
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
   * Helper to get subtree count from an ANode, for the IN-MEMORY probe above ONLY.
   *
   * Returns -1 for a Branch whose count is unknown, rather than computing it. That is what
   * tryComputeSubtreeCountFromChildren documents ("or has unknown count") and what the
   * ClojureScript twin has always done; this side computed instead, and computeSubtreeCount
   * descends through child(storage, i), which RESTORES every node below.
   *
   * The probe bails at the first non-resident child, scanning left to right, so it only ran
   * to completion when a resident prefix reached the end — which `remove` arranges by
   * materialising idx-1, idx and idx+1. At root fanout 2 that is every child, so one disj on
   * a cold tree pulled in the whole thing. Measured, n=80000, a storage that does not persist
   * counts:
   *
   *   bf  16  root fanout   2   11426 of 11426 blobs restored   (100%)
   *   bf  64  root fanout   2    2580 of  2580 blobs restored   (100%)
   *   bf  32  root fanout  19     547 restored for a LEFT-edge delete, 10 for a middle one
   *   bf 512  root fanout 312       3-4 restored
   *
   * It is a one-time warmup, not an ongoing cost — the walk caches counts into the children it
   * visits, so of 20 successive deletes only the first paid (11425, then 0 x19). What it costs
   * is a latency spike on the first write after a restore and, worse, a resident set of the
   * WHOLE tree, which is exactly the bound `:ref-type` exists to enforce.
   *
   * Returning -1 does not lose the count: the caller stores -1, and PersistentSortedSet already
   * treats a negative root count as "unknown" and defers to count(), which computes and caches
   * on demand. So the work moves to whoever actually asks for a count, instead of every delete.
   */
  private static long getSubtreeCount(ANode node, IStorage storage) {
    if (node instanceof ISubtreeCount) {
      long count = ((ISubtreeCount) node).subtreeCount();
      if (count >= 0) return count;
      // Branch with unknown count: propagate unknown. Computing here would restore the subtree.
      if (node instanceof Branch) return -1;
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

  // OWNER-THREAD ONLY (editable transient or not-yet-published node): make sure the
  // current snapshot carries a children array, allocating + republishing if absent.
  // Callers must use the RETURN VALUE. Never call on a possibly-shared node.
  protected Object[] ensureChildren() {
    NodeState<Address> s = _state;
    if (s.children == null) {
      Object[] children = new Object[_keys.length];
      _state = new NodeState<>(s.addresses, children, s.buf);
      return children;
    }
    return s.children;
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
    // ONE snapshot for the whole method: every copy below reads addresses, children and
    // buf from s0, so a concurrent settle on this shared node can never be observed torn
    // (cache-fills CASed in after this read are simply not seen — state 3 stays state 3).
    final NodeState<Address> s0 = _state;
    ANode oldChild = child(storage, ins);
    long oldChildCount = ((ISubtreeCount) oldChild).subtreeCount();
    // diff-buf: capture child ins's durable address BEFORE the mutation nulls it,
    // so a deposit at this level can record it as the buffer anchor.
    Object anchor0 = (_settings.diffBufSize() > 0 && s0.addresses != null) ? s0.addresses[ins] : null;
    ANode[] nodes = oldChild.add(storage, key, cmp, settings);

    if (PersistentSortedSet.UNCHANGED == nodes) { // child signalling already in set
      return PersistentSortedSet.UNCHANGED;
    }

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
      // Editable in-place path: exactly one element added — but ONLY without a leafProcessor.
      // A processor may compact or expand the leaf it is handed, so adding one KEY need not
      // raise the element count by one, and EARLY_EXIT does not mean "the processor did not
      // fire" — a level-1 Branch handles the processor correctly and STILL returns EARLY_EXIT
      // to its parent, which then applied +1 blindly. Same distinction the remove arms at
      // :1223/:1279 make. Reachable only at level >= 2 with a transient, which is why the
      // suite was green; measured before this guard, bf 8 / n 200 / conj:
      //     set count 221   seq count 220   drift level 3 221/220, level 2 85/84
      // and it is DURABLE — node->identity excludes :subtree-count, so the address does not
      // change and the wrong count travels in the blob, invisible to a merkle audit.
      //
      // Shaped as if/else rather than the ternary the remove arms use, so that the
      // processor-free path is EXACTLY what it was: delta when the count is known, and
      // nothing at all when it is unknown. Folding "unknown" into the probe would make
      // every such add walk the children — the very cost 04499a0 removed.
      if (_settings.leafProcessor() == null) {
        if (_subtreeCount >= 0) _subtreeCount += 1;
      } else {
        _subtreeCount = tryComputeSubtreeCountFromChildren(s0.children, _len, storage);
      }
      // Update measure: recompute from children (child's stats were updated in place)
      IMeasure measureOps = _settings.measure();
      if (measureOps != null && _measure != null) {
        _measure = tryComputeMeasure(storage);
      }
      // The child was mutated IN PLACE, so this node's addresses[ins] — which asserts
      // "that child's whole subtree is already durable" — is now a LIE. store() reads it
      // exactly that way (baseline Pass at :2073 `if (newAddresses[i] == null)`, diff-buf
      // Pass 1 at :2138 `if (newAddresses[i] != null) continue`), so without this clear the
      // subtree is skipped at every depth and the mutation NEVER REACHES DISK.
      //
      // Reachable from the public API: `store` a live transient, mutate it further, `store`
      // again — the checkpointed bulk-ingest shape. Measured before this clear, second store
      // silently missing the second batch:
      //     bf 64 dbs 0   level 1   in-mem 220  reloaded 219  missing [21]
      //     bf  8 dbs 0   level 3   in-mem 1220 reloaded 1219 missing [21]
      //     bf  8 dbs 256 level 2   in-mem 218  reloaded 219  extra   [5]
      // Baseline loses at every level; under diff-buf level 1 is masked because the slot
      // carries the real leaf-diff, while level >= 2 deposits a MARKER (diff == null) whose
      // content lives only in the live child, so the loss returns.
      //
      // Every other mutation path already clears it — child(int,ANode) at :328-337,
      // newAddresses[ins] = null at :749 and :1655, as.copyOne(null) in each rebuild stitch.
      // The EARLY_EXIT arms were the only ones that did not, because they install no new
      // node. `anchor0` was captured above, so under diff-buf the child now classifies as
      // dirty and Pass 2 re-points the address to that anchor.
      //
      // Free the address we are about to clear. The two pre-existing clear sites do this
      // (see the `_settings.diffBufSize() <= 0 && ...markFreed` blocks around child(idx,node)
      // in the non-EARLY_EXIT arms); these three did not, so a checkpointed transient left
      // blobs that were unreachable AND never reported freed. Measured over 40 checkpoint
      // rounds at bf 8 / dbs 0: disk 242, reachable 82, freed-reported 99, ORPHANS 61
      // (levels {2 -> 37, 1 -> 24}). Content was correct — this is unbounded storage growth
      // for any consumer that treats the freed stream as its GC candidate list, which
      // datahike does.
      //
      // GATED on diffBufSize <= 0, exactly as the other sites are: under diff-buf the old
      // address is re-pointed as the buffered anchor at store, so freeing it here would free
      // a LIVE node. That gate is also why the measurement only shows orphans at dbs 0.
      if (_settings.diffBufSize() <= 0 && storage != null
          && s0.addresses != null && s0.addresses[ins] != null) {
        storage.markFreed(s0.addresses[ins]);
      }
      child(ins, oldChild);  // clears addresses[ins] AND unwraps the child (a dirty child must be bare)
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
      if (_settings.diffBufSize() <= 0 && storage != null && s0.addresses != null && s0.addresses[ins] != null) {
        storage.markFreed(s0.addresses[ins]);
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
      // Baseline: the successor branch supersedes the old durable child at ins — free it
      // (mirrors the editable in-place hook above; per-level copy-unwind supersession).
      // diff-buf: DEFERRED to store (the old address is re-pointed as the buffered anchor).
      if (_settings.diffBufSize() <= 0 && storage != null && s0.addresses != null && s0.addresses[ins] != null) {
        storage.markFreed(s0.addresses[ins]);
      }
      // Always copy arrays — sharing them would allow a later transient
      // editable() path to mutate the original persistent branch's arrays.
      // Both copies come from s0 — the pair is coherent by construction.
      Key[] newKeys = Arrays.copyOfRange(_keys, 0, _len);
      newKeys[ins] = node.maxKey();

      Address[] newAddresses = null;
      if (s0.addresses != null) {
        newAddresses = Arrays.copyOfRange(s0.addresses, 0, _len);
        newAddresses[ins] = null;
      }

      Object[] newChildren = s0.children == null ? new Object[_keys.length] : Arrays.copyOfRange(s0.children, 0, _len);
      newChildren[ins] = node;

      // Exact subtree count using delta from old vs new child
      long newCount = (_subtreeCount >= 0 && oldChildCount >= 0 && newChildrenCount >= 0)
          ? _subtreeCount - oldChildCount + newChildrenCount : -1;
      Object newMeasure = tryComputeMeasureFromChildren(newChildren, _len, storage, measureOps);
      Branch<Key, Address> nb = new Branch(_level, _len, newKeys, newAddresses, newChildren, newCount, newMeasure, _projCmp, settings);
      // The SAME snapshot's {slots, entries} pair — carried together so the successor
      // can never mix a pre-settle total with post-settle slots (or vice versa).
      if (settings.diffBufSize() > 0) nb.carryAndDeposit(storage, s0.buf, ins, key, key, anchor0); // content-only: Present(key) / branch marker
      return new ANode[]{ nb };
    }

    // nodes.length >= 2: replace 1 child with N children
    // Baseline: the split child's old durable blob is superseded by the N new nodes in BOTH
    // the absorb and split outcomes below — free it exactly once here. (diff-buf frees it in
    // its own gated branches below via freeDroppedChild, which also covers slot anchors.)
    if (_settings.diffBufSize() <= 0 && storage != null && s0.addresses != null && s0.addresses[ins] != null) {
      storage.markFreed(s0.addresses[ins]);
    }
    int extra = nodes.length - 1;
    int newLen = _len + extra;

    // Build full merged arrays (children and addresses both from s0 — pair-coherent)
    Key[] allKeys = (Key[]) new Object[newLen];
    Stitch ks = new Stitch(allKeys, 0);
    ks.copyAll(_keys, 0, ins);
    for (int i = 0; i < nodes.length; i++) ks.copyOne(nodes[i].maxKey());
    ks.copyAll(_keys, ins + 1, _len);

    Object[] allChildren = new Object[newLen];
    Stitch cs = new Stitch(allChildren, 0);
    Object[] existingChildren = s0.children != null ? s0.children : new Object[_keys.length];
    cs.copyAll(existingChildren, 0, ins);
    for (int i = 0; i < nodes.length; i++) cs.copyOne(nodes[i]);
    cs.copyAll(existingChildren, ins + 1, _len);

    Address[] allAddresses = null;
    if (s0.addresses != null) {
      allAddresses = (Address[]) new Object[newLen];
      Stitch as = new Stitch(allAddresses, 0);
      as.copyAll(s0.addresses, 0, ins);
      for (int i = 0; i < nodes.length; i++) as.copyOne(null);
      as.copyAll(s0.addresses, ins + 1, _len);
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
        // absorbed a child split: structural → written (BUF_WRITE), but it still buffers
        // surviving siblings — carry their slots through the rebuild. nb is unpublished,
        // so this single publish installs the consistent {slots, BUF_WRITE} pair.
        nb.installSlots(stitchSlots(s0, ins, nodes.length, newLen), BUF_WRITE);
        freeDroppedChild(storage, s0, ins); // diff-buf: free the split child's old blob (replaced by N new nodes)
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
      // split: structural → written (BUF_WRITE), still buffer surviving siblings' slots.
      // sb1/sb2 are unpublished: one consistent publish each.
      freeDroppedChild(storage, s0, ins); // diff-buf: free the split child's old blob (replaced by N new nodes)
      Object[] all = stitchSlots(s0, ins, nodes.length, newLen); // carry buffered siblings' slots through the split
      sb1.installSlots(all != null ? Arrays.copyOfRange(all, 0, half1) : null, BUF_WRITE);
      sb2.installSlots(all != null ? Arrays.copyOfRange(all, half1, newLen) : null, BUF_WRITE);
    }
    return new ANode[]{ sb1, sb2 };
  }

  @Override
  public ANode[] remove(IStorage storage, Key key, ANode _left, ANode _right, Comparator<Key> cmp, Settings settings) {
    Branch left = (Branch) _left;
    Branch right = (Branch) _right;

    int idx = searchFirst(key, cmp);   // D1

    if (idx == _len) // not in set
      return PersistentSortedSet.UNCHANGED;

    assert 0 <= idx && idx < _len;

    ANode leftChild  = idx > 0      ? child(storage, idx - 1) : null,
          rightChild = idx < _len-1 ? child(storage, idx + 1) : null;
    int leftChildLen = safeLen(leftChild);
    int rightChildLen = safeLen(rightChild);
    // Ask the child to REPORT the element it removed, so the diff-buf deposits below record
    // Absent(<what the leaf actually held>) rather than Absent(<the caller's search key>).
    // Only at level 1: above it the slot is a branch marker whose diff is null, so the key
    // is unused. See ANode's six-arg `remove` for the measured failure.
    Object[] removedOut = (_settings.diffBufSize() > 0 && _level == 1) ? new Object[1] : null;
    ANode<Key, Address> mutatedChild = child(storage, idx);
    ANode[] nodes = mutatedChild.remove(storage, key, leftChild, rightChild, cmp, settings, removedOut);
    @SuppressWarnings("unchecked")
    Key removedKey = (removedOut != null && removedOut[0] != null) ? (Key) removedOut[0] : key;

    if (PersistentSortedSet.UNCHANGED == nodes) // child signalling element not in set
      return PersistentSortedSet.UNCHANGED;

    // ONE snapshot per source node for everything below (this + the passed siblings):
    // all address/children/slot copies read these, so a concurrent settle on a shared
    // source can never be observed torn. (Taken after the child() materializations so
    // the snapshot carries their cache-fills where this node is the owner.)
    final NodeState<Address> s0 = _state;
    final NodeState<Address> ls = (left  != null) ? left._state  : null;
    final NodeState<Address> rs = (right != null) ? right._state : null;
    // diff-buf: child idx's durable anchor (this method's own writes null it only later).
    Object anchor0 = (_settings.diffBufSize() > 0 && s0.addresses != null) ? s0.addresses[idx] : null;

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // child signalling nothing to update
      // Editable in-place path: exactly one element removed — but ONLY without a
      // leafProcessor, for the reason spelled out in the `add` EARLY_EXIT arm above.
      // Measured before this guard, bf 8 / n 200 / disj:
      //     set count 179   seq count 178   drift level 3 179/178, level 2 75/74
      // if/else, not a ternary — see the `add` arm: the processor-free path must stay a
      // pure delta-or-nothing and must not fall into the children walk when unknown.
      if (_settings.leafProcessor() == null) {
        if (_subtreeCount >= 0) _subtreeCount -= 1;
      } else {
        _subtreeCount = tryComputeSubtreeCountFromChildren(s0.children, _len, storage);
      }
      // Update measure: recompute from children (child's stats were updated in place)
      IMeasure measureOps = _settings.measure();
      if (measureOps != null && _measure != null) {
        _measure = tryComputeMeasure(storage);
      }
      // NOTE `key` here is the caller's SEARCH key, not the element the leaf removed.
      // Under a coarse operation comparator those differ, and this is the same shape as
      // the `replace` deposit defect fixed in this cycle: `projectLeaf` replays the diff
      // under the SET's comparator, so `Absent(<search key>)` would cancel nothing and the
      // removal would be lost on reload.
      //
      // NOT corrected, deliberately — but the earlier claim here that `disj` "produced no
      // slot at all" was WRONG and is corrected: a 40-element set at bf 8 with
      // diff-buf 256 DOES deposit, and this line does run. What has not been shown is any
      // resulting data loss. At level > 1 the slot is a branch marker whose diff is null,
      // so the key is unused; at level 1 no construction tried has produced a wrong
      // reload. Attempts: conj-built and bulk-built, bf 4/8/64, 40 and 400 elements,
      // 2- and 3-level trees, persistent and transient, cold-restored with the storage's
      // node settings carrying the same budget as the set, and searching with a COARSE
      // operation comparator whose key is not equal to the stored element — every reload
      // matched memory exactly.
      //
      // So this is a latent mismatch, not a demonstrated defect. Threading a
      // removed-element channel through `remove` the way `replace` has one is the fix if
      // it is ever shown to bite; doing it blind would be a signature change on a path
      // with no test to hold it, which is the trade that put the last two defects here.
      //
      // If a construction is ever found that makes `disj` deposit, fix this first and
      // treat the reproduction as the regression test.
      // The child was mutated IN PLACE, so this node's addresses[idx] — which asserts
      // "that child's whole subtree is already durable" — is now a LIE. store() reads it
      // exactly that way (baseline Pass at :2073 `if (newAddresses[i] == null)`, diff-buf
      // Pass 1 at :2138 `if (newAddresses[i] != null) continue`), so without this clear the
      // subtree is skipped at every depth and the mutation NEVER REACHES DISK.
      //
      // Reachable from the public API: `store` a live transient, mutate it further, `store`
      // again — the checkpointed bulk-ingest shape. Measured before this clear, second store
      // silently missing the second batch:
      //     bf 64 dbs 0   level 1   in-mem 220  reloaded 219  missing [21]
      //     bf  8 dbs 0   level 3   in-mem 1220 reloaded 1219 missing [21]
      //     bf  8 dbs 256 level 2   in-mem 218  reloaded 219  extra   [5]
      // Baseline loses at every level; under diff-buf level 1 is masked because the slot
      // carries the real leaf-diff, while level >= 2 deposits a MARKER (diff == null) whose
      // content lives only in the live child, so the loss returns.
      //
      // Every other mutation path already clears it — child(int,ANode) at :328-337,
      // newAddresses[ins] = null at :749 and :1655, as.copyOne(null) in each rebuild stitch.
      // The EARLY_EXIT arms were the only ones that did not, because they install no new
      // node. `anchor0` was captured above, so under diff-buf the child now classifies as
      // dirty and Pass 2 re-points the address to that anchor.
      //
      // Free the address we are about to clear. The two pre-existing clear sites do this
      // (see the `_settings.diffBufSize() <= 0 && ...markFreed` blocks around child(idx,node)
      // in the non-EARLY_EXIT arms); these three did not, so a checkpointed transient left
      // blobs that were unreachable AND never reported freed. Measured over 40 checkpoint
      // rounds at bf 8 / dbs 0: disk 242, reachable 82, freed-reported 99, ORPHANS 61
      // (levels {2 -> 37, 1 -> 24}). Content was correct — this is unbounded storage growth
      // for any consumer that treats the freed stream as its GC candidate list, which
      // datahike does.
      //
      // GATED on diffBufSize <= 0, exactly as the other sites are: under diff-buf the old
      // address is re-pointed as the buffered anchor at store, so freeing it here would free
      // a LIVE node. That gate is also why the measurement only shows orphans at dbs 0.
      if (_settings.diffBufSize() <= 0 && storage != null
          && s0.addresses != null && s0.addresses[idx] != null) {
        storage.markFreed(s0.addresses[idx]);
      }
      child(idx, mutatedChild);  // clears addresses[idx] AND unwraps the child (a dirty child must be bare)
      if (_settings.diffBufSize() > 0) depositInto(storage, idx, removedKey, Slot.ABSENT, anchor0); // content-only: Absent(removedKey) / branch marker
      return PersistentSortedSet.EARLY_EXIT;
    }

    // Child.remove() always returns exactly 3 elements: [left, center, right].
    // The processor cannot expand during remove (asserted in Leaf.remove), so
    // the center is always a single node and this convention is safe.
    assert nodes.length == 3 : "child.remove() must return exactly 3 elements, got " + nodes.length;
    boolean leftChanged = leftChild != nodes[0] || leftChildLen != safeLen(nodes[0]);
    boolean rightChanged = rightChild != nodes[2] || rightChildLen != safeLen(nodes[2]);

    // Baseline: child idx was superseded by this remove, and the left/right siblings'
    // durable versions are superseded iff the rebalance consumed/changed them — on EVERY
    // outcome below (editable in-place, persistent rebuild, join, borrow). Free them here,
    // exactly once, before any path overwrites the addresses. (rightChanged implies
    // idx < _len - 1, so addresses[idx + 1] is in the valid range.)
    // diff-buf keeps its own per-path discipline: content-only defers to store (anchor
    // re-point); structural drops via freeDroppedChild in each branch below.
    if (_settings.diffBufSize() <= 0 && storage != null && s0.addresses != null) {
      if (s0.addresses[idx] != null) storage.markFreed(s0.addresses[idx]);
      if (leftChanged && idx > 0 && s0.addresses[idx - 1] != null) storage.markFreed(s0.addresses[idx - 1]);
      if (rightChanged && s0.addresses[idx + 1] != null) storage.markFreed(s0.addresses[idx + 1]);
    }

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
        // Baseline frees were handled by the uniform hook above (before the path dispatch).
        // diff-buf: the CONTENT-ONLY sub-case re-points addresses[idx] to the child's anchor
        // (deferred to store); a STRUCTURAL sub-case (merge/borrow) consumes idx / changed
        // siblings — never re-pointed — so free them now (store has no slot/anchor for the
        // materialized structural child, so it can't free them later → they would leak).
        if (storage != null && s0.addresses != null && _settings.diffBufSize() > 0
            && (leftChanged || rightChanged || newLen != _len)) { // diff-buf, structural
          freeDroppedChild(storage, s0, idx);
          if (leftChanged && idx > 0) freeDroppedChild(storage, s0, idx - 1);
          if (rightChanged) freeDroppedChild(storage, s0, idx + 1);
        }

        // Owner-thread in-place stitches: on an EDITABLE node s0's arrays ARE the live
        // arrays (unshared by contract), so writing into them is the plain transient path.
        Stitch ks = new Stitch(_keys, Math.max(idx-1, 0));
        if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                              ks.copyOne(nodes[1].maxKey());
        if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
        if (newLen != _len)
          ks.copyAll(_keys, idx+2, _len);

        if (s0.addresses != null) {
          Stitch as = new Stitch(s0.addresses, Math.max(idx - 1, 0));
          if (nodes[0] != null) as.copyOne(leftChanged ? null : s0.addresses[idx - 1]);
                                as.copyOne(null);
          if (nodes[2] != null) as.copyOne(rightChanged ? null : s0.addresses[idx + 1]);
          if (newLen != _len)
            as.copyAll(s0.addresses, idx+2, _len);
        }

        Object[] children = ensureChildren();
        Stitch cs = new Stitch(children, Math.max(idx - 1, 0));
        if (nodes[0] != null) cs.copyOne(nodes[0]);
                              cs.copyOne(nodes[1]);
        if (nodes[2] != null) cs.copyOne(nodes[2]);
        if (newLen != _len)
          cs.copyAll(children, idx+2, _len);

        // diff-buf: STRUCTURAL only — mirror the address Stitch. Build the stitched slots
        // array on a copy-on-write of one snapshot; published below TOGETHER with the
        // BUF_WRITE poison as a single consistent {slots, entries} pair. (Content-only keeps
        // the slot at idx so the deposit below accumulates.) `structural` is computed before
        // `_len = newLen` overwrites the comparison basis.
        boolean structural = leftChanged || rightChanged || newLen != _len;
        Object[] stitched = null;
        if (_settings.diffBufSize() > 0 && structural) {
          BufState rb = s0.buf;                                  // same snapshot as the copies
          if (rb != null && rb.slots != null) {
            stitched = Arrays.copyOf(rb.slots, rb.slots.length); // prefix [0, idx-1) already right
            Stitch ss = new Stitch(stitched, Math.max(idx - 1, 0));
            if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(rb.slots, idx - 1));
                                  ss.copyOne(null);
            if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(rb.slots, idx + 1));
            if (newLen != _len)
              ss.copyAll(rb.slots, idx+2, _len);
          }
        }

        // Hygiene only — this clear frees NOTHING measurable, and the commit that added it
        // (854c32e) claimed otherwise. Isolated afterwards: with the five Leaf clears kept
        // and this one removed, the retention test passes with 0 failures and the measured
        // numbers are bit-identical to having it. The reason is structural: the arm is
        // guarded `editable() && idx < _len-2`, so `copyAll(children, idx+2, _len)` always
        // shifts the tail LEFT, which makes every slot in [newLen, _len) a duplicate of one
        // still live in [0, newLen). An instrumented run over the whole suite saw 4123
        // executions and 4123 stale child slots, none of which held a node that was not
        // still referenced below newLen.
        //
        // Kept because it costs four fills on a path that already copies arrays, and because
        // "no shape was found where it matters" is weaker than a proof. But do NOT cite it
        // as the source of any retention number: those all come from Leaf.
        if (newLen < _len) {                                                       // TAILCLEAR
          Arrays.fill(_keys, newLen, _len, null);
          if (s0.addresses != null) Arrays.fill(s0.addresses, newLen, _len, null);
          Arrays.fill(children, newLen, _len, null);
          if (stitched != null) Arrays.fill(stitched, newLen, _len, null);
        }
        _len = newLen;
        // Compute exact subtree count from children (accounts for processor changes)
        // DELTA, not a recompute. `remove` deletes exactly one element, and in this arm no
        // child leaves this node (any merge/borrow was between ITS OWN children), so the
        // subtree total is exactly one less. The probe would instead bail to -1 the moment a
        // single child is non-resident — discarding a number we already know — and that -1 is
        // then SERIALIZED (`impl.nodes/node->map` writes `subtreeCount()` raw), so every later
        // reader of the blob pays a subtree walk to recover it.
        //
        // 04499a0 stopped the probe restoring subtrees just to count them, which was right, but
        // it made -1 the answer far more often and so degraded the on-disk counts. This closes
        // that without reintroducing any IO. It also converges the runtimes: ClojureScript has
        // always used the delta here (`branch.cljs`, `new-sc (if (>= old-sc 0) (dec old-sc) -1)`),
        // and a census over 2259 nodes found 93 disagreeing on `:subtree-count`, 93 of 93 being
        // "JVM -1, cljs exact" — never two different real values. cljs was right.
        // ONLY without a leafProcessor. A processor may compact or expand a leaf, so removing
        // one KEY need not reduce the element count by one — `PersistentSortedSet.disjoin`
        // makes the same distinction ("count may differ from +1"). With a processor, fall back
        // to the probe. Caught by leaf_processor/test-mixed-processor, which reported
        // :subtree-count-mismatch {:branch-count 139, :children-sum 137} when this was
        // unconditional.
        _subtreeCount = (_settings.leafProcessor() == null && _subtreeCount >= 0)
                        ? _subtreeCount - 1
                        : tryComputeSubtreeCountFromChildren(children, newLen, storage);
        // Update measure: recompute from children
        if (measureOps != null && _measure != null) {
          _measure = tryComputeMeasure(storage);
        }
        if (_settings.diffBufSize() > 0) {
          if (!structural) {
            depositInto(storage, idx, removedKey, Slot.ABSENT, anchor0); // content-only: Absent(removedKey) / branch marker
          } else {
            // a child merged/borrowed with a sibling: structural → write in full. Single
            // publish of the stitched slots + BUF_WRITE poison (owner thread; carries the
            // current arrays).
            installSlots(stitched, BUF_WRITE);
          }
        }
        return PersistentSortedSet.EARLY_EXIT;
      }

      Branch newCenter = new Branch(_level, newLen, _projCmp, settings);

      // All copies below read s0 — addresses, children and slots stay pair-coherent.
      final Object[] myChildren = s0.children != null ? s0.children : new Object[_keys.length];

      Stitch ks = new Stitch(newCenter._keys, 0);
      ks.copyAll(_keys, 0, idx - 1);
      if (nodes[0] != null) ks.copyOne(nodes[0].maxKey());
                            ks.copyOne(nodes[1].maxKey());
      if (nodes[2] != null) ks.copyOne(nodes[2].maxKey());
      ks.copyAll(_keys, idx + 2, _len);

      if (s0.addresses != null) {
        Stitch as = new Stitch(newCenter.ensureAddresses(), 0);
        as.copyAll(s0.addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : s0.addresses[idx - 1]);
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : s0.addresses[idx + 1]);
        as.copyAll(s0.addresses, idx + 2, _len);
      }

      Object[] centerChildren = newCenter.ensureChildren();
      Stitch cs = new Stitch(centerChildren, 0);
      cs.copyAll(myChildren, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(myChildren, idx + 2, _len);

      // Compute exact subtree count from children (accounts for processor changes)
      // DELTA, not a recompute — same reasoning as the in-place arm above. `newCenter` covers
      // exactly this node's key range minus the one removed element: any merge/borrow here was
      // between THIS node's own children (newLen may shrink, but no element left the subtree).
      // The probe would bail to -1 at the first non-resident child and discard a number we
      // already know, and that -1 is serialized.
      // ONLY without a leafProcessor — see the in-place arm above.
      newCenter._subtreeCount = (_settings.leafProcessor() == null && _subtreeCount >= 0)
                                ? _subtreeCount - 1
                                : tryComputeSubtreeCountFromChildren(centerChildren, newLen, storage);
      newCenter._measure = tryComputeMeasureFromChildren(centerChildren, newLen, storage, measureOps);
      if (settings.diffBufSize() > 0) {
        if (!leftChanged && !rightChanged && newLen == _len) {
          // content-only: carry the SAME snapshot's {slots, entries} pair aligned and
          // ACCUMULATE Absent onto the center's existing diff (it may already hold
          // buffered Present/Absent for this leaf).
          newCenter.carryAndDeposit(storage, s0.buf, idx, removedKey, Slot.ABSENT, anchor0);
        } else {
          // structural: mirror the address Stitch; newCenter is unpublished, so one publish
          // installs the consistent {slots, BUF_WRITE} pair.
          // diff-buf: free this node's dropped children (consumed into the new structure,
          // never re-pointed) so they don't leak — store has no slot/anchor for them.
          freeDroppedChild(storage, s0, idx);
          if (leftChanged && idx > 0) freeDroppedChild(storage, s0, idx - 1);
          if (rightChanged) freeDroppedChild(storage, s0, idx + 1);
          final Object[] mySlots = (s0.buf != null) ? s0.buf.slots : null;
          Object[] ns = new Object[newCenter._keys.length];    // (center/changed siblings materialized → null slot)
          Stitch ss = new Stitch(ns, 0);
          slotCopyAll(ss, mySlots, 0, idx - 1);
          if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(mySlots, idx - 1));
                                ss.copyOne(null);
          if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(mySlots, idx + 1));
          slotCopyAll(ss, mySlots, idx + 2, _len);
          newCenter.installSlots(ns, BUF_WRITE);
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

      if (ls.addresses != null || s0.addresses != null) {
        Stitch as = new Stitch(join.ensureAddresses(), 0);
        as.copyAll(ls.addresses, 0, left._len);
        as.copyAll(s0.addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : addrAt(s0, idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : addrAt(s0, idx + 1));
        as.copyAll(s0.addresses, idx + 2, _len);
      }

      Object[] joinChildren = join.ensureChildren();
      Stitch cs = new Stitch(joinChildren, 0);
      cs.copyAll(ls.children, 0, left._len);
      cs.copyAll(s0.children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(s0.children, idx + 2, _len);

      // Compute exact subtree count from children (accounts for processor changes)
      join._subtreeCount = tryComputeSubtreeCountFromChildren(joinChildren, left._len + newLen, storage);
      join._measure = tryComputeMeasureFromChildren(joinChildren, left._len + newLen, storage, measureOps);
      if (settings.diffBufSize() > 0) {
        // merged with left: structural → written (BUF_WRITE), still buffers surviving
        // siblings. join is unpublished: one consistent publish.
        freeDroppedChild(storage, s0, idx);                    // diff-buf: free dropped (merged) children
        if (leftChanged && idx > 0) freeDroppedChild(storage, s0, idx - 1);
        if (rightChanged) freeDroppedChild(storage, s0, idx + 1);
        final Object[] mySlots = (s0.buf != null) ? s0.buf.slots : null,
                     leftSlots = (ls.buf != null) ? ls.buf.slots : null; // same snapshots as the copies above
        Object[] ns = new Object[join._keys.length];           // mirror the address Stitch above
        Stitch ss = new Stitch(ns, 0);
        slotCopyAll(ss, leftSlots, 0, left._len);
        slotCopyAll(ss, mySlots,   0, idx - 1);
        if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(mySlots, idx - 1));
                              ss.copyOne(null);
        if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(mySlots, idx + 1));
        slotCopyAll(ss, mySlots, idx + 2, _len);
        join.installSlots(ns, BUF_WRITE);
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

      if (s0.addresses != null || rs.addresses != null) {
        Stitch as = new Stitch(join.ensureAddresses(), 0);
        as.copyAll(s0.addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : addrAt(s0, idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : addrAt(s0, idx + 1));
        as.copyAll(s0.addresses, idx + 2, _len);
        as.copyAll(rs.addresses, 0, right._len);
      }

      Object[] joinChildren = join.ensureChildren();
      Stitch cs = new Stitch(joinChildren, 0);
      cs.copyAll(s0.children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(s0.children,  idx + 2, _len);
      cs.copyAll(rs.children, 0, right._len);

      // Compute exact subtree count from children (accounts for processor changes)
      join._subtreeCount = tryComputeSubtreeCountFromChildren(joinChildren, newLen + right._len, storage);
      join._measure = tryComputeMeasureFromChildren(joinChildren, newLen + right._len, storage, measureOps);
      if (settings.diffBufSize() > 0) {
        // merged with right: structural → written (BUF_WRITE), still buffers surviving
        // siblings. join is unpublished: one consistent publish.
        freeDroppedChild(storage, s0, idx);                    // diff-buf: free dropped (merged) children
        if (leftChanged && idx > 0) freeDroppedChild(storage, s0, idx - 1);
        if (rightChanged) freeDroppedChild(storage, s0, idx + 1);
        final Object[] mySlots = (s0.buf != null) ? s0.buf.slots : null,
                    rightSlots = (rs.buf != null) ? rs.buf.slots : null; // same snapshots as the copies above
        Object[] ns = new Object[join._keys.length];           // mirror the address Stitch above
        Stitch ss = new Stitch(ns, 0);
        slotCopyAll(ss, mySlots, 0, idx - 1);
        if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(mySlots, idx - 1));
                              ss.copyOne(null);
        if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(mySlots, idx + 1));
        slotCopyAll(ss, mySlots, idx + 2, _len);
        slotCopyAll(ss, rightSlots, 0, right._len);
        join.installSlots(ns, BUF_WRITE);
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

      if (ls.addresses != null) {
        ArrayUtil.copy(ls.addresses, 0, newLeftLen, newLeft.ensureAddresses(), 0);
      }
      if (ls.children != null) {
        ArrayUtil.copy(ls.children, 0, newLeftLen, newLeft.ensureChildren(), 0);
      }

      if (ls.addresses != null || s0.addresses != null) {
        Stitch as = new Stitch(newCenter.ensureAddresses(), 0);
        as.copyAll(ls.addresses, newLeftLen, left._len);
        as.copyAll(s0.addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : addrAt(s0, idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : addrAt(s0, idx + 1));
        as.copyAll(s0.addresses, idx + 2, _len);
      }

      Object[] centerChildren = newCenter.ensureChildren();
      Stitch cs = new Stitch(centerChildren, 0);
      cs.copyAll(ls.children, newLeftLen, left._len);
      cs.copyAll(s0.children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(s0.children, idx + 2, _len);

      Object[] newLeftChildren = newLeft._state.children;
      if (newLeftChildren != null) {
        newLeft._subtreeCount = tryComputeSubtreeCountFromChildren(newLeftChildren, newLeftLen, storage);
        newLeft._measure = tryComputeMeasureFromChildren(newLeftChildren, newLeftLen, storage, measureOps);
      }
      newCenter._subtreeCount = tryComputeSubtreeCountFromChildren(centerChildren, newCenterLen, storage);
      newCenter._measure = tryComputeMeasureFromChildren(centerChildren, newCenterLen, storage, measureOps);
      if (settings.diffBufSize() > 0) {
        // borrowed from left: structural → BUF_WRITE. newLeft/newCenter are unpublished:
        // one consistent {slots, entries} publish each.
        freeDroppedChild(storage, s0, idx);                      // diff-buf: free dropped (rebalanced) children
        if (leftChanged && idx > 0) freeDroppedChild(storage, s0, idx - 1);
        if (rightChanged) freeDroppedChild(storage, s0, idx + 1);
        final Object[] mySlots = (s0.buf != null) ? s0.buf.slots : null,
                     leftSlots = (ls.buf != null) ? ls.buf.slots : null; // same snapshots as the copies above
        Object[] nl = null;
        if (leftSlots != null) {                                 // newLeft keeps left's first newLeftLen slots
          nl = new Object[newLeft._keys.length];
          ArrayUtil.copy(leftSlots, 0, newLeftLen, nl, 0);
        }
        newLeft.installSlots(nl, BUF_WRITE);
        Object[] nc = new Object[newCenter._keys.length];        // mirror the newCenter address Stitch above
        Stitch ss = new Stitch(nc, 0);
        slotCopyAll(ss, leftSlots, newLeftLen, left._len);
        slotCopyAll(ss, mySlots, 0, idx - 1);
        if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(mySlots, idx - 1));
                              ss.copyOne(null);
        if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(mySlots, idx + 1));
        slotCopyAll(ss, mySlots, idx + 2, _len);
        newCenter.installSlots(nc, BUF_WRITE);
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

      if (s0.addresses != null || rs.addresses != null) {
        Stitch as = new Stitch(newCenter.ensureAddresses(), 0);
        as.copyAll(s0.addresses, 0, idx - 1);
        if (nodes[0] != null) as.copyOne(leftChanged ? null : addrAt(s0, idx - 1));
                              as.copyOne(null);
        if (nodes[2] != null) as.copyOne(rightChanged ? null : addrAt(s0, idx + 1));
        as.copyAll(s0.addresses, idx + 2, _len);
        as.copyAll(rs.addresses, 0, rightHead);
      }

      Object[] centerChildren = newCenter.ensureChildren();
      Stitch cs = new Stitch(centerChildren, 0);
      cs.copyAll(s0.children, 0, idx - 1);
      if (nodes[0] != null) cs.copyOne(nodes[0]);
                            cs.copyOne(nodes[1]);
      if (nodes[2] != null) cs.copyOne(nodes[2]);
      cs.copyAll(s0.children, idx + 2, _len);
      cs.copyAll(rs.children, 0, rightHead);

      if (rs.addresses != null) {
        ArrayUtil.copy(rs.addresses, rightHead, right._len, newRight.ensureAddresses(), 0);
      }
      if (rs.children != null) {
        ArrayUtil.copy(rs.children, rightHead, right._len, newRight.ensureChildren(), 0);
      }

      newCenter._subtreeCount = tryComputeSubtreeCountFromChildren(centerChildren, newCenterLen, storage);
      newCenter._measure = tryComputeMeasureFromChildren(centerChildren, newCenterLen, storage, measureOps);
      Object[] newRightChildren = newRight._state.children;
      if (newRightChildren != null) {
        newRight._subtreeCount = tryComputeSubtreeCountFromChildren(newRightChildren, newRightLen, storage);
        newRight._measure = tryComputeMeasureFromChildren(newRightChildren, newRightLen, storage, measureOps);
      }
      if (settings.diffBufSize() > 0) {
        // borrowed from right: structural → BUF_WRITE. newCenter/newRight are unpublished:
        // one consistent {slots, entries} publish each.
        freeDroppedChild(storage, s0, idx);                      // diff-buf: free dropped (rebalanced) children
        if (leftChanged && idx > 0) freeDroppedChild(storage, s0, idx - 1);
        if (rightChanged) freeDroppedChild(storage, s0, idx + 1);
        final Object[] mySlots = (s0.buf != null) ? s0.buf.slots : null,
                    rightSlots = (rs.buf != null) ? rs.buf.slots : null; // same snapshots as the copies above
        Object[] nc = new Object[newCenter._keys.length];        // mirror the newCenter address Stitch above
        Stitch ss = new Stitch(nc, 0);
        slotCopyAll(ss, mySlots, 0, idx - 1);
        if (nodes[0] != null) ss.copyOne(leftChanged ? null : slotAt(mySlots, idx - 1));
                              ss.copyOne(null);
        if (nodes[2] != null) ss.copyOne(rightChanged ? null : slotAt(mySlots, idx + 1));
        slotCopyAll(ss, mySlots, idx + 2, _len);
        slotCopyAll(ss, rightSlots, 0, rightHead);
        newCenter.installSlots(nc, BUF_WRITE);
        Object[] nr = null;
        if (rightSlots != null) {                                // newRight keeps right's tail slots
          nr = new Object[newRight._keys.length];
          ArrayUtil.copy(rightSlots, rightHead, right._len, nr, 0);
        }
        newRight.installSlots(nr, BUF_WRITE);
      }
      return new ANode[] { left, newCenter, newRight };
    }

    throw new RuntimeException("Unreachable");
  }

  @Override
  public ANode removeContent(IStorage storage, Key key, Comparator<Key> cmp, Settings settings) {
    int idx = searchFirst(key, cmp);   // D1
    if (idx == _len) return null; // key greater than everything → not present

    ANode oldChild = child(storage, idx);
    ANode newChild = oldChild.removeContent(storage, key, cmp, settings);
    if (newChild == null) return null; // not present below

    IBoundary boundary = settings.boundary();
    IMeasure measureOps = settings.measure();

    // The removed key was a boundary between child idx and idx+1 (at the children's level)
    // iff keyLevel(key) >= this branch's level. Then those two children merge.
    boolean mergeRight = idx < _len - 1 && boundary.keyLevel(key, settings) >= _level;

    // Preserve UNCHANGED children's durable addresses (so store() doesn't re-serialize whole
    // subtrees on a remove); the changed/merged child gets a null address (re-stored), and the
    // dropped/consumed old children are markFreed (mirrors the count remove's GC). addresses
    // null ⇒ in-memory, keep it null.
    // ONE snapshot for all address reads below (the child() calls resolve to bare ANodes,
    // so every emitted slot is coherent whatever a concurrent settle does).
    final NodeState<Address> s0 = _state;
    boolean hasAddr = s0.addresses != null;
    Object[] newChildren;
    Address[] newAddresses = null;
    if (newChild instanceof Leaf && newChild._len == 0) {
      // Child's whole subtree is gone (it collapsed to an empty Leaf — note its level no
      // longer matches its siblings, so it can never participate in a merge): drop it. This
      // subsumes the mergeRight case, since merging an empty node with its sibling yields
      // exactly that sibling.
      newChildren = new Object[_len - 1];
      if (hasAddr) newAddresses = (Address[]) new Object[_len - 1];
      for (int i = 0; i < idx; i++)        { newChildren[i] = child(storage, i);     if (hasAddr) newAddresses[i] = s0.addresses[i]; }
      for (int i = idx + 1; i < _len; i++) { newChildren[i - 1] = child(storage, i);  if (hasAddr) newAddresses[i - 1] = s0.addresses[i]; }
      freeDroppedChild(storage, s0, idx);
    } else if (mergeRight) {
      // merge newChild with the right sibling child (junction merges recursively down).
      ANode merged = newChild.mstMergeWith(child(storage, idx + 1), storage, settings);
      newChildren = new Object[_len - 1];
      if (hasAddr) newAddresses = (Address[]) new Object[_len - 1];
      for (int i = 0; i < idx; i++)            { newChildren[i] = child(storage, i);     if (hasAddr) newAddresses[i] = s0.addresses[i]; }
      newChildren[idx] = merged;                                      // merged is new ⇒ null address
      for (int i = idx + 2; i < _len; i++)     { newChildren[i - 1] = child(storage, i);  if (hasAddr) newAddresses[i - 1] = s0.addresses[i]; }
      freeDroppedChild(storage, s0, idx);
      freeDroppedChild(storage, s0, idx + 1);                         // both consumed into the merge
    } else {
      // no boundary removed at this level: just replace child idx
      newChildren = new Object[_len];
      if (hasAddr) { newAddresses = (Address[]) new Object[_len]; System.arraycopy(s0.addresses, 0, newAddresses, 0, _len); newAddresses[idx] = null; }
      for (int i = 0; i < _len; i++) newChildren[i] = child(storage, i);
      newChildren[idx] = newChild;
      freeDroppedChild(storage, s0, idx);
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
    wrapAddressedChildren(newChildren, newAddresses, n, settings);   // AFTER the probes read them
    return new Branch(_level, n, keys, newAddresses, newChildren, count, measure, _projCmp, settings);
  }

  @Override
  public ANode mstMergeWith(ANode right, IStorage storage, Settings settings) {
    Branch r = (Branch) right;
    IBoundary boundary = settings.boundary();
    IMeasure measureOps = settings.measure();

    // ONE snapshot per source node for the address reads below.
    final NodeState<Address> s0 = _state;
    final NodeState<Address> rst = r._state;
    boolean hasAddr = s0.addresses != null || rst.addresses != null;
    Object[] children;
    Address[] addrs = null;
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
      if (hasAddr) addrs = (Address[]) new Object[n];
      for (int i = 0; i < _len; i++)   { children[i] = child(storage, i);          if (hasAddr) addrs[i] = addrAt(s0, i); }
      for (int i = 0; i < r._len; i++) { children[_len + i] = r.child(storage, i);  if (hasAddr) addrs[_len + i] = (Address) addrAt(rst, i); }
    } else {
      ANode junction = child(storage, _len - 1).mstMergeWith(r.child(storage, 0), storage, settings);
      n = _len + r._len - 1;
      children = new Object[n];
      if (hasAddr) addrs = (Address[]) new Object[n];
      for (int i = 0; i < _len - 1; i++)  { children[i] = child(storage, i);              if (hasAddr) addrs[i] = addrAt(s0, i); }
      children[_len - 1] = junction;                                    // junction is new ⇒ null address
      for (int i = 1; i < r._len; i++)    { children[_len - 1 + i] = r.child(storage, i);  if (hasAddr) addrs[_len - 1 + i] = (Address) addrAt(rst, i); }
      // this's last child and r's first child were consumed into the junction
      freeDroppedChild(storage, s0, _len - 1);
      if (storage != null && addrAt(rst, 0) != null) storage.markFreed((Address) addrAt(rst, 0));
    }

    Key[] keys = (Key[]) new Object[n];
    for (int i = 0; i < n; i++) keys[i] = ((ANode<Key, Address>) children[i]).maxKey();
    long count = tryComputeSubtreeCountFromChildren(children, n, storage);
    Object measure = tryComputeMeasureFromChildren(children, n, storage, measureOps);
    wrapAddressedChildren(children, addrs, n, settings);             // AFTER the probes read them
    return new Branch(_level, n, keys, addrs, children, count, measure, _projCmp, settings);
  }

  /**
   * Wrap every rebuilt child that still has a durable address per `:ref-type`, exactly as the
   * count-mode settle in store() does.
   *
   * The MST rebuild paths (removeContent, mstMergeWith) copy each unchanged sibling forward as
   * `child(storage, i)` — a BARE ANode — while keeping its still-valid address, and nothing on
   * those paths ever called makeReference. `:ref-type :soft`/`:weak` therefore stopped bounding
   * the tree, because a bare strong child pins its WHOLE subtree: an unwrapped node near the
   * root makes everything beneath it unevictable regardless of how the rest is held.
   *
   * Measured, bf 8, n=20000, `:ref-type :soft`, 20 disj after a store and WITHOUT a second
   * store (a checkpoint re-wraps everything and hides it), before -> after:
   *
   *   MST     cleared    0 references, 1375 of 1375 nodes resident (100% pinned)
   *           after:   850 cleared,     525 of 1375 (38%)
   *   count   cleared 2565 references, 4033 of 6598 resident (61%), unchanged both ways
   *
   * A correction to an earlier version of this comment, which claimed "no Reference was created
   * anywhere in the tree" and cited `{:reference 4}`: that is false. A census on the unfixed
   * build found `{:bare-strong-with-address 126, :bare-strong-no-address 32, :reference 1216}` —
   * 1216 References did exist. What the MST paths unwrapped was 126 of them (~9%), and because
   * an eviction walk stops at a bare strong child, those 126 were enough to drive the CLEARABLE
   * count to zero. The pinning, not the reference count, is the defect.
   *
   * Only an addressed child may be wrapped: a cleared reference is recovered by restoring from
   * the address, so a freshly built child (null address — the removal's successor, or a merge
   * junction) must stay strong. Under `:ref-type :strong` makeReference returns the node itself,
   * so this is a no-op there.
   */
  private void wrapAddressedChildren(Object[] children, Address[] addresses, int n, Settings settings) {
    if (children == null || addresses == null) return;
    for (int i = 0; i < n; ++i) {
      if (addresses[i] != null && children[i] instanceof ANode) {
        // The SET's settings, not this node's. A node reconstructed by an IStorage carries the
        // STORAGE's Settings, and every in-tree storage builds them as `new Settings(bf, null, ..)`
        // whose null refType normalizes to SOFT. Reading `_settings` here therefore made
        // `:ref-type` on the set not govern this path in EITHER direction: measured MST, bf 8,
        // n=4000, 30 deletes -- a `:strong` set over default storage nodes got 87 SoftReferences
        // it never asked for (0 before), and a `:soft` set over strong-settings nodes got the wrap
        // not at all (87 bare-strong-with-address, i.e. inert). The successor Branch two lines
        // below is already built from `settings`; this now agrees with it.
        children[i] = settings.makeReference(children[i]);
      }
    }
  }

  /**
   * Must the parent's separator be refreshed because this child's max moved?
   *
   * BOTH tests are needed, and each alone has been wrong in this file:
   *
   *   - the OPERATION comparator (`cmp`) is wrong because it is deliberately coarser than the
   *     set's — datahike's upsert searches [e a _ _] — so it calls a value change "unchanged"
   *     and suppresses a propagation routing needs.
   *   - the element's `=` (`Util.equiv`) is wrong in the OPPOSITE direction: it fails when `=`
   *     is COARSER than the set's comparator. datahike's `equiv-datom` compares e/a/v while
   *     `cmp-datoms-eavt` orders by e/a/v/tx, so two datoms differing only in tx are `=` but
   *     not comparator-equal. Measured with an element type whose `=` ignores a field the set
   *     orders by, inside a transient: 9 unfindable at n=40 bf=4, 24 at n=100, 46 at n=3000 —
   *     identical to the numbers from before the `cmp` version was replaced, i.e. the same
   *     defect, reachable through a different door. 24 of them survived a store/restore,
   *     because the stale separator is a branch key and is serialized.
   *
   * So: propagate if the element CHANGED BY VALUE (content addressing needs the separator to
   * equal the child's max element) OR if it moved under the SET's comparator (routing needs
   * the separator to be in the right position). `_projCmp` is the set's comparator, seeded
   * unconditionally at `PersistentSortedSet.root()` and inherited by every successor; when it
   * is somehow absent we propagate rather than guess, which costs a spine rebuild and never
   * correctness.
   */
  private boolean separatorMoved(Key newMaxKey, Key oldSeparator) {
    if (!clojure.lang.Util.equiv(newMaxKey, oldSeparator)) return true;
    if (_projCmp == null) return true;
    return 0 != _projCmp.compare(newMaxKey, oldSeparator);
  }

  /**
   * -ea only: the cross-leaf half of `replace`'s no-equal-sibling precondition.
   *
   * `Leaf.replace` checks the neighbours INSIDE the leaf; this checks the two that sit
   * across a leaf boundary, which the leaf cannot see. Only meaningful at level 1, where
   * the children are leaves.
   */
  private boolean noEqualSiblingAcrossBoundary(IStorage storage, int idx, Key oldKey, Comparator<Key> cmp) {
    ANode<Key, Address> leaf = child(storage, idx);
    int j = leaf.search(oldKey, cmp);
    if (j < 0) return true;
    if (j == 0 && idx > 0) {
      ANode<Key, Address> prev = child(storage, idx - 1);
      if (prev._len > 0 && 0 == cmp.compare(prev._keys[prev._len - 1], oldKey)) return false;
    }
    if (j == leaf._len - 1 && idx < _len - 1) {
      ANode<Key, Address> next = child(storage, idx + 1);
      if (next._len > 0 && 0 == cmp.compare(next._keys[0], oldKey)) return false;
    }
    return true;
  }

  @Override
  public ANode[] replace(IStorage storage, Key oldKey, Key newKey, Comparator<Key> cmp, Settings settings) {
    assert 0 == cmp.compare(oldKey, newKey) : "oldKey and newKey must compare as equal (cmp.compare must return 0)";

    // Find which child contains the key
    int idx = searchFirst(oldKey, cmp);   // D1
    if (idx == _len) idx = _len - 1; // key might be in last child
    assert 0 <= idx && idx < _len;

    // ONE snapshot for the whole method (see add()): anchor, markFreed decision and both
    // array copies below read s0, so the pair can never be observed torn.
    final NodeState<Address> s0 = _state;
    // diff-buf: capture child idx's durable address before the mutation nulls it.
    Object anchor0 = (_settings.diffBufSize() > 0 && s0.addresses != null) ? s0.addresses[idx] : null;
    // diff-buf: the element this replace will actually REMOVE, captured before the
    // mutation removes it. Not the same thing as `oldKey`.
    //
    // `oldKey` is the caller's SEARCH key, matched under the operation comparator `cmp`,
    // which may be COARSER than the set's own (datahike's value-changing datom upsert
    // searches [e a _ _] and replaces the whole datom). The leaf-diff is keyed by the
    // SET's comparator, so `Absent(oldKey)` only cancels the element in the leaf when the
    // two happen to be equal under _projCmp — i.e. only while the leaf still holds the
    // element the caller searched for.
    //
    // After ONE buffered replace it no longer does: the leaf holds the previous
    // replacement, `Absent(oldKey)` matches nothing, and the new Present is ADDED
    // alongside the old one. Measured, 16 elements, two replaces of one key in a single
    // transient cycle, store, restore: [[5 1] [5 2]] for key 5, count 16 against a seq of
    // 17. In memory it looked right — the transient leaf is mutated in place, so nothing
    // projects the diff until a restore, and the corruption only surfaces on reload.
    //
    // Only at level 1 (leaf children) and only with buffering on: above level 1 the slot
    // is a branch anchor whose diff is null, so the key is unused, and at diffBufSize 0
    // there is no diff at all.
    // The CROSS-LEAF half of replace's no-equal-sibling precondition; `Leaf` checks the
    // in-leaf half. Assertion only -- it re-searches the leaf and may materialise a
    // sibling, so it is exactly the work the reporting overload below exists to avoid,
    // and it costs nothing once assertions are off.
    //
    // Needed because the two cmp-equal elements are NOT reliably in one leaf: measured at
    // bf 4, a set of [k 0] for k in 0..39 plus [5 7] splits as ... [[4 0] [5 0]] |
    // [[5 7] [6 0] [7 0]] ..., putting them either side of a boundary. A leaf-local check
    // alone silently passed that case, which is the case this precondition exists for.
    assert _level != 1 || noEqualSiblingAcrossBoundary(storage, idx, oldKey, cmp)
      : "replace(" + oldKey + " -> " + newKey + "): the ADJACENT leaf holds another element the"
      + " operation comparator calls equal, so which element is replaced is arbitrary and the"
      + " result may be UNSORTED. `replace` requires at most one cmp-equal element;"
      + " use disj+conj instead.";

    // Asking the child to REPORT what it removed, rather than searching it here first:
    // the leaf binary search is comparator-bound, and doing it twice cost +18% at bf 512,
    // +10% at bf 64 and +4% at bf 32 on a replace-heavy workload (see ANode). Only at level 1
    // and only with buffering on -- above level 1 the slot is a branch anchor whose diff
    // is null, so the key is unused, and at diffBufSize 0 there is no diff at all.
    Object[] removedOut = (_settings.diffBufSize() > 0 && _level == 1) ? new Object[1] : null;
    // Recursively replace in child
    ANode<Key, Address> mutatedChild = child(storage, idx);
    ANode[] nodes = mutatedChild.replace(storage, oldKey, newKey, cmp, settings, removedOut);
    @SuppressWarnings("unchecked")
    Key removedKey = (removedOut != null && removedOut[0] != null) ? (Key) removedOut[0] : oldKey;

    if (PersistentSortedSet.UNCHANGED == nodes) // key not found
      return PersistentSortedSet.UNCHANGED;

    if (PersistentSortedSet.EARLY_EXIT == nodes) { // replaced in transient child, no updates needed
      // Try to recompute measure from final state (after child replacement)
      IMeasure measureOps = settings.measure();
      if (measureOps != null && _measure != null) {
        _measure = tryComputeMeasure(storage);
      }
      // The child was mutated IN PLACE, so this node's addresses[idx] — which asserts
      // "that child's whole subtree is already durable" — is now a LIE. store() reads it
      // exactly that way (baseline Pass at :2073 `if (newAddresses[i] == null)`, diff-buf
      // Pass 1 at :2138 `if (newAddresses[i] != null) continue`), so without this clear the
      // subtree is skipped at every depth and the mutation NEVER REACHES DISK.
      //
      // Reachable from the public API: `store` a live transient, mutate it further, `store`
      // again — the checkpointed bulk-ingest shape. Measured before this clear, second store
      // silently missing the second batch:
      //     bf 64 dbs 0   level 1   in-mem 220  reloaded 219  missing [21]
      //     bf  8 dbs 0   level 3   in-mem 1220 reloaded 1219 missing [21]
      //     bf  8 dbs 256 level 2   in-mem 218  reloaded 219  extra   [5]
      // Baseline loses at every level; under diff-buf level 1 is masked because the slot
      // carries the real leaf-diff, while level >= 2 deposits a MARKER (diff == null) whose
      // content lives only in the live child, so the loss returns.
      //
      // Every other mutation path already clears it — child(int,ANode) at :328-337,
      // newAddresses[ins] = null at :749 and :1655, as.copyOne(null) in each rebuild stitch.
      // The EARLY_EXIT arms were the only ones that did not, because they install no new
      // node. `anchor0` was captured above, so under diff-buf the child now classifies as
      // dirty and Pass 2 re-points the address to that anchor.
      //
      // Free the address we are about to clear. The two pre-existing clear sites do this
      // (see the `_settings.diffBufSize() <= 0 && ...markFreed` blocks around child(idx,node)
      // in the non-EARLY_EXIT arms); these three did not, so a checkpointed transient left
      // blobs that were unreachable AND never reported freed. Measured over 40 checkpoint
      // rounds at bf 8 / dbs 0: disk 242, reachable 82, freed-reported 99, ORPHANS 61
      // (levels {2 -> 37, 1 -> 24}). Content was correct — this is unbounded storage growth
      // for any consumer that treats the freed stream as its GC candidate list, which
      // datahike does.
      //
      // GATED on diffBufSize <= 0, exactly as the other sites are: under diff-buf the old
      // address is re-pointed as the buffered anchor at store, so freeing it here would free
      // a LIVE node. That gate is also why the measurement only shows orphans at dbs 0.
      if (_settings.diffBufSize() <= 0 && storage != null
          && s0.addresses != null && s0.addresses[idx] != null) {
        storage.markFreed(s0.addresses[idx]);
      }
      child(idx, mutatedChild);  // clears addresses[idx] AND unwraps the child (a dirty child must be bare)
      if (_settings.diffBufSize() > 0) depositReplace(storage, idx, removedKey, newKey, anchor0); // Absent(removedKey)+Present(newKey) / branch marker
      return PersistentSortedSet.EARLY_EXIT;
    }

    // Child was replaced (nodes.length == 1)
    ANode<Key, Address> newChild = nodes[0];
    Key newMaxKey = newChild.maxKey();
    // Whether to propagate up (rebuild parent) vs EARLY_EXIT a transient. Count mode is
    // routing-only (by cmp), so the rightmost child's comparator-position change is the only
    // thing a parent cares about. MST mode content-addresses the separator VALUE, so ANY value
    // change must propagate so every separator up the spine stays canonical (mirrors the cljs
    // Branch.$replace value-based test). _keys[idx] is still the OLD separator here (overwritten
    // below). See doc/merkle-search-tree.md.
    // Whether the change must PROPAGATE to this node's parent. `_keys[idx]` is written
    // unconditionally below, so the immediate separator is always fresh; this decides
    // whether the GRANDPARENT's separator (= this branch's max = _keys[_len-1]) is stale.
    //
    // The test is VALUE equality, not `cmp`. Asking the OPERATION comparator whether the
    // max moved is wrong whenever that comparator is coarser than the one routing will
    // use — datahike's value-changing upsert searches [e a _ _], so `cmp.compare` returns
    // 0 for a datom whose v changed, propagation was suppressed, and every ancestor kept
    // a separator naming the OLD element. A later descent comparing the new element
    // against that stale separator routes past the child that holds it.
    //
    // Measured before this fix (elements [i 0], set cmp on (i,v), op cmp on i alone,
    // replace [i 0] -> [i 5] for every i, inside a transient): 9 unfindable at n=40 bf=4,
    // 24 at n=100 bf=4, 46 at n=3000 bf=16 — `contains?` false for an element that seq
    // still lists, in a set that is sorted and counts correctly. Through datahike at
    // branching-factor 8, 375 of 3000 datoms were invisible to `d/datoms db :eavt e a v`.
    // Only trees of THREE levels or more are affected: with two levels the parent is the
    // root and its separator is the one written unconditionally.
    //
    // `Util.equiv` rather than `Objects.equals` deliberately. Datom implements `equiv`
    // (Clojure `=`) but not `Object.equals`, so `Objects.equals` is identity there and
    // would propagate on EVERY upsert. `equiv` propagates exactly when the element really
    // changed, which is the cheapest test that is still correct.
    boolean maxKeyChanged = settings.boundary().contentDefined()
      ? !java.util.Objects.equals(newMaxKey, _keys[idx])
      : (idx == _len - 1) && separatorMoved(newMaxKey, _keys[idx]);
    IMeasure measureOps = settings.measure();

    // Transient: can modify in place
    if (editable()) {
      _keys[idx] = newMaxKey;
      // Mark old child address as freed before clearing. diff-buf: deferred to store
      // under diff-buf (old address may be re-pointed as a buffered anchor — see add()).
      if (_settings.diffBufSize() <= 0 && storage != null && s0.addresses != null && s0.addresses[idx] != null) {
        storage.markFreed(s0.addresses[idx]);
      }
      child(idx, newChild);
      // Note: child(idx, node) also clears the slot's address
      // Try to recompute measure from final state (after child replacement)
      if (measureOps != null && _measure != null) {
        _measure = tryComputeMeasure(storage);
      }
      if (_settings.diffBufSize() > 0) depositReplace(storage, idx, removedKey, newKey, anchor0); // Absent(removedKey)+Present(newKey) / branch marker
      if (maxKeyChanged)
        return new ANode[]{this};
      else
        return PersistentSortedSet.EARLY_EXIT;
    }

    // Persistent: create new branch with updated child
    // Baseline: the successor branch supersedes the old durable child at idx — free it
    // (per-level copy-unwind supersession, mirrors the editable in-place hook above).
    // diff-buf: DEFERRED to store (the old address is re-pointed as the buffered anchor).
    if (_settings.diffBufSize() <= 0 && storage != null && s0.addresses != null && s0.addresses[idx] != null) {
      storage.markFreed(s0.addresses[idx]);
    }
    // Always copy — sharing arrays would allow a later transient
    // editable() path to mutate the original persistent branch's arrays.
    // Both copies come from s0 — the pair is coherent by construction.
    Key[] newKeys = Arrays.copyOfRange(_keys, 0, _len);
    newKeys[idx] = newMaxKey;

    final Address[] newAddresses = s0.addresses != null ? Arrays.copyOfRange(s0.addresses, 0, _len) : null;
    if (newAddresses != null) {
      newAddresses[idx] = null; // child changed, address invalid
    }

    final Object[] newChildren = s0.children == null ? new Object[_keys.length] : Arrays.copyOfRange(s0.children, 0, _len);
    newChildren[idx] = newChild;

    // Try to recompute measure from final state (after child replacement) for new branch
    Branch<Key, Address> newBranch = new Branch(_level, _len, newKeys, newAddresses, newChildren, _subtreeCount, null, _projCmp, settings);
    if (measureOps != null && _measure != null) {
      newBranch._measure = newBranch.tryComputeMeasure(storage);
    }
    // The SAME snapshot's {slots, entries} pair — carried together so the successor
    // can never mix a pre-settle total with post-settle slots (or vice versa).
    if (settings.diffBufSize() > 0) newBranch.carryAndDepositReplace(storage, s0.buf, idx, removedKey, newKey, anchor0); // Absent(removedKey)+Present(newKey)

    return new ANode[]{newBranch};
  }

  // Null-tolerant per-slot address read from a caller-held snapshot.
  private Address addrAt(NodeState<Address> s, int i) {
    return (s.addresses != null) ? s.addresses[i] : null;
  }

  @Override
  public void walkAddresses(IStorage storage, IFn onAddress) {
    final NodeState<Address> s = _state;                        // one snapshot
    for (int i = 0; i < _len; ++i) {
      Address address = addrAt(s, i);
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
  // into slot i as Present(element) | Absent, with ĝ refreshed from the
  // (already-mutated, in-memory) child's count/measure. Structural returns
  // (split/merge/borrow) instead publish a BUF_WRITE snapshot and never deposit —
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
    // OWNER-THREAD (editable / freshly-built node). Single-snapshot read + single-publish
    // write (see NodeState): compute the new slots array (copy-on-write) AND the new
    // entries total from ONE snapshot, then publish the pair with ONE volatile write, so
    // a concurrent reader never sees them torn.
    BufState b = _state.buf;
    Object[] slots = (b == null || b.slots == null)
        ? new Object[_keys.length]
        : Arrays.copyOf(b.slots, b.slots.length);
    Slot prev = (Slot) slots[i];
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
    long cur = bufEntriesOf(b);                 // resolve our own LAZY (restored) value from the SAME snapshot
    long entries;
    if (cur == BUF_WRITE || newBE == BUF_WRITE) entries = BUF_WRITE;
    else                                        entries = cur - (prev != null ? slotBE(prev) : 0) + newBE;
    slots[i] = new Slot(diff, childCount(storage, i), child.measure(), anchor, newBE);
    installSlots(slots, entries);               // single publish: slots + total together
  }

  // diff-buf: a child's slot travels with its address. These mirror the per-element /
  // bulk copies of the address Stitch so a structural REMOVE rebuild carries surviving
  // siblings' buffered slots (null-source tolerant: a sibling branch may have no slots).
  // Both take the source slots ARRAY (from one caller-held snapshot) rather than re-reading
  // _state, so a rebuild reads each source node's state exactly once.
  private static Object slotAt(Object[] slots, int i) { return (slots != null) ? slots[i] : null; }
  private static void slotCopyAll(Stitch ss, Object[] src, int from, int to) {
    if (src == null) { for (int i = from; i < to; ++i) ss.copyOne(null); }
    else ss.copyAll(src, from, to);
  }

  // diff-buf: free child i's durable blob when a STRUCTURAL rebuild (merge/borrow/split)
  // drops it. Such a child is materialized into a new node and its old blob is never
  // re-pointed as a buffered anchor (unlike the content-only case, which IS deferred to
  // store), so it is dead now. The live durable address is s.addresses[i] if set, else the
  // anchor parked in slot i (the child was content-buffered earlier this txn, so its address
  // was nulled but its anchor lives in the slot). `s` is the caller's method-entry snapshot
  // of THIS node, read before the rebuild overwrites it — addresses and slots come from the
  // SAME generation. No-op when diff-buf is off (callers gate on diffBufSize() > 0).
  private void freeDroppedChild(IStorage storage, NodeState<Address> s, int i) {
    if (storage == null) return;
    if (s.addresses != null && s.addresses[i] != null) { storage.markFreed(s.addresses[i]); return; }
    Object[] slots = (s.buf != null) ? s.buf.slots : null;
    if (slots != null && slots[i] instanceof Slot) {
      Object a = ((Slot) slots[i]).anchor;
      if (a != null) storage.markFreed((Address) a);
    }
  }

  // Carry slots through a structural rebuild where child `ins` was replaced by `nNodes`
  // new nodes (split/absorb), producing a slots array of length newLen laid out exactly
  // like the rebuilt children: surviving siblings keep their slot; the new nodes get none
  // (they are materialized/written, no durable anchor). Null if this node has no slots.
  // `s` is the caller's method-entry snapshot of THIS node.
  private Object[] stitchSlots(NodeState<Address> s, int ins, int nNodes, int newLen) {
    Object[] slots = (s.buf != null) ? s.buf.slots : null;
    if (slots == null) return null;
    Object[] out = new Object[newLen];
    Stitch st = new Stitch(out, 0);
    st.copyAll(slots, 0, ins);
    for (int k = 0; k < nNodes; k++) st.copyOne(null);
    st.copyAll(slots, ins + 1, _len);
    return out;
  }

  // For persistent (non-editable) returns: carry the source branch's diff-buf state — ONE
  // caller-held snapshot, so slots and entries can never mix two generations — onto this
  // freshly-built (unpublished) branch, then deposit at i. (1-for-1 child replacement, so
  // indices are aligned with the source; the array is copied, never shared.)
  private void carryBuf(BufState src) {
    Object[] slots = (src != null && src.slots != null) ? Arrays.copyOf(src.slots, _keys.length) : null;
    installSlots(slots, bufEntriesOf(src)); // carry the source's running total (or BUF_WRITE)
  }

  private void carryAndDeposit(IStorage storage, BufState src, int i, Object mapKey, Object val, Object anchor0) {
    carryBuf(src);
    depositInto(storage, i, mapKey, val, anchor0);
  }

  private void carryAndDepositReplace(IStorage storage, BufState src, int i, Object oldKey, Object newKey, Object anchor0) {
    carryBuf(src);
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
    Object[] cSlots = c.slots();                                // one snapshot per node
    if (cSlots != null) {
      for (int j = 0; j < c._len; j++) {
        Slot sl = (Slot) cSlots[j];
        if (sl == null) continue;
        // A null `anchor` means this child has NO durable base to diff against, so store()
        // Pass 1 writes it WHOLESALE and `depositKV` leaves its diff null for exactly that
        // reason. There is no buffered difference to assemble, at any level.
        //
        // `Slot`'s javadoc says a null diff marks "a BRANCH anchor marker", and the
        // recursion below read it that way — casting the child to a Branch. But the
        // anchor-null case produces a null diff on a LEAF child too, and then the cast is
        // a ClassCastException out of a plain store. Measured: `c.level=1 j=22
        // childClass=Leaf slotDiffNull=true slotAnchor=false`.
        if (sl.anchor == null) continue;
        Object d;
        if (sl.diff != null) {
          d = (c._level == 1) ? leafDiffForStorage(sl.diff) : sl.diff;  // leaf child ⇒ comparator-agnostic storage form
        } else {
          // Null diff WITH an anchor: the branch-anchor marker proper. A leaf child cannot
          // be one — say so here rather than let the cast below report it three frames on.
          assert c._level > 1
              : "diff-buf: level-1 slot has a null diff but a non-null anchor — a leaf child "
              + "cannot be a branch-anchor marker (slot " + j + " of a level-" + c._level + " branch)";
          d = assembleNested(storage, (Branch) c.child(storage, j));
        }
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
    Object[] slots = slots();                                   // one snapshot
    if (slots == null) return null;
    IPersistentMap m = PersistentHashMap.EMPTY;
    for (int i = 0; i < _len; ++i) {
      Slot sl = (Slot) slots[i];
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
    // Project onto a COPY — never mutate `base`. Caching IStorage impls (e.g. datahike's
    // CachedStorage) return restored nodes SHARED BY ADDRESS across tree versions, and this
    // projection is VERSION-SPECIFIC: consecutive commits buffer against the same durable
    // anchor with different accumulated diffs (version N's exact child at address B is the
    // very object version N+1 projects {B, δ} onto). Mutating the shared object in place —
    // installing slots, rewriting separators/count/measure, cache-filling projected
    // children into it — leaked one version's projection into every other version's reads
    // (cross-version clobber; nondeterministic under an evicting cache). The cached object
    // must stay the pristine durable content; the projected copy is what the caller
    // cache-fills into ITS (version-specific) children. Mirrors projectLeaf, which has
    // always returned a fresh Leaf. children stay null on the copy: a grandchild with a
    // nested slot must be projected by the copy's own descent, and a passthrough
    // grandchild re-restores through the (pristine) cache.
    Object[] slots = new Object[base._keys.length];
    final NodeState<Address> bs = base._state;                   // one snapshot of base
    Address[] baseAddresses = bs.addresses;
    Key[] newKeys = Arrays.copyOf(base._keys, base._keys.length);
    for (ISeq s = RT.seq(sl.diff); s != null; s = s.next()) {
      IMapEntry e = (IMapEntry) s.first();
      int i = ((Number) e.key()).intValue();
      IPersistentMap entry = (IPersistentMap) e.val();
      long cnt = ((Number) entry.valAt(KW_COUNT)).longValue();
      Object measure = entry.valAt(KW_MEASURE);
      Object d = entry.valAt(KW_DIFF);
      Object mk = entry.valAt(KW_MAXKEY);
      slots[i] = new Slot(d, cnt, measure, baseAddresses[i]);    // anchor = grandchild's durable address
      // Restore the separator ON THE COPY: base came from the anchor (old durable object)
      // whose _keys[i] is the PRE-diff max. The diff changed child i's max, so fix the
      // separator here — otherwise search/contains route against a phantom max-key (the
      // verified diff-buf-v5 read bug).
      if (mk != null) newKeys[i] = (Key) mk;
    }
    // The copy's slots REPLACE any slots base's own blob carried (parent's nested diff is
    // the complete superseding state — same semantics as the historical in-place install).
    // ĝ.count / ĝ.measure — no child summing. installSlots on the unpublished copy is
    // single-threaded by construction.
    //
    // COPY the addresses array. This used to alias base's, justified as "read-only by the
    // shared-snapshot contract; the copy is sealed, never edited in place" — an invariant
    // that held only because a projected copy inherits `base._settings`, whose `_edit` is
    // null, so it is never editable and `child(int,ANode)` (which writes
    // `addresses[idx] = null` IN PLACE) never reaches it. That is a long chain of
    // reasoning protecting a caching IStorage's shared node: datahike's CachedStorage
    // returns the same object by address across tree versions, so one nulled entry there
    // would make another version see a phantom dirty child. `newKeys` and `slots` are
    // already allocated here, so this adds a third array copy of the same length to a
    // function that allocates two — and removes the aliasing entirely rather than
    // documenting why it is currently survivable.
    Address[] projAddresses = (baseAddresses != null)
        ? Arrays.copyOf(baseAddresses, baseAddresses.length) : null;
    Branch<Key, Address> proj = new Branch<>(base._level, base._len, newKeys, projAddresses,
                                             null, sl.count, sl.measure, _projCmp, base._settings);
    proj.installSlots(slots, BUF_LAZY);
    return proj;
  }

  /**
   * A copy of this branch that projects under `projCmp` instead of `_projCmp`.
   *
   * Needed because `_projCmp` is a FIELD on a node that a caching IStorage shares by address.
   * Two sets over one storage whose comparators order ties differently — `restore-by cmpA` and
   * `restore-by cmpB` on the same root, or `restore` (which hard-codes DEFAULT_COMPARATOR)
   * where `restore-by` was meant — both stamped the same object, and whichever ran last won.
   * `projectLeaf` then rebuilt a buffered leaf's key array in the OTHER set's order.
   *
   * Measured before this copy, two sets over one storage at bf 8 / diff-buf 64 / n 60, reading
   * interleaved so B re-stamps the shared root between two steps of A's lazy seq:
   *
   *     count 129 (correct)   seq sorted under cmp1: FALSE   first disorder at idx 18
   *     contains? false for [8 1] [9 0] [14 1] [14 2] [15 0] — all present in seq
   *
   * and it does not stay in memory: one `conj` into the mis-sorted cached leaf, one `store`,
   * and a cold reload through a fresh cache has one element PERMANENTLY unfindable — in `seq`,
   * `contains?` false — because the branch separator no longer bounds it.
   *
   * The children array is deliberately NOT shared. Sharing it would defeat the whole copy:
   * the cache holds children already projected under the other set's comparator, so the next
   * level down would hand back the same mis-ordered leaves. Addresses and keys are copied for
   * the reason `projectBranch` records — a shared node must not be reachable through an array
   * another version can write. `buf` IS shared: slots are immutable snapshots, and this copy
   * gets its own `_state`, so its CASes never touch the base's.
   *
   * Cost: nothing on the single-comparator path, which is every normal use — the callers copy
   * only when a node already carries a DIFFERENT comparator, and they publish the copy, so it
   * happens once per node rather than once per read.
   */
  /**
   * This node if it can carry `projCmp`, otherwise a copy that does.
   *
   * The seed is a CAS, not a plain write, so two threads first-touching the same shared node
   * with different comparators cannot both conclude "it was null, it is mine now". The loser
   * re-reads and takes the copy — see the `_projCmp` field comment for the measurement.
   */
  Branch<Key, Address> stampOrCopy(Comparator projCmp) {
    Comparator cur = _projCmp;
    if (cur == projCmp) return this;
    if (cur == null && PROJCMP_UPDATER.compareAndSet(this, null, projCmp)) return this;
    // Either it already carried a different comparator, or we lost the seed race. Re-read:
    // the winner may have stamped OUR comparator, in which case there is nothing to escape.
    return (_projCmp == projCmp) ? this : withProjCmp(projCmp);
  }

  Branch<Key, Address> withProjCmp(Comparator projCmp) {
    NodeState<Address> s = _state;
    Address[] addrCopy = (s.addresses != null)
        ? Arrays.copyOf(s.addresses, s.addresses.length) : null;
    Key[] keysCopy = Arrays.copyOf(_keys, _keys.length);
    // Carry over every child that has NO durable address. Dropping the children array
    // wholesale is right for a child that can be restored from its address and wrong for one
    // that cannot: a slot with `addresses[i] == null` and `children[i]` a bare dirty ANode is
    // the ONLY reference to that subtree, and nulling it produced the state the invariant
    // forbids — address null AND child null. Measured before this, root _len 7 with a null
    // address at index 6, at diffBufSize 0 and 64 alike:
    //     (seq copy)   -> AssertionError at Branch.child's precondition
    //     (store copy) -> AssertionError "dirty child must be a bare resident ANode ..."
    // and under -da an NPE or a silently truncated subtree. A dirty child is this version's
    // own, not the storage cache's, so carrying the reference shares nothing that the
    // fresh-children-array rule exists to keep separate.
    Object[] childCopy = null;
    if (s.children != null) {
      for (int i = 0; i < _len; ++i) {
        if ((addrCopy == null || addrCopy[i] == null) && s.children[i] != null) {
          if (childCopy == null) childCopy = new Object[s.children.length];
          childCopy[i] = s.children[i];
        }
      }
    }
    Branch<Key, Address> copy = new Branch<>(_level, _len, keysCopy, addrCopy, childCopy,
                                             _subtreeCount, _measure, projCmp, _settings);
    copy._state = new NodeState<>(addrCopy, childCopy, s.buf);
    return copy;
  }

  // -ea ONLY: detect two threads settling the SAME node concurrently.
  //
  // `store()` publishes the settled per-child state with a PLAIN write, not a CAS, and it
  // publishes BEFORE `storage.store(this)`. Both are correct under the documented contract —
  // one settle at a time per LINEAGE (doc/CONCURRENCY.md) — and neither is safe if two threads
  // settle versions that share dirty nodes. Structural sharing makes that easy to do by
  // accident: measured on a pipelining-writer shape at bf 8 / n 1000, 3 Branch objects were
  // reachable from BOTH roots and dirty in both, so storing either settles the same objects.
  //
  // The contract is not enforced and cannot cheaply be: serialising store() would cost every
  // single-threaded caller. So this DETECTS instead. Both methods run only inside `assert`, so
  // with -da the map is never touched and there is no field, no allocation and no lookup —
  // exactly the trade the rest of this class makes (an assertion that costs nothing in
  // production but fails loudly in a user's tests).
  //
  // Keyed by node identity: ANode/Branch/Leaf override neither equals nor hashCode, so the
  // ConcurrentHashMap compares by identity and cannot conflate two distinct nodes.
  //
  // What it CANNOT catch: two threads settling DISJOINT trees over one storage (legal), and a
  // race whose windows never overlap in a given run. It is a detector, not a proof.
  private static final java.util.concurrent.ConcurrentHashMap<Object, Thread> SETTLING =
      new java.util.concurrent.ConcurrentHashMap<>();

  private boolean beginSettle() {
    Thread me = Thread.currentThread();
    Thread other = SETTLING.putIfAbsent(this, me);
    if (other != null && other != me) {
      throw new AssertionError(
          "concurrent settle of the same node by " + me + " and " + other
          + ". store() may run on one thread at a time per LINEAGE (in practice per storage), "
          + "not per tree: two versions can share dirty nodes, and both settles publish with a "
          + "plain write before serializing. See doc/CONCURRENCY.md.");
    }
    return true;
  }

  private boolean endSettle() {
    SETTLING.remove(this);
    return true;
  }

  @Override
  public Address store(IStorage<Key, Address> storage) {
    assert beginSettle();
    try {
      return storeImpl(storage);
    } finally {
      assert endSettle();
    }
  }

  private Address storeImpl(IStorage<Key, Address> storage) {
    if (_settings.diffBufSize() <= 0) {                           // baseline ⇒ byte-identical (I0)
      // SETTLE, baseline: stage the whole {addresses, children} pair on LOCAL copies of
      // ONE snapshot and publish ONCE. This replaces the historical per-slot two-step
      // (write _addresses[i], then wrap _children[i]) that a concurrent copier could
      // observe torn — mixing a pre-settle (null) address with a post-settle wrapped
      // child (the forbidden dirty+Reference state, #17), or an old address with the
      // wrapper of a newer child (silent stale-data resurrection after a soft-ref
      // clearance). The publish happens BEFORE storage.store(this) so the serializer
      // sees the settled addresses, exactly as the old in-place mutation did.
      final NodeState<Address> s0 = _state;
      final Address[] newAddresses = (s0.addresses != null)
          ? Arrays.copyOf(s0.addresses, s0.addresses.length)
          : (Address[]) new Object[_keys.length];
      Object[] newChildren = s0.children;                        // copied lazily on first wrap
      boolean dirty = false;
      for (int i = 0; i < _len; ++i) {
        if (newAddresses[i] == null) {
          dirty = true;
          // PERMANENT slot invariant (-ea): within one snapshot a dirty child is a bare,
          // resident ANode — never null, never a Reference wrapper. With NodeState this
          // holds by construction (states are internally consistent); the readReference
          // unwrap stays as production robustness without -ea.
          assert s0.children != null && s0.children[i] instanceof ANode
              : "dirty child must be a bare resident ANode within one snapshot, got "
                + ((s0.children == null || s0.children[i] == null) ? "null" : s0.children[i].getClass().getName());
          ANode<Key, Address> dirtyChild = (ANode<Key, Address>) _settings.readReference(s0.children[i]);
          assert dirtyChild != null : "dirty child collected — dirty nodes must be strongly reachable";
          newAddresses[i] = dirtyChild.store(storage);
          if (newChildren != null && newChildren[i] instanceof ANode) {
            if (newChildren == s0.children) newChildren = Arrays.copyOf(s0.children, s0.children.length);
            newChildren[i] = _settings.makeReference(newChildren[i]);
          }
        } else if (newChildren != null && newChildren[i] instanceof ANode) {
          // CLEAN passthrough, but still a BARE (strong) child. `Branch.remove` writes an
          // UNCHANGED sibling into its successor as a bare ANode while keeping that
          // sibling's still-valid address (as.copyOne(s0.addresses[...]) next to
          // cs.copyOne(nodes[...]) in every arm), and copy-on-write carries it into every
          // later version. Skipping it here — the settle only ever wrapped null-address
          // children — meant each disj permanently converted up to two slots per level on
          // its path into strong references, so `:ref-type :soft`/`:weak` stopped bounding
          // the tree. Measured at bf 16 over 20000 elements, :ref-type :soft: after 400
          // disj and a store, {:ref 780, :bare-STRONG-with-address 119} — the 119 never
          // shrink. Present at diffBufSize 0 too, so it predates diff-buf.
          //
          // Safe to wrap for the same reason Pass 2 is: the address is non-null and still
          // durable, so a cleared reference reloads.
          if (newChildren == s0.children) newChildren = Arrays.copyOf(s0.children, s0.children.length);
          newChildren[i] = _settings.makeReference(newChildren[i]);
        }
      }
      if (dirty || s0.addresses == null) {
        _state = new NodeState<>(newAddresses, newChildren, s0.buf); // ONE publish
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
    final int budget = _settings.diffBufSize();
    refreshMarkerSlots(storage);   // D2
    assert assertBufEntries(storage);  // -ea oracle: delta-maintained total == fresh subtree walk

    // Single-snapshot / single-publish settle (see NodeState): all three passes read ONE
    // snapshot of the whole per-child state and stage their updates on LOCAL copies of
    // the arrays; the settled {addresses, children, slots, entries} snapshot is published
    // with ONE volatile write before serialization. A concurrent structural-sharing
    // reader (a pipelining writer copying this shared node while it is being committed)
    // therefore observes either the pre-settle or the post-settle snapshot — never
    // flushed children's slots already nulled with the running total still counting
    // them, and never re-pointed addresses paired with pre-settle slots.
    final NodeState<Address> s0 = _state;
    final BufState b0 = s0.buf;
    final Object[] slots0 = (b0 != null) ? b0.slots : null;
    final Object[] children0 = s0.children;
    // Declared here rather than at Pass 2: Pass 1 now also wraps clean passthrough children,
    // so the copy-on-write array must exist before that loop.
    Object[] newChildren = children0;
    final Address[] newAddresses = (s0.addresses != null)
        ? Arrays.copyOf(s0.addresses, s0.addresses.length)
        : (Address[]) new Object[_keys.length];

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
      Slot sl = (slots0 != null) ? (Slot) slots0[i] : null;
      if (newAddresses[i] != null) {
        if (sl != null) passthrough += slotBE(sl);             // clean buffered-passthrough subtree total
        // Wrap a clean passthrough child that is still BARE, exactly as the baseline settle
        // now does. `Branch.remove` writes an unchanged sibling into its successor as a bare
        // ANode while keeping its still-valid address, so without this each disj ratchets
        // more of the tree into permanently strong references and `:ref-type` stops
        // bounding anything. Measured at bf 16 / 20000 elements / :ref-type :soft, before:
        // after 400 disj and a store, {:ref 780, :bare-STRONG-with-address 119}.
        if (newChildren != null && newChildren[i] instanceof ANode) {
          if (newChildren == children0) newChildren = Arrays.copyOf(children0, children0.length);
          newChildren[i] = _settings.makeReference(newChildren[i]);
        }
        continue;
      }
      // addresses[i] == null: dirty this commit ⇒ child is resident; its slot is live (deposited).
      // PERMANENT slot invariant (-ea): within one snapshot a dirty child is a bare,
      // resident ANode (see the baseline settle above).
      assert children0 != null && children0[i] instanceof ANode
          : "dirty child must be a bare resident ANode within one snapshot, got "
            + ((children0 == null || children0[i] == null) ? "null" : children0[i].getClass().getName());
      if (sl == null || sl.anchor == null) {                    // no durable anchor ⇒ must write
        writeList.add(i);
      } else if (sl.bufEntries == BUF_WRITE) {                  // subtree rebalanced (poison) ⇒ must write
        writeList.add(i);
      } else {                                                  // content-only ⇒ bufferable
        // RESOLVE, don't read raw — the passthrough arm above already calls slotBE(sl) for
        // exactly this quantity. A slot reconstructed from storage carries Slot.LAZY (-2)
        // (the 4-arg Slot ctor), and the gate above only rejects BUF_WRITE (-1), so a LAZY
        // slot on a dirty child fell through to here and was sized as -2. That is not a
        // small error, it is a NEGATIVE size: `bufferable` sorts it first, the budget test
        // `embedded + csz[i] <= budget` always passes, and `embedded += csz[i]` moves the
        // running total BACKWARDS — so the per-node budget stops bounding the blob, and the
        // -2 is then written back into the slot as its settled size.
        //
        // Measured, budget 1, a level-2 root whose slot 0 carries a real restored diff, that
        // child made dirty: raw read buffered it (addresses[idx] == anchor, no write, slot
        // persisted with bufEntries -2) where the real diff must flush; with slotBE it
        // flushes. Not reached by the current suite — 0 hits over 38.8M child(int,ANode)
        // calls across the diff-buf namespaces — because every path that dirties a child
        // also re-deposits its slot with a computed size. That makes this a latent
        // inconsistency rather than a live defect, and the reason to close it is that the
        // two arms must not disagree about how to read the same field.
        csz[i] = slotBE(sl);
        cnested[i] = (sl.diff != null) ? sl.diff                // leaf-diff, or restored-nested branch-diff
                   : assembleNested(storage, (Branch)(ANode) _settings.readReference(children0[i])); // live branch marker
        bufferable.add(i);
      }
    }

    // Pass 2: buffer the SMALLEST bufferable children while the running total (passthrough +
    // buffered) stays within budget; the rest — the largest — are flushed (biggest-first).
    // Staged on newAddresses/newSlots, local copies — nothing is visible to other threads yet.
    bufferable.sort((x, y) -> Integer.compare(csz[x], csz[y]));
    Object[] newSlots = (slots0 != null) ? Arrays.copyOf(slots0, slots0.length) : null;
    // Children are WRAPPED here, exactly as the baseline settle wraps them. Publishing
    // `children0` unchanged (what this did before) left every child that had been dirty in
    // any commit a bare STRONG reference from its parent, and copy-on-write carried that
    // into every successor — so with diff-buf on, `:ref-type :soft`/`:weak` silently stopped
    // bounding anything and the resident set ratcheted toward the whole tree. Measured, bf 8
    // and `:ref-type :soft`: diff-buf 0 gave {:ref 5} at the root, diff-buf 256 gave
    // {:bare-strong 5}.
    //
    // Both settled kinds are safe to wrap because both end up with a durable address:
    // a BUFFERED child is re-pointed to its anchor and its assembled diff is written back
    // into the slot just below, so a cleared reference is re-derived as
    // restore(anchor) + project(slot) — which is precisely why that writeback exists. A
    // FLUSHED child is written outright. Copy-on-write, so the published array is never the
    // snapshot's own.
    int embedded = passthrough;
    for (int i : bufferable) {
      Slot sl = (Slot) slots0[i];
      if (embedded + csz[i] <= budget) {
        newAddresses[i] = (Address) sl.anchor;                  // re-point to durable anchor (no write)
        newSlots[i] = new Slot(cnested[i], sl.count, sl.measure, sl.anchor, csz[i]); // write back assembled diff + its size
        embedded += csz[i];
        if (newChildren != null && newChildren[i] instanceof ANode) {
          if (newChildren == children0) newChildren = Arrays.copyOf(children0, children0.length);
          newChildren[i] = _settings.makeReference(newChildren[i]);
        }
      } else {
        writeList.add(i);                                       // doesn't fit ⇒ flush
      }
    }

    // D3: the merge/borrow arms (`remove`'s join/borrow, `add`'s stitchSlots) concatenate two
    // nodes' slot arrays without re-checking the budget, so `passthrough` alone can start above
    // B — and Pass 2, which only ever flushes DIRTY children, can never bring it down. Measured
    // over ~157k written blobs before this: worst case ≈ 2B (B=1→2, 2→4, 4→8, 8→12, 16→26) on
    // ~0.1% of blobs. It does not compound (200 adversarial shrink/refill rounds stayed at ~2B)
    // and content was always correct — but the comment below claims the running total stays
    // within B strictly, and it did not.
    //
    // Flush already-settled children (clean passthrough and newly buffered alike — both now
    // carry an address AND a slot; Pass 1's must-write and Pass 2's flushed children have a null
    // address and so are correctly excluded) biggest-first until the total fits. Measured:
    // needed on 0.05% of written blobs, +0..3 writes out of 750-3500, and every flushed child
    // was already resident under :ref-type strong, soft AND weak — so the restore below is a
    // fallback, not a read in practice.
    if (embedded > budget) {
      java.util.ArrayList<Integer> pt = new java.util.ArrayList<>();
      for (int i = 0; i < _len; ++i)
        if (newAddresses[i] != null && newSlots != null && newSlots[i] != null) pt.add(i);
      final Object[] fs = newSlots;
      pt.sort((x, y) -> Integer.compare(slotBE((Slot) fs[y]), slotBE((Slot) fs[x])));
      for (int i : pt) {
        if (embedded <= budget) break;
        Object ref = (children0 != null) ? children0[i] : null;
        ANode c = (ref != null) ? (ANode) _settings.readReference(ref) : null;
        if (c == null) c = child(storage, i);                   // not resident ⇒ restore+project
        embedded -= slotBE((Slot) newSlots[i]);
        storage.markFreed((Address) newAddresses[i]);
        newAddresses[i] = ((ANode<Key, Address>) c).store(storage);
        newSlots[i] = null;
        if (newChildren != null && newChildren[i] instanceof ANode) {
          if (newChildren == children0) newChildren = Arrays.copyOf(children0, children0.length);
          newChildren[i] = _settings.makeReference(newChildren[i]);
        }
      }
    }

    // Pass 3: write the flushed/structural children (all resident ⇒ no read).
    for (int i : writeList) {
      ANode child = (ANode) _settings.readReference(children0[i]);
      Slot sl = (slots0 != null) ? (Slot) slots0[i] : null;
      if (sl != null && sl.anchor != null) storage.markFreed((Address) sl.anchor);
      newAddresses[i] = ((ANode<Key, Address>) child).store(storage);
      if (newSlots != null) newSlots[i] = null;
      if (newChildren != null && newChildren[i] instanceof ANode) {
        if (newChildren == children0) newChildren = Arrays.copyOf(children0, children0.length);
        newChildren[i] = _settings.makeReference(newChildren[i]);
      }
    }
    // Settle: this node now equals its durable object, whose remaining slots are exactly the
    // children we BUFFERED (passthrough + newly buffered) — the flushed ones were nulled. So
    // the settled buffered-entry total is `embedded`, not 0 (a later commit deltas from here).
    // This also clears any BUF_WRITE poison: the new structure is now materialized on disk.
    // ONE publish of the WHOLE per-child state replaces the old in-place address writes +
    // per-slot nulling + late total reset (the torn windows); it happens BEFORE
    // storage.store(this) so the serializer (slotsForStorage / addresses()) sees the
    // settled state, exactly as the old in-place mutation did. children are WRAPPED per
    // `:ref-type`, same as the baseline settle — see the staging comment above.
    _state = new NodeState<>(newAddresses, newChildren, new BufState(newSlots, embedded));
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
    final NodeState<Address> s = _state;                        // one snapshot
    for (int i = 0; i < _len; ++i) {
      sb.append("\n");
      ANode child = null;
      if (s.children != null) {
        Object ref = s.children[i];
        if (ref != null) {
          child = (ANode) _settings.readReference(ref);
        }
      }
      if (child != null)
        child.toString(sb, addrAt(s, i), indent + "  ");
      else
        sb.append(indent + "  " + addrAt(s, i) + ": <lazy> ");
    }
  }
}