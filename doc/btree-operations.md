# B-Tree Mutation Operations: Return Value Protocol and Correctness

This document describes how the persistent sorted set's B-tree operations work,
covering the return value protocol, the persistent/transient discipline, and
copy-on-write correctness. It covers both the Java (CLJ) and ClojureScript
implementations.

## Table of Contents

1. [Return Value Protocol](#return-value-protocol)
2. [Transient / Persistent Discipline](#transient--persistent-discipline)
3. [Operation: add](#operation-add)
4. [Operation: remove](#operation-remove)
5. [Operation: replace](#operation-replace)
6. [Parent Dispatch (cons / disjoin)](#parent-dispatch-cons--disjoin)
7. [Rebalancing: rotate / merge / merge-split / borrow](#rebalancing)
8. [ILeafProcessor (Java only)](#ileafprocessor-java-only)
9. [Copy-on-Write Correctness](#copy-on-write-correctness)
10. [Platform Differences (Java vs CLJS)](#platform-differences)
11. [Subtree Counts and Measures](#subtree-counts-and-measures)
12. [Test Coverage](#test-coverage)

---

## Return Value Protocol

Every node-level mutation (`add`, `remove`, `replace`) communicates its outcome
to its parent via the return value. The protocol uses sentinel arrays and
fixed-position conventions:

### Java Sentinels

```java
public static ANode[] EARLY_EXIT = new ANode[0];  // identity-compared
public static ANode[] UNCHANGED  = new ANode[0];  // identity-compared
```

### CLJS Equivalents

- `nil` = UNCHANGED (element not found / already present)
- `:early-exit` = EARLY_EXIT (replace only; add/remove don't use this)
- `#js [node]` = single replacement
- `#js [n1 n2]` = split into two nodes

### Return Value Table

| Return Value | Meaning | When |
|---|---|---|
| `UNCHANGED` / `nil` | No modification needed | Key already in set (add), key not in set (remove/replace) |
| `EARLY_EXIT` / `:early-exit` | Transient in-place edit, no structural change | Editable node modified its arrays directly, maxKey unchanged |
| `[node]` (length 1) | Single node replacement | Node modified but maxKey may have changed (needs parent key update) |
| `[n1, n2]` (length 2) | Split | Node overflowed, split into two children |
| `[left, center, right]` (length 3) | Remove rebalance result (Java) | After remove, reports what happened to the target and its neighbors |

### Key Distinction: EARLY_EXIT vs [node]

Both indicate the operation succeeded, but they differ in what the parent must do:

- **EARLY_EXIT**: The child was mutated in place (transient). The parent does not
  need to update its `_children` array because the pointer still points to the
  same object. But the parent may still need to update its subtree count and measure.

- **[node]** (length 1): The child was replaced with a new node. The parent must
  install `node` into its `_children[idx]` and update `_keys[idx]` to
  `node.maxKey()`. This happens when:
  - The maxKey changed (e.g., added at the end of the last child)
  - A persistent (non-editable) node was modified (always creates a copy)

### Why length-1 Returns Exist

When `add` inserts a key that becomes the new maximum of a child, the parent
needs to update its separator key. The child signals this by returning
`[this]` (transient) or `[newNode]` (persistent) instead of EARLY_EXIT.

When the maximum key does *not* change, the child returns EARLY_EXIT to avoid
unnecessary parent work.

### Java vs CLJS Remove Return Convention

The remove return convention differs between platforms:

**Java** uses a fixed 3-element array: `[left-sibling-or-null, center, right-sibling-or-null]`.
Null entries mean "this sibling was consumed (joined)." The parent uses
identity comparison (`left != nodes[0]`) to tell whether a sibling changed.
The parent extracts the new root from `nodes[1]` (the center position).

**CLJS** uses variable-length arrays via `util/return-array`, which drops nil
entries. So a join that consumes the left sibling returns `#js [joined right]`
(length 2) or `#js [joined]` (length 1, if right was also nil). The parent
extracts the new root from `(aget nodes 0)` (first element). This is because
`util/rotate` produces the result and `return-array` strips nils, so there is
no fixed-position convention.

---

## Transient / Persistent Discipline

### Java: AtomicBoolean-based

The `Settings` object carries an `AtomicBoolean _edit`:

```java
// Settings.java
public boolean editable() {
    return _edit != null && _edit.get();
}
```

- **Persistent** sets have `_edit = null` (never editable).
- **Transient** sets share a single `AtomicBoolean(true)` across all nodes
  created during the transient session.
- `persistent()` flips the boolean to `false`, freezing all nodes atomically.
- Nodes check `editable()` to decide whether to mutate in place or copy.

The critical property: all nodes in a transient tree share the **same**
AtomicBoolean instance. When `persistent()` is called, every node in the tree
immediately becomes non-editable.

```java
// PersistentSortedSet.java
public PersistentSortedSet asTransient() {
    return new PersistentSortedSet(..., _settings.editable(true), ...);
}

public PersistentSortedSet persistent() {
    _settings.persistent();  // flips AtomicBoolean to false
    return this;
}
```

### CLJS: Map key-based

The CLJS settings map uses an `:edit` key:

```clojure
;; In branch.cljs $replace
(let [editable? (:edit settings)
      ...]
  (if editable?
    ;; mutate in place
    ;; else create new node
    ))
```

The `:edit` key is set to a truthy value during transient sessions and removed
when making persistent.

### Which Operations Check Editability

| Operation | Java | CLJS |
|---|---|---|
| Leaf.add | Yes (editable path) | No (always creates new) |
| Leaf.remove | Yes (editable path) | No (always creates new) |
| Leaf.replace | Yes (in-place key swap) | No (always copies) |
| Branch.add | Yes (3 editable paths) | No (always creates new) |
| Branch.remove | Yes (2 editable paths) | No (always creates new) |
| Branch.replace | Yes (2 editable paths) | Code exists but `:edit` never set |

**CLJS has no working transient/editable paths.** The `:edit` flag that
`Branch.$replace` checks is never set by any code path — see [Platform
Differences](#platform-differences) for the full analysis. All CLJS operations
always create new nodes.

---

## Operation: add

### Leaf.add (Java)

```
Leaf.add(storage, key, cmp, settings):
  1. Binary search for key
  2. If found → return UNCHANGED
  3. If editable AND room in array:
     a. If inserting at end → shift nothing, increment _len → return [this]
        (maxKey changed, parent needs update)
     b. Else → shift elements right, insert → return EARLY_EXIT
        (maxKey unchanged, parent needs no structural update)
  4. If room without splitting:
     → Create new Leaf with key spliced in → return [newLeaf]
  5. If full (splitting):
     → Split into two halves, insert key in correct half
     → return [leaf1, leaf2]
```

**Why step 3a returns [this] instead of EARLY_EXIT**: When the new key goes at the
end, it becomes the new maxKey of the leaf. The parent's separator key
(`_keys[idx]`) is `child.maxKey()`, so the parent must update it. Returning
`[this]` signals "I changed and my max key changed."

**Note**: The result from steps 4 and 5 is passed through `processLeafNodes`
if an `ILeafProcessor` is configured — see [ILeafProcessor](#ileafprocessor-java-only).

### Leaf.add (CLJS)

The CLJS version has no editable path. It always creates new nodes:

```
Leaf.add(storage, key, cmp, opts):
  1. Binary search for key
  2. If found → return nil
  3. If full (splitting):
     → Split into two halves with key in correct half
     → return #js [leaf1 leaf2]
  4. Otherwise:
     → Create new Leaf with key spliced in
     → return #js [newLeaf]
```

### Branch.add (Java)

```
Branch.add(storage, key, cmp, settings):
  1. Binary search → find child index (ins)
  2. Recurse: nodes = child(ins).add(...)
  3. If UNCHANGED → return UNCHANGED
  4. If EARLY_EXIT → increment subtree count, update measure → return EARLY_EXIT
  5. If nodes.length == 1 AND editable:
     → Update _keys[ins], _children[ins] in place
     → If ins is last child: return [this] (propagate maxKey update)
     → Else: return EARLY_EXIT
  6. If nodes.length == 1 (persistent):
     → Copy _keys, _children, _addresses arrays
     → Install new child, create new Branch → return [newBranch]
  7. If nodes.length == 2 AND room (no split needed):
     → Stitch arrays: splice in two children where one was
     → return [newBranch]
  8. If nodes.length == 2 AND full (split):
     → Split this branch into two halves, placing the two new children
        in the correct half
     → return [branch1, branch2]
```

**Note on step 5**: The original code had a `node.maxKey() == maxKey()` check
in the condition. This was always true because `_keys[ins] = node.maxKey()`
is performed before the check, so when `ins == _len - 1`, `maxKey()` returns
`_keys[_len-1]` which was just set to `node.maxKey()`. This redundant check
has been removed.

### Branch.add (CLJS)

The CLJS version follows the same logical flow but without editable paths.
Steps 4 and 5 from Java don't exist; instead:

```
Branch.add(this, storage, key, cmp, opts):
  1. Binary search → find child index (idx)
  2. Recurse into child → nodes
  3. If nil → return nil (UNCHANGED)
  4. Compute new-keys, new-children via splice/check-n-splice
  5. If new-children fits in branching factor:
     → Create new Branch → return #js [newBranch]
  6. If overflow:
     → Split at middle → return #js [leftBranch, rightBranch]
```

Because CLJS always creates new nodes, there is no EARLY_EXIT return from
Branch.add. The parent always receives either `nil` or `#js [node...]`.

---

## Operation: remove

### Leaf.remove (Java)

```
Leaf.remove(storage, key, left, right, cmp, settings):
  1. Binary search for key
  2. If not found → return UNCHANGED
  3. If enough keys remain AND (not merging needed):
     a. If editable:
        → Shift elements left over the gap
        → If removed last element: return [left, this, right] (maxKey changed)
        → Else: return EARLY_EXIT
     b. Persistent:
        → Create new Leaf without the key
        → return [left, newLeaf, right]
  4. If can join with left neighbor:
     → Merge left + this-minus-key → return [null, joined, right]
  5. If can join with right neighbor:
     → Merge this-minus-key + right → return [left, joined, null]
  6. If can borrow from left:
     → Redistribute keys between left and this → return [newLeft, newCenter, right]
  7. If can borrow from right:
     → Redistribute keys between this and right → return [left, newCenter, newRight]
```

**Note**: Steps 3b through 7 pass the center leaf through `processSingleLeaf`
if an `ILeafProcessor` is configured. Step 4/5 joins are also processed.

### The Java [left, center, right] Convention

The Java remove return value always has exactly 3 elements when removal succeeds:
`[left-sibling-or-null, center-node, right-sibling-or-null]`.

- A **null** in position 0 means "left sibling was consumed into center" (join).
- A **null** in position 2 means "right sibling was consumed into center" (join).
- Non-null values mean the sibling was modified (borrowed from).
- Identity comparison (`left != nodes[0]`) tells the parent whether a sibling changed.

The parent uses this to update its arrays: it replaces the range
`[left-idx .. right-idx)` with the non-null entries from the result.

### CLJS Leaf.remove

CLJS Leaf.remove differs from Java in a fundamental way: it delegates all
rebalancing to `util/rotate`:

```
Leaf.$remove(this, storage, key, root?, left, right, cmp, opts):
  1. Binary search for key
  2. If not found → return nil (UNCHANGED)
  3. Create new Leaf without the key (via cut-n-splice)
  4. Update measure (remove-measure or recompute)
  5. Call util/rotate(newLeaf, root?, left, right, settings)
     → rotate decides: no rebalance / merge / merge-split / borrow
     → returns variable-length array via return-array (nils stripped)
```

The result from `rotate` is a variable-length array (1-3 elements, no nils),
not a fixed 3-element `[left, center, right]` array. The parent (Branch.$remove
or btset/disjoin) processes this differently from Java — see
[CLJS rotate](#cljs-the-rotate-function).

### Branch.remove (Java)

```
Branch.remove(storage, key, left, right, cmp, settings):
  1. Binary search → find child index (idx)
  2. Load left-child and right-child (siblings of child[idx])
  3. Recurse: nodes = child(idx).remove(storage, key, leftChild, rightChild, ...)
  4. If UNCHANGED → return UNCHANGED
  5. If EARLY_EXIT → decrement subtree count, update measure → return EARLY_EXIT
  6. Compute newLen from which siblings survived
  7. If newLen adequate (no rebalance with parent-siblings needed):
     a. Editable AND idx < _len - 2:
        → Stitch updated children into arrays in place
        → return EARLY_EXIT
     b. Persistent:
        → Create new Branch with stitched arrays
        → return [left, newCenter, right]
  8. If can join with left parent-sibling:
     → return [null, joinedBranch, right]
  9. If can join with right parent-sibling:
     → return [left, joinedBranch, null]
  10. Borrow from left or right parent-sibling
      → return [newLeft, newCenter, right] or [left, newCenter, newRight]
```

**Step 7a restriction**: The editable in-place path is restricted to
`idx < _len - 2`. This avoids the complexity of in-place stitching near the
array boundary where the last child's maxKey determines the branch's maxKey.
Modifying near the end would require maxKey propagation upward, which the
EARLY_EXIT return cannot express. The persistent path handles all positions
correctly.

**Borrow preference (Java)**: When both siblings are available, Java borrows
from the **larger** sibling: `left._len >= right._len` → borrow from left.
This keeps sibling sizes balanced.

### CLJS Branch.remove

CLJS Branch.$remove also delegates rebalancing to `util/rotate`:

```
Branch.$remove(this, storage, key, root?, left, right, cmp, opts):
  1. Binary search → find child index (idx)
  2. Load left-child and right-child (siblings of child[idx])
  3. Recurse into child → child-result (variable-length array)
  4. If nil → return nil (UNCHANGED)
  5. Splice child-result into new arrays (replacing idx-range with result nodes)
  6. Create new Branch from spliced arrays
  7. Call util/rotate(newBranch, root?, left, right, settings)
```

Because CLJS child results have variable length (nils already stripped by
`return-array`), step 5 uses `cut-n-splice` on keys/children/addresses to
install the 1-3 result nodes where the old 1-3 nodes (idx and its siblings) were.

---

## Operation: replace

Replace is simpler than add/remove because it never changes the tree structure.
The comparator must return 0 for both old and new keys, so the key occupies the
same logical position.

### Leaf.replace (Java)

```
Leaf.replace(storage, oldKey, newKey, cmp, settings):
  1. Binary search for oldKey
  2. If not found → return UNCHANGED
  3. If editable:
     → _keys[idx] = newKey in place, recompute measure
     → If last element (maxKey changed): return [this]
     → Else: return EARLY_EXIT
  4. Persistent:
     → Create new Leaf with newKey at found position
     → Recompute measure → return [newLeaf]
```

Note: Replace does **not** use ILeafProcessor because it never changes the
number of keys — only substitutes one key for another.

### Leaf.replace (CLJS)

CLJS Leaf.$replace always clones and returns `#js [newLeaf]`. It never returns
`:early-exit` because there is no editable path.

### Branch.replace (Java)

```
Branch.replace(storage, oldKey, newKey, cmp, settings):
  1. Binary search → find child index (idx)
  2. Recurse: nodes = child(idx).replace(...)
  3. If UNCHANGED → return UNCHANGED
  4. If EARLY_EXIT → recompute measure → return EARLY_EXIT
  5. If editable:
     → Update _keys[idx] and _children[idx] in place
     → If maxKey changed: return [this]; else: return EARLY_EXIT
  6. Persistent:
     → Copy all arrays (_keys, _children, _addresses)
     → Create new Branch → return [newBranch]
```

### CLJS Branch.$replace

This is the **only** CLJS operation that has an editable/persistent distinction:

```clojure
(if editable?
  ;; Transient: mutate in place
  (do (aset keys idx new-max-key) ...)
  ;; Persistent: clone ALL arrays
  (let [new-keys (arrays/aclone keys) ...]))
```

This is because replace is heavily used by Datahike (every transaction replaces
datoms), so the optimization matters for performance.

The CLJS Branch.$replace handles `:early-exit` from children by propagating it
upward. If the child returns `:early-exit`, the branch returns `:early-exit`
(line 291 in branch.cljs). The btset.cljs `$replace` function handles this
by returning a new BTSet with the same root (the root was modified in place)
and a cleared address.

---

## Parent Dispatch (cons / disjoin)

### PersistentSortedSet.cons (Java)

```
cons(key, cmp):
  1. nodes = root().add(storage, key, cmp, settings)
  2. If UNCHANGED → return this
  3. Mark old root address as freed
  4. If editable (transient):
     a. If nodes.length == 1 → _root = nodes[0]
     b. If nodes.length == 2 → create new Branch root above both
     c. If nodes.length == 0 (EARLY_EXIT) → no-op (tree modified in place)
     d. Clear _address, increment _count and _version
     e. Return this (same mutable object)
  5. If persistent:
     a. If nodes.length == 1 → new PersistentSortedSet with nodes[0] as root
     b. If nodes.length == 2 → new Branch root, new PersistentSortedSet
```

### BTSet conjoin (CLJS)

```
conjoin(set, key, cmp, opts):
  1. roots = root.add(storage, key, cmp, opts)
  2. If nil → return set (UNCHANGED)
  3. If roots.length == 1 → new BTSet with roots[0]
  4. If roots.length == 2 → create Branch root above both → new BTSet
```

No EARLY_EXIT handling because CLJS add never returns EARLY_EXIT.

### PersistentSortedSet.disjoin (Java)

```
disjoin(key, cmp):
  1. nodes = root().remove(storage, key, null, null, cmp, settings)
  2. If UNCHANGED → return this
  3. If EARLY_EXIT → clear address, decrement count, return this
  4. newRoot = nodes[1]  (center of [left, center, right])
  5. If newRoot is a Branch with 1 child → unwrap (shrink tree height)
  6. If editable → update _root in place
  7. If persistent → new PersistentSortedSet
```

The root has no siblings, so `remove` is called with `left=null, right=null`.
This means the root never merges/borrows—it only shrinks or stays. When the
root becomes a single-child Branch, we unwrap it to reduce tree height.

### BTSet disjoin (CLJS)

```
disjoin(set, key, cmp, opts):
  1. new-roots = root.$remove(storage, key, true, nil, nil, cmp, opts)
  2. If nil → return set (UNCHANGED)
  3. newRoot = (aget new-roots 0)   ← NOTE: position 0, not 1
  4. If newRoot is Branch with 1 child → unwrap
  5. Return new BTSet
```

**Important difference from Java**: CLJS extracts the new root from `nodes[0]`
(first element), while Java extracts from `nodes[1]` (center position). This
is because CLJS `util/rotate` with `root?=true` returns `(return-array node)`,
which produces a single-element array `#js [node]`. Java's root remove returns
`[null, center, null]` and the parent reads `nodes[1]`.

### PersistentSortedSet.replace / BTSet.$replace

**Java**:
```
replace(oldKey, newKey, cmp):
  1. nodes = root().replace(...)
  2. If UNCHANGED → return this
  3. If EARLY_EXIT → clear address, increment version, return this
  4. New root = nodes[0], create new PersistentSortedSet
```

**CLJS**:
```
$replace(set, oldKey, newKey, cmp, opts):
  1. nodes = root.$replace(...)
  2. If nil → return set
  3. If :early-exit → return new BTSet with same root, cleared address
  4. New root = (aget nodes 0), return new BTSet
```

---

## Rebalancing

### When Rebalancing Happens

Rebalancing occurs during `remove` when a node's key count drops below
`minBranchingFactor` (= `branchingFactor / 2`). The affected node tries, in
order:

1. **No rebalance needed**: If `newLen >= minBF`, or this is the root
   (root has no siblings to merge with).

2. **Join with left sibling**: If `left._len + newLen <= branchingFactor`.
   The left sibling is consumed into the center. Returns `[null, joined, right]`.

3. **Join with right sibling**: Same check with right.
   Returns `[left, joined, null]`.

4. **Borrow from a sibling**: Redistribute keys so both sides have ~half.
   Returns `[newLeft, newCenter, right]` or `[left, newCenter, newRight]`.

### Borrow Preference Difference

When both siblings are available for borrowing, the platforms differ:

| Platform | Borrow preference | Rationale |
|---|---|---|
| **Java** | Larger sibling (`left._len >= right._len`) | Keeps sibling sizes balanced |
| **CLJS** | Smaller sibling (`(< (node/len left) (node/len right))` → borrow from left) | Different heuristic (borrow-from-smaller fills the underflowing node more) |

Both approaches are correct — they just produce differently-shaped trees for
the same logical content.

### CLJS: The `rotate` Function

CLJS consolidates the rebalancing decision into `util/rotate`:

```clojure
(defn rotate [node root? left right settings]
  (let [min-len (/ (:branching-factor settings 32) 2)]
    (cond
      root? (return-array node)
      (> (node/len node) min-len) (return-array left node right)
      (and left (<= (node/len left) min-len)) (return-array (node/merge left node) right)
      (and right (<= (node/len right) min-len)) (return-array left (node/merge node right))
      ;; borrow from smaller sibling
      (and left (or (nil? right) (< (node/len left) (node/len right))))
        (let [nodes (node/merge-split left node)]
          (return-array (aget nodes 0) (aget nodes 1) right))
      :else
        (let [nodes (node/merge-split node right)]
          (return-array left (aget nodes 0) (aget nodes 1))))))
```

The `return-array` function drops nil entries, so `(return-array nil joined right)`
produces `#js [joined right]` (or `#js [joined]` if right is also nil).

This is a key architectural difference from Java: Java inlines the rebalancing
logic inside `Leaf.remove` and `Branch.remove` with the fixed 3-element
`[left, center, right]` convention. CLJS factors it into a single `rotate`
function that both `Leaf.$remove` and `Branch.$remove` call, producing
variable-length arrays.

### Merge and Merge-Split

- **merge**: Concatenates two nodes' arrays. Used when two siblings together fit
  in one node.
- **merge-split**: Concatenates then splits at midpoint. Used when the combined
  length is too large for one node but the current node is too small on its own.

Both operations exist on Branch and Leaf via the `INode` protocol.

---

## ILeafProcessor (Java only)

### Overview

`ILeafProcessor` is a Java-only hook interface that allows post-processing of
leaf contents after add/remove operations. It enables custom compaction,
splitting, or merging of entries based on application-specific logic (e.g.,
merging small chunks in a columnar index).

**CLJS does not have an equivalent.** This is a Java-only extension point.

### Interface

```java
public interface ILeafProcessor<Key> {
  List<Key> processLeaf(List<Key> entries, IStorage storage, Settings settings);
  default boolean shouldProcess(int leafSize, Settings settings) { return false; }
}
```

- `processLeaf`: Called with the leaf's entries after add/remove. Can return
  same entries (no change), fewer entries (merged), or more entries (split).
- `shouldProcess`: Optimization to skip allocation when processing isn't needed.
  Default returns `false` (no processing).

### When It Fires

The processor is configured via `Settings.leafProcessor()`. When present:

1. **After Leaf.add** (steps 4-5): The result array from add is passed through
   `processLeafNodes()`, which processes each Leaf in the result. If the
   processor splits a leaf, the result array may grow.

2. **After Leaf.remove** (join/borrow steps): The center leaf is passed through
   `processSingleLeaf()`, which may split it into multiple leaves.

3. **Not after replace**: Replace only substitutes one key for another without
   changing the count, so no processing is needed.

### Zero-Overhead When Unused

When `settings.leafProcessor()` returns `null` (the default), both
`processLeafNodes` and `processSingleLeaf` return their input immediately.
There is no allocation, no list creation, no overhead.

### Interaction with Parent Dispatch

The processor can change the number of leaf nodes returned. For example, a
Leaf.add that normally returns `[leaf]` (length 1) might return `[leaf1, leaf2, leaf3]`
after processing. The parent (Branch.add or PersistentSortedSet.cons) currently
only expects length 1 or 2 from add. If a processor produces more than 2 nodes
from an add operation, the current Branch.add code would not handle it correctly.

This is an area for future development — the processor is designed for remove-time
compaction more than add-time splitting.

---

## Copy-on-Write Correctness

### The Core Invariant

**No persistent node may share mutable arrays with any transient node.**

When `asTransient()` creates a transient set, the transient initially shares the
root node (and all its children) with the persistent original. From this point,
every mutation must ensure that any array it writes to is exclusively owned by
the transient session. This is the fundamental correctness requirement for
structural sharing between persistent and transient trees.

### Why This Invariant Is Necessary

Consider the lifecycle:

1. A persistent set `P` exists with root branch `B`, which has `_keys` array `K`.
2. `T = transient(P)` — `T` shares `B` and `K`.
3. `T` performs an `add` that modifies a *different* child of `B`. If the
   editable path writes to `K[j]` in place, it corrupts `P`'s separator keys.
4. Future lookups on `P` descend via binary search on `K`, which now has
   incorrect separator keys, causing elements to become unfindable.

This corruption is silent — `P` still contains all elements in its leaves, but
the root-descend navigation path is broken.

### How Java Enforces This

Each node checks `editable()` before deciding whether to mutate in place or
copy. The discipline is:

- **Editable path** (`editable() == true`): The node belongs to the current
  transient session. Its arrays can be mutated directly.
- **Persistent path** (`editable() == false`): The node may be shared with
  persistent trees. All arrays must be freshly copied before any modification.

```java
// Branch.add, same-len case
if (1 == nodes.length && editable()) {
    // SAFE: this node is owned by the transient session
    _keys[ins] = node.maxKey();
    child(ins, node);
    return ...;
}

if (1 == nodes.length) {
    // REQUIRED: copy all arrays, even if only one element differs
    Key[] newKeys = Arrays.copyOfRange(_keys, 0, _len);
    newKeys[ins] = node.maxKey();
    Object[] newChildren = Arrays.copyOfRange(_children, 0, _len);
    newChildren[ins] = node;
    return new ANode[]{ new Branch(..., newKeys, ..., newChildren, ...) };
}
```

The persistent path **always** creates fresh arrays via `Arrays.copyOfRange`.
This must hold even when the value being stored is identical to what was there
before — the array itself must not be shared, because a future transient may
modify a *different* index of that same array.

This applies uniformly across all three arrays: `_keys`, `_children`, and
`_addresses`. Every persistent code path in `add`, `remove`, and `replace` must
copy all arrays that will be installed in the new node.

### How CLJS Enforces This

CLJS takes a simpler approach for most operations: `add` and `$remove` always
create new nodes with fresh arrays (via `util/splice`, `util/check-n-splice`,
`.slice`). There is no editable fast-path for these operations, so array sharing
cannot occur.

For `Branch.$replace`, which does have an editable/persistent distinction, the
persistent path uses explicit cloning:

```clojure
;; Persistent path in $replace — clone ALL arrays
(let [new-keys     (arrays/aclone keys)
      new-children (arrays/aclone children)
      new-addrs    (when addrs (arrays/aclone addrs))
      ...]
```

The `util/check-n-splice` helper always creates a fresh array (delegates to
`splice`) rather than ever returning the original array. This ensures that even
when the spliced content is identical to the original, the returned array is a
distinct allocation that can be safely mutated by a transient.

### Why Leaf.replace Copies in the Persistent Path

In the persistent path, both Java and CLJS `Leaf.replace` create a new leaf with
a copied keys array. This is required because `replace` substitutes the actual
key object (e.g., a datom whose non-compared fields differ). Sharing the keys
array would mean the replaced key appears in both the old and new logical
versions of the tree, violating persistence.

Java's editable (transient) path can safely write `_keys[idx] = newKey` in
place, because the transient owns the array exclusively.

### Correctness Argument Summary

The copy-on-write invariant holds because:

1. **Persistent nodes never mutate arrays.** Every persistent code path
   allocates fresh arrays (`Arrays.copyOfRange`, `arrays/aclone`, `splice`,
   `.slice`) and installs the new values into the copy.
2. **Editable nodes only mutate arrays they own.** The `editable()` check
   ensures that in-place mutation only happens on nodes created within the
   current transient session (sharing the same `AtomicBoolean`/`:edit` token).
3. **`persistent!` atomically freezes all nodes.** Setting the `AtomicBoolean`
   to `false` (Java) or removing `:edit` (CLJS) prevents any future in-place
   mutations on the now-persistent tree.
4. **Array identity is never reused across the persistent/transient boundary.**
   Even when a persistent operation produces a value identical to what was
   already stored, it writes to a fresh copy of the array.

---

## Platform Differences

| Aspect | Java (CLJ) | ClojureScript |
|---|---|---|
| Editable check mechanism | `AtomicBoolean` in `Settings._edit` | `:edit` key in settings map |
| Leaf.add editable path | Yes (in-place insert/append) | No |
| Leaf.remove editable path | Yes (in-place shift) | No |
| Leaf.replace editable path | Yes (in-place key swap) | No (always copies) |
| Branch.add editable path | Yes (EARLY_EXIT + [this]) | No |
| Branch.remove editable path | Yes (in-place stitch, restricted to `idx < _len-2`) | No |
| Branch.replace editable path | Yes | Dead code (`:edit` never set) |
| EARLY_EXIT sentinel | `new ANode[0]` (identity) | `:early-exit` keyword |
| UNCHANGED sentinel | `new ANode[0]` (identity) | `nil` |
| Remove return convention | Fixed 3-element `[left, center, right]` with nulls | Variable-length via `return-array` (nils stripped) |
| Root extraction (disjoin) | `nodes[1]` (center position) | `(aget nodes 0)` (first element) |
| Remove rebalancing | Inline in Branch.remove | `util/rotate` + `INode.merge` / `INode.merge-split` |
| Borrow preference | Larger sibling | Smaller sibling |
| Leaf post-processing | `ILeafProcessor` hook | Not available |
| Async support | No | Yes (via `async+sync` macro) |

### CLJS Transients Are Not Implemented

The most significant mismatch: **CLJS has no working transient support.**

BTSet's transient protocol is a pass-through:

```clojure
IEditableCollection
(-as-transient [this] this)           ;; no edit flag, returns same object

ITransientCollection
(-conj! [this key] (conjoin this key ...))  ;; calls persistent conjoin
(-persistent! [this] this)            ;; no-op

ITransientSet
(-disjoin! [this key] (disjoin this key ...))  ;; calls persistent disjoin
```

Neither `-as-transient` nor any other code path sets `:edit` in the settings map.
This means:

- `(transient s)` returns `s` itself unchanged
- `(conj! t v)` creates a new BTSet (the normal persistent path)
- `(persistent! t)` returns `t` unchanged
- `(persistent! (reduce conj! (transient s) xs))` works correctly but is
  identical in performance to `(reduce conj s xs)`

The `:edit` check in `Branch.$replace` (line 277) reads from settings, but since
no code ever sets `:edit`, the editable branch is **dead code**.

### Consequence: No EARLY_EXIT Propagation in CLJS

In Java, EARLY_EXIT is the key optimization for transients. When a transient
leaf modifies itself in place, it returns EARLY_EXIT. Its parent branch (also
editable) increments the count in place and returns EARLY_EXIT. This propagates
up the entire stack — zero allocations from leaf to root.

In CLJS, because there are no editable paths, every `conj`/`disj` allocates:
- A new Leaf (via splice)
- A new Branch at every level (via splice/check-n-splice)
- A new BTSet wrapper

This means O(depth) node allocations per operation in CLJS, versus O(1)
(amortized) in Java transients.

### Correctness Equivalence

Despite the missing transient optimization, both platforms maintain the same
**correctness** invariants:

1. Persistent operations never modify existing nodes (always return new trees).
2. `conj`/`disj`/`replace` return a logically new set with the expected contents.
3. No array is shared between a returned persistent set and any future mutation.

The CLJS implementation is correct because it always creates fresh nodes — the
copy-on-write invariant is trivially satisfied when nothing is ever written in
place.

---

## Subtree Counts and Measures

### Subtree Count

Each Branch stores `_subtreeCount`: the total number of leaf keys in its subtree.
This enables O(1) `count()` and O(log n) `countSlice()`.

- **Known count** (`>= 0`): The count is accurate and cached.
- **Unknown count** (`-1`): The count must be computed by traversing children.
  This happens for trees restored from old storage formats.

### Count Maintenance Rules

1. **EARLY_EXIT (transient add)**: `_subtreeCount += 1` (if known).
2. **EARLY_EXIT (transient remove)**: `_subtreeCount -= 1` (if known).
3. **New branch (persistent)**: `newCount = oldCount + delta` (if old was known).
4. **Split**: Use `tryComputeSubtreeCountFromChildren` to compute from children.
5. **Join**: `leftCount + rightCount - 1` (if both known), else `-1`.
6. **Borrow**: Use `tryComputeSubtreeCountFromChildren` for redistributed halves.

### Measure

Similar to subtree counts, each node caches an aggregate measure (`_measure`)
computed from `IMeasure` operations (extract, merge, remove, identity, weight).

- Measures are maintained incrementally when possible (add/remove one element).
- After structural changes (splits, borrows), measures are recomputed from
  children via `tryComputeMeasure`/`tryComputeMeasureFromChildren`.
- `null` measure means "not yet computed" — it will be computed lazily when
  accessed via `forceComputeMeasure`.

### Non-Deterioration Guarantee

For **fresh** trees (not restored from storage), subtree counts are always known
(`>= 0`) and measures are always computed. Operations maintain these invariants
through all structural changes, including splits and borrows. The
`tryComputeSubtreeCountFromChildren` helper ensures that split/borrow operations
compute accurate counts from their (in-memory) children rather than defaulting
to `-1`.

This guarantee is verified by the `subtree-counts-always-known-for-fresh-trees`
and `subtree-counts-always-known-after-transient` generative tests.

---

## Test Coverage

The test suite verifies correctness of every operation described in this document
across both CLJ and CLJS. Tests are in `test-clojure/org/replikativ/persistent_sorted_set/test/`.

### Property-Based / Generative Tests

**`invariants.cljc`** — B-tree structural invariant tests (200 samples each):
- `structural-invariants-after-ops`: Arbitrary conj/disj sequences maintain valid
  B-tree structure (key ordering, fill factor, level consistency).
- `structural-invariants-after-transient`: Same via transient operations.
- `structural-invariants-with-replace`: Identity replacement preserves structure.
- `structural-invariants-heavy-rebalancing`: Removing 75% of elements triggers
  extensive merge/borrow cascades — all invariants still hold.
- `invariants-after-every-op`: Checks invariants after **each individual** operation,
  not just at the end (catches transient corruption).
- `subtree-counts-always-known-for-fresh-trees`: Verifies the non-deterioration
  guarantee — fresh trees never have unknown (-1) subtree counts.
- `subtree-counts-always-known-after-transient`: Same via transient.
- `transient-does-not-corrupt-persistent`: Regression test for shared `_keys`
  array between persistent and transient (upstream issue #19).
- `structural-sharing-preserves-invariants`: Two derived sets from the same base
  don't corrupt each other.
- `compact-preserves-elements`: Compaction produces valid tree with same elements.
- `navigation-valid-after-ops`: Element-wise navigation (binary search descend)
  finds every element.
- Equational properties: `first-equals-min`, `last-equals-max`, `conj-is-idempotent`,
  `disj-nonmember-is-noop`, `count-after-conj`, `count-after-disj`,
  `contains-consistent-with-seq`, `lookup-returns-stored-element`.

**`generative.cljc`** — Model-based and cross-cutting generative tests (100 samples each):
- `operations-match-sorted-set`: PSS matches `clojure.core/sorted-set` behavior
  for arbitrary operation sequences.
- `lazy-set-count-after-operations`: Count correct after storage roundtrip + mutations.
- `lazy-set-count-with-interleaved-count-calls`: Count correct when `count` is
  called between operations (forces lazy computation).
- `transient-persistent-roundtrip`: Transient and persistent paths produce same result.
- `rebalancing-preserves-count`: Small branching factor (8) triggers heavy
  rebalancing — count stays correct.
- `split-heavy-operations` / `merge-heavy-operations`: BF=4 stress tests for splits and merges.
- `derived-sets-independent-counts`: Two sets derived from same lazy base have correct counts.
- `interleaved-transient-operations`: Two transients from same base don't interfere.
- `slice-contents-correct` / `rslice-contents-correct`: Forward and reverse slices match expected.
- `lazy-slice-contents-correct` / `lazy-rslice-contents-correct`: Same via lazy sets.
- `count-slice-matches-filter-count` / `count-slice-on-lazy-set` / `count-slice-after-modifications`:
  `count-slice` matches naive filter+count at all times.
- Stats/measure tests: `stats-matches-expected`, `stats-after-modifications`,
  `measure-slice-matches-filter`, `measure-slice-after-modifications`,
  `replace-preserves-measure`, `get-nth-matches-vec-nth`,
  `rebalancing-preserves-measure`, `transient-preserves-measure`,
  `stats-after-roundtrip-and-modifications`, `measure-slice-after-roundtrip`.

### Deterministic Tests

**`measure_test.cljc`** — Focused measure correctness:
- Add measure correctness (100 elements, multi-level tree).
- Remove measure correctness (including min/max boundary elements).
- Transient remove with measure.
- `count-slice` / `get-nth` / `measure-slice` functional correctness.
- Measure consistency through conj/disj/replace sequences.
- Subtree count propagation through add/remove/replace and heavy rebalancing.
- Large tree stress (2000 elements, add/remove half).

**`replace_and_lookup.cljc`** — Replace + lookup with custom comparators:
- Persistent and transient replace with identity and non-default comparators.
- Replace at boundaries (first/last element).
- Replace preserving measure.
- `get-nth` accuracy after replace operations.

**`core.cljc`** — Slice/rslice boundary tests, reduces, count-slice, seek.

**`small.cljc`** — Basic smoke tests (add, remove, contains, seq).

**`stress.cljc`** — 100-iteration stress: random conj/disj, persistent + transient,
  full removal, comparison with `clojure.core/sorted-set`.

**`storage.clj`** (CLJ only) — Lazy loading, roundtrip, stable addresses,
  incremental persist, walk-addresses, loaded/durable ratio tracking.

### Diagnostics Library

The `diagnostics.cljc` namespace provides validation functions used by tests:
- `validate-full`: Complete B-tree invariant check (key ordering, fill factors,
  level consistency, separator key correctness, element-wise navigation).
- `validate-counts-known`: Verifies no node has unknown (-1) subtree count.
- `validate-navigation`: Verifies every element can be found via binary search descend.
- `tree-stats`: Returns depth, element count, fill histograms.
