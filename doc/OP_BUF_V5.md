# Write-optimized PSS — v5 (per-node diffs at the serialization boundary)

Status: design, converged. Supersedes v4 (`OP_BUF_V4.md`, materialized-root
overlay) and v3. Branch: TBD (off `main`).

The in-memory tree stays an ordinary B-tree (all query/mutate logic unchanged).
Write amplification is reduced entirely at the **serialization boundary**: every
branch node carries **per-child diff slots**; `store` writes only the resident
nodes we actually touched, recording the rest as diffs against their durable
versions; `restore` projects those diffs back as it descends, reusing the normal
`add`/`remove`/`replace`. Diffs are a storage concern only.

Grounded in the literature (Bε-trees / TokuDB-PerconaFT / BetrFS) and the
datopia hitchhiker-tree post; grounded in the actual PSS mutation code
(`Leaf`/`Branch` split/merge/borrow + count/measure maintenance).

---

## 0. Goal and the hard constraint

Cut datahike's write amplification on object storage — today a commit rewrites
each index's root→leaf path (≈7 object PUTs/commit) — toward **~1 PUT per
commit**, without changing query semantics and without the Bε 1/ε read penalty.

The constraint that shapes everything: **on immutable content-addressed storage,
persisting any one node changes its hash → changes its parent's pointer →
cascades to the root.** A naïve buffer-on-disk Bε port rewrites the whole spine
and defeats buffering. The escape: **record a child's change as a diff inside the
node we rewrite anyway, the diff referencing the child's *unchanged durable
address*.** Only the rewritten spine-nodes become new objects; everything they
point at keeps its hash.

---

## 1. The model

- **In-memory tree** — an ordinary, balanced, lazily-loaded PSS B-tree. Reads and
  mutations use it unchanged (splits/merges/borrows happen normally). It is the
  authoritative logical content.
- **Durable representation** — immutable content-addressed objects forming the
  **last-written ("anchored") structure**, where each branch object additionally
  carries, per child, a **diff** (the child's logical changes since it was last
  written) and an **aggregate snapshot** `ĝ = (count, measure)` of the child's
  *current* subtree.
- **The buffer-vs-write decision rides the existing PSS return protocol.** A
  mutation's return value already says whether a level changed *content-only*
  (`EARLY_EXIT` / single `{node}`, incl. a maxKey bump) or *structurally* (split
  `{a,b,…}` / merge / borrow). On the return path each node accumulates the op's
  logical diff (`key → Present|Absent`) + refreshes `ĝ`; at `store` a node that
  only changed content (no rebalance below it, no child written) is **buffered**
  as a diff in its parent, while a node that **rebalanced** (or any node a child
  was written under) is **written** in full — cascading to the root.
- **A buffered node has the *same structure* as its durable anchor** (only its
  content differs), so the in-memory and durable structures **never diverge**:
  rebalancing is materialized on write the instant it happens. This is baseline
  PSS's store with one change — content-only dirty nodes buffer instead of being
  written. (Earlier drafts let structure diverge and reconciled at flush; that
  "W0" problem is gone, and so is any merge-on-read sibling load.)

A commit deposits its logical changes into diff slots along the path (constructed
recursively on the mutation's return path); `store` buffers content-only dirty
nodes and writes rebalanced ones (and, to honor `B`, any buffered child whose diff
would overflow the parent's budget); `restore` projects diffs back on descent.

```
            logical content (one truth)
                       │
     in-memory                         durable (content-addressed)
     normal B-tree,                    anchored structure (last write) +
     splits/merges                     per-child { diff, ĝ } against the
     normally, lazily                  child's unchanged durable address.
     loaded. Reads here.               Diverges from in-memory until a flush
                                       reconciles by writing touched nodes.
```

---

## 2. Objects and definitions

- **Durable branch object**: `⟨ pivots pᵢ, child-refs aᵢ, slotsᵢ ⟩`, where `pᵢ`
  is child *i*'s max key (PSS high-key convention), `aᵢ` is child *i*'s durable
  content-address, and `slotᵢ = (diffᵢ, ĝᵢ)`:
  - `diffᵢ` — the **node-diff** carrying child *i*'s logical changes since `aᵢ`
    was written (grammar in §6);
  - `ĝᵢ = (count, measure)` — absolute snapshot of child *i*'s **current**
    subtree.
- **Durable leaf object**: its keys (leaves carry no slots — like BetrFS basement
  nodes).
- **`reconstruct(a)` / materialize**: load object `a`; a leaf yields its keys; a
  branch materializes each child *i* as `apply(diffᵢ, reconstruct(aᵢ))` (§4.3),
  setting the branch's per-child `(count, measure)` from `ĝᵢ` (no re-summing).
  `materialize(D) := reconstruct(root)`.

Pivots are recomputed from materialized children, not stored in the diff: a
buffered key can extend a child's max past its stored pivot, and the in-memory
materialization recomputes it.

---

## 3. Invariants

- **CI — Correctness.** `materialize(D) == M` (logical content) after every
  commit. The diffs reconstruct the exact content. ("Diffs travel correctly.")
- **BI — Budget.** Every durable branch object has total diff `Σ|diffᵢ| ≤ B`.
  Bounds object size and cold-reconstruction cost. `B < 100` entries/node to
  start; swept later.
- **AGG — Aggregate exactness.** `ĝᵢ == (count, measure)` of child *i*'s current
  subtree. Lets a reader answer rank/`countSlice`/`measure` for a child *without
  loading it*, and lets materialization set a branch's aggregates without summing
  (loading) children.
- **Recency.** A node's `diffᵢ` is, by construction, the delta to apply **on top
  of the fully-materialized child** (which already has its own deeper diffs
  applied). Reconstruction is strict top-down projection — depth encodes order,
  **no timestamps/MSN**.
- **I0 — Disabled is identical.** Feature off (`B = ∞`) ⇒ every path is exactly
  baseline PSS, byte-identical on disk. (All changes fork on the feature gate.)
- **Back-compat.** A node with no slots deserializes as empty diffs; the new
  format is a strict superset. Old dbs read; rewritten nodes are new format.

---

## 4. Operations

### 4.1 Commit (deposit, recursively on the return path)

```
commit(batch):
  M := apply(batch, M)                 # normal B-tree; exact count/measure maintained
  # As each op returns up the path, record its logical effect into the parent's
  # slot for the child it descended into, and refresh that slot's ĝ from M:
  #   insert k  → slot gains  Present(k)
  #   remove k  → slot gains  Absent(k)
  #   replace   → slot gains  Present(k')
  # (no tree-diffing; the op states its own effect)
  store(root)                          # §4.2
```

Within one slot, a later op on the same `cmp`-key overwrites (net latest-wins).
The diff is **nested**: a branch child's slot holds the child's own per-grandchild
slots (built recursively), bottoming out at leaf key-states.

### 4.2 Store — write the resident touched children, buffer the rest

A dirty node carries two bits set on the mutation return path:
**rebalanced?** (a split/merge/borrow happened at this node) and **child-written?**
(set at store when any child gets written). A node is **bufferable** iff it is
dirty, *not* rebalanced, and no child of it was written — i.e. only its content
changed, so its structure equals its durable anchor's.

```
store(node):
  if clean (has _address): return _address              # unchanged → reuse durable object
  child-written? := false
  for each dirty child cᵢ:
     if cᵢ.rebalanced? or cᵢ.child-written? or (Σ embedded diff would exceed B):
        aᵢ := store(cᵢ); markFreed(old aᵢ); child_refᵢ := aᵢ; slotᵢ := (∅, ĝᵢ)
        child-written? := true                          # cᵢ's address changed → I am structural
     else:
        slotᵢ := (cᵢ's accumulated nested diff, ĝᵢ)      # BUFFER: do NOT write cᵢ, do NOT markFreed
  write node (base + slots); return new _address         # written because rebalanced/child-written/root
```

- The **root is always written** ⇒ ≥1 PUT.
- **Structural changes (split/merge/borrow) are written**, materializing the new
  structure immediately, so the durable structure always matches in-memory — no
  divergence, no re-derive, no merge-on-read sibling load. A rebalancing commit
  writes the affected path (~depth PUTs); rebalances are ~1/BF of ops, so
  amortized ≈ 1 + depth/BF ≈ 1.01 PUT/commit. Bulk-then-store writes once.
- **`BI` is enforced here too**: if buffering a child would push this node's
  embedded diff over `B`, write that child instead (materializing its diff). So
  every stored object keeps `Σ embedded diff ≤ B`.
- **`markFreed` only when a node is written** — a buffered child keeps its durable
  address as the anchor its diff is against; its diff is replaced each commit and
  reset when it is written ⇒ apply-once, never a chain.

### 4.3 Restore — project diffs on descent

```
child(node, i):                                      # lazy, on first descent into child i
  if loaded: return cached
  base := load(aᵢ)                                   # 1 GET (durable object)
  mat  := reconstruct(base)                          # recursively materialize, applying base's OWN slots
  for (k, st) in diffᵢ (this parent's slot — newer — on top):   # recency by depth
     Absent(k)     → mat.remove k     # underflow → merge/borrow: may load a sibling (see below)
     Present(k,e)  → mat.add/replace e # overflow → split: LOCAL, no IO
  set mat's per-child aggregates from ĝ (no re-summing); recompute pivots locally
  cache mat ; return mat
```

Application is **IO-free and never restructures on read**:
- A buffered child changed *content only* (its structure equals the durable
  anchor), so applying its diff inserts/removes keys within the **same** node
  shape — a buffered insert/delete may push a leaf transiently over/under the
  fill bounds, but it is **not** split/merged on read (rebalancing was already
  materialized on the writer's store). So no sibling load, no local restructure.
- **Aggregates never force IO** — a materialized branch takes its `(count,
  measure)` from `ĝ`; children are not summed.

(Because structure is materialized on write, a reconstructed tree is structurally
identical to the writer's, not merely content-equal.)

### 4.4 Writing a child

When `store` writes child *i* of `N` (because it rebalanced, a grandchild was
written, or its buffered diff would overflow `B`), it serializes the in-memory
subtree rooted at *i* in full — its current (already-correct) structure — pushing
*its* still-buffered, content-only descendants one level down into *that* node's
slots. `aᵢ := addr(written)`, `slotᵢ.diff := ∅`. All written nodes are resident
(they are the dirty in-memory nodes), so writing never reads. Because structure is
always materialized on write, there is no slot-structure reconciliation to do.

---

## 5. Cost

**Writes (PUT/commit).**
- Common (content-only commit, no rebalance): **~1 PUT** (root); the changed path
  is buffered as diffs in the root's object.
- A **rebalancing commit** (split/merge/borrow) writes the affected path
  (~depth PUTs). Rebalances are ~1/BF of ops ⇒ amortized ≈ 1 + depth/BF ≈ 1.01
  PUT/commit. Bulk-then-store writes the tree once (like baseline).
- A **BI-overflow** commit writes the over-budget buffered children to materialize
  their diffs (keeps every object's embedded diff ≤ B).
- Composes with datahike **root fusion**: index-root slots ride inside the fused
  db-record → a small commit can be ~1 PUT *total* across indexes.

**Reads.** Warm = normal materialized B-tree, `O(log_B N)`, no 1/ε penalty (full
fanout; projection paid once on load, then cached). Cold = load path + applying
each node's buffered logical diff (bounded by `B`); **no read-path restructuring
and no sibling loads** (rebalancing was materialized on write). `ĝ` answers cold
rank/count for a child without loading it.

**`B` tension.** Larger `B` buffers more commits (fewer flushes) but bloats every
object and raises cold-reconstruction cost; smaller `B` the reverse. Start
`< 100`; sweep.

---

## 6. The diff language

Grounded in the PSS mutation code: Branch `_keys[i]` = maxKey of child *i*; leaf
splits at `totalLen > BF` into `ceil(totalLen/BF)` leaves; branch splits at
`newLen > BF`; underflow `< BF>>1`, merge-before-borrow, left-before-right;
`Branch._measure` always recomputed from children; non-invertible measures
(min/max) handled by `IMeasure.remove`'s recompute fallback.

### 6.1 Grammar (recursive, nested, per node)

```
node-diff   ::= leaf-diff | branch-diff
leaf-diff   ::= { cmp-key → Absent | Present(element) }   # net latest-wins per key
branch-diff ::= { child-index → (node-diff, ĝ) }          # nested, per child
ĝ           ::= (count, measure)                          # absolute snapshot
```

One recursive form. `Present(element)` carries the stored element, expressing
**add** (key absent) and **replace** (key present as a `cmp`-equal but different
element); `Absent` expresses **remove**. The aggregate is **absolute state**, not
a delta — so non-invertible measures are fine, and it is free to collect (the
in-memory node already maintains `count`/`measure`).

### 6.2 Why logical states suffice — no structural encoding

A node is buffered **only when its structure equals its durable anchor's** (it
changed content only). So its slot needs to record just the logical key changes —
there is no structural delta to express. A node that **rebalanced** (split/merge/
borrow) is **written** in full, materializing the new structure, so it too needs
no structural diff. So: **content-only ⇒ logical node-diff; rebalanced ⇒ full
node.** No structural diff language, no base-delta. The cost is that a rebalance
forces writing its path (W3), but rebalances are rare (~1/BF) and this is exactly
baseline PSS's write behavior for those nodes — so structural correctness is
inherited from existing, tested code, and the durable structure never diverges
from the in-memory one.

### 6.3 Construction, application, recency

- **Construct** recursively on the mutation's return path: each node records its
  effect in the parent's slot; a branch child's slot is the child's own
  (nested) slots. No tree-diffing.
- **Apply** top-down on descent (§4.3): a node's slot is applied on top of the
  fully-materialized child, so a key removed-after-being-flushed-deeper resolves
  correctly (parent `Absent` over child's `Present`) — recency by depth, no MSN.
- **Aggregates** come from `ĝ` on materialization (no child summing).

### 6.4 Determinism note

Because rebalancing is materialized on write, a reconstructed tree is
**structurally identical** to the writer's, not merely content-equal — applying a
buffered node's logical diff only changes keys within an unchanged node shape. The
probe asserts content + count + measure; structure equality holds as a bonus.

---

## 7. Probe gate (soundness spec)

Reference model first (Clojure, à la `dev/op_buf_v4_reference.clj`), then the Java
port against the **same** gate. Assert, every cycle:
1. **Content exact** vs a reference `sorted-set` across
   `restore → mutate(adds+removes) → store → restore`, ≥ 8 cycles.
2. **Count + measure exact**, including for a **skipped, not-yet-loaded child**
   (read `ĝᵢ` from a parent without materializing the child). (AGG)
3. **Cold-retouch recency**: `add k → store/flush (k baked deep) → remove k
   (deposited shallow) → store → restore` ⇒ *k absent*. (Recency)
4. **Rebalance correctness**: a delete batch that triggers leaf/branch merges/
   borrows, then `store → restore`, reconstructs exact content **and structure**
   (merges were written, not re-derived); assert the reconstructed tree is a valid
   B-tree (min-fill respected) and equals `M` structurally.
5. **Writes-per-commit** behavioral: content-only commits ~1 PUT; a rebalancing
   commit writes only the affected path (~depth); assert no extra writes; assert
   reads during store are 0 (all written nodes are resident).
6. **CI structural**: after each store+restore, the reconstructed tree equals `M`
   in content, count, measure **and structure**.
7. **I0**: feature off ⇒ full existing PSS suite byte-identical to baseline.
8. **Generative**: random conj/disj/replace/store/restore/flush over a small key
   range (frequent collisions) vs a reference set; 0 failures.

Never flip a default before the gate plus audit (#40), cljs (#41), migration
(#42), and end-to-end (#43) are green.

---

## 8. Known risks / weak spots (explicit)

- **W0 — RESOLVED.** Earlier drafts let the in-memory and durable structures
  diverge (frozen durable + re-derive). The structural-vs-content store rule (§1,
  §4.2) materializes every rebalance on write, so structures **never diverge** —
  no frozen-slot reconciliation, no re-split/re-merge on read. The decision rides
  the existing return protocol, which the existing suite already exercises.
- **W1 — write-path bookkeeping cost.** Maintaining per-child diffs + `ĝ` on the
  mutation return path is the main throughput risk. In-place on transients;
  benchmark small-`B` overhead; `I0` must be speed-identical, not just byte-
  identical.
- **W2 — RESOLVED.** Merges are written (structural), so a cold read loads the
  already-merged durable structure and never loads a sibling to re-derive a merge.
  No read-path IO beyond the lazy node loads a baseline read would do.
- **W3 — rebalance write-amp.** A rebalancing commit writes the affected path
  (~depth PUTs) instead of buffering it. Amortized ≈ 1 + depth/BF ≈ negligible for
  steady small commits; bulk-then-store writes once. The trade for dropping W0/W2.
- **W4 — audit/merkle determinism.** A durable object embeds diffs whose
  distribution depends on flush history, so two representations of the same set
  hash differently (plain B-trees already vary by insertion order; diffs amplify
  it). Decision for #40: the merkle root attests to the **physical**
  representation (matches HHT), unless a canonicalization is required.
- **W5 — single-writer precondition.** "In-memory shape becomes durable on flush;
  readers reconstruct read-only" requires one writer producing durable objects.
  Consistent with the datahike single-writer + failover model; the index depends
  on it.
- **Eviction safety (resolved):** diffs are durable-after-commit (embedded in
  committed ancestor objects), so evicting a node after a commit loses nothing;
  between commits they hold the volatile open txn (expected).

---

## 9. Phasing

- **A** — core transport soundness on the reference model
  (`dev/op_buf_v5_reference.clj`): per-node nested diffs (frozen base + leaf ops),
  flush re-snapshot on budget, materialize-on-restore, `ĝ` snapshots. **PASSES**
  the probe gate it can cover: multi-cycle content+count exact (8 cycles),
  generative (6×1500, 0 fails), AGG cold-child-count, within-leaf recency, BI.
  *Deferred to the Java port* (need the real PSS split/merge): the
  structural-vs-content store decision via the return protocol, rebalance-writes-
  the-path, structurally-identical reconstruction, and the ~1-PUT/commit
  write-count. **(#47 done)**
- **B** — Java port against the same gate; throughput benchmark vs baseline; `B`
  sweep; `I0` byte+speed parity. **(#48, in progress — M1 done: Settings gate)**
- **C** — audit/merkle (diff in hashed node form). [#40]
- **D** — cljs (BTSet) parity. [#41]
- **E** — format flip + back-compat, no per-db flag. [#42]
- **F** — comprehensive end-to-end + migration tests. [#43]

---

## 10. Relationship to prior work

- **vs v4** (materialized-root overlay): v4 routed reads through a separately
  rebuilt overlay root (rebalanced, 1/ε-like cost). v5 keeps the normal
  materialized tree and confines diffs to store/restore. The v4 soundness model
  (canonical base + carried delta) carries over; the realization is simpler.
- **vs Bε / TokuDB / BetrFS**: same buffering idea, but (a) full fanout (no `B^ε`
  reduction → no taller tree / 1/ε read penalty; projection paid once on load),
  (b) commit == checkpoint with the root hash as the durability point (no separate
  WAL → avoids BetrFS's ≥2× write-amp and Toku's checkpoint-serialization stalls),
  (c) rebalancing is written (not re-derived), so structures never diverge and
  reads never restructure or load siblings, (d)
  **aggregate state in the diff** — exact subtree counts/rank under buffering,
  which is *unsolved* in that literature (they keep message counts, not order
  statistics); we get it free by materializing in memory and snapshotting `ĝ`.
  Grounding: TokuDB triggers flush on whole-node gorged-ness (total budget), and
  reconstructs structure from checkpoint+message-replay (splits unlogged). BetrFS
  "key lifting" (strip the separator-implied prefix from buffered keys) is a future
  slot-compaction optimization — biggest win near leaves, potentially large for
  datahike datoms with shared e/a prefixes.
