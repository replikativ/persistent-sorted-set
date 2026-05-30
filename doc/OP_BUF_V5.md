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

### 4.3 Restore — push the diff down, one level per (lazy) deserialization

Projection is a **deserialization** concern, confined to node materialization
(`Branch.child(node, i)` / `restore`). **Reads stay baseline**:
`lookup`/`contains`/`slice`/`seek` binary-search already-materialized nodes and
never see a diff. Materialization is lazy and per-node, so a read merely
*triggers* it; the projection lives at the restore boundary, runs **once** per
node, then is cached (warm reads = plain `O(log_B N)`, no re-projection).

```
materialize(node, i):                        # runs once, when child i is first descended into
  if cached: return cached
  base   := load(child-ref aᵢ)               # 1 GET; aᵢ is a DURABLE address (the anchor)
  (d, ĝ) := node._slots[i]                    # parent's slot for child i; null ⇒ load base verbatim
  if base is a Leaf:                          # bottom: apply the leaf-diff in ONE pass
     mat := rebuild base.keys with d          #   net Present(upsert) / Absent(delete); no split/merge
  else:                                       # branch: push ONE level down, stay lazy
     mat := base ; mat._slots := d ; mat.(count,measure) := ĝ   # grandchildren project on their OWN descent
  cache and return mat
```

- **One level per deserialization.** Materializing child *i* consumes exactly
  `node._slots[i]`: at a branch it *installs* the nested remainder as the child's
  own `_slots`; grandchildren are untouched until separately descended. (Eager
  full push-down would materialize the whole changed subtree and defeat laziness.)
- **Leaf application is batch, never op-by-op.** A buffered child changed content
  only ⇒ its *net* diff keeps every leaf within `[min, BF]` (else the writer would
  have rebalanced and *written* it). So rebuilding `durable-keys ⊕ net-diff` in one
  pass yields that in-bounds leaf with **no split/merge and no sibling load**.
  Replaying op-by-op could transiently overflow → forbidden.
- **Aggregates from `ĝ`, never summed.** A materialized branch takes
  `(count, measure)` from the slot's `ĝ`; children are not loaded to re-sum.
  Optional `ĝ`-without-load: a `count`/`rank`/`measure` query may read
  `node._slots[i].ĝ` without materializing child *i* — the lone read that consults
  a slot; opt-in, and omitting it (materialize, then baseline) is still correct.
- **OPEN — composition (W6/§8).** `mat._slots := d` *installs* the parent's diff
  as the child's slots. That is correct only if `base` is itself **slot-free**. If
  a *written* node is allowed to buffer its own descendants (slots in non-root
  objects), `base` can carry stored slots `S` and materialization must **merge**
  `S` with `d` (d newer — recency by depth) rather than install — a bounded,
  IO-free merge of two nested diff maps. The alternative (a written node fully
  materializes its dirty subtree ⇒ every anchor is slot-free ⇒ plain install, no
  merge) trades a few extra PUTs on flush/rebalance for a composition-free restore.
  Decided at M4/M5.

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

### 6.5 Two ops; replace folds into Present

The language has exactly **two** ops, not three. `Present(element)` is "this
cmp-key should be present holding *this* element" — applied to a leaf it *upserts*
(insert if absent, overwrite if a `cmp`-equal element is present), so **add =
Present(key absent), replace = Present(key present)**. `Absent(key)` is delete.
Replace is not a third op and is never delete-then-add (which could transiently
restructure); it rides inside the leaf rebuild.

### 6.6 Worked example (depth 3) and serialization shape

Tree (BF 4; pivots = each child's max key):

```
R(level 2) keys[6,16,31] → B0,B1,B2
B0 keys[2,6]  → L0[1,2]  L1[5,6]
B1 keys[11,16]→ L2[10,11] L3[15,16]
B2 keys[22,31]→ L4[20,21,22] L5[30,31]
```

One content-only commit — `add 3`(→L1), `add 12`(→L3), `delete 20`(→L4),
`replace 30→30*`(→L5). No node crosses a fill bound ⇒ B0,B1,B2 all buffered; only
R is written (1 PUT). R's stored object:

```clojure
{:level 2 :keys [6 16 31]
 :addresses [aB0 aB1 aB2]              ; all three = children's UNCHANGED durable anchors
 :slots {0 {:count 5 :measure mB0       ; ĝ_B0
            :diff {1 {:count 3 :measure mL1 :diff {3 3}}}}        ; Present(3)
         1 {:count 5 :measure mB1
            :diff {1 {:count 3 :measure mL3 :diff {12 12}}}}      ; Present(12)
         2 {:count 4 :measure mB2
            :diff {0 {:count 2 :measure mL4 :diff {20 :ABSENT}}   ; Absent(20)
                   1 {:count 2 :measure mL5 :diff {30 30*}}}}}}   ; Present(30*) = replace
```

A slot is `{:count :measure :diff}` (ĝ + the child's node-diff). `:diff` is keyed
by **child-index** at a branch level and by **cmp-key** at a leaf level; which one
is known from the child's level (tracked on descent). **Sparse**: unchanged
children (L0, L2, …) have no entry — their durable objects and ĝ stand. `:slots`
absent/empty ⇒ exactly the baseline format (I0, back-compat). A branch's `:diff`
*is* that branch's `_slots`; the whole nested thing lives only in the topmost
written object (here R), so no duplication.

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
- **W6 — RESOLVED: install, single source, no merge.** Restore materializes a node
  by applying **exactly one** diff: the parent's slot for it if present, else the
  node's own durable slots (used only when restoring at the very commit the node
  was written). They never need composing, because the diff that reaches the
  nearest written ancestor is the **complete, accumulated** delta of the subtree
  vs its children's current anchors — it *supersedes* (is a superset of) any older
  diff baked in the node's durable object, rather than stacking on it. This holds
  because (1) leaf-diffs accumulate in their leaf-parent until the leaf is actually
  rewritten, and (2) the `_childWritten` cascade guarantees a *buffered* node has no
  rewritten descendant, so its durable child-pointers stay valid and only the
  complete diff floats up. So a *written* node **may** still buffer its descendants
  (model A — "per-node slots everywhere") with no merge cost: structurally the diff
  nests down a single object (diffs-of-diffs); temporally it stays one flat complete
  diff per buffered subtree.
- **W7 — REVISED (M4b): anchor marker at every level + leaf-diff at leaf-parents.**
  *Earlier "leaf-parents only" was incomplete:* to hit ~1 PUT/commit we must buffer
  the **branch** nodes on a content-only path, and buffering branch child C at store
  needs C's durable **anchor** — which the mutation nulls and (under leaf-parents-only)
  was never captured. Fix: on the content-only return path, every level deposits a
  Slot — a **leaf-parent** slot carries the leaf-diff `{cmp-key→op}` + anchor + ĝ; a
  **branch** slot is an *anchor marker* (`diff=null`) holding the child's durable
  address + ĝ. The anchor is captured at the top of `add`/`remove`/`replace`
  (`anchor0 = _addresses[i]`, before the mutation nulls it). The nested diff for a
  buffered branch child is then **derived at store** by recursing the live subtree
  (its leaf-parents' leaf-diffs) / **passed through** from `_slots` for a
  not-yet-materialized restored child. This also keeps in-memory ≈ stored (slot at
  every buffered level), the stated preference. Structural (rebalanced) nodes don't
  deposit (written; they re-store dirty children). `Slot.diff` is `Object`
  (PersistentTreeMap leaf-diff | null marker | nested map post-restore).
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

---

## 11. Java build sequence (#48)

Each step is a self-contained change with a **mandatory gate**: recompile
(`clojure -T:build java`) + full suite (`clojure -M:test`) green, with the suite
running at `opBufSize=0` so it asserts **I0** (baseline-identical) at every step.
A separate opBufSize>0 probe (the reference's gate, ported to drive the real PSS)
validates the new behavior. All new behavior forks on `_settings.opBufSize() > 0`.

- **M1 — Settings gate. DONE** (commit a745d8c). `_opBufSize` + `opBufSize()` +
  `pss.opBufSize` sysprop + Clojure `:op-buf-size`; no behavior forks ⇒ I0.
- **M2 — data model.** `Branch._slots` (nullable `Object[]`; a slot is a per-child
  diff: a `PersistentTreeMap<Key, Op>` under `_cmp` where `Op ∈ {ADD, REMOVE}`,
  plus a cached `(count, measure)` snapshot) + accessors + the two dirty bits
  (`_rebalanced`, `_childWritten`) used only by store. Null/unset at opBufSize=0.
  Gate: inert ⇒ suite green.
- **M3 — deposit at leaf-parents. DONE.** In `cons`/`disjoin`/`replace`, when
  `opBufSize>0` and `_level == 1` (the changed child is a leaf), on the
  content-only return (`EARLY_EXIT` / single-`{node}` with no rebalance) record the
  leaf-op into the leaf-parent's `_slots[i]` (`{cmp-key → element|ABSENT}`, net
  latest-wins) + refresh its `ĝ`. Higher branches (`_level > 1`) do **not** deposit
  — their nesting + ĝ are derivable from the live tree and are assembled at store
  (W7). On any structural return set `_rebalanced` (all levels — store needs it).
  Persistent path copies the leaf-parent's slot array forward then deposits;
  transient mutates in place. *No store/restore change yet* — slots accumulate and
  are ignored; suite green at 0 (I0), and opBufSize>0 stays correct (slots unused).
  Gate (met): compile + suite green; `dev/op_buf_v5_m3_probe.clj` — content exact,
  deposit fires at level 1 with exact ĝ, latest-wins, Absent recorded, **and slots
  appear only at level-1 leaf-parents (0 higher-level slots)**.
- **M4 — store (the write decision).** Sub-stepped:
  - **M4a — anchor capture. DONE.** `Slot` gains `anchor` (the buffered child's
    durable address). Captured at deposit from `_addresses[i]` *before* the mutation
    nulls it (`anchor0` read at the top of `add`/`remove`/`replace`, `_level==1`);
    a slot keeps its first-captured anchor as ops accumulate. Purely additive
    (anchor unused until M4b) ⇒ I0. Probe: the slot's anchor restores to the
    pre-mutation durable leaf.
  - **M4b — write decision + serialization.** `Branch.store`/`PSS.store`: a dirty
    child is **buffered** (no write; `addresses[i] := slot.anchor`; nested diff
    assembled from the live subtree + ĝ; serialized into this node's object) iff
    content-only (`!_rebalanced && !_childWritten`, leaf child has a slot) **and**
    under budget (`Σ embedded entries ≤ B`); else **written** — fall back to the
    baseline recursive store of the `address==null` children (set `_childWritten`,
    `markFreed` the anchor). `markFreed` is deferred from mutation to store (a
    buffered child's anchor must survive). Serialize `:slots` **only when present**
    (opBufSize=0 = old format byte-for-byte). Slot-aware test storage.
  - Gate: suite green at 0 (I0); opBufSize>0 store→restore (with M5) content/count
    exact.
  - **Store decision (worked out).** `Branch.store`, per child `i`:
    - `_addresses[i] != null && slot == null` → **clean**, reuse.
    - `_addresses[i] != null && slot != null` → **passthrough** (buffered in a prior
      commit / restored, untouched this commit): keep address + slot, add its diff to
      `embedded`. *Does not touch the child* (it may be evicted) — works because the
      slot's diff was **written back** in full when it was first buffered (below).
    - `_addresses[i] == null` (dirty, child resident) → **buffer** iff it has an
      anchor and its whole dirty subtree is content-only (no `_rebalanced` anywhere —
      a recursive check, since a deep rebalance leaves intermediate nodes
      content-only) **and** `embedded + size ≤ B`; then set `_addresses[i] := anchor`,
      **write the assembled nested diff back into the slot** (`assembleNested`
      recurses the live subtree once, turning branch markers into the full nested
      map so passthrough/eviction works), `embedded += size`. Else **write**: recurse
      `child.store` (which itself buffers its content-only children — W6), `markFreed`
      the anchor, clear the slot, set `_childWritten`.
    - Root always reaches `storage.store(this)` ⇒ ≥1 PUT. A rebalance makes the whole
      affected path non-bufferable (recursive check) ⇒ that path is written (W3).
  - **Serialization.** `slotsForStorage` wraps `_slots` into
    `{idx → {:count :measure :diff}}` (diffs pre-assembled; sparse). `Slot.ABSENT`
    is a namespaced **keyword** so leaf-diffs are edn-serializable directly. Storage
    emits `:slots` only when non-null (opBufSize=0 ⇒ absent ⇒ old format).
- **M5 — restore (projection). DONE (core).** `Branch.child` projects at the lazy
  materialization boundary: a **leaf** child is rebuilt in one pass from durable keys
  ⊕ leaf-diff (`projectLeaf`, no restructure); a **branch** child has the nested diff
  installed as its own `_slots` + aggregates set from `ĝ` (`projectBranch`), pushing
  one level down. The set's comparator is threaded via `Settings._comparator` (set in
  `map->settings`, preserved by `editable()`). `depositInto` accumulates onto a
  restored (plain-edn) leaf-diff by rebuilding it under `cmp`. Test storage `restore`
  reconstructs `_slots`. Gate green: I0 (142 tests); probe `store→FRESH restore content
  exact` (P-roundtrip) and **8-cycle fresh-restore→mutate→store** (P-multicycle).
  - **Slot-carry through rebuild (add DONE; remove TODO).** When a node rebalances it is
    written but still buffers surviving siblings, so a structural `Stitch` rebuild must
    carry `_slots` (a child's slot travels with its address; new split/merge nodes get
    none). `add` absorb+split do this (`stitchSlots`); **`remove` join/borrow/in-place
    do NOT yet → committed-buffered siblings lose their diff across a later merge.**
    Confirmed repro: store(buffer evens) → restore → remove(odds, merges) → store →
    restore ⇒ `[4 9]` reverts to `[4 0]`. **Fix (next): mirror each remove address-Stitch
    with a parallel `_slots` Stitch** (`_addresses`/`left._addresses`/`right._addresses`
    → `_slots`/`left._slots`/`right._slots`; nulls → null slots).
- **M6 — benchmark + B sweep.** Throughput vs baseline (W1); PUT-count probe via
  the konserve-s3 instrumentation; `B` sweep; confirm I0 speed-parity.

Then remove-path slot-carry, audit (#40), cljs (#41), format-flip (#42), e2e (#43).
