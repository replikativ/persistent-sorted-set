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
- The two **diverge between flushes**: the in-memory tree may have split/merged
  while the durable anchored structure has not. They are **reconciled at flush**,
  when the touched (resident) nodes are written and the durable structure catches
  up. Diffs bridge the gap; reads never see them.

A commit deposits its logical changes into diff slots along the path (constructed
recursively on the mutation's return path); `store` writes the resident touched
children and buffers the rest; `restore` projects diffs back on descent. Between
flushes a node accrues diff; when a node's total diff exceeds the budget `B`, a
flush peels one level down by writing the resident dirty children.

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

A node is **dirty** iff modified since its last write (`_address == null`, the
existing PSS mechanism), which implies it is **resident**.

```
store(node):
  if clean (has _address): return _address          # unchanged → reuse durable object
  while Σ|node.slots| > B:                           # BI overflow → peel one level
     for each dirty (resident) child cᵢ:             # the children this commit touched
        aᵢ := store(cᵢ)                              # write cᵢ (materialize its current structure),
                                                     # absorbing slotᵢ's diff into cᵢ's own object
        markFreed(old aᵢ); child_refᵢ := aᵢ; slotᵢ := (∅, ĝᵢ)
  for each remaining dirty child cⱼ (under budget):
     slotⱼ := (cⱼ's accumulated nested diff, refresh ĝⱼ)   # buffer; do NOT write cⱼ, do NOT markFreed
  write node (base + slots); return new _address
```

- The **root is always dirty** ⇒ always written ⇒ ≥1 PUT.
- **Flush victim = the resident dirty children** (the ones the commit just
  touched) — never a possibly-cold "heaviest" child, so **flush never does a
  read**. Writing a child *reconciles* its durable slot structure to the current
  in-memory structure (a buffered range that split in memory becomes several
  durable children; one that merged becomes fewer).
- **Structural changes do not force a write.** Splits/merges/borrows that happened
  in memory are absorbed into the logical diff and either re-derived on read (§4.3)
  or materialized when a flush writes that node. Even split-causing commits stay
  ~1 PUT.
- **`markFreed` only on flush** — a buffered child keeps its durable address as
  the anchor its diff is against; only a flush obsoletes the prior version. The
  diff is replaced each commit and reset on flush ⇒ apply-once, never a chain.

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

Application is **IO-free except merges**:
- **Splits are local** — an overflow divides the node's own keys; no sibling, no
  IO; the re-split re-derives exactly what the writer did. Splits never need
  structural encoding or a forced write, at any depth.
- **Merges/borrows may load a sibling.** An underflow calls `Leaf`/`Branch`
  merge/borrow, which needs the adjacent sibling (a child of the same parent). If
  it is not resident we **load it (1 GET) and merge properly**, faithfully
  re-deriving the writer's structure. This sibling-read is **bounded and counted**
  (merges are rare, esp. for datahike's insert-heavy workload); it is the
  deliberate price of faithful reconstruction (vs. leaving under-full nodes).
- **Aggregates never force IO** — a materialized branch takes its `(count,
  measure)` from `ĝ`; children are not summed.

### 4.4 Flush detail

Writing child *i* of `N` serializes the in-memory subtree for slot *i*'s range in
full (its current structure, incl. re-derived splits/merges), with the still-
buffered deeper changes pushed one level into *that* node's own slots; sets
`aᵢ := addr(written)`, `slotᵢ.diff := ∅`. If the written child itself exceeds
`B`, recurse (cascade one more level). One PUT per peeled level; resident, so no
reads. A flush reconciles the durable slot structure to the in-memory structure
(slots split or merge to match).

---

## 5. Cost

**Writes (PUT/commit).**
- Common (write-locality; datahike monotonic eids → hot EAVT segment): **~1**
  (root); occasional **+1..k** on a flush that peels touched children one level.
- Splits/merges do **not** add forced writes — re-derived on read or materialized
  on a normal flush. So even structural-change commits stay ~1 PUT.
- Flush writes only **resident** touched children ⇒ **zero read IO on flush**.
- Composes with datahike **root fusion**: index-root slots ride inside the fused
  db-record → a small commit can be ~1 PUT *total* across indexes.

**Reads.** Warm = normal materialized B-tree, `O(log_B N)`, no 1/ε penalty (full
fanout; projection paid once on load, then cached). Cold = load path + level-by-
level projection (bounded by `B`); **merges on the path may add bounded sibling
GETs** (rare). `ĝ` answers cold rank/count for a child without loading it.

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

A buffered subtree writes **no** node, so nothing in it gets a new durable
address — there is **nothing structural to record**. The split/merge/borrow that
happened in memory is **re-derived** on read by replaying the logical states
through the ordinary `add`/`remove`/`replace` (the same code that splits/merges
in normal operation — so structural correctness is inherited, not re-implemented).
When a node *is* written (flush), it is serialized in full and needs no diff. So:
**buffered ⇒ logical node-diff; written ⇒ full node.** No structural diff, no
base-delta, no cascade. (Writing structural changes would be worse: a written
split's new addresses can't be buffered by logical diffs, forcing the parent —
and so the whole path — to be written. Logical re-derivation avoids this.)

### 6.3 Construction, application, recency

- **Construct** recursively on the mutation's return path: each node records its
  effect in the parent's slot; a branch child's slot is the child's own
  (nested) slots. No tree-diffing.
- **Apply** top-down on descent (§4.3): a node's slot is applied on top of the
  fully-materialized child, so a key removed-after-being-flushed-deeper resolves
  correctly (parent `Absent` over child's `Present`) — recency by depth, no MSN.
- **Aggregates** come from `ĝ` on materialization (no child summing).

### 6.4 Determinism note

Reconstruction replays states through the real mutation code (incl. sibling loads
for merges), so it produces a valid B-tree with **exact content/count/measure**;
its *shape* may differ slightly from the writer's (B-trees are non-canonical;
batch-replay vs incremental). One writer produces durable objects (W5); its
in-memory shape is what a flush writes. The probe asserts content/count/measure
equality, never shape.

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
4. **Merge-on-load correctness**: a delete batch that triggers leaf/branch merges,
   then `store → restore`, reconstructs exact content (exercises the sibling-load
   path); assert the reconstructed tree is a **valid** B-tree (min-fill respected).
5. **Writes-per-commit** behavioral: ~1 PUT/commit between flushes; flushes write
   only resident children (assert **0 reads during flush**).
6. **CI structural**: after each store+restore, content/count/measure equal `M`;
   shape may differ.
7. **I0**: feature off ⇒ full existing PSS suite byte-identical to baseline.
8. **Generative**: random conj/disj/replace/store/restore/flush over a small key
   range (frequent collisions) vs a reference set; 0 failures.

Never flip a default before the gate plus audit (#40), cljs (#41), migration
(#42), and end-to-end (#43) are green.

---

## 8. Known risks / weak spots (explicit)

- **W0 — in-memory ↔ durable structure reconciliation (the central
  implementation challenge).** The in-memory tree (normal, possibly split/merged)
  and the durable anchored structure (last-written, keyed by its own separators)
  diverge between flushes. The implementation must: route each op's logical state
  into the correct durable slot(s) along the durable path; maintain per-node
  diff + `ĝ` incrementally on the return path; and on flush *reconcile* the durable
  slot structure to the in-memory structure (slots split/merge to match). This is
  where the reference model must be precise; it is the thing most likely to harbor
  a subtle bug, so it gets the most probe attention (cases 3, 4, 6).
- **W1 — write-path bookkeeping cost.** Maintaining per-child diffs + `ĝ` on every
  mutation's return path is the main throughput risk. In-place on transients;
  benchmark small-`B` overhead; `I0` must be speed-identical, not just byte-
  identical.
- **W2 — bounded merge-read on cold reconstruction.** A merge on the read path may
  load a sibling (1 GET). Bounded and counted; rare for insert-heavy datahike.
  Accepted (the alternative — leaving under-full nodes — was rejected in favor of
  faithful structure).
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
  *Deferred to the Java port* (need the real PSS split/merge): cross-level
  recency, peel-one-level write-count (~1 PUT/commit), and merge-sibling-load
  mechanics. **(#47 done)**
- **B** — Java port against the same gate; throughput benchmark vs baseline; `B`
  sweep; `I0` byte+speed parity.
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
  (c) flush only resident touched children (zero read-amp on flush), (d)
  **aggregate state in the diff** — exact subtree counts/rank under buffering,
  which is *unsolved* in that literature (they keep message counts, not order
  statistics); we get it free by materializing in memory and snapshotting `ĝ`.
  Grounding: TokuDB triggers flush on whole-node gorged-ness (total budget), and
  reconstructs structure from checkpoint+message-replay (splits unlogged). BetrFS
  "key lifting" (strip the separator-implied prefix from buffered keys) is a future
  slot-compaction optimization — biggest win near leaves, potentially large for
  datahike datoms with shared e/a prefixes.
