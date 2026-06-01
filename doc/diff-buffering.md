# Diff buffering — write-amplification reduction at the serialization boundary

Diff buffering cuts the number of object writes per commit when a PersistentSortedSet
is persisted to immutable, content-addressed storage (e.g. konserve on S3/GCS), where
each `PUT` is the dominant cost and latency. A normal commit rewrites the whole
root→leaf path it touched (`≈ depth+1` new objects); diff buffering brings a
content-only commit down to **~1 object per commit**, with no change to query
semantics and no read penalty.

It is **off by default** and gated by a per-node budget `diffBufSize` (`Settings`,
clojure `:diff-buf-size`, sysprop `pss.diffBufSize`). `diffBufSize = 0` is *byte-identical*
to baseline PSS. See **Configuration** below — enabling it places a requirement on the
`IStorage` implementation.

The in-memory tree stays an ordinary B-tree: all `lookup`/`slice`/`seek`/`add`/`remove`/
`replace` logic is unchanged. Diffs exist only at the `store`/`restore` boundary.

> **Background.** Diff buffering rides two existing mechanisms documented separately:
> the mutation **return-value protocol** that distinguishes content-only from structural
> changes (see [`btree-operations.md`](btree-operations.md)), and the per-node **aggregate
> statistics** `(count, measure)` that `ĝ` snapshots (see
> [`statistical-queries.md`](statistical-queries.md)). This document assumes both.

## Motivation and the hard constraint

On immutable content-addressed storage, persisting any one node changes its hash → changes
its parent's pointer → cascades to the root. So a naïve "buffer pending operations in
interior nodes" port of a Bε-tree still rewrites the whole spine and defeats buffering.

The escape: **record a child's change as a diff inside the node we have to rewrite anyway,
with the diff referencing the child's *unchanged* durable address.** Only the rewritten
spine nodes become new objects; everything they point at keeps its hash. The commit is
still a single checkpoint whose root hash is the durability point — no separate write-ahead
log.

## The model

- **In-memory tree** — an ordinary, balanced, lazily-loaded B-tree. It is the authoritative
  logical content; splits/merges/borrows happen normally.
- **Durable representation** — immutable content-addressed objects forming the
  last-written ("anchored") structure, where each *branch* object additionally carries, per
  child, a **diff** (the child's logical changes since it was last written) and an
  **aggregate snapshot** `ĝ = (count, measure)` of the child's *current* subtree.
- **The buffer-vs-write decision rides the existing return protocol.** A mutation's return
  value already distinguishes a *content-only* change (a single returned node, incl. a
  max-key bump) from a *structural* one (a split/merge/borrow). On the return path each node
  accumulates the op's logical diff (`key → Present | Absent`) and refreshes `ĝ`; at `store`,
  a node that only changed content (no rebalance in its subtree, no child written under it)
  is **buffered** as a diff in its parent, while a node that **rebalanced** (or under which
  any child was written) is **written** in full, cascading to the root.
- **A buffered node has the same *structure* as its durable anchor** — only its content
  differs. So the in-memory and durable structures never diverge: a rebalance is
  materialized on write the instant it happens. This is baseline PSS's `store` with exactly
  one change — content-only dirty nodes buffer instead of being written.

## Objects

- **Durable branch object**: `⟨ pivots pᵢ, child-addresses aᵢ, slotsᵢ ⟩`, where `pᵢ` is
  child *i*'s max key (PSS high-key convention), `aᵢ` is child *i*'s durable
  content-address, and `slotᵢ = (diffᵢ, ĝᵢ, maxKeyᵢ)`:
  - `diffᵢ` — child *i*'s logical changes since `aᵢ` was written (grammar below);
  - `ĝᵢ = (count, measure)` — absolute snapshot of child *i*'s *current* subtree;
  - `maxKeyᵢ` — child *i*'s *current* (post-diff) max key = its parent separator.
- **Durable leaf object**: just its keys (leaves carry no slots).
- **`materialize` / reconstruct**: load object `a`; a leaf yields its keys; a branch
  materializes each child *i* lazily as `apply(diffᵢ, reconstruct(aᵢ))`, sets the branch's
  per-child `(count, measure)` from `ĝᵢ` (no re-summing), and its separator `pᵢ` from
  `maxKeyᵢ`.

### Durable layout

Each box below is one content-addressed object — one `PUT`, one `GET`. A branch object lays
out three parallel arrays plus a *sparse* slot map keyed by child index:

```
  ┌ branch object  @aR  ─────────────────────────────────────────────┐
  │ level    2                                                       │
  │ pivots   [  p0     p1     p2  ]   child max keys (high-key sep.) │
  │ addrs    [  a0     a1     a2  ]   children's DURABLE addresses   │
  │ slots    {  0:S0          2:S2 }  per-child diff — SPARSE        │
  └────────┬────────────────────┬────────────────────┬───────────────┘
       a0  │                a1  │                a2  │    (child 1 has no slot →
           ▼                    ▼                    ▼     load @a1 verbatim)
   ┌ @a0 ──────────┐    ┌ @a1 ──────────┐    ┌ @a2 ──────────┐
   │ child 0's     │    │ child 1's     │    │ child 2's     │  each aᵢ points at the child's
   │ last-WRITTEN  │    │ last-WRITTEN  │    │ last-WRITTEN  │  last-written object (its *anchor*),
   │ object        │    │ object        │    │ object        │  unchanged even when Sᵢ holds a diff
   └───────────────┘    └───────────────┘    └───────────────┘
```

- `slotᵢ = Sᵢ = (diffᵢ, ĝᵢ = (count, measure), maxKeyᵢ)`. A child with no pending change has
  **no entry** (child 1 above) → it is loaded verbatim from `aᵢ`.
- `aᵢ` is the child's *anchor* — the address of its last fully-written object. Buffering a
  change to child *i* leaves `aᵢ` untouched and records the change in `Sᵢ`; the anchor is only
  replaced (and the old one `markFreed`) when the child is itself written.
- The arrays/slots are stored verbatim only in *written* nodes; a *buffered* branch has no
  object of its own this commit — it lives entirely inside its parent's slot (see the
  lifecycle below).

### Separators must travel in the diff (a correctness subtlety)

A child's max key is stored *twice*: inside the child (its last element) and one level up
as the parent's separator `pᵢ`. A buffered `Present`/`Absent` can change that max. A
*written* node serializes its live (correct) separators, but a *buffered* branch is
reconstructed from its stale anchor `aᵢ` (pre-diff separators) and is never eagerly
re-summed — so each modified child's current max key is carried in its slot (`maxKeyᵢ`,
serialized `:max-key`, read from the live `_keys[i]` at store) and reinstalled on restore
(projection sets `pᵢ := maxKeyᵢ`). Without this, a deleted max leaves a phantom separator
and `contains`/routing break.

## Invariants — why it is correct

- **Content.** `materialize(D) == M` after every commit: the diffs reconstruct exactly the
  in-memory logical content.
- **Aggregate exactness.** `ĝᵢ == (count, measure)` of child *i*'s current subtree. A reader
  can answer `rank`/`countSlice`/`measure` for a child *without loading it*, and
  materialization sets a branch's aggregates without summing (loading) children.
- **Recency by depth, no timestamps.** A node's `diffᵢ` is the delta to apply *on top of the
  fully-materialized child* (which already has its own deeper diffs applied). Reconstruction
  is strict top-down projection — depth encodes order. So a key removed-after-being-flushed-
  deeper resolves correctly (a parent `Absent` applied over a child's `Present`).
- **Structure never diverges.** Content-only ⇒ logical diff; a rebalance ⇒ the node (and its
  path) is *written* in full, materializing the new structure. There is therefore **no
  structural diff language** and no base-delta to encode; structural correctness is inherited
  unchanged from baseline PSS's tested split/merge/borrow code, and a reconstructed tree is
  structurally identical to the writer's, not merely content-equal.
- **One diff per materialization (no merge).** Restoring a node applies *exactly one* diff:
  the parent's slot for it if present, else the node's own durable slots (only at the very
  commit the node was written). They never need composing: a node is buffered only when its
  whole dirty subtree is content-only, and its in-memory slots already composed any older
  diff with the new ops before the diff was assembled — so the diff that reaches the nearest
  written ancestor is the **complete, accumulated** delta of the subtree and *supersedes*
  (is a superset of) any diff baked in the node's durable object. Projection therefore
  *installs* the parent's diff, overwriting the restored base's stored slots.
- **Anchor safety.** A buffered child keeps its durable address as the anchor its diff is
  against; its diff is replaced wholesale each commit and reset when the child is finally
  written ⇒ apply-once, never a chain. `markFreed` is issued **only when a node is written**,
  so a buffered child's anchor is never freed while still referenced.
- **Disabled is identical (I0).** `diffBufSize = 0` ⇒ every path is exactly baseline PSS,
  byte-identical on disk. All behavior forks on the gate.
- **Forward-compatible format.** A node with no slots deserializes as empty diffs; the new
  on-disk format is a strict superset (the `:slots` field is simply absent on baseline
  nodes). Old data reads unchanged; rewritten nodes get the new format.

## Operations

### Commit (deposit on the return path)

`apply(batch, M)` runs the normal B-tree mutation. As each op returns up the path it records
its logical effect into the parent's slot for the child it descended into and refreshes that
slot's `ĝ` from `M`: `insert k → Present(k)`, `remove k → Absent(k)`,
`replace(old → new) → Absent(old) + Present(new)` (collapses to just `Present(new)` when `old`
and `new` are equal under the set comparator). Within one slot a later op on the same key (under
the set comparator) overwrites (net latest-wins). No tree-diffing — the op states its own effect.

### Store (write the touched nodes, buffer the rest)

```
store(node):
  if clean (has _address): return _address          # unchanged → reuse durable object
  pass := Σ diffSize(slotⱼ) over CLEAN children j    # buffered-passthrough diffs (kept as-is)
  cand := []                                         # dirty, content-only children (bufferable)
  for each dirty child cᵢ:
     if cᵢ rebalanced (structure ≠ anchor) or has no anchor:
        store(cᵢ); markFreed(old anchor); slotᵢ := ∅  # must write
     else: cand += (i, szᵢ := diffSize of cᵢ's subtree diff, assembled nested diff)
  embedded := pass                                   # BIGGEST-FIRST: buffer small, flush large
  for (i, szᵢ, diffᵢ) in cand sorted by szᵢ ASCENDING:
     if embedded + szᵢ ≤ B:
        child_addrᵢ := cᵢ.anchor; slotᵢ := (diffᵢ, ĝᵢ, maxKeyᵢ); embedded += szᵢ   # BUFFER
     else: store(cᵢ); markFreed(old anchor); slotᵢ := ∅                            # FLUSH
  write node (base + slots); return new _address     # written because rebalanced / child-written / root
```

- The **root is always written** ⇒ ≥ 1 PUT.
- **Structural changes are written**, so the durable structure always matches in-memory —
  no divergence, no re-derive, no sibling load on read. A rebalancing commit writes the
  affected path (~depth PUTs); rebalances are ~1/BF of ops, so amortized ≈ `1 + depth/BF`.
- **Budget `B`** caps the buffered diff of one written object, measured in *entries* (number
  of buffered element-changes, summed over the node's slots — not bytes). It is enforced
  strictly: a child is buffered only if it keeps the running total `≤ B`, so every stored
  object satisfies `Σ embedded diff ≤ B`.
- **Eviction is biggest-first.** When the bufferable children don't all fit, the ones with
  the *largest* diffs are flushed (written) and the small ones kept buffered. A slot that
  regularly consumes a big share of the budget is thus written proportionally often (it can't
  jam the buffer), and flushing the largest reclaims the most budget per PUT. Clean
  *passthrough* children (buffered in a prior commit, untouched now) consume budget but are
  never flushed here — flushing them would require loading their anchor.
- All flushed/written children are **dirty this commit ⇒ resident**, so a flush never reads.

### Restore (project the diff down, lazily, one level per deserialization)

Projection is confined to node materialization (`Branch.child` / `restore`); reads stay
baseline (`lookup`/`contains`/`slice`/`seek` binary-search already-materialized nodes and
never see a diff). It runs **once** per node, then is cached.

```
materialize(node, i):                       # when child i is first descended into
  base := load(child_addrᵢ)                 # 1 GET; child_addrᵢ is a DURABLE anchor
  (d, ĝ, mk) := node.slots[i]               # null ⇒ load base verbatim
  if base is a Leaf:
     rebuild base.keys ⊕ d in ONE pass      # Present upserts, Absent deletes; no split/merge
  else:                                      # branch: push ONE level down, stay lazy
     base.(count, measure) := ĝ
     for (j → (dⱼ, ĝⱼ, mkⱼ)) in d:           # install each grandchild's slot AND
        base.slots[j] := (dⱼ, ĝⱼ); base.pⱼ := mkⱼ   #   repair its separator from the stored max-key
```

- **Leaf application is batch, never op-by-op.** A buffered child changed content only ⇒ its
  *net* diff keeps the leaf within `[min, BF]` (else the writer would have rebalanced and
  written it), so `durable-keys ⊕ net-diff` in one pass yields the in-bounds leaf with no
  split/merge and no sibling load. (Op-by-op replay could transiently overflow — forbidden.)
- **One level per deserialization.** Materializing a branch child installs the nested
  remainder as that child's own slots and repairs each grandchild's separator; grandchildren
  stay untouched until separately descended (eager full push-down would defeat laziness).
- **Aggregates from `ĝ`, never summed.** Optionally a `count`/`rank`/`measure` query may read
  `node.slots[i].ĝ` without materializing child *i* at all; omitting that optimization
  (materialize, then read baseline) is still correct.

## The diff language

```
node-diff   ::= leaf-diff | branch-diff
leaf-diff   ::= Absent(element)* + Present(element)*          # net latest-wins per element
branch-diff ::= { child-index → (node-diff, ĝ, max-key) }    # nested, per child
ĝ           ::= (count, measure)                             # absolute snapshot
```

One recursive form, exactly **two** ops. `Present(element)` carries the stored element and
expresses both **add** (element absent ⇒ insert) and the value side of a **replace** — it is an
upsert applied during the leaf rebuild, never a delete-then-add (which could transiently
restructure). `Absent(element)` expresses **remove**. The aggregate is *absolute state*, not a
delta, so non-invertible measures (min/max) are fine and it is free to collect (the in-memory
node already maintains `count`/`measure`).

**A `replace(old → new)` records `Absent(old) + Present(new)`.** When `old` and `new` are equal
under the set's comparator (the common in-place edit — only a non-key field changed) the
`Present` upsert overwrites at the same key, so this collapses to a plain `Present(new)`. When
they *differ* under the set's comparator — a replace driven by a **coarser** operation comparator
(e.g. datahike's upsert locates the datom by `(e,a)` while the index orders by `(e,a,v,tx)`) —
both entries survive, so projection removes `old` and inserts `new` with no comparator logic.
Recording the concrete `Absent`/`Present` elements (not "the op the comparator implied") is what
lets projection replay the diff later under one fixed comparator. See *Two representations* below.

### Two representations: in-memory vs serialized

The leaf-diff has **two forms**, and the distinction is load-bearing:

- **In memory** it is a `PersistentTreeMap` keyed by the **set's comparator** (`Branch._projCmp`)
  with values `element | Absent`. Keying by the set comparator gives O(log n) net-latest-wins
  accumulation and is what keeps two elements the order distinguishes (e.g. two datoms differing
  only in `tx`) as *separate* entries.

- **On the wire** it is the **comparator-agnostic** form `{:absent [element…] :present [element…]}`
  — two vectors. Storage carries no comparator (the projection comparator lives on the tree, not
  on `IStorage` — see *Projection comparator* below), so the serialized diff must not be a map
  keyed by the element: an element's own `.equals`/`.hashCode` can be **coarser** than the set
  comparator (datahike `Datom` equality is `(e,a,v)`; the index order is `(e,a,v,tx)`). A map
  round-tripped through that coarser equality would **collapse** two order-distinct entries — a
  tx-only replace's `Absent(old)+Present(new)` would merge into one and lose the removal, leaving
  a stale element after restore. Two vectors round-trip losslessly regardless of element equality.

`Branch.slotsForStorage`/`assembleNested` convert leaf-diffs to the wire form on store
(`leafDiffForStorage`); `projectLeaf`/`depositKV`/`diffSize` accept both. Branch-diffs are keyed
by integer child-index, never by an element, so they have no such hazard.

### Projection comparator

Projecting a buffered leaf rebuilds it in **stored order**, which requires the **set's** stable
comparator — never the per-operation/navigation comparator that drove a given descent (datahike
slices and upserts with *partial* `(e,a,v)`/`(e,a)` comparators). The set's comparator therefore
lives **on the tree** as `Branch._projCmp`, propagated from the root: `PersistentSortedSet.root()`
stamps the root; `Branch.child()` stamps each branch restored from storage (storage has no
comparator); and internally-created branches (split/merge/rebuild) inherit it through the
constructor. There is no comparator on `IStorage` and none threaded through node operations.

## Worked lifecycle (BF 4, depth 3)

One tree carried through the three write regimes — **content-only** (buffer), **rebalance**
(write the path), **budget overflow** (flush a child) — and one **read** in between. `*`
marks a changed value; addresses written as `@x`.

**Stage 0 — initial durable tree** (after a full write; no slots yet):

```
R  level 2  keys[6,16,31]   → B0,B1,B2            @R
B0 level 1  keys[2,6]       → L0[1,2]   L1[5,6]   @B0
B1 level 1  keys[11,16]     → L2[10,11] L3[15,16] @B1
B2 level 1  keys[22,31]     → L4[20,21,22] L5[30,31] @B2
```

Every node clean (each has its own address); `lookup k` descends `R → Bᵢ → Lⱼ` as in
baseline. 10 objects on disk (1 root + 3 branches + 6 leaves).

### Stage 1 — content-only commit (buffer up → 1 PUT)

Batch: `add 3`(→L1), `add 12`(→L3), `delete 20`(→L4), `replace 30→30*`(→L5). No leaf crosses
a fill bound, so nothing rebalances. On the return path each op deposits into its parent's
slot (`L1: Present(3)`, `L3: Present(12)`, `L4: Absent(20)`, `L5: Present(30*)`) and refreshes
`ĝ`. At `store`, walking the dirty children of each node:

```
store(R):
  B0 dirty, subtree content-only, under budget  → BUFFER: addr[0]:=@B0, slot[0]:=nested(B0)
  B1 dirty, subtree content-only, under budget  → BUFFER: addr[1]:=@B1, slot[1]:=nested(B1)
  B2 dirty, subtree content-only, under budget  → BUFFER: addr[2]:=@B2, slot[2]:=nested(B2)
  R is the root                                 → WRITE  (base + slots) ⇒ @R'   ← the only PUT
```

`B0,B1,B2,L1,L3,L4,L5` are **not written**; their addresses still point at the Stage-0
anchors. No `markFreed` (no anchor was superseded). **1 PUT.** `@R'`'s stored object:

```clojure
{:level 2 :keys [6 16 31]
 :addresses [@B0 @B1 @B2]                  ; all three = children's UNCHANGED Stage-0 anchors
 :slots {0 {:count 5 :measure mB0 :max-key 6
            :diff {1 {:count 3 :measure mL1 :max-key 6  :diff {:present [3]}}}}        ; Present(3)
         1 {:count 5 :measure mB1 :max-key 16
            :diff {1 {:count 3 :measure mL3 :max-key 16 :diff {:present [12]}}}}       ; Present(12)
         2 {:count 4 :measure mB2 :max-key 31
            :diff {0 {:count 2 :measure mL4 :max-key 22 :diff {:absent [20]}}          ; Absent(20)
                   1 {:count 2 :measure mL5 :max-key 30* :diff {:present [30*]}}}}}}   ; replace ⇒ Present(new)
```

A **branch**-level `:diff` is a map keyed by **child-index**; a **leaf**-level `:diff` is the
comparator-agnostic `{:absent […] :present […]}` form (known from the child's level — see
*The diff language → Two representations*). The whole nested structure lives only in the topmost
written object (`@R'`); unchanged children (L0, L2) have **no entry** — their objects and `ĝ` stand.

### Reading Stage 1 back — project down (lazy, one level per load)

Fresh process, `lookup 30*`. Diffs flow back **down** the path actually traversed:

```
restore @R'              1 GET → a normal in-memory branch that happens to carry _slots
descend R→child 2:       materialize(R,2):
   load @B2              1 GET → STALE anchor  keys[22,31] → L4[20,21,22], L5[30,31]
   install R.slots[2]    onto B2:  B2.slots[0]=(Absent 20, ĝ), B2.slots[1]=(Present 30*, ĝ)
   repair separators     B2.keys[1] := 30  (from :max-key — anchor said 31)   ← the v5 read fix
                         B2.(count,measure) := ĝ   (no child summing)
descend B2→child 1:      materialize(B2,1):
   load @L5             1 GET → [30,31]
   project leaf          [30,31] ⊕ {30→30*} in ONE pass → [30*,31]
lookup 30* in [30*,31]   → found
```

L0, L1, L2, L3, L4 are **never loaded**. Each visited node projects exactly once, then is
cached as an ordinary materialized node — later `lookup`s on this path see no diff.

### Stage 2 — rebalancing commit (write the affected path)

Starting from the Stage-1 in-memory tree, batch `add 7, add 8, add 9` (all →L1). `L1[5,6]`
grows to `[5,6,7,8]` (full at BF 4), then `add 9` **overflows → splits** into `[5,6]` and
`[7,8,9]`. B0 absorbs the split child: `keys[2,6] → keys[2,6,9]`, now 3 children. That is a
**structural** return — B0's `_rebalanced` is set:

```
store(R):
  B0._rebalanced = true                          → WRITE B0 in full ⇒ @B0'  (new structure,
                                                     keys[2,6,9] → L0,[5,6],[7,8,9]);
                                                     markFreed(@B0)     ← Stage-1 anchor
  B1 still content-only-dirty / clean            → BUFFER (addr[1]:=@B1, slot re-emitted)
  B2 still content-only-dirty / clean            → BUFFER (addr[2]:=@B2, slot re-emitted)
  R points at a new child address (@B0')         → WRITE ⇒ @R''
```

**2 PUTs** (`@B0'`, `@R''`) — the path from the rebalanced node to the root, exactly baseline
PSS for those nodes. The new leaves `[5,6]`, `[7,8,9]` are written as part of `@B0'`'s subtree
(structure is materialized, never buffered), and `B0`'s slots are reset (it now *is* its
durable object). B1/B2 keep buffering. Rebalances are ~1/BF of ops, so amortized cost stays
≈ `1 + depth/BF`.

### Stage 3 — budget overflow (flush an over-budget child)

Suppose this commit dirties B1 and B2, and folding both diffs into R would exceed the per-node
budget `B`. `store(R)` sorts the bufferable dirty children by diff size and keeps the small
ones while the running total fits, **flushing the largest** (biggest-first):

```
store(R):  B = 6, passthrough = 0
  bufferable dirty: B1 (size 2), B2 (size 5)        → sort ascending: [B1(2), B2(5)]
  B1: embedded 0+2 ≤ 6                              → BUFFER (addr[1]:=@B1, slot[1] kept); embedded=2
  B2: embedded 2+5 = 7 > 6                          → FLUSH ⇒ @B2'  (its diff MATERIALIZED into
                                                       the rewritten subtree); markFreed(@B2); slot[2]:=∅
  R                                                → WRITE ⇒ @R'''
```

The large diff (B2) is the one written, so a slot that regularly fills a big share of the
budget is flushed proportionally often and can't jam the buffer; the small diff (B1) keeps
riding. Every stored object stays within `Σ embedded diff ≤ B`, and only resident (dirty)
children are flushed, so the flush reads nothing. The budget chooses how many cheap
content-only commits ride between flushes.

## Serialized format and the storage contract

A buffered branch serializes one extra field, `:slots` — a sparse map
`{child-index → {:count :measure :diff :max-key}}` (`Branch.slotsForStorage` produces it,
restore reconstructs it). `:slots` is **emitted only when present**, so a baseline node (or
any node at `diffBufSize = 0`) is byte-for-byte the pre-diff-buf format.

A **leaf**-level `:diff` is serialized in the comparator-agnostic form
`{:absent [element…] :present [element…]}` — two vectors, never a map keyed by the element.
This is mandatory, not stylistic: the deserializer has no comparator and would re-key an
element-keyed map by the element's own `.equals`/`.hashCode`, which can be coarser than the
set's order and silently collapse order-distinct entries (a tx-only `Absent`+`Present` → one,
dropping the removal). A **branch**-level `:diff` stays a `{child-index → …}` map (integer keys
never collapse). See *The diff language → Two representations*. The elements and the `:absent`/
`:present` vectors are directly edn/fressian-serializable.

**Storage contract.** `store` re-points a buffered child's address to its anchor and parks
the child's change in the parent's `_slots`. Therefore an `IStorage` implementation **must
serialize and restore `_slots`** (via `Branch.slotsForStorage` / reconstructing `_slots` on
restore) whenever diff buffering is enabled. A storage that ignores `_slots` will persist the
re-pointed (stale) anchor address while dropping the diff — **silently losing the buffered
mutation**. This is why the default is off: pre-diff-buf storages do not serialize `_slots`,
and the library cannot detect that capability through the `IStorage` protocol.

## Configuration

- **Default: off** (`diffBufSize = 0` ⇒ baseline, invariant I0).
- Enable per set via `:diff-buf-size N` (clojure `sorted-set*` / `from-sorted-array` opts),
  or the JVM system property `pss.diffBufSize=N`, or the cljs compile-time define
  `org.replikativ.persistent-sorted-set/default-diff-buf-size`.
- Only enable it with a **slots-aware `IStorage`** (see the storage contract above).
- Readers without diff-buf support cannot read slot-bearing objects, so a consumer should
  bump its store/version guard when it starts writing them.

`B` (the per-node budget, in diff *entries*) trades buffering depth against object size and
cold-reconstruction cost: larger `B` buffers more commits before a flush but bloats every
object and slows cold reads; smaller `B` does the reverse. When a node overflows, eviction is
**biggest-first** (flush the largest dirty diffs, keep the small ones) — see *Store*.

## Cost and trade-offs

- **Writes.** Content-only commit: ~1 PUT (the root). Rebalancing commit: writes the affected
  path (~depth PUTs); amortized ≈ `1 + depth/BF`. A budget-overflow commit writes the
  largest over-budget dirty children (biggest-first eviction). Bulk-then-store writes the tree
  once, like baseline. Composes
  with datahike index-root fusion (the index roots' slots ride inside the fused db-record), so
  a small commit can approach ~1 PUT *total* across indexes.
- **Reads.** Warm = normal materialized B-tree, `O(log_B N)`, full fanout, no read penalty
  (projection paid once per node on load, then cached). Cold = the load path plus applying
  each node's buffered diff (bounded by `B`); no read-path restructuring and no sibling loads,
  because rebalancing was materialized on write. `ĝ` answers cold rank/count for a child
  without loading it.
- **Trade-offs.** A rebalance forces writing its path (rather than buffering it) — rare
  (~1/BF) and exactly baseline PSS behavior for those nodes. Buffered leaf-diffs hold whole
  *elements*, so for very large elements (e.g. multi-KB leaf values) the per-PUT byte savings
  shrink and the budget `B` should be kept small; the PUT-count win still holds.

## Relationship to prior work

The buffering idea is shared with Bε-trees / TokuDB-PerconaFT / BetrFS, but the realization
differs in ways that matter here:

- **Full fanout.** No `B^ε` fanout reduction, so no taller tree and no `1/ε` read penalty —
  projection is paid once on load.
- **Commit == checkpoint**, with the root hash as the durability point; no separate
  write-ahead log (avoiding BetrFS's ≥2× write-amp and Toku's checkpoint-serialization
  stalls).
- **Rebalancing is written, not re-derived**, so durable and in-memory structures never
  diverge and reads never restructure or load siblings.
- **Aggregate state in the diff** (`ĝ`) gives exact subtree counts / order statistics under
  buffering — something that literature leaves unsolved (it keeps message counts, not order
  statistics); here it is free because the in-memory node already maintains `count`/`measure`.

The name "diff buffering" (rather than "operation buffer", the Bε/hitchhiker-tree term) is
deliberate: the in-memory tree is a plain B-tree, and what is buffered is a per-child *diff*
recorded only at the serialization boundary — not pending operations that flush lazily
through the live tree.

A future optimization not yet implemented: BetrFS-style "key lifting" (stripping the
separator-implied prefix from buffered keys) would compact slots, with the biggest win near
leaves — potentially large for datahike datoms with shared `e`/`a` prefixes.
