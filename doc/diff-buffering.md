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
slot's `ĝ` from `M`: `insert k → Present(k)`, `remove k → Absent(k)`, `replace → Present(k')`.
Within one slot a later op on the same `cmp`-key overwrites (net latest-wins). No tree-diffing
— the op states its own effect.

### Store (write the touched nodes, buffer the rest)

```
store(node):
  if clean (has _address): return _address          # unchanged → reuse durable object
  for each dirty child cᵢ:
     if cᵢ's whole dirty subtree is content-only (no rebalance anywhere) and under budget B:
        child_addrᵢ := cᵢ.anchor                     # BUFFER: do not write cᵢ, do not markFreed
        slotᵢ       := (assembled nested diff, ĝᵢ, maxKeyᵢ)
     else:
        child_addrᵢ := store(cᵢ); markFreed(old anchor); slotᵢ := ∅
  write node (base + slots); return new _address     # written because rebalanced / child-written / root
```

- The **root is always written** ⇒ ≥ 1 PUT.
- **Structural changes are written**, so the durable structure always matches in-memory —
  no divergence, no re-derive, no sibling load on read. A rebalancing commit writes the
  affected path (~depth PUTs); rebalances are ~1/BF of ops, so amortized ≈ `1 + depth/BF`.
- **Budget `B`** is enforced here: if buffering a child would push this node's embedded diff
  over `B`, that child is written (materializing its diff) instead, keeping every stored
  object's `Σ embedded diff ≤ B`.
- All written nodes are resident (the dirty in-memory nodes), so writing never reads.

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
leaf-diff   ::= { cmp-key → Absent | Present(element) }      # net latest-wins per key
branch-diff ::= { child-index → (node-diff, ĝ, max-key) }    # nested, per child
ĝ           ::= (count, measure)                             # absolute snapshot
```

One recursive form, exactly **two** ops. `Present(element)` carries the stored element and
expresses both **add** (key absent ⇒ insert) and **replace** (a `cmp`-equal but different
element present ⇒ overwrite) — it is an upsert applied during the leaf rebuild, never a
delete-then-add (which could transiently restructure). `Absent` expresses **remove**. The
aggregate is *absolute state*, not a delta, so non-invertible measures (min/max) are fine and
it is free to collect (the in-memory node already maintains `count`/`measure`).

### Worked example (BF 4, depth 3)

```
R(level 2) keys[6,16,31] → B0,B1,B2
B0 keys[2,6]   → L0[1,2]     L1[5,6]
B1 keys[11,16] → L2[10,11]   L3[15,16]
B2 keys[22,31] → L4[20,21,22] L5[30,31]
```

One content-only commit — `add 3`(→L1), `add 12`(→L3), `delete 20`(→L4),
`replace 30→30*`(→L5). No node crosses a fill bound ⇒ B0,B1,B2 are all buffered and only R
is written (1 PUT). R's stored object:

```clojure
{:level 2 :keys [6 16 31]
 :addresses [aB0 aB1 aB2]                 ; all three = children's UNCHANGED durable anchors
 :slots {0 {:count 5 :measure mB0 :max-key 6
            :diff {1 {:count 3 :measure mL1 :max-key 6  :diff {3 3}}}}      ; Present(3)
         1 {:count 5 :measure mB1 :max-key 16
            :diff {1 {:count 3 :measure mL3 :max-key 16 :diff {12 12}}}}    ; Present(12)
         2 {:count 4 :measure mB2 :max-key 31
            :diff {0 {:count 2 :measure mL4 :max-key 22 :diff {20 :ABSENT}} ; Absent(20)
                   1 {:count 2 :measure mL5 :max-key 30* :diff {30 30*}}}}}}; replace
```

`:diff` is keyed by **child-index** at a branch level and by **cmp-key** at a leaf level
(known from the child's level). The `:max-key` on nested entries repairs each reconstructed
grandchild's separator on restore. Unchanged children (L0, L2, …) have **no entry** — their
durable objects and `ĝ` stand. The whole nested structure lives only in the topmost written
object (here R), so there is no duplication.

## Serialized format and the storage contract

A buffered branch serializes one extra field, `:slots` — a sparse map
`{child-index → {:count :measure :diff :max-key}}` (`Branch.slotsForStorage` produces it,
restore reconstructs it). `:slots` is **emitted only when present**, so a baseline node (or
any node at `diffBufSize = 0`) is byte-for-byte the pre-diff-buf format. `Absent` is a
namespaced keyword so leaf-diffs are directly edn/fressian-serializable.

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

`B` (the per-node budget) trades buffering depth against object size and cold-reconstruction
cost: larger `B` buffers more commits before a flush but bloats every object and slows cold
reads; smaller `B` does the reverse.

## Cost and trade-offs

- **Writes.** Content-only commit: ~1 PUT (the root). Rebalancing commit: writes the affected
  path (~depth PUTs); amortized ≈ `1 + depth/BF`. A budget-overflow commit writes the
  over-budget buffered children. Bulk-then-store writes the tree once, like baseline. Composes
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
