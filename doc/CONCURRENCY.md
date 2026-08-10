# Concurrency & Memory Model (JVM)

This document is the contract for every mutable field in the JVM implementation
(`src-java`).

The ClojureScript implementation is single-threaded, which is **not** the same as
concurrency-free, and the difference has a contract of its own — see
[ClojureScript: one writer per set](#clojurescript-one-writer-per-set) at the end. In
short: every `await` inside `async+sync` is a yield point, and `store` mutates node state
across those yields, so two overlapping async stores on one set interleave. The
single-writer-store rule that follows from that is a caller obligation, not something the
runtime prevents.

## Value semantics

A `PersistentSortedSet` is a persistent value: `conj`/`disj`/`replace` return a new
set that shares structure with the old one. Two consequences drive everything below:

1. **Nodes are shared.** A node reachable from any set that has been returned to a
   caller (or stored) may be referenced by arbitrarily many trees on arbitrarily many
   threads, indefinitely. Such a node is **SHARED** and is semantically immutable.
2. **Some interior state is still mutable.** For laziness (restore-on-demand, cached
   counts/measures) and for durability (the commit-time settle rewrites dirty slots
   into durable addresses), shared nodes carry caches and settle-visible state that
   *do* change. Every such mutation must follow the interior-mutability contract
   below, or it is a bug of the #15/#17 class.

## The interior-mutability contract

A write to a **possibly-shared** node is legal only if it is one of:

- **(a) a benign idempotent single-word cache fill** — a single reference/word write
  whose value every racing writer would compute identically, with no invariant
  coupling it to any other field (losing or duplicating the write is harmless); or
- **(b) a snapshot publication** — the state lives in one immutable snapshot object
  reachable through one `volatile` field, writers stage a complete new snapshot and
  publish it with a single volatile write (single writer) or CAS (racing writers).

Multi-field state on a shared node MUST use (b). The addresses/children/diff-buf
state of `Branch` is exactly such state and is covered by `NodeState` (below).

## Branch.NodeState — the settle-visible per-child state

```java
public static final class NodeState<Address> {
  public final Address[] addresses;  // may be null
  public final Object[]  children;   // ANode | Soft/WeakReference<ANode> | null per slot
  final BufState buf;                // diff-buf {slots, entries}; null == settled-empty
}
public volatile NodeState<Address> _state;   // never null
```

Per slot `i`, a snapshot is in exactly one state:

1. **Not stored** — `addresses[i] == null`, `children[i]` is a **bare** `ANode`
   (dirty children are strongly reachable by definition);
2. **Stored** — `addresses[i] != null`, `children[i]` is a `Soft/WeakReference`
   (or a bare `ANode` under `:strong` ref-type / paths that keep it resident);
3. **Not restored yet** — `addresses[i] != null`, `children[i] == null` (or the
   whole `children` array is null).

The forbidden state — `addresses[i] == null` with `children[i]` a `Reference` — is
impossible **within one snapshot** by construction, and `store()` enforces it as a
permanent `-ea` assertion. Historically the settle wrote `_addresses[i]` and then
wrapped `_children[i]` as two separate plain writes; a concurrent copier
(`Arrays.copyOfRange` of the two arrays at different moments) could mix generations,
producing either the forbidden dirty+Reference state (crashed baseline commits, #17)
or — the silent variant — an *old* address paired with the wrapper of a *newer*
child, which after a soft-reference clearance would restore and resurrect stale
data. `NodeState` generalizes the single-publish discipline `BufState` introduced
for the diff-buf pair in #15; `BufState` is folded into the snapshot so the settle
publishes `{addresses, children, slots, entries}` with **one** volatile write.

### Writers of `_state` on a SHARED node

| Writer | Mechanism | Why safe |
|---|---|---|
| `store()` settle (baseline & diff-buf) | stage full copies locally, ONE plain volatile publish, then `storage.store(this)` | **single writer**: only the commit thread settles a tree (see "single-writer settle" below); publish-before-serialize keeps the durable form identical to the old in-place behavior |
| `child(storage, idx)` restore cache-fill | copy-on-write of `children` + **CAS** retry; adopts a concurrent winner's child | a fill must never clobber a settle (CAS fails, re-read); a settle clobbering a fill merely loses a cache entry (re-restored later) |
| `bufEntries()` LAZY resolve | new `BufState` carrying the same slots + **CAS** retry | same asymmetry: resolve never clobbers a settle; a settle overwriting a resolve is strictly newer |

### Readers / copiers

Every method takes **one** `_state` snapshot and uses only its fields; the
structural-sharing copy paths (`add`/`remove`/`replace`/`removeContent`/
`mstMergeWith`) copy addresses, children **and** slots from that single snapshot, so
a derived tree can never bake in a torn pair. Sibling nodes passed into `remove`
get their own single snapshot each (`ls`/`rs`).

### EDITABLE / unpublished nodes (the dual-mode decision)

We chose the **mutate-in-place-inside-the-snapshot** variant rather than separate
plain working arrays with a seal point:

- An **editable** node (its `Settings._edit` flag is true — transients) or a
  **not-yet-published** node (a fresh copy still being stitched) is owned by one
  thread and unshared *by contract* (the transient ownership rule). The owner
  mutates the arrays inside the current `NodeState` directly (`Stitch` writes,
  `child(idx, node)`, cache fills); replacing a null array with a fresh one
  (`ensureAddresses`/`ensureChildren`) republishes a `NodeState` carrying the other
  components, and callers use the returned array (the republish invalidates older
  local references).
- Rationale: a separate plain-array mode with an explicit seal at `persistent!`
  would need a per-node seal walk (transients share committed nodes, so "the nodes
  of this transient" is not a cheap set) and a second representation for every
  reader. In-place mutation inside the snapshot has zero overhead on the hot
  transient paths, and the shared-node rules above are unaffected because
  editability is per-node (`ANode.editable()` reads the node's own settings): a
  committed node is never editable, an editable node is never shared.
- The seal point is `persistent!` (`Settings.persistent()` nulls the shared
  `AtomicReference<Thread>`), after which every node that carried those settings
  answers `editable() == false` and falls under the shared-node rules.
- `persistent!` returns a NEW `PersistentSortedSet` — as every Clojure transient
  does — and the stale handle then throws `IllegalAccessError("Transient used
  after persistent! call")` on any mutation. While the two were the same object
  the stale handle could only degrade silently: `conj!` took the persistent path
  and returned a new set, so the caller's mutation landed somewhere they were not
  looking.

### Transient ownership

A transient may be mutated by **one thread at a time**. A HANDOFF is fine — one
thread finishes, publishes across a happens-before edge, another continues, which
is what `fold` does and what a parking core.async block does. CONCURRENT mutation
is undefined.

This is a contract, **not enforced by default**, following Clojure: an owner
comparison sees thread IDENTITY and so cannot tell handoff from concurrency, and
enforcing it would forbid the safe pattern to prevent the unsafe one. Clojure's
`PersistentVector.persistent()` carries exactly this check commented out.

Breaking the rule is not a near-miss. Measured, 4 threads x 5000 `conj!` on one
transient: `sorted?` **false** — the keys come out of order, so every later
`binarySearch` is arbitrary — with `seq` and `count` disagreeing (19 318 vs
14 793), and one trial throwing `ArrayIndexOutOfBoundsException` from
`Seq/first`. Unlike Clojure's, this corruption becomes **durable** the moment the
set is stored.

Set `-Dpss.strictTransients=true` to have a foreign thread refused with
`IllegalAccessError("Transient used by non-owner thread")`. Intended for test
suites that know no handoff occurs; the flag is a `static final` read at class
init, so the branch folds away when it is off.

## Single-writer settle

`store()` may be called by **one thread at a time per LINEAGE** — in practice, per
storage — not per tree. "Per tree" was the earlier wording and it is too weak:
structural sharing means two trees are not disjoint. Measured on the pipelining-writer
shape (derive v1 from a base, then v2 from v1, touching different subtrees), **3 Branch
objects were reachable from both roots AND dirty in both** at bf 8 / n 1000. Storing
either settles those same objects in place, so "one thread per tree" permits exactly
the interference it means to forbid.

Concurrent `store()` of two versions sharing dirty nodes is not supported. The
consequences, in increasing order of severity:

* Both settles publish with a **plain write** (`_state = new NodeState<>(...)`), not a
  CAS, so the loser's state is discarded and children can be written twice. Measured
  over 1600 rounds with barrier-synchronised threads: at bf 8 / diff-buf 0, 20 writes
  where a sequential run does 15, in 196 of 200 rounds. Wasted work, not corruption.
* The publish happens **before** `storage.store(this)`, so one thread can serialise a
  blob built from the OTHER thread's address array. With a fresh-address storage both
  arrays are equally valid, which is why the measured runs stayed content-correct; under
  a content-addressed store, or when one thread buffered a child the other flushed, they
  are not interchangeable.
* `assembleNested` is **not snapshot-atomic across nodes** — it reads a child's slots,
  then its keys, then recurses into grandchildren's slots as separate volatile reads. A
  concurrent settle landing mid-walk mixes generations into a diff written against an
  anchor it no longer describes. That is a wrong-data-on-reload path, and it is the
  reason this is a hard constraint rather than a performance note.

No content corruption was observed in 1600 rounds, and no `-ea` oracle tripped; the
third point above is argued from the code, not measured. Nothing enforces or detects
any of it. `test/concurrent_store.clj` and `concurrent_diff.clj` cover settle-vs-READER,
never settle-vs-settle.

Readers and derivers (the pipelining apply thread) are unrestricted — that is the race
`NodeState` exists for.

`markFreed` calls happen at the same program points as before the `NodeState`
rework (method-entry snapshots feed the same decisions), so GC accounting
(freed-tracking, #16) is unchanged.

## Field inventory

### `ANode` (base of `Branch`/`Leaf`)

| Field | Writers | Readers | Protection |
|---|---|---|---|
| `_len` (int) | editable in-place `remove` | everything | plain + owner; frozen before sharing (handoff rule) |
| `_keys` (final ref; contents mutable) | editable stitches; `projectBranch` separator fix-up (pre-publication) | everything | plain + owner / pre-publication |
| `_settings` | — (final) | everything | immutable (its `_edit` AtomicBoolean is the transient seal) |
| `_measure` | editable paths; `forceComputeMeasure` cache fill on shared nodes; `projectBranch` (pre-publication) | measure queries | plain + owner; shared fill is contract (a): idempotent single-reference write |

### `Branch`

| Field | Writers | Readers | Protection |
|---|---|---|---|
| `_level` | — (final) | everything | immutable |
| `_state` (volatile `NodeState`) | ctors (install); owner-thread in-place mutation + `ensure*`/`installSlots`/deposit republishes (unshared); settle (single plain publish); `child()` fill + `bufEntries()` resolve (CAS) | everything | contract (b): volatile snapshot; see table above |
| `_subtreeCount` (long) | editable paths; `count()` / `computeSubtreeCount` cache fills on shared nodes (−1 → computed value) | `count`, `subtreeCount` | plain + owner; shared fill is contract (a). *Caveat:* a non-volatile `long` is not guaranteed atomic on 32-bit JVMs (JLS 17.7); all supported deployments are 64-bit HotSpot where it is. The only shared-node transition is −1 → the (unique) computed value |
| `_projCmp` | ctors; `PersistentSortedSet.root()` stamping; `child()` stamping of a freshly restored child (pre-publication) | projection, deposits | contract (a): idempotent single-reference write of the set's one comparator |

### `Leaf`

Only the `ANode` fields; same rules. Leaf `store()` writes nothing into the node.

### `PersistentSortedSet`

The set object itself is a lightweight handle; after safe publication it may be
read by many threads.

| Field | Writers | Readers | Protection |
|---|---|---|---|
| `_address` | commit thread (`store`, `address`); editable ops (clear on mutation) | `store`, walks | plain; commit is single-writer, editable ops are owner-thread |
| `_root` | `root()` restore fill (idempotent); `store()` re-wrap; editable ops | everything | contract (a) for the lazy fill; editable/commit writes are single-writer |
| `_count` | `count()` cache fill; editable ops | `count()` | contract (a): idempotent fill |
| `_version` | editable ops | seq invalidation | plain + owner (transients only) |
| `_settings` | `root()` self-describing adoption: branching factor, boundary, diff-buf budget — staged on a local, ONE publish | everything | contract (a), *because of the staging* |
| `_storage` | `store(IStorage)` | traversals | set by the owner before sharing / by the commit thread |

The `_settings` row earns contract (a) only because the three adoptions are staged on a
local `Settings` and published with a single write. They used to be three chained
read-modify-writes, and this table called that "one-time, idempotent" — which described
each adoption in isolation, not the sequence. Two threads taking the restore path on one
set could both read the pre-adoption value, and the second write would drop the first
adoption. Never a wrong value (both compute the same targets), but a MISSING one, and the
comments at the site record what each omission costs: leaves wider than the set believes
its branching factor to be, or "81 elements silently gone". If a fourth adoption is ever
added, add it to the staged local — not as another write to the field.

### `Settings`

All fields final except the `AtomicBoolean _edit` **content**: `persistent()` sets
it to false (the transient seal). `editable()` is a volatile read.

## Transient ownership & handoff rules

- **Transient ownership**: a tree obtained from `asTransient()` (and every node
  created while it is editable) belongs to one thread. No other thread may read or
  write it until `persistent!`.
- **Handoff**: passing any tree (persistent or freshly-persisted) to another thread
  must use safe publication — a `core.async` channel, a volatile/atomic field, a
  lock, or a thread start/join edge. The `_edit.set(false)` volatile write in
  `persistent!` is *not* relied upon as the publication edge (not every reader path
  performs the pairing volatile read); the publication channel provides the
  happens-before edge for the plain fields above (`_len`, `_keys` contents,
  `_subtreeCount`, …). This is the standard Clojure/JMM contract for transients and
  is what datahike's writer pipeline does.
- After handoff, the receiving thread sees a frozen value plus the lawful interior
  mutations described above (volatile `_state` snapshots and benign cache fills),
  each of which is independently safe to observe at any time.

## Oracles

- `test/concurrent_store.clj / concurrent-store-vs-pipelined-writer` — the #15
  diff-buf torn-pair race (settle vs pipelining deriver; `-ea` store()-time
  `assertBufEntries` cross-check).
- `test/concurrent_store.clj / baseline-settle-pair-tearing` — the #17-class
  addresses/children pair tear (baseline settle vs deriving copier; resident-walk
  slot-invariant check). Fails deterministically-statistically on the pre-NodeState
  code; recorded output in the test docstring.
- `store()`'s permanent `-ea` slot-invariant assert: dirty child ⇒ bare resident
  `ANode` within one snapshot.
- `test/baseline_store_softref.clj` — the forbidden state, injected artificially,
  is rejected loudly under `-ea` (and unwrapped without, #17's production behavior).

## ClojureScript: one writer per set

The ClojureScript runtime has one thread, so none of the JMM machinery above applies:
there are no torn reads, no publication problem, no need for `volatile`. What it does
have is **interleaving**, and the contract that follows from it was previously left
unstated — the opening of this document said only "single-threaded and out of scope",
which reads as "nothing to worry about" and is wrong.

`async+sync` compiles to a CPS chain, so **every `await` is a yield point**: control
returns to the event loop and any other pending continuation may run before the next
line does. Two places make that observable.

1. **`Branch.store` mutates node state across its yields.** The baseline arm loops over
   the children, `await`s each child's `store`, and writes the returned address into
   `this.addresses[i]` *in place* (`branch.cljs`, `(address this i child-address)`).
   The diff-buf arm likewise re-points addresses and rewrites `_slots` across `await`s.
   This is the same two-step settle that `NodeState` was introduced to eliminate on the
   JVM — it is safe here only because nothing else runs concurrently, which is precisely
   what a second in-flight store breaks.

2. **A set does not become "already stored" until its store finishes.** `btset.cljs`
   assigns `(set! (.-address set) …)` only *after* `await`ing the root store, so a second
   `store` entered while the first is still in flight does not see an address and runs in
   full — both walking and mutating the same nodes.

**The rule.** At most one `store` may be in flight per set at a time. Await the first
before starting another. This is a caller obligation: the library does not detect the
overlap, and nothing in the single-threaded runtime prevents it.

Reads (`seq`, `count`, `slice`, `lookup`) may interleave freely with each other. What
they may not interleave with is a `store` on the same set, for the reason in (1) — a
reader crossing a yield mid-settle can observe a node whose addresses are half-updated.

This is the ClojureScript half of the JVM's single-writer rule, and the JVM's ownership
argument (see `Branch._state`) is the same argument: exclusivity, not atomicity.
