# Changelog

Notable changes to persistent-sorted-set. Earlier releases predate this file; `git log` is
the record for those.

## Unreleased

75 commits since 0.4.139. **Read the data-integrity section before upgrading** — one of these
defects can leave a datom present in the index but unreachable by a fully-specified lookup,
and upgrading alone does not repair a database that already has one.

### Data integrity — please read

**A stale separator could make an element unreachable by a fully-specified lookup.**
When `replace` changed an element that was a node's maximum, the parent updated its own
separator but decided whether to propagate that upward by asking the **operation** comparator
rather than the **set's**. Under an operation comparator coarser than the set's — datahike's
cardinality-one upsert — the answer was "nothing moved", so propagation stopped one level
early and an ancestor kept a separator naming the superseded element.

The element stays in the correct leaf, correctly ordered: `seq`, `count`, sortedness and any
prefix-descending read are unaffected. Only a lookup specifying **every** component of the
key misroutes past it. Requires a tree of **three or more levels** — with two levels the
leaf's parent is the root, whose separator is written unconditionally, so smaller trees are
structurally immune. Present in every release that has `replace` (0.3.114 onward).

Through datahike, both `d/datoms db :eavt e a v` and `d/datoms db :avet a v` could miss such
a datom, while `d/q`, `d/pull` and prefix `d/datoms` always found it. See
[doc/advisory-stale-separator.md](doc/advisory-stale-separator.md) for the affected-version
matrix, a check you can run on your own data, and the repair (export + fresh import recovers
everything).

**`store()` never wrote a child that was mutated in place.** Three `EARLY_EXIT` arms in
`add`/`remove`/`replace` mutated a child in place and returned without clearing
`addresses[i]`. That address asserts "this subtree is already durable", and `store()` read it
that way, so the change never reached disk. At `diffBufSize 0` — the default — at every
level. Trigger: store a live transient, mutate it further, store again. Measured, bf 64:
220 elements in memory, 219 after reload. Inherited from upstream, dating to 2022-08-31.

**`disj`/`replace` and `lookup` disagreed on which of several comparator-equal elements they
act on.** `lookup` took the leftmost; the mutating operations took whatever
`Arrays.binarySearch` returned, which explicitly disclaims which of several equal elements it
finds. For the `[id value]`-compared-by-id pattern the docs advertise, that meant `disj`
could remove a different element than `lookup` reported — and ClojureScript's binary search
converges leftmost, so the same `disj` produced a **different set and a different merkle
root** on the two runtimes.

**A backward `seek` returned a silently wrong range.** Both climb loops tested only the
direction of travel, so a target on the other side never climbed the parent chain. Measured
on `(apply sorted-set (range 10000))`: `seek 5000` then `seek 2500` yielded 5008 elements
starting at 4992, where 7500 starting at 2500 was documented — elements missing *and*
already-consumed elements re-emitted.

**diff-buf: `disj` deposited the caller's search key**, so under an operation comparator
coarser than the set's, deleted elements came back on reload — 40 elements read back from a
39-element set, with `contains?` still answering false.

**`IStorage`'s contract omitted `slotsForStorage`.** A storage written strictly to the
documented interface lost committed elements under diff-buf: 43 in memory, 40 after restore.
The javadoc now states what a storage must persist, and that `markFreed` is a candidate list
requiring the consumer to establish liveness — not an authoritative free.

**`NumericStatsOps.remove` discarded the recomputed statistics it had just paid for**,
yielding variance `-4.0` and standard deviation `NaN`. Serialized, therefore durable.

### Fixed

- `root()` published the root before the settings it had just adopted from it, so another
  thread could pair a published root with pre-adoption settings and run at the wrong
  branching factor. `_root` is now volatile and read acquire-first.
- The three adoptions in `root()` (branching factor, boundary, diff-buf budget) were chained
  read-modify-writes; two threads could each drop the other's. They are now staged and
  published once.
- A restored set adopts the branching factor, boundary and diff-buf budget its nodes carry.
  Reopening a store with a different `:branching-factor` than it was written with previously
  overran arrays (or, on the persistent path, silently built oversized leaves).
- `restore` now honours `:comparator`, as ClojureScript always did.
- `from-sequential` on an empty collection returned `#{nil}` on the JVM.
- `false` is a legal element; a buffered diff recorded the search probe instead.
- Two entry points allowed `nil` into a set.
- `JavaIter.next` ignored `_over`, so it returned the last element forever and never threw
  `NoSuchElementException`.
- A node shrinking in place left stale references past its new length, retaining removed
  elements — and, at a branch, entire superseded subtrees.
- `:ref-type` stopped bounding the tree after the first `disj`: an unchanged sibling was
  written into the successor bare-but-addressed, and the settle only wrapped null-address
  children.
- A leaf shrunk by a sibling rebalance kept its pre-shrink measure.
- Two sets sharing one caching storage overwrote each other's projection comparator, which
  could reorder a buffered leaf under the wrong comparator and leave an element permanently
  unfindable after a store.
- `get-nth` treats weight as a real contract and no longer requires a measure to be
  configured.
- A stale transient handle now throws instead of silently taking the persistent path.
- `node->identity` strips warmth caches at every depth, so a node's content address no longer
  depends on whether its caches happened to be populated.
- Counts: `remove` takes the delta instead of discarding a count it already has, and a join
  cascade no longer erases every count it writes. A cold reader's `count` after 25
  generations of churn went from 256 node restores to 15.

### Fixed — ClojureScript

- MST + storage + `conj` was broken outright: `mst-branch-add` discarded unchanged siblings'
  durable addresses, crashing on a cold tree and rewriting the whole index on a warm one.
- `:measure` never bootstrapped on the incremental path, so the shipped validator failed on
  every conj-built set.
- An incoherent diff-buf reconstruction (slots handed to a node whose settings say buffering
  is off) silently dropped buffered elements; it is now refused, as on the JVM.
- `count` on a stored set loaded the whole tree to add up numbers it already held.
- A restored set kept its own branching factor over nodes written at another.
- Projection wrote onto the node being projected from rather than a copy.
- `:diff-buf-size` is honoured, and what it writes can be read back.
- Four ClojureScript-only public API defects, plus `from-sorted-array` length, compact
  meta/storage, and backward `seek` bound handling.

### Added

- `from-sorted-seq`: streaming bulk build in O(depth) memory, on both runtimes.
- `diff`: what changed between two versions of a set.

### Changed

- `validate-full` no longer checks measures by default. The check compares a cached measure
  against a fresh fold with `=`, which is only valid for an **exact** measure — an incremental
  float measure legitimately differs in the last bits, and 55 of 72 healthy shapes were
  reported corrupt. Pass `{:check-measures? true}` to opt in.
- A `leafProcessor` together with diff-buf is refused rather than silently corrupting; opening
  a store whose nodes carry buffered elements with a processor now fails loudly.
- A node reconstructed with `diffBufSize <= 0` but handed buffered slots is refused.
- Supported branching factors are named, and `from-sorted-array` guards against values below
  the minimum.

### Performance

- The streaming build retained its whole input, making it O(n) rather than O(depth).
- One `disj` no longer restores a subtree merely to count it.
- diff-buf, measured for the first time: ~3x fewer stored objects and ~2.4x fewer bytes on a
  churn workload, against ~45% more CPU on transient `disj` and ~25% on persistent
  `conj`/`disj`. Reads are unaffected. Note the benchmark suite cannot show the benefit —
  `store-50K` stores a fresh tree, where nothing is bufferable by construction.

### Documentation

- `doc/CONCURRENCY.md` gained the ClojureScript contract: single-threaded is not
  concurrency-free, every `await` is a yield point, and at most one `store` may be in flight
  per set.
- `IStorage.markFreed` documents that the stream is a candidate list, that a content-addressed
  store may re-issue a freed address as live in the same commit, and that under diff-buf it is
  only sound for a linear history.
- Four comments that claimed concurrency properties the code does not provide were corrected.
