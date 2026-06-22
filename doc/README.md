# PersistentSortedSet documentation

Deep-dives behind the features summarized in the [project README](../README.md).

## Storage & durability
- **[Serialization](serialization.md)** — the optional canonical Fressian read/write handlers
  for nodes and roots (`pss/leaf`, `pss/branch`, `pss/set`); self-describing blobs; resolver
  registries for shared/wire serializers.
- **[Diff buffering](diff-buffering.md)** — write-amplification reduction on content-addressed
  storage: bring a content-only commit down to ~1 object by buffering each unchanged-structure
  child's diff at the serialization boundary. Model, invariants, on-disk format, storage
  contract. Opt-in, off by default.

## Content-addressing & sync
- **[Merkle Search Tree (content-defined boundaries)](merkle-search-tree.md)** — *prolly* mode:
  split decisions derived from key hashes so that **equal sets are byte-identical trees**,
  independent of operation history. For CRDT state sync, cross-replica dedup, and O(1)
  idempotence. Opt-in, off by default.

## Queries & statistics
- **[Statistical queries](statistical-queries.md)** — subtree counts and the monoidal
  `:measure` mechanism behind `count-slice`, `get-nth` (rank/percentile access), `measure`, and
  `measure-slice`.

## Internals
- **[B-tree operations](btree-operations.md)** — the split/merge/borrow mechanics, transients,
  and the node-level invariants shared by all boundary modes.

---

For the public API and worked examples, start with the [README](../README.md). The version
history is in [CHANGES.md](../CHANGES.md).
