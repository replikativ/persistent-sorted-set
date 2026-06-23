# 0.4.x

- **Content-defined boundary mode (Merkle Search Tree / "prolly" trees)** — _**experimental**_,
  opt-in per set via `{:boundary (mst-boundary lzpl)}`. The API and on-disk boundary descriptor
  may still change, and it is not yet hardened for production sync workloads; the default count
  B-tree is unaffected. Split points are derived from key hashes (`hasch.fast`,
  byte-identical JVM↔cljs) instead of node fill, so the tree is a pure function of its element
  set: the same elements always produce the byte-identical, content-addressed structure
  regardless of `conj`/`disj` order or which platform built it. Aimed at CRDT state sync and
  cross-replica dedup. Self-describing (the policy rides in the serialized blob and
  self-restores with no consumer configuration). Forces diff-buffering off and rejects leaf
  processors — both would break canonical addressing. Implemented on JVM + cljs, including
  storage-aware durable removal (address-preserving + `markFreed`). See
  `doc/merkle-search-tree.md`.
- Pluggable split-decision seam: the historical count B-tree is now the default `CountBoundary`
  (`IBoundary`) / `PBoundary` policy, byte-identical to before, with the split point factored
  out so MST (and future policies) plug in. No measurable performance change to the default
  path (verified by an interleaved criterium A/B against the pre-seam baseline).

The following also shipped in the 0.4 line but were not previously recorded here:

- **Diff buffering** (opt-in, off by default) — on immutable content-addressed storage, brings a
  content-only commit down to ~1 written object by buffering each unchanged-structure child's
  diff at the serialization boundary, re-pointing to the child's existing durable address.
  Gated by `:diff-buf-size` / `-Dpss.diffBufSize`; `0` is byte-identical to baseline. See
  `doc/diff-buffering.md`.
- **Subtree counts** — branch nodes carry subtree element counts; `count-slice` counts a range
  in O(log n) without iterating, and `hasSubtreeCounts()` is an O(1) check before using it.
- **Rank-based access** — `get-nth` reaches an element by position in O(log n) for
  percentile/quantile queries (requires a `:measure` with a `weight`).
- **Aggregate statistics** — an optional monoidal `:measure` (`IMeasure` interface / protocol),
  maintained incrementally; `measure` and `measure-slice` answer range aggregates (sum, count,
  min/max, variance, …) in O(log n). Built-in `NumericStatsOps`. See
  `doc/statistical-queries.md`.
- **Faster iteration**, plus `lookup` (retrieve the actually-stored key) and `replace`
  (single-traversal in-place update at the same logical position).

# 0.4.0

- Added `org.replikativ.persistent-sorted-set.fressian` — an **optional** canonical Fressian
  read/write handler set for PSS nodes (`pss/leaf`/`pss/branch`) and roots (`pss/set`), so
  konserve/kabel-backed consumers (datahike, yggdrasil, proximum, stratum) share one wire
  form. JVM (`clojure.data.fressian`) + cljs (`fress`); `data.fressian` is a `provided` dep.
- Nodes are self-describing: `:branching-factor`/`:diff-buf-size` ride in the blob (a store
  may hold mixed branching factors); the content-hash projection (`node->map`) is unchanged.
- The non-serializable bits (live `IStorage`, comparator, measure-ops) resolve at read via
  consumer-supplied resolvers — lexical closures for a one-store serializer, or id-keyed
  registries (`register-storage!`/`registry-storage-resolver`, …) for a shared/wire serializer.
- A node's `ref-type` (the soft/weak/strong caching policy) now rides in the blob as data, so it
  survives a round-trip / rootless replication (a node reconstructs with the writer's policy even
  when the reader knows nothing about it); SOFT (the default) is omitted, and a read-time
  `:ref-type` overrides what was serialized. ref-type is a JVM caching policy (cljs has no
  soft/weak refs), but cljs still **carries it through** inertly so it round-trips losslessly
  even via a cljs relay. Content addresses are unchanged.
- See `doc/serialization.md`.

# 0.3.0

- JVM: Per-set branching factor
- JVM: Choose type of reference for stored nodes (strong, soft, weak) per set
- JVM: Defaults to 512 branching factor, soft ref-type
- Added `settings` and `sorted-set*`
- JVM: Added `storage` and `opts` args to ctors
- JVM: Short-circuit `walkAddresses`

# 0.2.3

- Support set > 1M in CLJS (< 16^6 = 16M for fast path, up to 32^10 = 10^15 theoretically)

# 0.2.2

- Made Seq class public #11 via @FiV0

# 0.2.1

Added:

- `seek` to jump ahead during iteration #9 via @FiV0

# 0.2.0

Added:

- Durability in Clojure version #7 with @whilo
- `IStorage`, `store`, `restore`, `restore-by`, `walk-addresses`, `set-branching-factor!`

# 0.1.4

Special handling of nils in slice/rslice in CLJS, matching CLJ behaviour #6

# 0.1.3

Fixed NPE in `org.replikativ.persistent-sorted-set.arrays/array?` #4 #5 thx @timothypratley

# 0.1.2

Throw if iterating over a transient set that has been mutated.

# 0.1.1

Recompiled for Java 8.

# 0.1.0

Initial.