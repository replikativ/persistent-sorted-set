# 0.4.x

- **Fix (measure): a leaf shrunk by a sibling rebalance kept its pre-shrink measure.**
  `Leaf.remove`'s borrow-from-sibling paths shrink the SIBLING in place (`left._len = …`,
  `right._len = …`) but guarded the measure recomputation on `this._measure` — the measure of
  the leaf being removed FROM, not of the one being modified. Whenever the center leaf had no
  cached measure and the sibling did, the sibling kept a total describing keys it no longer
  held. Measured: a leaf holding two elements summing to 827 carrying a cached measure of 1659,
  its content before the rebalance moved two keys out.

  It compounds because `forceComputeMeasure` is not a force: it recurses into a child only when
  that child's measure is `null`, so a stale non-null value is trusted and merged upward, and the
  wrong aggregate is then written into the parent — overwriting a previously CORRECT cached
  value. `getNth` (rank-select) takes the same path, so a wrong rank could be returned.

  Only reachable with a user-supplied `:measure`; datahike configures none. ClojureScript is
  unaffected — its `Leaf` is immutable and rebuilds rather than shrinking in place.

  Found by the diff-buf stress sweep after the harness repairs below made it reachable: 108 of
  1800 trials failed. The sweep is now 1700/1700 and the oracle was strengthened from a
  root-level total to a PER-NODE invariant — every node's cached measure must equal a
  recomputation from its own content — which names the offending node instead of reporting a
  number that is 832 too large at the root.

- **Fix (diff-buf): the store → restore → modify → store cycle no longer corrupts the set.**
  Two defects, both present since diff-buffering landed, both single-threaded, and both only
  observable AFTER a reload — in memory the tree looked correct throughout, because a transient
  leaf is mutated in place and nothing projects the buffered diff until a restore.

  1. **`replace` recorded the wrong element as removed.** `Branch.replace` deposited
     `Absent(oldKey)` using the CALLER's search key. When the operation comparator is coarser
     than the set's — datahike's value-changing datom upsert searches `[e a _ _]` and replaces
     the whole datom — that is not the element the leaf holds once an earlier buffered replace
     has already changed it, so the `Absent` cancelled nothing and the new `Present` was added
     ALONGSIDE the old one. Measured: 16 elements, two replaces of one key in a single transient
     cycle, store, restore → key 5 came back as both `[5 1]` and `[5 2]`, with `count` 16 against
     a `seq` of 17. The deposit now uses the element the replace actually removes, captured
     before the mutation removes it.
  2. **A null slot `diff` does not always mean a branch anchor.** `Slot`'s contract documents a
     null `diff` as "a BRANCH anchor marker" and `assembleNested` recursed on that reading,
     casting the child to a `Branch`. But `depositKV` also leaves the diff null when the slot has
     no `anchor` — the "no durable base, write this child wholesale" case — which occurs on LEAF
     children too, so a plain store threw `ClassCastException`. Anchor-less slots are now skipped
     (a wholesale-written child has no buffered difference to assemble, at any level), and the
     remaining case is asserted rather than left to the cast.

  A third, found by repairing the stress harness afterwards: a node reconstructed with
  `diffBufSize 0` while carrying buffered slots is now **refused** at `installSlots` rather
  than read. That storage is incoherent — it persisted the node's diff buffer and then rebuilt
  the node declaring there is none — and it returned wrong CONTENT with a matching count
  (substituted elements, 5 of 5 seeds at bf 8). The settings are the storage's to get right;
  they describe a blob the storage itself wrote.

  Projection is also no longer gated on `diffBufSize()`: the budget is a write-side policy, while
  projecting a slot that carries a diff is a read-side obligation. A set whose storage rebuilt
  nodes with a different budget silently dropped buffered diffs on read. `diff-buf-size 0` is
  byte-identical to before (no slot is ever created, so nothing changes).

  The leaf REPORTS the element it removed (`removedOut` on the JVM, `:removed-out` in the cljs
  opts) rather than being searched a second time from the parent. Two reasons. It is FREE: the
  leaf binary search is comparator-bound, and a parent-side second search measured +18% at bf 512,
  +10% at bf 64 and +4% at bf 32 on a replace-heavy workload, while reporting lands within noise
  of the unfixed baseline. And the two searches must AGREE: a search is arbitrary among elements
  equal under the operation comparator, so a second one could name a different element than the
  one the leaf overwrote — recording `Absent` for one while replacing another, reintroducing the
  very duplicate this fixes.

### `replace` under a coarse comparator left a stale separator, and elements became unfindable

  `maxKeyChanged` asked the OPERATION comparator whether a child's max had moved. Routing uses the
  SET's comparator, which can see a change the coarse one calls equal — so propagation was
  suppressed and ancestors kept separators naming the OLD element. A later descent comparing the
  new element against a stale separator routes past the child that holds it.

  The element is still there: `seq` lists it, the set is sorted, `count` is right, and a lookup
  with the coarse comparator finds it. Only a lookup with the set's own comparator misses.

      JVM, inside a transient     n=40 bf=4: 9   n=100 bf=4: 24   n=3000 bf=16: 46
      ClojureScript, persistent   n=40 bf=4: 13  n=100 bf=4: 33   n=3000 bf=16: 250

  Through datahike at the DEFAULT branching factor of 512, measured against `d/datoms db :eavt e a v`:

      100 000 elements   root level 1   0 unfindable   (structurally impossible)
      200 000            root level 2   3
      400 000            root level 2   6

  Three levels are required — with two the parent is the root, whose separator is written
  unconditionally. So an index crosses into exposure somewhere between 100k and 200k datoms, and
  above that roughly one datom per 66k, silently, for cardinality-one value upserts. `d/q` and
  `d/pull` still found them; they scan by prefix.

  The test is `Util.equiv`, not `Objects.equals`: a Datom implements `equiv` but not
  `Object.equals`, so `Objects.equals` is identity there and would propagate on EVERY upsert.
  Measured cost of the fix: within noise at bf 512, +5-10% at bf 32/64.

  The two runtimes failed on different paths. The JVM writes `_keys[idx]` unconditionally and its
  persistent path always returns the successor, so only its TRANSIENT path was wrong — the one
  datahike transactions use. ClojureScript gated the keys rebuild on the same flag, so its default
  persistent path was wrong, once per leaf, while its comment claimed it "Mirrors JVM
  Branch.replace, which always writes `_keys[idx] = newMaxKey`".

### The measure subtracted the search key, and ClojureScript lost it entirely

  `IMeasure.remove` removes the contribution of the key being removed; the leaf passed the
  CALLER's search key. Those differ under a coarse operation comparator — the `[id value]`
  compared-by-id pattern `lookup`'s own docstring advertises. Measured, 200 longs removed via a
  decade comparator inside a transient: the set ended EMPTY with a cached sum of 24.0 (bf 8) and
  48.0 (bf 16). `node->map` serializes `:measure`, so that is durable and changes the node's
  content address.

  ClojureScript had it at TWO sites (the leaf, and again at the branch), and something worse
  underneath: `Leaf/merge` and `merge-split` built successors with a nil measure under a comment
  saying "Measure will be recomputed lazily if needed" — nothing recomputes it, and a branch above
  a nil-measure child can only postpone, so one merged leaf erased the aggregate for its whole
  spine. A per-level census made it plain:

      JVM  after removes   level 2: 1 with     level 1: 6 with           level 0: 33 with
      cljs after removes   (bf 8)  level 1: 2 WITHOUT / 3 with, level 0: 2 WITHOUT / 23 with
                           (bf 16) level 1 (the ROOT): WITHOUT

  With the leaves carrying their measures again, ClojureScript's branch now RECOMPUTES from its
  children instead of subtracting, matching the JVM. That is not a refinement: a measure is a
  MONOID, not a group, so a non-invertible one — min/max, which stratum uses — cannot be un-merged
  at all. `remove-measure`'s recompute-fn exists so the implementation can decide; a branch never
  needs it, since its children already hold their own measures.

### `:ref-type` was silently defeated whenever diff-buf was on

  The diff-buf settle published its children array with no `makeReference`, while the baseline
  settle wraps. Every child that had been dirty in any commit stayed a bare STRONG reference from
  its parent, and copy-on-write carried that into every successor — so the bound a user configured
  stopped applying to the hot part of the index, in exactly the deployment diff-buf exists for.
  Measured at bf 8 with `:ref-type :soft`: diff-buf 0 gave `{:ref 5}` at the root, diff-buf 256
  gave `{:bare-strong 5}`.

  Both settled kinds are safe to wrap because both end with a durable address: a BUFFERED child is
  re-pointed to its anchor and its assembled diff written back into the slot, a FLUSHED child is
  written outright.

  Wrapping switches on the re-derive path — `restore(anchor) + project(slot)` — which had NEVER
  executed in-process, precisely because a buffered child was never weakly held. `test/ref_type_diff_buf.clj`
  exercises it deterministically by clearing the references itself rather than waiting for a GC,
  and checks that an evicted tree still reads, still stores, and still restores.

### `disj`'s deposit uses the search key — documented, not corrected

  `Branch.remove` deposits `Absent(<the caller's search key>)`, the same shape as the `replace`
  defect above. It could not be shown to be reachable: six constructions (conj-built and
  bulk-built, bf 4/8/64, cold-restored with the storage's node settings carrying the same budget as
  the set) produced no slot at all for `disj`, so the line never ran and no reload resurrected an
  element. Threading a removed-element channel through `remove` would be a signature change on a
  path with no test to hold it — the trade that produced the last two defects here. The hazard is
  written at the deposit site instead, with what to do if a reproduction is ever found.

### `diff` reads its address/slot pair from one snapshot

  `diff`'s frontier decides, per child, whether that child's ADDRESS still stands for its
  contents — an address held by both sides prunes the whole subtree without reading it. Under
  diff-buf that needs two facts about the same child, its address and its `slots[i]`, and
  `child-refs` was taking them from two independent volatile reads of `_state`. `addressArray`'s
  own comment forbids exactly that: "Pair-coherent only when both come from ONE snapshot."

  Not reachable today, and the reason is not local: `store()`'s settle skips every child whose
  pre-settle address is non-null, so such a child keeps both its address and its slot and there is
  nothing to tear; a child that IS settled had a null address going in, and `prune-shared` never
  prunes on a null address. Measured while trying to build the failure — a child mutated this
  generation goes `[nil, SLOT] -> [ADDR, SLOT]`, an untouched one stays `[ADDR, SLOT]` across both
  a sibling's budget overflow and a structural split elsewhere.

  So `child-refs` was correct by a coupling to which children `store()` happens to touch, and
  nothing asserted that coupling. `Branch.addressesAndSlots()` makes the pairing correct by
  construction, and `test/concurrent_diff.clj` now pins the coupling itself across six
  generations, so a settle that starts re-pointing an already-addressed child is caught here
  rather than in a consumer's diff output.

### `from-sorted-seq` honours `:meta` on the JVM

  It built with a hardcoded `{}` while `sorted-set*` and the ClojureScript `from-sorted-seq` both
  passed `(:meta opts)` through. Measured: `(meta (from-sorted-seq … {:meta {:x 1}}))` was `{}` on
  the JVM against `{:x 1}` everywhere else. With no `:meta` the JVM now returns `nil` rather than
  `{}`, matching `sorted-set*` and ClojureScript. datahike never hit this — it applies `with-meta`
  after the build on both runtimes.

### CI was testing materially less than the local suite

  The `:ci` ClojureScript build set no `:closure-defines`, so `default-diff-buf-size` fell back to
  the library default of 0: every test not passing `:diff-buf-size` explicitly ran with buffering
  OFF. Both ClojureScript defects of this cycle live in the diff-buf path, so CI could not have
  caught either. It also skipped the `storage` and `generative` namespaces, plus
  `structural-invariants`, which does not exist — a dead exclusion. None of these was
  dependency-driven; the classpath is shared with `:node-tests`. `:ci` now tracks `:node-tests`:
  187 tests where it previously ran a subset unbuffered.

### Tests that could not fail for the reason they claimed

  * `order-check-is-lazy` asserted that an exploding lazy tail throws — which an EAGER pre-scan
    does just as well, as its own comment conceded. It now asserts nodes were already STORED when
    the tail explodes (42 of them, at bf 4), which only a streaming build produces.
  * `diff-of-a-set-with-itself-is-empty` called `(diff a a)`, which returns on the identical-address
    early-out before the algorithm starts. Split into the short-circuit (asserting zero reads) and a
    real walk over two independently built, equal-content sets on a cold handle (asserting reads
    happened).
  * `diff-of-unrelated-sets-is-correct-if-not-cheap` used 100 elements at the default fanout, which
    is ONE LEAF per side (measured: `:nodes-on-disk 1, :root-level 0`) — it compared two leaves and
    never descended. Now multi-level with the depth asserted, plus a different-depth pair that
    exercises the level-synchronisation the name is about.
  * `stored-tree-restores` asserted elements only, on a builder whose entire claim is SHAPE. It now
    compares the full structural signature of built vs restored, with a multi-level precondition.

### `replace`'s precondition is now asserted

  `replace` writes the new element into the OLD element's slot, which keeps the set sorted only
  while the replacement belongs in that same position. If the set holds another element the
  OPERATION comparator calls equal, the search picks an arbitrary one and writing over it can move
  it past a sibling — leaving the sorted set UNSORTED. Measured on both runtimes: a set containing
  `[5 0]` and `[5 7]`, `(replace s [5 0] [5 9] by-k)`, gave `[[5 9] [5 7]]`. On the JVM
  `contains? s [5 7]` then returned FALSE for an element still in the set, because every later
  lookup binary searches an array that is no longer ordered. Silent, runtime-dependent, and
  durable once stored.

  This is a PRECONDITION rather than a bug: repositioning would make `replace` a disj+conj with a
  different cost, and it is checked with an ASSERTION — `-ea` on the JVM, `:elide-asserts` on
  ClojureScript — so it costs nothing in production and names the misuse in a consumer's tests.

  The check is split because the two equal elements are not reliably in one leaf: at bf 4 a set of
  `[k 0]` for k in 0..39 plus `[5 7]` splits as `... [[4 0] [5 0]] | [[5 7] [6 0] [7 0]] ...`,
  either side of a boundary. `Leaf.noEqualSibling` covers the interior,
  `Branch.noEqualSiblingAcrossBoundary` the boundary (best-effort on ClojureScript, where it reads
  only already-resident siblings so it can stay synchronous). A leaf-local check alone silently
  passed the very case the precondition exists for.

  datahike is not affected: it replaces only on CARDINALITY-ONE attributes, where no second datom
  shares `[e a]`, and `:avet` uses disj+conj because v is part of that index's leading key. Its
  suite runs under `-ea` against this assertion.

  Defect 1 was present in BOTH implementations, independently — the ClojureScript port mirrored
  the JVM faithfully, bug included. Measured there before the fix: five replaces of one key came
  back from a reload as all five elements. Fixed on both.

  Nothing caught any of this because the existing stress harness drives ONE live set — it never
  takes the address, builds a new set over it, modifies that and stores again — and it passes one
  `:diff-buf-size` into both the set's opts and the storage's `Settings`, so the two can never
  disagree. Both gaps now have tests: `test/diff_buf_restore_cycle.clj` and
  `test/concurrent_restore.clj` (JVM), `test/diff_buf_replace_cycle.cljs` (ClojureScript).

- **Complete freed-address (`markFreed`) tracking on the JVM at `diff-buf-size 0`** — a parent
  replacing a durable child pointer now frees the old address at EVERY level of the root→leaf
  unwind (persistent copy AND editable in-place, across add/replace/remove, including
  split/absorb), not just the old root and a few scattered sites. Consumers running online GC
  previously leaked 51–84% of superseded blobs per flush cycle. The current tree never contains
  a freed address, frees are exactly-once, and the accounting identity
  `stored = freed ⊎ reachable` is now enforced by a regression test at both `diff-buf-size` 0
  and 256 (diff-buf deferral semantics unchanged). The cljs implementation already tracked
  these; this brings the JVM to parity.

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