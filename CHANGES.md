# Unreleased

- **Fix (measure): a write to a cold tree erased the aggregate, and the next reader restored
  the subtree to rebuild it.** `_subtreeCount` is delta-maintained — `Branch.add` does
  `_subtreeCount += 1`, with no reference to the siblings. `_measure` was not: every arm of
  `Branch.add` recomputed it with `tryComputeMeasure`, which folds the measures of ALL `_len`
  children and returns `null` the moment one of them is not resident.

  On a lazily restored tree that is the ordinary case — only the descent path is in memory — so
  one `conj` erased the cached measure at every level from the touched leaf's parent to the
  root. The value was never WRONG, it was absent, which is why no contents assertion anywhere
  in the suite saw it; the bill landed entirely on the next reader, which rebuilt it through
  `forceComputeMeasure` and its `child(storage, i)` descent.

  Measured with an exact long measure, bf 64, 200 000 elements, `:ref-type :weak`, against a
  storage that counts blob reads — five `conj` that read 4 blobs between them:

      cold `measure`                 1 blob   (the root's own, restored alongside it)
      5 conj                         4 blobs  root _measure -> NIL
      the `measure` right after     79 blobs  = 39 branches and 40 LEAVES

  and it did not amortise: under `:weak` the pulled children are dropped again, so every round
  paid the same 79 — 399 blob reads over five write-then-read rounds on a 6451-blob tree. At
  bf 512 the same shape pulled 194 of 196 blobs, the whole tree, because the cost is
  `fanout x depth` rather than a function of how much was written. The 40 are LEAVES, and a
  consumer who configures a measure usually has fat ones: stratum carries a column chunk inline
  per leaf entry, so a stats query read hundreds of MB to produce numbers already cached in the
  branches it walked past.

  When the recompute is unavailable the inserted key is now folded in instead —
  `merge(_measure, extract(key))`, exact because `add` inserts exactly one element. Same
  measurement after: 0 blob reads, at every branching factor and every ref-type.

  This is the JVM catching up to ClojureScript, not a new strategy. `branch.cljs` has always
  folded the inserted key on the same-len and absorb arms and consulted children only on the
  SPLIT arm, which is exactly the boundary drawn here — a split PARTITIONS a measure, and the
  monoid has no operation that divides one. The recompute is still preferred wherever it is
  available, so a fully resident node is byte-identical to before and the stress oracle's
  per-node equality stays exact.

  **`IMeasure.merge` must be commutative** for an insert into the middle of the set to be
  measured exactly, since the key is folded in at the end rather than at its sorted position.
  That was always true of `Leaf.add`'s in-place arms; it is now written on the interface rather
  than left implicit in one implementation. Every shipped measure (count, sum, sumSq, min, max)
  satisfies it.

  **Not applied to `remove` or `replace`**, deliberately. A subtraction delta needs the element
  the LEAF actually removed rather than the caller's search key — `removedOut` is threaded only
  at level 1 and only under diff-buf — and `IMeasure/remove` is invertible only for measures
  that are, since min/max must consult the remaining children, which is the very IO this
  avoids. Those paths still null and still force.

  Regression test: `test.measure-cold-maintenance`, the measure twin of
  `durable-count-read-side`. Red against the unfixed build with 13 failures across both
  invariants (the root measure is nil after a write; reading it restores the subtree), green
  after, and every measure-VALUE assertion in it passes either way — which is the point.

# 0.5.x

A correctness release, and the version is 0.5 for that reason rather than for the feature list.
`diff` is new, but the reason not to treat this as a patch on 0.4 is that behaviour existing
callers depend on has moved — see **Compatibility** immediately below.

**Read the data-integrity items before upgrading.** Three of the fixes below can leave a set
that is wrong ON DISK, and upgrading alone does not repair a database that already has one:

- **`replace` under a coarse comparator left a stale separator** — an element stays in the set,
  correctly ordered, but a lookup specifying every component of the key routes past it. Through
  datahike this reached both `d/datoms db :eavt e a v` and `d/datoms db :avet a v`, while `d/q`,
  `d/pull` and prefix `d/datoms` were unaffected. Requires a tree of three or more levels.
  [doc/advisory-stale-separator.md](doc/advisory-stale-separator.md) has the affected-version
  matrix, a check to run against your own data, and the repair.
- **`store()` never wrote a child that was mutated in place** — inherited from upstream, at the
  default settings, at every level.
- **`disj` recorded the caller's search key** — deleted elements come back on reload.

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

### Compatibility — what a 0.4 caller should check before upgrading

  Nothing here breaks a stored set: trees written by 0.4 still read. These are the places where
  the same code does something different than it did.

  * **Tree SHAPE changed, so content addresses changed.** ClojureScript `conj`/`disj` now cut
    where the JVM cuts, and the JVM's transient remove now picks its sibling the way the
    persistent path does. Stored trees are unaffected, but the same data REBUILT now produces
    different node addresses — so nodes written by 0.4 and by 0.5 no longer dedup against each
    other, and a merkle root computed over rebuilt data will differ. Both changes are fixes:
    before them, the two runtimes and the two mutation paths disagreed with each other.
  * **`node->identity` strips warmth caches**, so a consumer that followed `node->map`'s old
    "hash this map for content-addressing" advice gets different addresses — and correct ones:
    the old advice made the address depend on which caches happened to be populated.
  * **Two configurations now throw that were accepted before.** A `leafProcessor` together with
    diff-buf, and a node reconstructed with `diffBufSize <= 0` while carrying buffered slots.
    Both silently corrupted; code that appeared to work will now fail loudly.
  * **`validate-full` no longer checks measures by default.** Pass `{:check-measures? true}` for
    the old behaviour.
  * **`restore` now honours `:comparator`**, which it previously ignored on the JVM. A call that
    passed one and silently got the default will now get what it asked for.
  * **`from-sorted-seq` with no `:meta` returns `nil` rather than `{}`** on the JVM, matching
    `sorted-set*` and ClojureScript.
  * **A stale transient handle throws** instead of silently taking the persistent path, and
    `replace`'s precondition is asserted under `-ea`.

### store() never wrote a child that was mutated in place — INHERITED FROM UPSTREAM

  Three `EARLY_EXIT` arms — in `add`, `remove` and `replace` — mutated a child IN PLACE and
  returned without clearing `addresses[i]`. That address asserts "this child's whole subtree is
  already durable", and `store()` reads it exactly that way, so the subtree was skipped at every
  depth and the mutation NEVER REACHED DISK.

  Reachable from the public API: `store` a live transient, mutate it further, `store` again — the
  checkpointed bulk-ingest shape. `persistent!` between the stores is clean, which is why nothing
  caught it. Measured:

      bf 64 dbs 0    level 1   in-mem 220  reloaded 219  missing [21]
      bf  8 dbs 0    level 3   in-mem 1220 reloaded 1219 missing [21]
      bf  8 dbs 256  level 2   in-mem 218  reloaded 219  extra   [5]

  BASELINE — it does not depend on diff-buf. Under diff-buf, level 1 is masked (the slot carries
  the real leaf-diff) while level >= 2 deposits a MARKER whose content lives only in the live
  child, so the loss returns.

  Every other mutation path already cleared the address; these three did not, because they install
  no new node. Fixed with `child(idx, mutatedChild)` — the existing idiom that clears the address
  AND unwraps the child. (Nulling the address alone trips the settle's own assertion, "dirty child
  must be a bare resident ANode": the child was still a SoftReference from an earlier settle.)

  Git archaeology: introduced by Nikita Prokopov's durability work — born 2022-08-31 (765d9b3),
  modern shape 2022-10-13 (dfce4c9), shipped in upstream 0.2.0 and 0.3.0. Reproduced at every one
  of those revisions. The fork point is 461df32 (0.3.0, 2023-08-04), which already has it, so this
  is INHERITED, not introduced here. Upstream still has it today. Notably, upstream commit 3d1837a
  (2022-10-08) audited this very invariant — replacing three `// FIXME check if left really
  changed` sites — and left the EARLY_EXIT arms untouched.

### disj recorded the caller's search key, resurrecting deleted elements

  `Branch.remove` deposited `Absent(<the caller's search key>)`. Under an operation comparator
  coarser than the set's, that is not the element the leaf holds; `projectLeaf` replays the diff
  under the SET's comparator, so the Absent cancels nothing. Measured, 40 elements bulk-built at
  bf 8 with diff-buf 256, cold-restored, `(disj s [17 999] by-first)`: in memory 39 without
  [17 0]; reloaded, 40 WITH it, while `contains?` still answered false because the separator was
  updated. So `seq` yields an element that `contains?`, `lookup` and `slice` cannot find.

  Needs a LEVEL-1 root — at level 2+ the slot is a branch marker whose diff is null. Six earlier
  attempts used conj-built trees, which are deeper with underfull leaves at these sizes, put the
  deposit at level 2, and came back green; the conclusion drawn from them (that the deposit was
  unreachable) was wrong and had been written into the source as fact.

  Fixed by the same reporting channel `replace` uses: `ANode`/`Leaf` gained a six-arg `remove`
  that reports the element actually removed. ClojureScript had already plumbed `:removed-out`
  through for the measure, bound it, and then deposited `key` anyway.

### :ref-type stopped bounding the tree after the first disj

  `Branch.remove` writes an UNCHANGED sibling into the successor as a bare strong ANode while
  keeping that sibling's still-valid address, and both settles only ever wrapped NULL-address
  children. So each disj permanently converted up to two slots per level on its path into strong
  references. Measured at bf 16 over 20000 elements with `:ref-type :soft`, after 400 disj and a
  store: `{:ref 780, :bare-STRONG-with-address 119}` — the 119 never shrank. Now `{:ref 899}`.
  Present at diffBufSize 0, so it predates diff-buf.

### The measure produced a negative variance and a NaN standard deviation

  `NumericStatsOps.remove` called the recompute supplier, got the exact answer, and DISCARDED it,
  keeping the subtracted `sum`/`sumSq` — which are only invertible in exact arithmetic. Measured
  through the public API (build and remove inside ONE transient, so the leaf is editable when the
  remove lands): elements [1.0 2.0], cached sum 4.0 against a truth of 3.0, sumSq 0.0 against 5.0,
  variance -4.0, stdDev NaN. `node->map` serializes `:measure`, so that lands on disk and is read
  back as authoritative.

### A leafProcessor with diff-buf corrupts, and is refused

  Every deposit records the ONE element the caller named; a processor rewrites the WHOLE leaf, and
  when it does not expand past the branching factor the parent classifies the change as
  content-only and buffers it. Every entry the processor added, dropped or rewrote is then absent
  from the diff while `Slot.count` still counts them. Measured, compacting processor at bf 4 with
  diff-buf 100: a set of 8 came back from store/restore with 9 elements; under `:ref-type :weak`
  the same corruption appears IN PROCESS at the next GC.

  diff-buf is now forced off when a processor is configured — NEUTRALISE rather than throw,
  because the `:test` alias sets `-Dpss.diffBufSize=256` and 41 existing processor tests inherit a
  budget they never asked for. That also shows the broken combination has been silently ACTIVE
  across the suite all along; those tests pass only because none drives a store/restore or an
  eviction. This is a STOPGAP — enabling the combination properly is planned.

  `withDiffBufSize` REFUSES rather than neutralising, because the stopgap otherwise reintroduced
  the loss one level up: `PersistentSortedSet.root()` adopts a restored node's budget through it,
  and measured, it returned 256 without a processor and 0 with one — silently running at 0 over
  nodes carrying slots, which is the "81 elements gone" shape the adoption exists to prevent.

### node->map is not a content address; node->identity is

  `node->map`'s docstring said "for content-addressing (hash this map)". Two of its keys are
  CACHES: `:measure` is null until forced (and `forceComputeMeasure` ASSIGNS, so a read-only query
  changes what a node later serializes as), and `:subtree-count` reaches storage as -1 whenever a
  child's count was unavailable. Hashing the map gives an address that is not a function of
  content — measured, the same set under the same operation produced 0 of 4 addresses in common
  between two stores differing only in which caches were populated.

  Both real consumers already avoid this independently, which is the strongest evidence the advice
  was the bug: datahike hashes `[addresses (canon slots)]`, stratum hashes the address vector.
  `node->identity` is that subset, named — `{:level :keys :addresses :slots}`. `:slots` is
  deliberately IN: buffered diffs are content, and a hash omitting them collides on logically
  different trees.

### IStorage told storage authors to write a blob that loses data

  The javadoc said: "For Leaf, store node.keys(). For Branch, store node.level(), node.keys() and
  node.addresses()." That list omits `slotsForStorage()` — the diff-buf diffs. A buffered child is
  anchor PLUS slot; persist only the address and every buffered element is gone on the next
  restore. Measured with a storage written strictly to the old list: 40 elements, bf 8, diff-buf
  256, three added and committed — in memory 43, after restore 40, elements 100 and 101 silently
  absent. Now points at `node->blob`/`blob->leaf` and says what each omission costs.

  `markFreed` now also states that under a CONTENT-ADDRESSED store an address reported there may
  be re-issued as live, including by the same commit — not allocator reuse, but content addressing
  working as intended. `(-> s (conj x) (disj x))` reproduces the old node exactly; measured, 2 of
  2 freed addresses came back live in the same commit, one of them the published root. The two
  in-tree GC tests assert the opposite; they are correct for THEIR storages, which allocate fresh
  addresses, and now say so.

### ClojureScript parity repairs

  * `-root` adopted the boundary but not the diff-buf budget, so a set restored from a bare
    address ran at 0 over nodes at N — the JVM's "81 elements silently gone" shape, on the one
    path cljs left exposed.
  * `assemble-nested` lacked the JVM's anchorless-slot skip, so it recursed into a Leaf whose
    `_slots` is undefined and emitted an entry anchored at a DIFFERENT, older child's address.
  * `from-sorted-array`/`from-sequential` dropped `:meta` on both runtimes — the same defect fixed
    in `from-sorted-seq` one builder over.
  * `map->settings` silently ignored a `Settings` INSTANCE (every keyword lookup returns nil), so
    a configured one produced all-defaults: measured, branching-factor 4 came back as 512. It is
    now honoured.

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
  unconditionally. So an index crosses into exposure somewhere between 100k and 200k datoms.
  Do NOT read a rate off that table: how many datoms are affected depends on how many
  cardinality-one upserts land on a node's maximum, and a later probe over a different upsert
  pattern found fewer at the same size. Size tells you whether you are exposed, not how much.
  `d/q` and `d/pull` still found them; they scan by prefix.

  [doc/advisory-stale-separator.md](doc/advisory-stale-separator.md) carries the affected-version
  matrix (pss 0.3.114 through 0.4.139, datahike 0.7.1615 through 0.8.1775), the access paths
  verified affected and unaffected, a self-audit to run against your own database, and the
  repair — export and re-import recovers everything, because the exporter scans rather than
  looks up.

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

### `disj`/`replace` and `lookup` disagreed on which comparator-equal element they act on

  `lookup` takes the LEFTMOST element equal under the operation comparator; the mutating
  operations took whatever `Arrays.binarySearch` returned, which explicitly disclaims which of
  several equal elements it finds. For the `[id value]`-compared-by-id pattern the docs
  advertise, `disj` could therefore remove a different element than `lookup` had just reported.
  ClojureScript's binary search converges leftmost, so the same `disj` produced a different set
  — and a different merkle root — on the two runtimes.

### A backward `seek` returned a silently wrong range

  Both climb loops tested only the direction of travel, so a target on the other side of the
  current position never climbed the parent chain. Measured on `(apply sorted-set (range 10000))`:
  `seek 5000` then `seek 2500` yielded 5008 elements starting at 4992, where 7500 starting at
  2500 is what the documented contract says — elements missing AND already-consumed elements
  re-emitted. Both runtimes.

### `root()` published the root before the settings it had just adopted from it

  A restored node carries the branching factor, boundary and diff-buf budget it was written
  with, and `root()` adopts them. It published `_root` first, so another thread could pair the
  new root with pre-adoption settings and run over it at the wrong branching factor. `_root` is
  now volatile and read acquire-first.

  The three adoptions were also chained read-modify-writes on `_settings`, so two threads could
  each drop the other's. They are now staged on one local and published once.

### Counts: a cold reader no longer pays to recover them

  `remove` discarded a count it already had rather than taking the delta, and a join cascade
  erased counts it had just written — so a reader that trusted the persisted count had to
  restore nodes to recompute it. After 25 generations of churn, a cold `count` went from 256
  node restores to 15. `disj` also no longer restores a subtree merely to count it.

### `validate-full` no longer checks measures by default

  The check compared a cached measure against a fresh fold with `=`, which is only valid for an
  EXACT measure. An incremental floating-point measure legitimately differs in the last bits, and
  55 of 72 healthy shapes were reported corrupt — a validator that cries wolf on correct trees
  trains its users to ignore it. Pass `{:check-measures? true}` to opt in.

### Smaller correctness fixes

  * `from-sequential` on an empty collection returned `#{nil}` on the JVM.
  * `false` is a legal element; a buffered diff recorded the search probe instead of it.
  * Two entry points allowed `nil` into a set.
  * `JavaIter.next` ignored `_over`, so it returned the last element forever and never threw
    `NoSuchElementException`.
  * `restore` now honours `:comparator`, as ClojureScript always did.
  * A stale transient handle throws instead of silently taking the persistent path.
  * A node shrinking in place left stale references past its new length, retaining removed
    elements — and, at a branch, entire superseded subtrees.
  * Two sets sharing one caching storage overwrote each other's projection comparator, which
    could reorder a buffered leaf under the wrong comparator and leave an element permanently
    unfindable after a store.
  * `get-nth` treats weight as a real contract, and no longer requires a measure to be
    configured in order to answer.

### Added

  * `diff`: what changed between two versions of a set, pruning shared subtrees by address —
    a subtree whose address both sides hold is skipped without being read. Both runtimes.
  * `from-sorted-seq` on ClojureScript. The JVM has had it since 0.4.139; the streaming builder
    is now on both runtimes.

### `from-sorted-seq` was not actually O(depth) — it retained its whole input

  Shipped in 0.4.139 under the title "streaming bulk build in O(depth) memory", which is what it
  exists to be and what it was not. `streaming-split` fed the remainder forward as
  `(subvec buf avg)`; a SubVector shares its base, and `conj` on one does `base.assocN(end, o)`,
  so the base grew without bound and every element ever consumed stayed reachable.

  Measured: 4M elements OOM'd at `-Xmx128m` before the fix and complete after it; live heap
  sampled mid-stream grew 2.7x between 250k and 2M elements before, 0.99x after. The fix is a
  forced copy, `(into [] (subvec …))`.

  The test that was meant to catch this sampled the heap AFTER `from-sorted-seq` returned, when
  every intermediate was already garbage — it measured the residue, an address and a count, and
  reported a flat 1.4 MB at 4M while the same build died under a smaller heap. It now samples
  mid-stream.

### Performance, measured

  diff-buf had never been measured. On a churn workload it stores ~3x fewer objects and ~2.4x
  fewer bytes, against ~45% more CPU on transient `disj` and ~25% on persistent `conj`/`disj`.
  Reads are unaffected. Note that the benchmark suite cannot show the benefit — `store-50K`
  stores a fresh tree, where nothing is bufferable by construction.

### Documentation

  * `doc/CONCURRENCY.md` gained the ClojureScript contract: single-threaded is not
    concurrency-free, every `await` is a yield point, and at most one `store` may be in flight
    per set.
  * `IStorage.markFreed` documents that the stream is a CANDIDATE list, that a content-addressed
    store may re-issue a freed address as live in the same commit, and that under diff-buf it is
    only sound for a linear history.
  * Four comments that claimed concurrency properties the code does not provide were corrected.

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

# 0.4.x

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