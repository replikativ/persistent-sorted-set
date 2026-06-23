# Content-defined boundaries (Merkle Search Tree / prolly mode)

> **⚠️ Experimental.** This mode is new and opt-in. The public API (`mst-boundary`, the
> `:boundary` setting) and the serialized boundary descriptor (`{:type :mst :lzpl N}`) may still
> change; it has not yet been hardened in production sync workloads, so don't depend on the
> on-disk format for long-lived MST data yet. The cross-platform determinism and
> `conj`/`disj`/`replace` canonicality are property-tested on both JVM and ClojureScript, but
> treat the feature as unstable. The default count B-tree (no `:boundary`) is unaffected and
> stable.

By default PersistentSortedSet is an ordinary B+-tree: a node splits when it reaches the
branching factor, so the **shape of the tree depends on the order** keys were inserted and
removed. Two sets with identical contents but different histories are equal as sets, yet they
are laid out differently in memory and on disk.

**Content-defined boundary mode** (a *Merkle Search Tree*, the family informally called
*prolly trees*) removes that history dependence. Where a node splits is decided by a
deterministic function of the **keys themselves**, not by insertion order or node fill. The
result is a tree that is a pure function of its element set:

> **Same elements ⇒ byte-identical tree**, regardless of the order of `conj`/`disj`, of bulk
> vs incremental construction, or of which JVM/JS peer built it.

This is opt-in, per set, and the default count B-tree is completely unaffected (it needs no
hashing). It exists primarily for **CRDT state synchronization and content-addressed dedup**:
when a node's address is the hash of its canonical content, two replicas that have converged
to the same logical state share the same nodes, so syncing them ships *zero* objects and
idempotence is an O(1) root-hash comparison instead of an O(n) element walk.

## Quick start

```clojure
(require '[org.replikativ.persistent-sorted-set :as set]
         '[org.replikativ.persistent-sorted-set.boundary :as b])

;; lzpl = "leading-zeros per level" — sets the target fanout (avg node ≈ 2^lzpl keys).
;; 4 ⇒ ~16, 5 ⇒ ~32, 6 ⇒ ~64. Larger ⇒ shallower, more uniform nodes.
(def s (into (set/sorted-set* {:boundary (b/mst-boundary 5)})
             (shuffle (range 10000))))

;; History-independence: any build order yields the identical structure.
(= (into (set/sorted-set* {:boundary (b/mst-boundary 5)}) (range 10000))
   (into (set/sorted-set* {:boundary (b/mst-boundary 5)}) (shuffle (range 10000))))
;=> true, and the two trees are node-for-node identical
```

`mst-boundary` exists on both Clojure and ClojureScript and is determinism-compatible across
them: a tree built on the JVM and a tree built in the browser for the same key set are
node-for-node identical, so they content-address to the same hashes.

## How the boundary is decided

Every key is assigned a **level** by hashing it:

```
key-level(K) = leadingZeros( top-32-bits( hash(K) ) ) / lzpl
```

- `hash` is [hasch](https://github.com/replikativ/hasch)'s `hasch.fast/edn-hash` — a binary
  encoding that is **byte-identical on JVM and JS** (this is what makes the tree
  cross-platform).
- `leadingZeros` is `Integer/numberOfLeadingZeros` ≡ `js/Math.clz32` — also identical across
  platforms.
- Dividing by `lzpl` (leading-zeros-per-level) buckets keys into levels with a **geometric**
  distribution: `P(level ≥ n) = 2^(−n·lzpl)`. With `lzpl = 5`, ~1/32 of keys reach level ≥ 1,
  ~1/1024 reach level ≥ 2, and so on.

A key's level is a pure function of the key. A key that reaches level `L` becomes a
**boundary** (a separator) at every level below `L`. The split rule is then trivial and the
same for leaves and branches:

> A node ends immediately after each interior key whose level reaches this node's level + 1.
> The node's last key is its terminator (the parent's separator / the global maximum) and is
> never treated as an interior cut.

Because boundaries are intrinsic to the keys, inserting or deleting one key can only change
the node it lands in (and possibly promote a new spine) — it can never shift where *other*
keys split. That locality is exactly what makes the structure history-independent.

### Why levels, not a rolling hash

This is *textbook* MST: the level is a function of the **single key's hash**. Some prolly-tree
designs (e.g. Dolt) instead run a rolling/content-defined chunker over the serialized stream,
which couples chunk boundaries to byte size and gives more uniform node sizes — at the cost of
unbounded "boundary ripple" that needs a streaming chunker to bound. The per-key scheme trades
size uniformity for **strict locality and O(1) split decisions**, which is the better fit for
an in-memory persistent tree that also serves point and range queries.

### The geometric size distribution

A consequence of per-key levels is that node sizes follow a geometric distribution (mean
`2^lzpl`, coefficient of variation ≈ 0.9) rather than the tight `[B/2, B]` band of a classic
B-tree. Occasional single-key nodes and the odd deep spine are expected — the tree is, by
design, slightly out of balance "by bad luck" (the same level-distribution that drives skip
lists and HNSW). At realistic fanouts this is a non-issue: at `lzpl = 6` (B≈64) only ~1% of
nodes are singletons, and the tree stays height-balanced (all leaves at the same depth).

## What it costs and what it buys

| | default (count B-tree) | content-defined (MST) |
|---|---|---|
| split decision | node fill ≥ branching factor | `key-level` (one hash per inserted key) |
| structure | depends on insert/remove order | pure function of the element set |
| node sizes | tight `[B/2, B]` | geometric (mean `2^lzpl`) |
| per-op cost | no hashing | ~1 hash/op (hasch.fast) |
| cross-replica dedup | none (history-dependent) | full (converged subtrees share addresses) |
| serialization | count B-tree handlers | self-describing `{:type :mst :lzpl …}` |

Use MST when you content-address nodes and care that **equal sets are equal trees**: CRDT
merge + idempotence detection (the [yggdrasil](https://github.com/replikativ/yggdrasil) use
case), efficient replica-to-replica sync, deduplicating snapshot storage. Stick with the
default for plain in-memory or single-writer durable use — it is faster (no hashing) and the
node sizes are tighter.

## conj / disj are canonical

`conj` and `disj` behave exactly as for any sorted set; the only difference is the split/merge
decision. After **any** sequence of operations the tree equals a fresh build of the surviving
elements:

```clojure
(let [a (-> (into (set/sorted-set* {:boundary (b/mst-boundary 4)}) (range 100))
            (conj 250) (disj 7) (conj 7) (disj 99))
      b (into (set/sorted-set* {:boundary (b/mst-boundary 4)}) (-> (range 100) set (conj 250) (disj 99)))]
  (= a b))            ;=> true — and structurally identical, not merely set-equal
```

Internally:
- **Insert** uses an O(1) decision: only the inserted key (or, on append, the displaced old
  maximum) can introduce a new boundary, since an MST node has no interior boundaries except
  its terminator.
- **Remove** rebuilds only the affected node and re-derives boundaries locally; merges across
  a live boundary are forbidden (a key that is still a boundary keeps its node separate), which
  is what preserves canonicality. Deleting the maximum can collapse a degenerate spine.

This is exercised on both platforms by a model-based generative test that drives a random
**interleaved** `conj`/`disj` sequence against `clojure.core/sorted-set` and asserts (a) the
elements match the oracle, (b) the tree satisfies the MST structural invariants, and (c) it is
node-for-node identical to a fresh build of the survivors. See
`test-clojure/.../test/mst.cljc`.

## Durability and serialization

MST sets store and restore through the same `IStorage` path as any other PSS (see
[Durability](../README.md#durability)). Two points specific to content-defined mode:

- **The boundary policy is self-describing.** Each node serializes a small descriptor
  (`{:type :mst :lzpl N}`) into its blob via the
  [canonical Fressian handlers](serialization.md). On `restore`, PSS reconstructs the MST
  boundary internally from that descriptor — **no consumer-supplied configuration is needed**.
  The split strategies are known to PSS, so a durable store round-trips its own policy.
- **A node's content address excludes the boundary descriptor.** The Merkle address is
  `hash({:level … :keys … :addresses …})` — the descriptor is *configuration*, not content, so
  two stores that agree on contents content-address identically even if recorded separately.

Removal on a durable (lazily-loaded) MST set is storage-aware on both JVM and ClojureScript:
it preserves the addresses of untouched children and calls `markFreed` for the nodes it
replaces, so a content-addressed backend can garbage-collect the obsolete objects.

## Incompatibilities

A content-defined boundary is deliberately **incompatible with two features**, and PSS rejects
or neutralizes the combination rather than letting it be constructed silently:

- **Diff buffering is forced off.** [Diff buffering](diff-buffering.md) addresses a buffered
  spine node by `hash(anchor + diff)` — a function of *flush history*, not canonical content —
  which is precisely the history-dependence MST exists to remove, and it would break
  cross-peer dedup. The two make opposite promises (diff-buf: *content changed without changing
  address*; MST: *address ≡ content hash*), so enabling MST sets `:diff-buf-size` to `0`. See
  diff-buffering.md for the full argument.
- **Leaf processors are rejected.** The O(1) insert decision assumes a single key was inserted
  per op; a leaf processor that rewrites multiple entries per op would violate that invariant,
  so combining the two throws.

## API

| | |
|---|---|
| `(b/mst-boundary lzpl)` | build a boundary policy; `lzpl` sets fanout (avg node ≈ `2^lzpl`) |
| `(set/sorted-set* {:boundary …})` | create a set in content-defined mode |
| `(set/from-sorted-array cmp arr n {:boundary …})` | bulk-build in content-defined mode |
| `(b/key-level key lzpl)` | the level a key rises to (the determinism primitive) |

The default — omitting `:boundary` — is the count B-tree and is byte-identical to every prior
release.
