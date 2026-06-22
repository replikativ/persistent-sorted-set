# PersistentSortedSet

<p align="center">
<a href="https://clojurians.slack.com/archives/CB7GJAN0L"><img src="https://badgen.net/badge/-/slack?icon=slack&label"/></a>
<a href="https://clojars.org/io.replikativ/persistent-sorted-set"><img src="https://img.shields.io/clojars/v/io.replikativ/persistent-sorted-set.svg"/></a>
<a href="https://circleci.com/gh/replikativ/persistent-sorted-set"><img src="https://circleci.com/gh/replikativ/persistent-sorted-set.svg?style=shield"/></a>
<a href="https://github.com/replikativ/persistent-sorted-set/tree/main"><img src="https://img.shields.io/github/last-commit/replikativ/persistent-sorted-set/main"/></a>
</p>

**A fast, durable B-tree sorted set for Clojure and ClojureScript.**

PersistentSortedSet is an almost drop-in replacement for `clojure.core/sorted-set` — a
persistent, immutable sorted set backed by a B+-tree with structural sharing. Beyond the
standard set API it adds efficient slicing, transients, custom comparators, lazy durable
storage on any backend, range statistics, and a content-addressed mode for CRDT sync. The only
behavioral difference from `clojure.core/sorted-set` is that it can't store `nil`.

Implementations are provided for both Clojure and ClojureScript.

## Key features

- **[Drop-in sorted set](#usage)** — `sorted-set`, `sorted-set-by`, `conj`, `disj`,
  `contains?`, custom comparators, transients.
- **[Efficient slices & seeking](#slices-and-seeking)** — `slice`/`rslice` iterate any
  sub-range, with `rseq` and `seek` to reposition an open iterator in O(log n).
- **[`lookup` & `replace`](#lookup-and-replace)** — retrieve the actual stored key, or update a
  key in place at the same logical position in a single traversal.
- **[Durable storage](#durability)** — store the tree to disk/DB/object-storage through a small
  `IStorage` interface; restore lazily, loading only the nodes an operation touches. Automatic
  GC of unreachable nodes via `markFreed`. Async storage on ClojureScript.
- **[Canonical serialization](#serialization)** — optional Fressian read/write handlers so
  konserve/kabel-backed consumers share one wire form; self-describing, content-addressed
  nodes. See [doc/serialization.md](doc/serialization.md).
- **[Range statistics](#range-counting-rank-access-and-aggregate-statistics)** — subtree counts
  give O(log n) `count-slice`; a monoidal `:measure` gives O(log n) rank/percentile access
  (`get-nth`) and range aggregates (`measure`, `measure-slice`). See
  [doc/statistical-queries.md](doc/statistical-queries.md).
- **[Diff buffering](#diff-buffering-write-amplification)** — opt-in write-amplification
  reduction that brings a content-only commit on content-addressed storage down to ~1 object.
  See [doc/diff-buffering.md](doc/diff-buffering.md).
- **[Content-defined boundaries (Merkle Search Tree)](#content-defined-boundaries-merkle-search-tree)**
  — opt-in *prolly* mode where equal sets are byte-identical trees regardless of history, for
  CRDT state sync and cross-replica dedup. See
  [doc/merkle-search-tree.md](doc/merkle-search-tree.md).

Full documentation index: **[doc/README.md](doc/README.md)**.

## Installation

[![Clojars Project](https://img.shields.io/clojars/v/io.replikativ/persistent-sorted-set.svg)](https://clojars.org/io.replikativ/persistent-sorted-set)

```clj
;; deps.edn
io.replikativ/persistent-sorted-set {:mvn/version "LATEST"}

;; Leiningen
[io.replikativ/persistent-sorted-set "LATEST"]
```

The version follows the pattern `0.4.{commit-count}` and is incremented automatically on each
commit; the Clojars badge above shows the current release.

## Usage

```clj
(require '[org.replikativ.persistent-sorted-set :as set])

(set/sorted-set 3 2 1)
;=> #{1 2 3}

(-> (set/sorted-set 1 2 3 4)
    (conj 2.5))
;=> #{1 2 2.5 3 4}

(-> (set/sorted-set 1 2 3 4)
    (disj 3))
;=> #{1 2 4}

(-> (set/sorted-set 1 2 3 4)
    (contains? 3))
;=> true

(set/sorted-set-by > 1 2 3)
;=> #{3 2 1}
```

### Slices and seeking

`slice` and `rslice` return a lazy seq over a sub-range, iterating only the nodes they need:

```clj
(-> (apply set/sorted-set (range 10000))
    (set/slice 5000 5010))
;=> (5000 5001 5002 5003 5004 5005 5006 5007 5008 5009 5010)

(-> (apply set/sorted-set (range 10000))
    (set/rslice 5010 5000))
;=> (5010 5009 5008 5007 5006 5005 5004 5003 5002 5001 5000)
```

`seek` repositions an existing iterator forward in O(log n) without re-traversing from the
start — useful for merge-join-style consumption:

```clj
(-> (seq (into (set/sorted-set) (range 10)))
    (set/seek 5))
;=> (5 6 7 8 9)

(-> (into (set/sorted-set) (range 100))
    (set/rslice 75 25)
    (set/seek 60)
    (set/seek 30))
;=> (30 29 28 27 26 25)
```

### lookup and replace

`lookup` returns the *actual stored key* (handy when keys carry payload compared by a partial
comparator); `replace` updates a key in place when the old and new keys compare equal, in a
single traversal:

```clj
(-> (set/sorted-set 1 2 3 4)
    (set/lookup 3))
;=> 3

(-> (set/sorted-set 1 2 3 4)
    (set/lookup 99 :not-found))
;=> :not-found

;; replace updates a key at the same logical position (old and new must compare equal)
(defn cmp-by-id [[id1 _] [id2 _]] (compare id1 id2))

(-> (set/sorted-set-by cmp-by-id [1 :old] [2 :old] [3 :old])
    (set/replace [2 :old] [2 :new]))
;=> #{[1 :old] [2 :new] [3 :old]}
```

## Durability

The Clojure version can store a PersistentSortedSet on disk / in a DB / anywhere, and restore
it lazily. Implement the `IStorage` interface:

```clojure
(defrecord Storage [*storage]
  IStorage
  (store [_ node]
    (let [address (random-uuid)]
      (swap! *storage assoc address
        (pr-str
          (if (instance? Branch node)
            {:level     (.level ^Branch node)
             :keys      (.keys ^Branch node)
             :addresses (.addresses ^Branch node)}
            (.keys ^Leaf node))))
      address))

  (restore [_ address]
    (let [value (-> (get @*storage address)
                  (edn/read-string))]
      (if (map? value)
        (Branch. (int (:level value)) ^java.util.List (:keys value) ^java.util.List (:addresses value))
        (Leaf. ^java.util.List value))))

  (markFreed [_ address]
    ;; Optional: track addresses that become obsolete during modifications.
    ;; Called automatically when tree nodes are replaced during conj/disj.
    ;; Enables garbage collection of unreachable nodes.
    nil)

  (accessed [_ address]
    ;; Optional: track node access for cache management (e.g., LRU).
    nil)

  (isFreed [_ address]
    ;; Optional: check if address has been marked as freed.
    false)

  (freedInfo [_ address]
    ;; Optional: return debug information about freed addresses.
    nil))
```

Storing works per node, writing each node once:

```clojure
(def set     (into (set/sorted-set) (range 1000000)))
(def storage (Storage. (atom {})))
(def root    (set/store set storage))
```

Storing again issues no writes — the root is unchanged:

```clojure
(assert (= root (set/store set storage)))
```

Modify and store again, and only the changed nodes are written. For a tree of depth 3 that's
usually ~3 nodes; the root is always new:

```clojure
(def set2 (into set [-1 -2 -3]))
(assert (not= root (set/store set2 storage)))
```

Reconstruct a set from a stored snapshot using its root address:

```clojure
(def set-lazy (set/restore root storage))
```

`restore` is **lazy**: nothing loads until you access the set, and then only the nodes a
particular operation needs are fetched via `IStorage::restore`. `(first set-lazy)` loads ~3
nodes for a depth-3 tree; `(take 5000 set-lazy)` loads ~50 on default settings. Every operation
that works on an in-memory set works transparently on a lazy one, which can live arbitrarily
long without being fully realized:

```clojure
(conj set-lazy [-1 -2 -3])
(disj set-lazy [4 5 6 7 8])
(contains? set-lazy 5000)
```

PSS does not cache restored nodes itself — implement caching inside your `IStorage` (see
`IStorage::accessed` for LRU stats). Use `walk-addresses` to enumerate the nodes a set
actually references, e.g. to GC unreferenced objects from your storage:

```clojure
(let [*alive-addresses (volatile! [])]
  (set/walk-addresses set #(vswap! *alive-addresses conj %))
  @*alive-addresses)
```

See [test-clojure/org/replikativ/persistent_sorted_set/test/storage.clj](test-clojure/org/replikativ/persistent_sorted_set/test/storage.clj)
for more examples.

### ClojureScript durability

ClojureScript also supports durable storage. The `IStorage` interface works the same way, but
`store`/`restore` return promises/async values, so it integrates with IndexedDB, remote storage
APIs, and other async backends. Async-aware operations take a `:sync?` option.

## Serialization

For consumers that share a wire format (datahike, yggdrasil, proximum, stratum), the
`org.replikativ.persistent-sorted-set.fressian` namespace provides an **optional, canonical**
Fressian read/write handler set for PSS nodes (`pss/leaf`, `pss/branch`) and roots (`pss/set`),
on both JVM (`clojure.data.fressian`) and ClojureScript (`fress`). Nodes are self-describing —
`:branching-factor`, `:diff-buf-size`, `:ref-type`, and the content-defined boundary descriptor
ride in the blob — so a single store can hold a mix of settings and round-trip losslessly. The
non-serializable bits (live `IStorage`, comparator, measure-ops) resolve at read time via
consumer-supplied resolvers or id-keyed registries. `data.fressian` is a `provided` dependency.

See [doc/serialization.md](doc/serialization.md).

## Range counting, rank access, and aggregate statistics

Branch nodes carry subtree counts, and an optional monoidal `:measure` lets the tree answer
range aggregates and rank queries in O(log n) without iterating. See
[doc/statistical-queries.md](doc/statistical-queries.md) for the full model.

### Range counting

Count elements in a range without iterating through them — O(log n) via subtree counts:

```clj
(def s (into (set/sorted-set) (range 10000)))

(set/count-slice s 1000 2000)   ;=> 1001   ; [1000, 2000]
(set/count-slice s nil 500)     ;=> 501    ; .. to 500
(set/count-slice s 9000 nil)    ;=> 1000   ; 9000 ..
(set/count-slice s 1000 2000 my-comparator)
```

### Rank-based access (`get-nth`)

Access elements by position (rank) in O(log n), enabling percentile/quantile queries. Requires
a `:measure` with a `weight` implementation; the built-in `NumericStatsOps` uses count as
weight (each element weight 1):

```clj
(def s (into (set/sorted-set* {:measure (NumericStatsOps.)}) (range 1000)))

(set/get-nth s 500)   ;=> [500 0]   ; [value local-offset]
(set/get-nth s 950)   ;=> [950 0]

(defn percentile [s p]
  (let [n (count s), rank (long (* n p))]
    (first (set/get-nth s rank))))

(percentile s 0.5)    ;=> 500   (median)
(percentile s 0.95)   ;=> 950   (95th percentile)
```

`get-nth` returns `[entry local-offset]` (`local-offset` supports weighted statistics where an
entry represents multiple elements), or `nil` if the rank is out of bounds. On ClojureScript
the same API is available via `org.replikativ.persistent-sorted-set.impl.numeric-stats`, in
both sync and async (`:sync?`) modes.

### Aggregate statistics

A `:measure` maintains an aggregate incrementally as elements are added/removed, giving O(log n)
sum/count/min/max/variance over any range. The built-in numeric measure:

```clj
(import '[org.replikativ.persistent_sorted_set NumericStatsOps])

(def s (into (set/sorted-set* {:measure (NumericStatsOps.)}) [1 2 3 4 5]))

(set/measure s)         ;=> NumericStats{count=5, sum=15.0, min=1, max=5, ...}
(set/measure-slice s 2 4) ;=> NumericStats{count=3, sum=9.0, min=2, max=4, ...}
```

`NumericStats` exposes `count`, `sum`, `sumSq`, `min`, `max`, plus derived `mean()`,
`variance()`, and `stdDev()`. To implement your own, provide a **monoid** (identity +
associative merge) via the `IMeasure` interface (JVM) or `IMeasure` protocol (ClojureScript,
`org.replikativ.persistent-sorted-set.impl.measure`). Non-invertible aggregates like min/max
receive a `recompute` supplier in `remove` for the case where a removed element changes the
result. Full interface, custom examples, and the ClojureScript protocol are in
[doc/statistical-queries.md](doc/statistical-queries.md).

## Diff buffering (write amplification)

On immutable, content-addressed storage (e.g. konserve on S3/GCS), each `store` of a node is a
new object, and a commit normally rewrites the whole root→leaf path it touched (`≈ depth+1`
objects). Where each `PUT` dominates cost, **diff buffering** brings a content-only commit down
to **~1 object**: the in-memory tree stays an ordinary B-tree, and at the serialization
boundary a rewritten branch buffers each unchanged-structure child's *diff* (plus an aggregate
snapshot) into its own object, re-pointing to the child's existing durable address instead of
rewriting it. Reads, queries, counts, and measures are unaffected.

It is **off by default** and gated by a per-node budget; `0` is byte-identical to baseline.

```clojure
;; opt in per set:
(set/sorted-set* {:diff-buf-size 256 :storage my-storage ...})
;; or globally via the JVM system property -Dpss.diffBufSize=256
```

Enabling it requires an `IStorage` that serializes and restores the per-child slots
(`Branch.slotsForStorage` / reconstructing `_slots`); a storage that ignores them silently
drops buffered changes, which is why the default is off. See
[doc/diff-buffering.md](doc/diff-buffering.md) for the model, invariants, on-disk format, and
storage contract.

## Content-defined boundaries (Merkle Search Tree)

By default the tree shape depends on insertion/removal order. **Content-defined boundary mode**
(a *Merkle Search Tree*, the family informally called *prolly trees*) decides splits from a
deterministic hash of the keys, so the tree becomes a pure function of its contents:

> Same elements ⇒ byte-identical tree — regardless of `conj`/`disj` order, bulk vs incremental
> construction, or which JVM/JS peer built it.

This is opt-in per set and the default count B-tree is unaffected. It exists for
content-addressed **CRDT state sync and dedup**: converged replicas share nodes, so syncing
ships zero objects and idempotence is an O(1) root-hash comparison.

```clojure
(require '[org.replikativ.persistent-sorted-set.boundary :as b])

;; lzpl = leading-zeros per level; sets target fanout (avg node ≈ 2^lzpl keys)
(def s (into (set/sorted-set* {:boundary (b/mst-boundary 5)})
             (shuffle (range 10000))))
```

It is determinism-compatible across Clojure and ClojureScript (a tree built on the JVM and one
built in the browser for the same key set are node-for-node identical), serializes its policy
self-describingly, and is intentionally incompatible with diff buffering (forced off) and leaf
processors (rejected). See [doc/merkle-search-tree.md](doc/merkle-search-tree.md) for the level
function, the geometric size distribution, canonicality of `conj`/`disj`, and the rationale.

## Performance

To reproduce: install `[com.datomic/datomic-free "0.9.5703"]` locally and run `lein bench`.

`PersistentTreeSet` is Clojure's red-black-tree sorted-set. `BTSet` is Datomic's B-tree sorted
set (no transients, no disjoins). `PersistentSortedSet` is this implementation. Numbers on a
3.2 GHz i7-8700B:

**Conj 100k randomly sorted Integers**

```
PersistentTreeSet                 143..165ms
BTSet                             125..141ms
PersistentSortedSet               105..121ms
PersistentSortedSet (transient)   50..54ms
```

**`contains?` 100k times with random Integer on a 100k set**

```
PersistentTreeSet     51..54ms
BTSet                 45..47ms
PersistentSortedSet   46..47ms
```

**Iterate with java.util.Iterator over 1M Integers**

```
PersistentTreeSet     70..77ms
PersistentSortedSet   10..11ms
```

**Iterate with ISeq.first/next over 1M Integers**

```
PersistentTreeSet     116..124ms
BTSet                 92..105ms
PersistentSortedSet   56..68ms
```

**Iterate over a part of a 1M-Integer set (1 .. 999999)**

For `PersistentTreeSet`: `(take-while #(<= % 999999) (.seqFrom set 1 true))`. For
`PersistentSortedSet`: `(.slice set 1 999999)`.

```
PersistentTreeSet     238..256ms
PersistentSortedSet   70..91ms
```

**Disj 100k elements in randomized order from a 100k set**

```
PersistentTreeSet                 151..155ms
PersistentSortedSet               91..98ms
PersistentSortedSet (transient)   47..50ms
```

## Projects using PersistentSortedSet

- [DataScript](https://github.com/tonsky/datascript) — persistent in-memory database
- [Datahike](https://github.com/replikativ/datahike) — durable Datalog database with persistent
  storage
- [yggdrasil](https://github.com/replikativ/yggdrasil) — CRDTs over content-addressed storage

## Development

### Building

```bash
export JAVA8_HOME="/path/to/jdk1.8.0_202"   # Java 8 required for the bootclasspath
lein jar
```

### Testing

```bash
lein test                       # Clojure
yarn shadow-cljs release test   # ClojureScript
```

### CI/CD

This project uses CircleCI: both Clojure and ClojureScript tests run on every commit, formatting
is checked with cljfmt, and successful `main` builds are deployed to
[Clojars](https://clojars.org/io.replikativ/persistent-sorted-set) and released to
[GitHub](https://github.com/replikativ/persistent-sorted-set/releases). Versions follow
`0.4.{commit-count}`.

### Code formatting

```bash
clj -M:format   # check
clj -M:ffix     # auto-fix
```

## License

Copyright © 2019 Nikita Prokopov
Copyright © 2024 Christian Weilbach

Licensed under MIT (see [LICENSE](LICENSE)).
