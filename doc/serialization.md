# Canonical node serialization (`org.replikativ.persistent-sorted-set.fressian`)

An **optional** namespace providing one canonical Fressian read/write handler set for PSS
B-tree **nodes** (`Leaf`/`Branch`) **and roots** (`PersistentSortedSet`/`BTSet`), so every
konserve/kabel-backed consumer shares a single wire form. It is the reference that
datahike, yggdrasil, proximum and stratum square their storage and wire layers against.

The namespace is not loaded unless you require it, and it depends on `clojure.data.fressian`
(JVM) / `fress` (cljs) as a **`provided`** dependency — a consumer that uses it brings its
own version.

## Why one shared codec

On a single kabel websocket there is exactly **one** fressian write-handler per type, so
once two PSS-backed systems put nodes on the same socket the node codec is shared *by
construction*. A single socket is also what preserves **causal ordering** across systems
(two sockets = two message orderings = a record can arrive before the nodes it references).
So: centralize the node codec, get one wire form + one place that fixes JVM/cljs drift, and
keep everything else per-project.

## The node ⇄ root seam (the load-bearing distinction)

A **node** carries neither a comparator nor storage nor the set's identity — the
constructors prove it: `Leaf(keys, settings)`, `Branch(level, keys, addresses, settings)`.
Those live on the **root**. So the split is:

| concern | lives on | who handles it |
|---|---|---|
| node structure (`:keys`, `:level`, `:addresses`, `:subtree-count`, `:measure`, diff-buf `:slots`) | the node | **this ns (canonical, shared)** |
| comparator (re-stamped lazily on descent) | the root | a resolver you pass |
| storage (lazy child loading, live `IStorage`) | the root | a resolver you pass |
| element types (Datom, ChunkEntry, RegistryEntry, maps, …) | inside `:keys`/`:slots` | the consumer's **element** handlers (by recursion) |

The node codec never inspects an element type; element values recurse through the consumer's
own handlers.

## What travels vs what is resolved

A blob carries plain data but not live objects or functions:

- **Serialized (in the blob):** `keys` / `addresses` / `level` / `subtree-count` /
  `measure`-value / `slots`, **and the node's own `:branching-factor` + `:diff-buf-size`**.
  Because branching/diff-buf ride in the blob, each node is **self-describing** for its
  structure — its `Settings` are reconstructed per node — so one store may hold nodes of
  different branching factors. (These two are *not* part of `node->map`, the content-hash
  projection, so content addresses are unchanged.)
- **Resolved at read (runtime):** the live `IStorage`, the comparator (fn), and the
  measure-ops (`IMeasure`) — supplied by the consumer via three resolvers, each
  `(fn [meta] -> thing)`:
  - **lexical** — a serializer that owns *one* store closes over its storage/cmp/measure; no
    ids needed, and old roots (without ids) just read. The convenient default.
  - **registry** — a shared/wire serializer over *many* stores resolves by an id the root
    stamps in its `meta` (`:storage-id` / `:comparator-id` / `:measure-id`) via the
    `registry-*-resolver` helpers + the `register-*!` registries in the ns.

## Blob shapes

```
pss/leaf   → {:keys <elements>                                  :branching-factor n :diff-buf-size d}
pss/branch → {:level n :keys <separators> :addresses <addrs>
              :subtree-count c (:measure m) (:slots <diff-buf>)  :branching-factor n :diff-buf-size d}
pss/set    → {:meta <root-meta> :address <root-addr> :count n   :branching-factor n :diff-buf-size d}
```

`subtree-count`/`measure` are carried for completeness; `:slots` appears only when diff-buf
is enabled (`Settings.diffBufSize > 0`), where a leaf child's `:diff` is the comparator-agnostic
`{:absent [..] :present [..]}` form (`Slot/leafDiffForStorage`) and slots reconstruct into
`_slots` with `anchor = addresses[idx]` (re-derived, not stored). cljs `keys`/`addresses` are
JS arrays, normalized to/from vectors here, once.

## Usage

Assemble your full handler maps via the bundle builders — node handlers + the root handler +
your element handlers — in one call.

```clojure
(require '[org.replikativ.persistent-sorted-set.fressian :as pss-fress])

;; A LOCAL store serializer (owns one store): lexical resolvers, no ids needed.
(pss-fress/canonical-read-handlers
  {:resolve-storage (fn [_] my-storage)   ; the live IStorage
   :resolve-cmp     (fn [_] my-comparator)
   :measure-ops     nil                   ; node-level IMeasure (nil for most)
   :default-bf      512                   ; fallback bf for any pre-bf legacy blob
   :element-read-handlers {…}})           ; e.g. a Datom / ChunkEntry handler
(pss-fress/canonical-write-handlers {:element-write-handlers {…}})

;; A WIRE peer (one serializer over many stores): registry resolvers, by id in root meta.
(pss-fress/canonical-read-handlers
  {:resolve-storage (pss-fress/registry-storage-resolver)
   :resolve-cmp     (pss-fress/registry-cmp-resolver)
   :resolve-measure (pss-fress/registry-measure-resolver)
   :default-bf      512
   :element-read-handlers {…}})
;; …with the peer registering its store/comparator/measure beforehand:
;;   (pss-fress/register-storage! store-id my-storage)   ; per-connect (live)
;;   (pss-fress/register-comparator! cmp-id my-cmp)      ; static, at ns load
```

The lower-level pieces (`write-handlers`, `(read-handlers {:measure-ops :default-bf})`,
`root-write-handlers`, `(root-read-handler {…})`, `node->map`) are public too if you need to
compose them directly. A `.transit` sibling could mirror the same tags/shape for transit
consumers.

## Consumer characteristics (what made the design general)

| | datahike | yggdrasil | proximum | stratum |
|---|---|---|---|---|
| element/key types | `Datom` | kw/str/vec/tuple/record | maps; Long/Str/UUID | `ChunkEntry` record |
| comparator (root) | index-type→cmp | `compare` | 3 custom, dual-mode | lex vector-of-longs |
| branching | 512 | const | 512 | **64** |
| Settings extras | — | — | — | **IMeasure** |
| diff-buf `:slots` | **opt-in** | — | — | — |
| wire | kabel | konserve-sync | local | local |
| platform | JVM+cljs | JVM+cljs | JVM | JVM |

All variation is **root / Settings / element-level**, none touches the node codec:
comparators and storage inject at the root via resolvers; `ChunkEntry`/`Datom`/… serialize by
recursion through the consumer's element handlers; branching/measure-ops inject via the
per-node `Settings` at restore. So one node codec serves all four. Stratum (measure-ops +
branching 64) is the strictness oracle; yggdrasil is the cross-platform oracle; datahike is
the kabel-wire oracle.
