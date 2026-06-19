# Canonical node serialization (`org.replikativ.persistent-sorted-set.fressian`)

An **optional** namespace providing one canonical Fressian read/write handler set for
PSS B-tree **nodes** (`Leaf`/`Branch`), so every konserve/kabel-backed consumer shares a
single node wire form. This is the reference that datahike, yggdrasil, proximum and
stratum square their storage/wire layers against.

## Why one shared codec

On a single kabel websocket there is exactly **one** fressian write-handler per type, so
once two PSS-backed systems put nodes on the same socket the node codec is shared *by
construction*. A single socket is also what preserves **causal ordering** across systems
(two sockets = two message orderings = a record can arrive before the nodes it
references). So: centralize the node codec, get one wire form + one place that fixes the
JVM/cljs drift, and keep everything else per-project.

## The node ⇄ root seam (the load-bearing distinction)

A **node** carries neither a comparator nor storage nor the set's identity — the
constructors prove it: `Leaf(keys, settings)`, `Branch(level, keys, addresses, settings)`.
Both live on the **root** (`_cmp` and `_storage` on the set). Therefore:

| concern | lives on | who handles it |
|---|---|---|
| node structure (`:keys`, `:level`, `:addresses`, diff-buf `:slots`) | the node | **this ns (canonical, shared)** |
| comparator (re-stamped lazily on descent) | the root | the consumer's root/record handler |
| storage (lazy child loading) | the root (`IStorage`) | the consumer |
| element types (Datom, ChunkEntry, RegistryEntry, maps, …) | inside `:keys`/`:slots` | the consumer's **element** handlers (recursion) |
| set identity / re-attach by store-config `:id` | the root | the consumer's root/record handler |

So this ns is parameterized only by `settings` (branching-factor + measure-ops), supplied
at read time: `(read-handlers settings)`. It never inspects an element type.

## Canonical store-map (the format `IStorage` already prescribes)

```
pss/leaf   → {:keys <elements>}
pss/branch → {:level n :keys <separators> :addresses <child-addrs> :slots? <diff-buf>}
```

- `subtree-count` and `measure` are **not** serialized — they recompute lazily on restore
  (the library's own reference storage relies on the same), which keeps the wire minimal
  and byte-identical across JVM/cljs.
- cljs `keys`/`addresses` are JS arrays; normalized to/from vectors here, once.

### diff-buf (`Settings.diffBufSize > 0`, opt-in)

A `Branch` additionally carries
`:slots {idx -> {:count :measure :diff :max-key}}`, where a **leaf** child's `:diff` is the
comparator-agnostic `{:absent [..] :present [..]}` form (`Slot/leafDiffForStorage`, produced
by `slotsForStorage`) and a **branch** child's `:diff` is the nested map. All plain data;
on restore the slots reconstruct into `_slots` with `anchor = addresses[idx]` (re-derived,
not stored). At `diffBufSize = 0` `_slots` is nil ⇒ `:slots` absent ⇒ baseline-identical.
The comparator is **not** needed: a restored leaf-diff is projected into a `_projCmp`-sorted
map lazily on descent.

## Consumer requirements gathered (what made the design general)

| | datahike | yggdrasil | proximum | stratum |
|---|---|---|---|---|
| node form today | objects+handlers | plain maps | objects+handlers | objects+handlers |
| element/key types | Datom | kw/str/vec/tuple/record | maps; Long/Str/UUID | `ChunkEntry` record |
| comparator (root) | index-type→cmp | `compare` | 3 custom, dual-mode | lex vector-of-longs |
| branching | 512 | const | 512 | **64** |
| Settings extras | — | — | — | **IMeasure** + RefType/WEAK |
| diff-buf `:slots` | **opt-in** | — | — | — |
| wire | kabel | konserve-sync | local | local |
| platform | JVM+cljs | JVM+cljs | JVM | JVM |

All variation is **root/Settings/element-level**, none touches the node codec:
comparators inject at the root; `ChunkEntry`/Datom/… serialize by recursion through the
consumer's element handlers; branching/measure-ops inject via `settings` at restore. So
one node codec serves all four. Stratum (measure-ops + branching 64) is the strictness
oracle; yggdrasil is the cross-platform + plain-map-migration oracle; datahike is the
dual-tag-migration + kabel-wire oracle.

## Usage

```clojure
(require '[org.replikativ.persistent-sorted-set.fressian :as pss-fress]
         '[clojure.data.fressian :as fress])

;; write: merge the node handlers next to your element handlers
(fress/create-writer out :handlers
  (-> (merge fress/clojure-write-handlers
             pss-fress/write-handlers          ; {Leaf {...} Branch {...}}
             my-element-write-handlers)        ; e.g. Datom
      fress/associative-lookup fress/inheritance-lookup))

;; read: settings supplies branching + measure-ops; comparator stays on the root
(fress/create-reader in :handlers
  (-> (merge fress/clojure-read-handlers
             (pss-fress/read-handlers settings)
             my-element-read-handlers)
      fress/associative-lookup))
```

`data.fressian` is a **`provided`** dependency — consumers bring their own version. A
`.transit` sibling can mirror the same tags/shape for transit-based consumers.

## Squaring the storage layers (follow-up)

Each consumer registers these node handlers **+ its own element + root handlers**, in both
its store serializer and (datahike/yggdrasil) its kabel peer:

- **datahike** — its two drifted node-handler sets (in-store 7-arg `Branch`+`@storage`;
  kabel 4-arg `Branch`) collapse into this one codec; domain handlers (Datom/DB/TxReport)
  stay datahike's. On-disk migration = tag rename with dual-tag read during transition.
- **yggdrasil** — switch from plain-map nodes to this codec; `storage.cljc`'s store/restore
  simplify; the cljs to-array fix lives here now. Wire-identical to datahike.
- **proximum / stratum** — replace their private `"proximum.*"` / `"stratum.*"` node tags
  with the canonical ones (free drift fixes); local-only, validating the generality.

## Status

JVM handlers + round-trip test (baseline, heterogeneous elements, diff-buf slots): green.
cljs handlers (`fress`) + a portable test: next. Then the per-consumer adoption above.
