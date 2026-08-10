# Advisory: a datom can be present but unreachable by a fully-specified lookup

**Affects** persistent-sorted-set **0.3.114 – 0.4.139**, and therefore datahike
**0.7.1615 – 0.8.1775**.
**Fixed in** persistent-sorted-set *(pending release)*.
**Data is recoverable.** Nothing was lost; re-importing repairs an affected database completely.

---

## In one paragraph

Under a cardinality-one upsert, a B-tree separator could be left naming the element that was
just superseded. A search that descends the tree with a **fully specified** key then routes
past the node that actually holds the datom, and reports nothing. The datom is still there —
in the right leaf, in the right order — so `seq`, `count`, sortedness, `d/q` and `d/pull` are
all unaffected. Only the two lookups that specify every component miss it.

## What is and is not affected

Verified on datahike 0.8.1775 with persistent-sorted-set 0.4.137, against a datom confirmed
present by a full index scan:

| access path | result |
|---|---|
| `d/datoms db :eavt e a v` | **misses it** |
| `d/datoms db :avet a v` | **misses it** |
| `d/datoms db :eavt e a` (prefix) | finds it |
| `d/datoms db :eavt e` (prefix) | finds it |
| `d/pull db '[*] e` | finds it |
| `d/q` — entity and attribute bound, value free | finds it |
| `d/q` — entity, attribute and value all bound | finds it |
| `d/q` — attribute and value bound | finds it |

The rule behind the table: a lookup that specifies **every** component of an index descends
on a complete key and can be misrouted. Anything that descends on a **prefix** and then
iterates — which is what `q`, `pull` and partial `d/datoms` do — walks the leaves in order and
sees everything.

**Your application logic was almost certainly never wrong.** If you read through `q` or
`pull`, this defect was invisible to you. It bites code that calls `d/datoms` with a complete
pattern — most often `d/datoms db :avet attr value` to find an entity by attribute value.

Datahike's own machinery is unaffected: `export-db` scans `:eavt` unbounded, and garbage
collection marks by walking the tree. Both are scans, not point lookups.

## Am I affected?

Two conditions must both hold:

1. **Cardinality-one upserts.** They are the only operation that reaches the affected code
   path. A write-once database is not affected.
2. **An index at least three levels deep** — above roughly 100 000 datoms at the default
   branching factor of 512.

Below that size the defect is **structurally impossible**, not merely unlikely: with two
levels the leaf's parent is the root, its separator is written unconditionally, and there is
no higher node left to inform.

**How many datoms are affected depends on your write pattern**, and we deliberately do not
quote a rate. A stale separator is often repaired by a later write touching the same node, so
workloads differ enormously — in our own tests, sequentially ordered upserts left exactly one
affected datom in a 200 000-entity database, while the same workload with shuffled upsert
order left none. Rather than guess, measure your own database with the check below.

## Check your database

```clojure
(require '[datahike.api :as d])

(defn unreachable-datoms
  "Datoms the index yields on a full scan but cannot find again by a fully specified
   lookup. Empty result => this database is not affected.

   O(n) lookups over the whole database: run it on a maintenance window or a copy, not
   in a hot path."
  [db]
  (into []
        (remove (fn [dt] (seq (d/datoms db :eavt (:e dt) (:a dt) (:v dt)))))
        (d/datoms db :eavt)))

;; usage
(let [bad (unreachable-datoms (d/db conn))]
  (println (count bad) "unreachable datom(s)")
  (doseq [dt (take 10 bad)] (println "  " (pr-str dt))))
```

An empty result means this database is not affected and you need do nothing beyond
upgrading. A non-empty result lists exactly which datoms are affected.

## Repair

Upgrading alone does **not** repair an existing database: the stale separators are part of
the stored index, and a fixed reader reading an old image still cannot find those datoms.

Because the datoms are physically present and correctly ordered, an **export followed by a
fresh import** recovers everything — the import rebuilds every separator from scratch:

```clojure
(d/export-db conn "/path/to/dump")        ; a full :eavt scan; includes affected datoms
;; then, on the fixed version, into a NEW database:
(d/import-db new-conn "/path/to/dump")
```

The export path is a full scan, so it is complete even on an affected database. You may
export from the affected version — you do not need to upgrade first to get a good dump.

## What caused it, briefly

When a `replace` changes an element that is a node's maximum, the parent's separator for that
node must be updated, and the parent must in turn tell *its* parent that its own maximum
moved. That second step consulted the **operation** comparator rather than the **set's**
comparator. Under a cardinality-one upsert the operation comparator compares only entity and
attribute — which are unchanged — so it reported "nothing moved" and propagation stopped one
level too early.

The defect was introduced together with the `replace` operation itself, so every release that
has `replace` has it.

## Prevention

The class of mistake — *the caller's search key is not the element the tree holds, once the
operation comparator is coarser than the set's* — has appeared more than once, in different
places, and each time it was fixed as an individual case.

The fix here ships with a regression test that fails against the unfixed code. The more
durable answer is to state the class as a property rather than remember it as a list of
sites: **every element the index yields must be findable by the index's own comparator.**
That is precisely what the check above computes, and running it over a generated
upsert-heavy workload deep enough to have three levels turns this whole class into something
a test suite can catch on its own. That property test is the intended follow-up; it is not
in place yet, and this note will be updated when it is.
