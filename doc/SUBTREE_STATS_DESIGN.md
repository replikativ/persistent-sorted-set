# PSS Subtree Statistics Design

## Motivation

Currently, `PersistentSortedSet.count()` and range counting operations traverse the entire tree structure, making them O(n). For:

1. **Datahike query planning**: Need fast cardinality estimation for index scan ranges
2. **Sagitta columnar aggregations**: Need O(log n) range aggregations instead of O(chunks)

## Analysis: Datalevin vs Datahike

### Datalevin (Cost-Based Planning)
- Has `av-size`, `a-size`, `e-size`, `cardinality` for cardinality estimation
- Uses `size-filter` with predicates and time budgets for filtered counts
- Bayesian blending of sample statistics with priors for link estimation
- LMDB provides O(log n) key-range counting natively

### Datahike (Rule-Based Planning)
- Selects index (EAVT/AEVT/AVET) based on bound variables, not cardinality
- No cardinality estimation used for query planning
- Filtering happens **after** index lookup (no predicate pushdown)
- PSS `count()` traverses entire tree - O(n)

### The Heterogeneous Index Challenge

Datahike's EAVT/AEVT/AVET indices are **heterogeneous** - they store datoms with mixed value types:

```clojure
;; Same entity, different value types:
[123 :age 42]           ; number
[123 :name "Alice"]     ; string
[123 :friend #ref[456]] ; reference
[123 :id #uuid "..."]   ; UUID
```

This means:
- **Subtree counts**: ✅ Universal - work for all types
- **Subtree stats (sum/min/max)**: ❌ Only meaningful for homogeneous numeric columns

### Filtered vs Unfiltered Counts

For a query like `[?e :age ?a] [(> ?a 30)]`:

| Approach | Accuracy | Performance | When to Use |
|----------|----------|-------------|-------------|
| Unfiltered count | Upper bound | O(log n) | Query planning (acceptable error) |
| Range count (AVET) | Exact for range | O(log n) | If predicate maps to range slice |
| Scan with predicate | Exact | O(n) | Arbitrary predicates |

**Recommendation**: Use unfiltered counts as upper-bound estimates for query planning.
Even overapproximated cardinalities enable cost-based optimization.

## Proposal

### Phase 1: Subtree Counts

Add `_subtreeCount` to Branch nodes to enable O(1) total count and O(log n) range counting.

#### Changes to Branch.java

```java
public class Branch<Key, Address> extends ANode<Key, Address> {
    // Existing fields...

    // NEW: Subtree count - total elements in this branch's subtree
    public long _subtreeCount;

    // NEW: Constructor updates
    public Branch(int level, int len, Key[] keys, Address[] addresses,
                  Object[] children, long subtreeCount, Settings settings) {
        // ... existing init ...
        _subtreeCount = subtreeCount;
    }

    // NEW: O(1) count
    @Override
    public int count(IStorage storage) {
        return (int) _subtreeCount;
    }
}
```

#### Count Propagation

When adding/removing elements, counts bubble up:

```java
// In add() - when child count changes
ANode[] nodes = child(storage, ins).add(...);
if (nodes != UNCHANGED && nodes != EARLY_EXIT) {
    // Child changed, need to recompute subtree count
    // For single child replacement (nodes.length == 1):
    //   newCount = _subtreeCount - oldChildCount + newChild.subtreeCount
    // For split (nodes.length == 2):
    //   newCount = _subtreeCount - oldChildCount + nodes[0].count + nodes[1].count
}
```

#### Range Counting

With subtree counts, `count(slice(from, to))` becomes O(log n):

```java
public long countRange(IStorage storage, Key from, Key to, Comparator<Key> cmp) {
    // Binary search to find boundaries
    // Sum counts of fully-contained subtrees
    // Recursively count partial subtrees at boundaries
}
```

### Phase 2: Subtree Statistics (Context-Dependent)

Statistics beyond counts are only meaningful for **homogeneous** data. This creates different strategies for different use cases:

#### Use Case A: Sagitta (Homogeneous Numeric Columns)

For sagitta's columnar indices, all values in a column are the same type (e.g., `:float64`).
Subtree stats in PSS branches make perfect sense:

```java
public class NumericStats {
    public final long count;
    public final double sum;
    public final double sumSq;  // For variance: Var = E[X²] - E[X]²
    public final double min;
    public final double max;

    public NumericStats merge(NumericStats other) {
        return new NumericStats(
            count + other.count,
            sum + other.sum,
            sumSq + other.sumSq,
            Math.min(min, other.min),
            Math.max(max, other.max)
        );
    }
}
```

This enables O(log n) range aggregations:
```clojure
(idx-sum-range column 100.0 500.0)  ;; O(log n) instead of O(chunks)
```

#### Use Case B: Datahike (Heterogeneous EAVT Indices)

For Datahike's indices, numeric stats per-branch don't work because:
1. Branch contains datoms for multiple attributes
2. Each attribute has different value types
3. Aggregating `:age` (number) with `:name` (string) is meaningless

**Alternative: Per-Attribute Statistics Stored Separately**

```clojure
;; Maintained alongside the index, not inside the PSS
(def attribute-stats
  {:age    {:count 10000 :sum 420000 :min 0 :max 120 :cardinality 100}
   :salary {:count 5000 :sum 250000000 :min 30000 :max 500000}
   :name   {:count 10000 :cardinality 8500}  ;; non-numeric: count only
   :friend {:count 25000}})  ;; refs: count only
```

This is similar to Datalevin's approach with `a-size`, `av-size`, `cardinality`.

**When to Update:** During transaction processing, update affected attribute stats.

#### Configuration for PSS

```java
public class Settings {
    // ... existing ...

    // Phase 1: Always enabled when trackSubtreeCounts = true
    public boolean _trackSubtreeCounts = false;

    // Phase 2: Optional, for homogeneous numeric PSS only
    public IFn _statsExtractor;  // (fn [key] -> stats-map) or null
    public IFn _statsMerger;     // (fn [stats1 stats2] -> merged) or null
}
```

### Implementation Strategy

1. **Backward compatible**: Default behavior unchanged
2. **Opt-in via Settings**: Enable subtree counts/stats through Settings
3. **Incremental**: Start with counts (Phase 1), add stats later (Phase 2)
4. **Storage format**: Include counts/stats in serialized Branch format

### Performance Impact

| Operation | Current | With Counts | With Stats |
|-----------|---------|-------------|------------|
| count() | O(n) | O(1) | O(1) |
| count(slice) | O(n) | O(log n) | O(log n) |
| sum(slice) | N/A | N/A | O(log n) |
| add/remove | O(log n) | O(log n) | O(log n) |
| Memory | baseline | +8 bytes/branch | +40 bytes/branch |

### Use Cases

#### Datahike Query Planning (Cost-Based Optimization)

Currently Datahike uses rule-based planning. With subtree counts, we can add cost-based optimization:

```clojure
;; Fast cardinality estimation for index scans
(defn estimate-scan-cardinality [db index-type from to]
  ;; O(log n) instead of O(n) traversal
  (pss/count-range (get-index db index-type) from to))

;; Example: Query [?e :likes ?food] [?e :age ?a] [(> ?a 30)]
;;
;; Option A: Start with :likes
;;   count(:likes) = 50000  (from AEVT subtree count)
;;
;; Option B: Start with :age
;;   count(:age) = 10000    (from AEVT subtree count)
;;
;; Even without filtered count, Option B is clearly better.
;; Query planner chooses :age first, filters, then joins with :likes.

(defn query-cost-estimate [db clause]
  (let [[e a v] (pattern-elements clause)
        ;; Use subtree counts for cardinality estimation
        card (cond
               (and e a v) 1  ;; Fully bound
               (and e a) (pss/count-range eavt (datom e a nil) (datom e a max-v))
               a (pss/count-range aevt (datom nil a nil) (datom max-e a nil))
               e (pss/count-range eavt (datom e nil nil) (datom e max-a nil))
               :else (pss/count eavt))]
    {:clause clause :cardinality card}))
```

**Note on Filtered Counts:** For predicates like `[(> ?a 30)]`, we use the unfiltered
`:age` count as an upper bound. This is acceptable for planning since:
1. It's still more informative than no cardinality info
2. Actual selectivity is learned during execution (like Datalevin's adaptive approach)
3. Range predicates on AVET *could* use `count-range` if we detect them

#### Sagitta Range Aggregations

For homogeneous numeric columns, O(log n) aggregations:

```clojure
;; With Phase 2 subtree stats in PSS:
(defn idx-sum-range [column lo hi]
  ;; Binary search for range boundaries
  ;; Sum stats from fully-contained subtrees: O(log n)
  ;; Scan partial overlap at boundaries: O(chunk-size)
  (pss/stats-range (:tree column) lo hi :sum))

;; Currently sagitta achieves O(chunks) via chunk-level stats.
;; With PSS subtree stats, this becomes O(log n).
```

#### Per-Attribute Statistics for Datahike

Since EAVT is heterogeneous, maintain attribute stats separately:

```clojure
(defn update-attribute-stats [db tx-data]
  ;; During transaction processing:
  (reduce (fn [stats datom]
            (let [attr (:a datom)
                  schema (get-in db [:schema attr])]
              (if (numeric-type? (:db/valueType schema))
                (update-in stats [attr] merge-numeric-stats datom)
                (update-in stats [attr :count] inc))))
          (:attribute-stats db)
          tx-data))

;; Query planner uses:
(defn a-size [db attr]
  (get-in db [:attribute-stats attr :count] 0))

(defn av-selectivity [db attr value-pred]
  ;; For range predicates, estimate selectivity from min/max
  (let [{:keys [min max count]} (get-in db [:attribute-stats attr])]
    (estimate-selectivity value-pred min max count)))
```

## Next Steps

1. **Phase 1a**: Add `_subtreeCount` to PSS Branch, update during add/remove
2. **Phase 1b**: Add `count-range` to Clojure API
3. **Phase 1c**: Integrate with Datahike query planner for cost-based optimization
4. **Phase 2a**: Add optional `_subtreeStats` for homogeneous numeric PSS (sagitta)
5. **Phase 2b**: Per-attribute statistics infrastructure for Datahike

## Open Questions

1. **Storage format**: How to serialize subtree counts for konserve persistence?
2. **Transient mode**: Should counts be maintained during transient batch operations?
3. **Migration**: How to compute counts for existing databases?
