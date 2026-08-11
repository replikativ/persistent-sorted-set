package org.replikativ.persistent_sorted_set;

import java.util.function.Supplier;

/**
 * IMeasure implementation for numeric data.
 *
 * Handles both Number types (with sum/sumSq) and general Comparable types (min/max only).
 * Use this for Sagitta columnar indices where columns are homogeneous numeric types.
 *
 * @param <Key> the key type, should be Comparable (and ideally Number for full stats)
 */
@SuppressWarnings("unchecked")
public class NumericStatsOps<Key> implements IMeasure<Key, NumericStats> {

    private static final NumericStatsOps<?> INSTANCE = new NumericStatsOps<>();

    /**
     * Get a singleton instance.
     */
    public static <K> NumericStatsOps<K> instance() {
        return (NumericStatsOps<K>) INSTANCE;
    }

    @Override
    public NumericStats identity() {
        return NumericStats.IDENTITY;
    }

    @Override
    public NumericStats extract(Key key) {
        if (key == null) {
            return NumericStats.IDENTITY;
        }
        if (key instanceof Number) {
            return NumericStats.of((Number) key);
        }
        if (key instanceof Comparable) {
            return NumericStats.ofComparable((Comparable) key);
        }
        // Non-comparable keys: just count
        return new NumericStats(1, 0.0, 0.0, null, null);
    }

    @Override
    public NumericStats merge(NumericStats s1, NumericStats s2) {
        if (s1 == null) return s2;
        if (s2 == null) return s1;
        return s1.merge(s2);
    }

    @Override
    public NumericStats remove(NumericStats current, Key key, Supplier<NumericStats> recompute) {
        if (current == null || key == null) {
            return current;
        }

        NumericStats result;
        if (key instanceof Number) {
            result = current.removeNumeric((Number) key);
        } else if (key instanceof Comparable) {
            result = current.removeComparable((Comparable) key);
        } else {
            // Non-comparable: just decrement count
            result = new NumericStats(
                current.count - 1,
                current.sum,
                current.sumSq,
                current.min,
                current.max
            );
        }

        // If min or max was affected (set to null), recompute from children
        if (result.needsRecompute() && recompute != null) {
            NumericStats recomputed = recompute.get();
            if (recomputed != null) {
                // Take the recomputed stats WHOLESALE. Keeping the subtracted sum/sumSq
                // here — which is what this did — threw away exact values that had just
                // been paid for, and `sum`/`sumSq` are only invertible in exact
                // arithmetic. In doubles the subtraction loses catastrophically:
                // a leaf holding [1.0, 2.0, 1.0E16] with 1.0E16 removed gave
                //     sum 4.0 (truth 3.0), sumSq 0.0 (truth 5.0)
                // and therefore variance -4.0 — impossible for a variance — and a
                // stdDev of NaN. `node->map` serializes `:measure`, so that lands on
                // disk, is read back as authoritative, and changes the node's content
                // address. Reachable from the public API: build and remove inside ONE
                // transient, so the leaf is editable when the remove arrives, and the
                // in-place path at Leaf.remove takes the incremental branch.
                //
                // The recompute is only requested when min or max was invalidated, so
                // this costs nothing on the common path; when it IS requested, the
                // accurate answer is already in hand.
                result = recomputed;
            }
        }

        return result;
    }

    @Override
    public long weight(NumericStats stats) {
        if (stats == null) return 0;
        return stats.count;
    }
}
