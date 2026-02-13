package me.tonsky.persistent_sorted_set;

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
                // Preserve the invertible stats (count, sum, sumSq) from result,
                // but take min/max from recomputed
                result = new NumericStats(
                    result.count,
                    result.sum,
                    result.sumSq,
                    recomputed.min,
                    recomputed.max
                );
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
