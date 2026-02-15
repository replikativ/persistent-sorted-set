package org.replikativ.persistent_sorted_set;

import java.util.Objects;

/**
 * Statistics for numeric data in a subtree.
 * Tracks count, sum, sum of squares (for variance), min, and max.
 *
 * All fields are immutable. Create new instances for updates.
 */
public final class NumericStats {

    public static final NumericStats IDENTITY = new NumericStats(0, 0.0, 0.0, null, null);

    public final long count;
    public final double sum;
    public final double sumSq;
    public final Comparable min;
    public final Comparable max;

    public NumericStats(long count, double sum, double sumSq, Comparable min, Comparable max) {
        this.count = count;
        this.sum = sum;
        this.sumSq = sumSq;
        this.min = min;
        this.max = max;
    }

    /**
     * Create stats for a single numeric value.
     */
    public static NumericStats of(Number value) {
        double d = value.doubleValue();
        return new NumericStats(1, d, d * d, (Comparable) value, (Comparable) value);
    }

    /**
     * Create stats for a single comparable (non-numeric) value.
     * Only tracks count, min, max (sum and sumSq are 0).
     */
    public static NumericStats ofComparable(Comparable value) {
        return new NumericStats(1, 0.0, 0.0, value, value);
    }

    /**
     * Merge two stats together.
     */
    @SuppressWarnings("unchecked")
    public NumericStats merge(NumericStats other) {
        if (other == null || other == IDENTITY) return this;
        if (this == IDENTITY) return other;

        Comparable newMin;
        if (this.min == null) newMin = other.min;
        else if (other.min == null) newMin = this.min;
        else newMin = this.min.compareTo(other.min) <= 0 ? this.min : other.min;

        Comparable newMax;
        if (this.max == null) newMax = other.max;
        else if (other.max == null) newMax = this.max;
        else newMax = this.max.compareTo(other.max) >= 0 ? this.max : other.max;

        return new NumericStats(
            this.count + other.count,
            this.sum + other.sum,
            this.sumSq + other.sumSq,
            newMin,
            newMax
        );
    }

    /**
     * Check if removing a key affects min or max.
     */
    @SuppressWarnings("unchecked")
    public boolean affectsMinMax(Comparable key) {
        if (key == null) return false;
        return (min != null && key.compareTo(min) == 0) ||
               (max != null && key.compareTo(max) == 0);
    }

    /**
     * Remove a numeric key's contribution (for invertible stats only).
     * Min/max are set to null if they might be affected (requires recomputation).
     */
    @SuppressWarnings("unchecked")
    public NumericStats removeNumeric(Number key) {
        double d = key.doubleValue();
        Comparable ckey = (Comparable) key;

        Comparable newMin = (min != null && ckey.compareTo(min) == 0) ? null : min;
        Comparable newMax = (max != null && ckey.compareTo(max) == 0) ? null : max;

        return new NumericStats(
            count - 1,
            sum - d,
            sumSq - d * d,
            newMin,
            newMax
        );
    }

    /**
     * Remove a comparable (non-numeric) key's contribution.
     */
    @SuppressWarnings("unchecked")
    public NumericStats removeComparable(Comparable key) {
        Comparable newMin = (min != null && key.compareTo(min) == 0) ? null : min;
        Comparable newMax = (max != null && key.compareTo(max) == 0) ? null : max;

        return new NumericStats(
            count - 1,
            sum,
            sumSq,
            newMin,
            newMax
        );
    }

    /**
     * Check if min or max needs recomputation (is null after removal).
     */
    public boolean needsRecompute() {
        return (count > 0) && (min == null || max == null);
    }

    /**
     * Compute variance from the stats.
     * variance = E[X^2] - E[X]^2 = sumSq/n - (sum/n)^2
     */
    public double variance() {
        if (count == 0) return 0.0;
        double mean = sum / count;
        return (sumSq / count) - (mean * mean);
    }

    /**
     * Compute standard deviation.
     */
    public double stdDev() {
        return Math.sqrt(variance());
    }

    /**
     * Compute mean.
     */
    public double mean() {
        return count == 0 ? 0.0 : sum / count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NumericStats)) return false;
        NumericStats that = (NumericStats) o;
        return count == that.count &&
               Double.compare(that.sum, sum) == 0 &&
               Double.compare(that.sumSq, sumSq) == 0 &&
               Objects.equals(min, that.min) &&
               Objects.equals(max, that.max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, sum, sumSq, min, max);
    }

    @Override
    public String toString() {
        return "NumericStats{" +
               "count=" + count +
               ", sum=" + sum +
               ", sumSq=" + sumSq +
               ", min=" + min +
               ", max=" + max +
               '}';
    }
}
