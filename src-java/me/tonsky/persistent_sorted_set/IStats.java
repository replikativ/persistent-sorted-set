package me.tonsky.persistent_sorted_set;

import java.util.function.Supplier;

/**
 * Interface for computing and maintaining statistics over tree nodes.
 * Statistics form a monoid with identity and associative merge operation.
 *
 * @param <Key> the type of keys in the set
 * @param <S> the type of statistics object
 */
public interface IStats<Key, S> {

    /**
     * Returns the identity (empty) statistics.
     * This is the monoid identity element.
     */
    S identity();

    /**
     * Extract statistics from a single key.
     * For a leaf with one element, this gives the stats for that element.
     */
    S extract(Key key);

    /**
     * Merge two statistics objects.
     * This operation must be associative: merge(a, merge(b, c)) == merge(merge(a, b), c)
     */
    S merge(S s1, S s2);

    /**
     * Remove a key's contribution from statistics.
     *
     * For invertible statistics (count, sum, sum-squared), this can be computed directly.
     * For non-invertible statistics (min, max), this may need to recompute from children.
     *
     * @param current the current statistics
     * @param key the key being removed
     * @param recompute a supplier that recomputes stats from children (called only if needed)
     * @return the updated statistics
     */
    S remove(S current, Key key, Supplier<S> recompute);
}
