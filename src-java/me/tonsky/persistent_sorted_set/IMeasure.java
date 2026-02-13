package me.tonsky.persistent_sorted_set;

import java.util.function.Supplier;

/**
 * Interface for computing and maintaining measures over tree nodes.
 * Measures form a monoid with identity and associative merge operation.
 *
 * @param <Key> the type of keys in the set
 * @param <M> the type of measure object
 */
public interface IMeasure<Key, M> {

    /**
     * Returns the identity (empty) measure.
     * This is the monoid identity element.
     */
    M identity();

    /**
     * Extract measure from a single key.
     * For a leaf with one element, this gives the measure for that element.
     */
    M extract(Key key);

    /**
     * Merge two measure objects.
     * This operation must be associative: merge(a, merge(b, c)) == merge(merge(a, b), c)
     */
    M merge(M m1, M m2);

    /**
     * Remove a key's contribution from a measure.
     *
     * For invertible measures (count, sum, sum-squared), this can be computed directly.
     * For non-invertible measures (min, max), this may need to recompute from children.
     *
     * @param current the current measure
     * @param key the key being removed
     * @param recompute a supplier that recomputes measure from children (called only if needed)
     * @return the updated measure
     */
    M remove(M current, Key key, Supplier<M> recompute);

    /**
     * Extract the element weight (count) from a measure object.
     * Used by rank-based navigation (getNth) to traverse the tree
     * by accumulated element counts rather than by key comparison.
     *
     * Must satisfy: weight(merge(a, b)) == weight(a) + weight(b)
     *
     * @param measure the measure object
     * @return the number of data elements represented by this measure
     */
    default long weight(M measure) {
        return 1; // default: each entry has weight 1 (= subtreeCount behavior)
    }
}
