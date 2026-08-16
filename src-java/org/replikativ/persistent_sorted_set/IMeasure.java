package org.replikativ.persistent_sorted_set;

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
     *
     * IT MUST ALSO BE COMMUTATIVE — merge(a, b) == merge(b, a) — if you want an insert into
     * the middle of the set to be measured exactly. Both `Leaf.add` and `Branch.add` maintain
     * a node's cached measure incrementally as merge(cached, extract(insertedKey)), which folds
     * the new element in at the END rather than at its sorted position. Every measure the
     * library ships (count, sum, sumSq, min, max) is commutative, and so is every aggregate
     * this is useful for; a genuinely order-sensitive measure (a first/last, a concatenation)
     * will read correctly only after a recomputation from children.
     *
     * This was always the case — the incremental arms of `Leaf.add` have folded at the end
     * since the measure landed. It is written down here rather than left implicit in one
     * implementation because `Branch.add` now relies on it as well, on the path where the
     * children are not resident.
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
        throw new UnsupportedOperationException(
            "This IMeasure does not implement weight(), which getNth requires.\n"
          + "\n"
          + "weight must be a MONOID HOMOMORPHISM from your measure monoid to (long, +):\n"
          + "    weight(identity())     == 0\n"
          + "    weight(merge(a, b))    == weight(a) + weight(b)\n"
          + "\n"
          + "It answers 'how many logical items does this subtree hold', which is what lets\n"
          + "getNth descend by accumulated position. For a set whose elements are RUNS or\n"
          + "CHUNKS, that is the item count inside them, and getNth returns the containing\n"
          + "element plus an offset within it.\n"
          + "\n"
          + "If you only want the nth ELEMENT by position, do not write a measure at all:\n"
          + "getNth works without one, navigating by the subtree counts the tree already\n"
          + "maintains.\n"
          + "\n"
          + "This default used to return 1, which satisfies NEITHER law — weight(identity)\n"
          + "must be 0, and a merged measure must sum rather than stay 1. The result was that\n"
          + "getNth believed every tree weighed one and returned null for every index above 0.");
    }
}
