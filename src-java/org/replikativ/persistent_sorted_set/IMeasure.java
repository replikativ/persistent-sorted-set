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
     * It is NOT required to be commutative. If yours is not, say so by overriding
     * {@link #commutativeMerge()} — see there for what changes.
     */
    M merge(M m1, M m2);

    /**
     * May the tree fold a newly inserted key in at the END of a node's cached measure, rather
     * than at that key's sorted position? True iff {@link #merge} is COMMUTATIVE.
     *
     * The incremental arms of `Leaf.add` and `Branch.add` maintain a node's measure as
     * merge(cached, extract(insertedKey)). For an insert into the MIDDLE of a node that folds
     * the new element out of order, which is exact for a commutative merge and wrong for an
     * order-sensitive one (a concatenation, a first/last, an order-dependent hash). Returning
     * false makes both recompute from content instead — a leaf from its own keys, a branch
     * from its children, postponing (caching null) when a child is not resident.
     *
     * The default is true because that is what the tree has always assumed. It was assumed
     * SILENTLY, and unevenly: `Leaf.add`'s delta fires only on the transient in-place path,
     * so an order-sensitive measure used through the ordinary persistent API was correct on
     * the JVM, while ClojureScript — whose `branch.cljs` has taken the delta unconditionally
     * since measures landed — was not. `Branch.add` now takes it on the persistent path too,
     * when the children are not resident, which is what turned an implicit assumption into
     * one worth being able to decline.
     *
     * Cost of returning false: the measure of a cold-tree write is postponed rather than
     * maintained, so the reader that needs it pays a `forceComputeMeasure` descent — the
     * behaviour every measure had before this was introduced.
     *
     * Every measure the library ships (count, sum, sumSq, min, max) is commutative and leaves
     * this alone. NOTE that commutativity does not buy EXACTNESS for a floating-point measure:
     * `+` on doubles is commutative but not associative, so a delta and a recomputation can
     * differ in the last bits and the cached value is the one that reaches disk. That is the
     * same inexactness `validate-full`'s `{:check-measures? true}` already refuses to police.
     */
    default boolean commutativeMerge() {
        return true;
    }

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
