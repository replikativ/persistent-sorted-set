package me.tonsky.persistent_sorted_set;

/**
 * Interface for nodes that maintain subtree element counts.
 *
 * Enables O(1) count() and O(log n) countSlice() operations.
 *
 * For Leaf nodes: subtreeCount() == _len
 * For Branch nodes: subtreeCount() == sum of all children's subtreeCounts
 */
public interface ISubtreeCount {
    /**
     * Returns the total number of elements in this subtree.
     * O(1) when counts are maintained.
     */
    long subtreeCount();
}
