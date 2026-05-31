package org.replikativ.persistent_sorted_set;

import java.util.Comparator;

public interface IStorage<Key, Address> {
    /**
     * The comparator of the set whose traversal is using this storage, or null.
     *
     * DIFF_BUF_V5: projecting a buffered leaf-diff onto a durable leaf (Branch.child)
     * needs the set's comparator. A node is deserialized by address with no knowledge
     * of which set/index it belongs to, so embeddings that share one node deserializer
     * across several comparators (e.g. datahike's per-index sets sharing a fressian
     * serializer) cannot stamp it onto Settings. The storage object, by contrast, flows
     * with the traversal, so it is the natural carrier. Returns null ⇒ projection falls
     * back to Settings.comparator().
     */
    default Comparator<Key> comparator() {
        return null;
    }

    /**
     * Given address, reconstruct and (optionally) cache the node.
     * Set itself would not store any strong references to nodes and
     * might request them by address during its operation many times.
     *
     * Use ANode.restore() or Leaf(keys)/Branch(level, keys, addresses) ctors
     */
    ANode<Key, Address> restore(Address address);

    /**
     * Tell the storage layer that address is accessed.
     * Useful for e.g. implementing LRU cache in storage.
     */
    default void accessed(Address address) {
    }

    /**
     * Will be called after all children of node has been stored and have addresses.
     *
     * For node instanceof Leaf, store node.keys()
     * For node instanceof Branch, store node.level(), node.keys() and node.addresses()
     * Generate and return new address for node
     * Return null if doesn't need to be stored
     */
    Address store(ANode<Key, Address> node);

    /**
     * Mark an address as freed/obsolete during tree modifications,
     * including when the root address is updated.
     * Called when a stored node's address is being replaced (node no longer reachable).
     * Storage implementations can track these for later deletion or compaction.
     * This may be invoked in both persistent and editable/transient modes.
     */
    default void markFreed(Address address) {
    }

    /**
     * Check if an address has been marked as freed.
     * Used for testing and debugging.
     */
    default boolean isFreed(Address address) {
        return false;
    }

    /**
     * Get debug information about a freed address.
     * Used for testing and debugging.
     */
    default Object freedInfo(Address address) {
        return null;
    }
}
