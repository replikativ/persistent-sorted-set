package me.tonsky.persistent_sorted_set;

public interface IStorage<Key, Address> {
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
     * Delete addresses that are no longer needed.
     * Called to clean up obsolete nodes after modifications.
     *
     * Default implementation is no-op. Override in storage implementations
     * that want to reclaim storage space during batch operations.
     *
     * @param addresses Collection of addresses to delete
     */
    default void delete(java.util.Collection<Address> addresses) {
    }

    /**
     * Mark an address as freed (obsolete) during tree modifications.
     * The storage may collect these for later batch deletion.
     *
     * Default implementation is no-op.
     *
     * @param address Address that is no longer needed
     */
    default void markFreed(Address address) {
    }

    /**
     * Delete all addresses that have been marked as freed via markFreed().
     * Call this at the end of a batch operation to clean up obsolete nodes.
     *
     * Default implementation is no-op.
     */
    default void deleteFreed() {
    }
}
