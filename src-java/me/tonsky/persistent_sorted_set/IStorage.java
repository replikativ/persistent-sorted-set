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
     * Mark an address as freed/obsolete during tree modifications.
     * Called when a stored node's address is being replaced (node no longer reachable).
     * Storage implementations can track these for later deletion.
     * Only called in editable/transient mode where modifications happen in-place.
     */
    default void markFreed(Address address) {
    }
}
