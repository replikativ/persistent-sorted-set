package org.replikativ.persistent_sorted_set;

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
     * Generate and return new address for node.
     *
     * MUST return a non-null address. An earlier version of this doc said "return
     * null if doesn't need to be stored", which is not implementable: the null is
     * propagated into the parent's serialized addresses(), and a later cold restore
     * calls restore(null). If a store wants to skip work for a node it already holds,
     * it should return that node's existing address.
     */
    Address store(ANode<Key, Address> node);

    /**
     * Mark an address as SUPERSEDED BY THE VERSION BEING PRODUCED — including the
     * root address when it is updated. Storage implementations can track these for
     * later deletion or compaction.
     *
     * This is a HINT, not a reachability claim. An earlier version of this doc said
     * "node no longer reachable", which is not what the call site knows: only STORED
     * nodes have addresses, a stored node belongs to some published version, and
     * deriving a new version never makes the old one's nodes unreachable. A consumer
     * must establish for itself that no live version needs an address before acting
     * on it.
     *
     * It is also NOT at-most-once. Two versions derived from one stored parent both
     * legitimately supersede its nodes, so both report them — ordinary structural
     * sharing, not a race. Treat the stream as a MULTISET: measured on plain
     * `(conj base x)` / `(conj base y)`, 6 calls for 3 addresses, every one doubled.
     * A consumer that reuses addresses must de-duplicate, or it will hand one address
     * to two different nodes.
     *
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
