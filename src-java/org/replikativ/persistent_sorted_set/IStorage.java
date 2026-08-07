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
     * Persist the node's FULL content projection and return a new address for it.
     *
     * The reliable way to get that projection is
     * `org.replikativ.persistent-sorted-set.impl.nodes/node->blob` (with
     * `blob->leaf` / `blob->branch` on the read side); the shipped CBOR, transit and
     * fressian handlers all go through it. Hand-rolling the field list is where
     * storages lose data.
     *
     * An earlier version of this doc listed only:
     *
     *     Leaf   -> node.keys()
     *     Branch -> node.level(), node.keys(), node.addresses()
     *
     * That list is INCOMPLETE and silently loses committed elements. It omits
     * `Branch.slotsForStorage()` — the diff-buf buffered diffs. A buffered child is
     * stored as its durable anchor address PLUS the parent's slot; persist only the
     * address and every element buffered against that child is gone on the next
     * restore. Measured with a storage written strictly to the old list: 40 elements,
     * bf 8, diff-buf 256, three elements added and committed — in memory count 43,
     * after restore count 40, elements 100 and 101 silently absent. It also omits
     * `Branch.subtreeCount()` and the node's `_measure`; dropping those costs a full
     * recount / recompute rather than data, but both are part of the content
     * projection and therefore of the node's content ADDRESS.
     *
     * Note the asymmetry this closes: `Branch.installSlots` already throws a named
     * error for the REVERSE mistake (a blob carrying slots restored at diffBufSize 0).
     * There was no corresponding guard for a storage that simply never wrote them,
     * because nothing at write time can tell the difference between "this storage
     * dropped the slots" and "this node had none".
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
     * UNDER A CONTENT-ADDRESSED STORE, AN ADDRESS REPORTED HERE MAY BE RE-ISSUED AS LIVE
     * — INCLUDING BY THE SAME COMMIT. This is not address reuse by an allocator; it is
     * content addressing working as intended. When a node's content returns to a value it
     * previously had, its address is BY CONSTRUCTION the address it had then.
     * `(-> s (conj x) (disj x))` is enough. Measured, 40 elements at bf 8 with the address
     * computed as a hash of the node's content: 2 of 2 freed addresses came back live in the
     * same commit, one of them the ROOT of the version being published.
     *
     * So `freed ∩ stored-this-commit` can be non-empty, and a consumer that deletes the
     * freed set after a commit lands will delete live nodes. Establish liveness yourself —
     * reachability from the roots you intend to keep — and treat this stream only as a
     * candidate list.
     *
     * A store that allocates a FRESH address per write (a sequential or random UUID) is not
     * affected: a re-created node gets a new address and a freed one is dead forever. Both
     * regimes exist in practice — datahike's `gen-address` is a content hash under
     * `:crypto-hash?` and a `squuid` otherwise.
     *
     * Note the two in-tree GC tests (test/gc_leak.cljs, test/stress_diff_buf.clj) assert that
     * no reachable node is ever reported here. That holds for THEIR storages, which allocate
     * fresh addresses — it is not a general invariant, and those tests say so.
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
     * Concretely, under a content-addressed regime the SAME commit that reports an address
     * here can go on to publish it as live: a free is reported at MUTATION time (the old
     * root address is handed over inside `cons`/`disjoin`, before `store` has decided any
     * new addresses), and a commit that ends holding the content it started with hashes
     * back to the address it just superseded. Measured in datahike under `:crypto-hash?`,
     * one entity transacted and then retracted at bf 8: 6 of 39 reachable addresses were on
     * the freed list. The cheap enforcement is at the storage: publishing an address makes
     * it live, so a write should cancel any pending free of that address.
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
