package org.replikativ.persistent_sorted_set;

import java.util.*;

/**
 * Optional hook for processing leaf contents during add/remove operations.
 *
 * The processor is called AFTER the entry is added/removed but BEFORE
 * the tree's split/merge/borrow decisions. This means the processor
 * transforms entries, and normal B-tree rebalancing handles the result.
 *
 * Use cases: compacting small adjacent entries, splitting oversized entries
 * (e.g., chunk management in a columnar index).
 *
 * Only runs on persistent (non-transient) code paths.
 * Zero overhead if no processor is configured (null in Settings).
 *
 * @param <Key> The type of keys stored in the set
 */
@SuppressWarnings("rawtypes")
public interface ILeafProcessor<Key> {

  /**
   * Process leaf entries after an add or remove operation.
   *
   * The processor can:
   * 1. Examine entries for compaction opportunities (e.g., multiple small objects)
   * 2. Split oversized entries (e.g., chunks exceeding a size limit)
   * 3. Merge adjacent entries based on custom criteria
   *
   * The returned list can be:
   * - Same as input (no changes)
   * - Fewer entries (merged)
   * - More entries (split)
   *
   * The PSS tree will handle rebalancing if the returned list causes the leaf
   * to exceed branchingFactor (splits) or fall below minBranchingFactor (merges).
   *
   * IMPORTANT: The returned entries MUST maintain sort order according to the
   * comparator used by the tree. Violating this will corrupt the tree.
   *
   * @param entries The leaf entries after add/remove, in sorted order
   * @param storage The storage backend (may be null for in-memory sets)
   * @param settings The settings for this tree (contains comparator, branching factor, etc.)
   * @return List of processed entries, possibly compacted/split, in sorted order
   */
  List<Key> processLeaf(List<Key> entries, IStorage storage, Settings settings);

  /**
   * Optional: check if processing is needed before allocating the list.
   *
   * Default implementation returns false (no processing). Override to return
   * true when you want to process the leaf (e.g., if leaf size suggests
   * compaction opportunities). This avoids the allocation of the entries list
   * when no processing is needed.
   *
   * @param leafSize Number of entries in the leaf
   * @param settings The settings for this tree
   * @return true if processLeaf should be called, false to skip
   */
  default boolean shouldProcess(int leafSize, Settings settings) {
    return false;
  }
}
