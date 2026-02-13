package org.replikativ.persistent_sorted_set;

import java.util.*;

/**
 * Optional hook for post-processing leaf contents after add/remove operations.
 *
 * Allows custom compaction, splitting, or merging of entries based on
 * application-specific logic (e.g., merging small chunks in a columnar index).
 *
 * The processor is called AFTER normal PSS add/remove logic completes but
 * BEFORE the result is returned to the parent branch. This allows the PSS
 * tree to properly handle any structural changes (splits/merges) that the
 * processor introduces.
 *
 * Performance: If no processor is configured (null), there is ZERO overhead.
 * Only settings with a processor pay the cost of the callback.
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
