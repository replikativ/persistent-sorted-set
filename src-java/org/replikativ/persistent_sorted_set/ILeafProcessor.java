package org.replikativ.persistent_sorted_set;

import java.util.*;

/**
 * Optional hook for processing leaf contents during add/remove operations.
 *
 * <p>The processor is called AFTER the entry is added/removed but BEFORE
 * the tree's split/merge/borrow decisions. This means the processor
 * transforms entries, and normal B-tree rebalancing handles the result.
 *
 * <h3>Use cases</h3>
 * <ul>
 *   <li>Compacting small adjacent entries (e.g., merging small chunks in a columnar index)</li>
 *   <li>Splitting oversized entries during add (e.g., chunks exceeding a size limit)</li>
 * </ul>
 *
 * <h3>Constraints (enforced by assertions)</h3>
 * <ol>
 *   <li><b>Sort order:</b> The returned list MUST be in strictly ascending order
 *       according to the tree's comparator. Violating this corrupts the tree.</li>
 *   <li><b>Non-empty:</b> The returned list MUST contain at least one entry.</li>
 *   <li><b>No expansion on remove:</b> During a remove operation, the returned list
 *       must NOT exceed {@code settings.branchingFactor()} entries. Expansion
 *       (returning more entries than received) is only supported during add operations,
 *       where the tree will automatically multi-split the result if needed.</li>
 * </ol>
 *
 * <h3>Execution context</h3>
 * <ul>
 *   <li>Runs on both persistent and transient code paths. When {@code shouldProcess}
 *       returns true during a transient operation, the leaf falls through to the
 *       persistent-style path (allocating new arrays) so the processor can run.
 *       This ensures batch operations via transients still trigger processing
 *       (e.g., compaction after bulk deletes).</li>
 *   <li>Zero overhead if no processor is configured (null in Settings).</li>
 *   <li>The processor receives only the current leaf's entries, not neighboring leaves.</li>
 *   <li>Called once per affected leaf per add/remove operation.</li>
 * </ul>
 *
 * @param <Key> The type of keys stored in the set
 */
@SuppressWarnings("rawtypes")
public interface ILeafProcessor<Key> {

  /**
   * Process leaf entries after an add or remove operation.
   *
   * <p>The processor can compact (return fewer entries) or, during add only,
   * expand (return more entries). The tree handles rebalancing automatically:
   * splits if the result exceeds branchingFactor, merges/borrows if it falls
   * below minBranchingFactor.
   *
   * <p><b>Contract:</b>
   * <ul>
   *   <li>Must return a non-empty list</li>
   *   <li>Entries must be in strictly ascending sorted order per the tree's comparator</li>
   *   <li>During remove: must not return more than {@code settings.branchingFactor()} entries</li>
   * </ul>
   *
   * @param entries The leaf entries after add/remove, in sorted order
   * @param storage The storage backend (may be null for in-memory sets)
   * @param settings The settings for this tree (contains branching factor, etc.)
   * @return List of processed entries in sorted order, never empty
   */
  List<Key> processLeaf(List<Key> entries, IStorage storage, Settings settings);

  /**
   * Check if processing is needed before calling processLeaf.
   *
   * <p>Default implementation returns false (no processing). Override to return
   * true when you want to process the leaf (e.g., if leaf size suggests
   * compaction opportunities).
   *
   * <p>This method is called before allocating the entries list, so returning
   * false avoids unnecessary allocation.
   *
   * @param leafSize Number of entries in the leaf
   * @param settings The settings for this tree
   * @return true if processLeaf should be called, false to skip
   */
  default boolean shouldProcess(int leafSize, Settings settings) {
    return false;
  }
}
