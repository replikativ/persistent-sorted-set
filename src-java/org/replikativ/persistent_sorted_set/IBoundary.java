package org.replikativ.persistent_sorted_set;

/**
 * Pluggable split/merge decision (the "boundary policy"). Factors the only thing that
 * varies between a count B-tree and a content-defined prolly/MST tree out of the node
 * code: WHERE a node splits and WHETHER a run overflows. The array / Stitch / slot
 * mechanics in Leaf/Branch are unchanged — they are driven by the answers here.
 *
 * The default policy is {@link CountBoundary} (even-division at branchingFactor), which
 * reproduces the historical tree byte-for-byte. A content policy (e.g. MST leading-zeros)
 * makes the structure a deterministic function of the keys (history-independent).
 *
 * Interface is grown incrementally as call sites are routed through it: this first cut
 * covers the add/bulk overflow split. The remove-side predicates (underflows / joins /
 * redistribute) and the MST root-promotion hooks are added with those sites.
 */
public interface IBoundary {

  /**
   * The incremental single-insert splitter. {@code run} is the node's keys AFTER a single key
   * was inserted at index {@code ins} (so {@code run[ins]} is the new key). Returns the
   * successive node lengths if the node must split, or {@code null} if it stays one node.
   *
   * O(1) for MST: a node built by MST has no interior boundaries (only its terminator), so the
   * only key that can create a new boundary is the inserted one — or, if it was appended past
   * the old max ({@code ins == len-1}), the now-interior old max at {@code run[len-2]}. Count
   * ignores {@code ins} and applies its size rule. Used by Leaf.add / Branch.add; bulk load
   * chunks via {@link #keyLevel} directly.
   */
  int[] splitOnInsert(Object[] run, int len, int ins, int level, Settings s);

  /**
   * The level a key "rises to": 0 ⇒ leaf-only (no promotion), ≥1 ⇒ separator at that
   * level. Count returns 0 for every key (⇒ purely length-driven). MST returns
   * leadingZeros(hash(key)) / leadingZerosPerLevel.
   */
  int keyLevel(Object key, Settings s);

  /**
   * Whether boundaries are content-defined. False (count) keeps the legacy borrow path
   * and in-place transient edits; true (MST) switches removes to merge-and-re-partition
   * and enables multi-level root promotion.
   */
  boolean contentDefined();

  /**
   * A serializable descriptor that lets a reader reconstruct this policy (e.g. the Clojure map
   * {@code {:type :mst :lzpl 6}}), or {@code null} for the default count policy. Rides in the
   * fressian blob's per-node config (NOT in the content hash), so a restored MST store is
   * self-describing and cannot be silently mutated as a count tree. Returned as Object because
   * the descriptor is consumer-domain EDN; the matching {@code boundary-resolver} rebuilds it.
   */
  default Object descriptor() { return null; }
}
