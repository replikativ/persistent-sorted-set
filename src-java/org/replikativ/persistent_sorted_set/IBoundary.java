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
   * add/bulk: must this run be split into more than one node? Count: {@code len > bf}
   * (ignores keys). MST: some key at index {@code < len-1} rises to {@code level+1}
   * (a content boundary falls before the run's end). The run is the merged keys.
   */
  boolean overflows(Object[] run, int len, int level, Settings s);

  /**
   * add/bulk: partition an overflowing run into successive node lengths (summing to
   * {@code len}). Count ignores {@code run} and divides evenly toward branchingFactor;
   * MST cuts after each boundary key. The returned lengths drive the existing copy loop.
   * Precondition: {@code overflows(len, level, s)} is true (callers handle the fit case).
   */
  int[] splitLengths(Object[] run, int len, int level, Settings s);

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
}
