package org.replikativ.persistent_sorted_set;

/**
 * Default boundary policy: the historical count B-tree. Stateless singleton. Reproduces
 * the existing split arithmetic byte-for-byte, so routing the node code through the seam
 * with this policy is a pure refactor (verified against the full test suite).
 *
 * Two split shapes, keyed on level because leaves are always level 0 and branches ≥ 1:
 *   - level 0 (leaf, {@code Leaf.java:139-156}): ceil(len / bf) even pieces, remainder to
 *     the later pieces.
 *   - level ≥ 1 (branch, {@code Branch.java:590}): exactly two, split at the midpoint.
 */
public final class CountBoundary implements IBoundary {
  public static final CountBoundary INSTANCE = new CountBoundary();

  @Override
  public int[] splitOnInsert(Object[] run, int len, int ins, int level, Settings s) {
    if (len <= s.branchingFactor()) return null;   // fits in one node
    return splitLengths(run, len, level, s);        // count: position-independent, ignores ins
  }

  private int[] splitLengths(Object[] run, int len, int level, Settings s) {
    if (level == 0) {
      // Leaf rule: ceil(len / bf) pieces, evenly sized, remainder distributed to the
      // later pieces (matches Leaf.add's baseLen + (i >= numLeaves - remainder) layout).
      int bf = s.branchingFactor();
      int numLeaves = (len + bf - 1) / bf;
      int[] lengths = new int[numLeaves];
      int baseLen = len / numLeaves;
      int remainder = len % numLeaves;
      for (int i = 0; i < numLeaves; i++) {
        lengths[i] = baseLen + (i >= numLeaves - remainder ? 1 : 0);
      }
      return lengths;
    }
    // Branch rule: two halves at the midpoint (half1 = len >>> 1).
    int half1 = len >>> 1;
    return new int[]{ half1, len - half1 };
  }

  @Override
  public int keyLevel(Object key, Settings s) {
    return 0;
  }

  @Override
  public boolean contentDefined() {
    return false;
  }
}
