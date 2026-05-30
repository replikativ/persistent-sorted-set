package org.replikativ.persistent_sorted_set;

import java.util.Comparator;
import clojure.lang.ISeq;
import clojure.lang.PersistentTreeMap;

/**
 * OP_BUF_V5 per-child buffered diff (only allocated when Settings.opBufSize() &gt; 0;
 * at opBufSize==0 Branch._slots stays null, so everything is byte-identical to
 * baseline PSS — invariant I0).
 *
 * <p>A Slot is the logical diff of one branch child against its durable
 * (last-written) version, plus an absolute aggregate snapshot:
 * <ul>
 *   <li>{@code diff}: a PersistentTreeMap under the set's comparator, mapping a
 *       cmp-key to either the element (a <em>Present</em> entry, covering both
 *       insert and replace — a restore upserts the element into the materialized
 *       leaf, no op-type distinction needed) or {@link #ABSENT} (a remove).
 *       Net latest-wins per key (a later op on the same cmp-key overwrites).</li>
 *   <li>{@code ĝ = (count, measure)}: the absolute aggregate of the child's
 *       <em>current</em> subtree (not a delta — so non-invertible measures like
 *       min/max are fine, and it is free to collect since the in-memory node
 *       already maintains count/measure).</li>
 * </ul>
 *
 * <p>Slots are immutable values; deposit produces a new Slot (the underlying
 * PersistentTreeMap is itself persistent).
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class Slot {
  /** Sentinel marking a removed key (an <em>Absent</em> entry). */
  public static final Object ABSENT = new Object();

  /** cmp-key -&gt; element (Present) | ABSENT (remove). */
  public final PersistentTreeMap diff;

  /** ĝ.count — element count of the child's current subtree. */
  public final long count;

  /** ĝ.measure — measure of the child's current subtree (may be null). */
  public final Object measure;

  public Slot(PersistentTreeMap diff, long count, Object measure) {
    this.diff = diff;
    this.count = count;
    this.measure = measure;
  }

  /** Number of buffered entries (contributes to the per-node budget B). */
  public int size() {
    return diff == null ? 0 : diff.count();
  }

  static PersistentTreeMap emptyDiff(Comparator cmp) {
    return PersistentTreeMap.create(cmp, (ISeq) null);
  }
}
