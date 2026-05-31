package org.replikativ.persistent_sorted_set;

import java.util.Comparator;
import clojure.lang.ISeq;
import clojure.lang.Keyword;
import clojure.lang.PersistentTreeMap;

/**
 * diff-buf per-child buffered diff (only allocated when Settings.diffBufSize() &gt; 0;
 * at diffBufSize==0 Branch._slots stays null, so everything is byte-identical to
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
  /** Marks a removed key (an <em>Absent</em> entry). A namespaced keyword so leaf-diffs
   *  are directly edn/fressian-serializable (interned ⇒ identity comparison still works). */
  public static final Object ABSENT = Keyword.intern("org.replikativ.persistent-sorted-set", "absent");

  /** The child's node-diff. One of:
   *   - a {@link PersistentTreeMap} cmp-key -&gt; element (Present) | ABSENT (remove) — a LEAF child's leaf-diff;
   *   - null — a BRANCH anchor marker (the nested diff is derived from the live subtree at store,
   *            or reconstructed into the child's own _slots on restore push-down);
   *   - a nested slots structure — a restored, not-yet-materialized BRANCH child's diff. */
  public final Object diff;

  /** ĝ.count — element count of the child's current subtree. */
  public final long count;

  /** ĝ.measure — measure of the child's current subtree (may be null). */
  public final Object measure;

  /** Durable address of the child this diff is against (the buffer anchor).
   *  null ⇒ the child has no durable base yet (created this txn) ⇒ must be written, not buffered.
   *  At store, a buffered child's parent re-points addresses[i] := anchor. */
  public final Object anchor;

  public Slot(Object diff, long count, Object measure, Object anchor) {
    this.diff = diff;
    this.count = count;
    this.measure = measure;
    this.anchor = anchor;
  }

  static PersistentTreeMap emptyDiff(Comparator cmp) {
    return PersistentTreeMap.create(cmp, (ISeq) null);
  }
}
