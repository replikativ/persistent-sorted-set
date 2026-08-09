package org.replikativ.persistent_sorted_set;

import java.util.*;
import clojure.lang.*;

class JavaIter implements Iterator {
  final Seq _seq;
  boolean _over;

  JavaIter(Seq seq) {
    _seq = seq;
    _over = seq == null;
  }
  public boolean hasNext() { return !_over; }
  /** `Iterator.next` MUST throw NoSuchElementException when exhausted. This ignored
   *  `_over` entirely, which broke the contract in two directions:
   *
   *    * past the end of a NON-empty set it re-read `_seq.first()` and handed back the
   *      LAST element again, forever. Anything driven by `hasNext()` never sees this —
   *      which is why it survived — but the documented way to detect exhaustion without
   *      `hasNext()`, and everything generic that wraps an Iterator expecting the
   *      exception, got a silent infinite supply of a duplicate instead.
   *    * on an EMPTY set the constructor sets `_over` from `seq == null`, so `_seq` is
   *      null and the very first `next()` threw NullPointerException rather than
   *      NoSuchElementException.
   *
   *  One predicate fixes both, on a path Clojure itself does not take (`seq`/`reduce` go
   *  through `Seq` directly), so this costs a branch only for Java interop callers. */
  public Object next() {
    if (_over) throw new NoSuchElementException();
    Object res = _seq.first();
    _over = false == _seq.advance();
    return res;
  }
}
