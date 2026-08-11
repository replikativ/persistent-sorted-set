package org.replikativ.persistent_sorted_set;

import java.util.*;
import java.lang.reflect.Array;
import clojure.lang.*;

public class ArrayUtil {
  public static <T> T[] copy(T[] src, int from, int to, T[] target, int offset) {
    System.arraycopy(src, from, target, offset, to-from);
    return target;
  }

  public static Object indexedToArray(Class type, Indexed coll, int from, int to) {
    int len = to - from;
    Object ret = Array.newInstance(type, len);
    for (int i = 0; i < len; ++i)
      Array.set(ret, i, coll.nth(i+from));
    return ret;
  }

  /**
   * Compacts `arr` in place to its distinct prefix under `cmp` and returns that prefix's length.
   *
   * The empty case must be spelled out: the loop below starts at index 1, so for a zero-length
   * array it never runs and `to + 1` would report ONE distinct element in an array that has
   * none. `from-sequential` — the only caller — then built a set from a 1-element array whose
   * sole slot was null, so `(from-sequential compare [])` returned a set containing nil:
   *
   *     count 1, (= s #{}) false, (empty? s) false, (contains? s nil) true
   *
   * and the nil survived store/restore. That is the one value the set refuses to accept
   * anywhere else — `from-sequential` itself throws on a nil ELEMENT. ClojureScript's
   * `sorted-arr-distinct` short-circuits on `alength <= 1` and was always correct here.
   */
  public static int distinct(Comparator<Object> cmp, Object[] arr) {
    if (arr.length == 0) return 0;
    int to = 0;
    for (int idx = 1; idx < arr.length; ++idx) {
      if (cmp.compare(arr[idx], arr[to]) != 0) {
        ++to;
        if (to != idx) arr[to] = arr[idx];
      }
    }
    return to + 1;
  }
}