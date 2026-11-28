(ns org.replikativ.persistent-sorted-set.impl.measure
  "Protocol for computing and maintaining measures over tree nodes.
   Measures form a monoid with identity and associative merge operation.")

(defprotocol IMeasure
  "Interface for computing and maintaining measures over tree nodes.
   Measures form a monoid with identity and associative merge operation."

  (identity-measure [this]
    "Returns the identity (empty) measure.
     This is the monoid identity element.")

  (extract [this key]
    "Extract measure from a single key.
     For a leaf with one element, this gives the measure for that element.")

  (merge-measure [this m1 m2]
    "Merge two measure objects.
     This operation must be associative: merge(a, merge(b, c)) == merge(merge(a, b), c)")

  (remove-measure [this current key recompute-fn]
    "Remove a key's contribution from a measure.

     For invertible measures (count, sum, sum-squared), this can be computed directly.
     For non-invertible measures (min, max), this may need to recompute from children.

     Parameters:
       current - the current measure
       key - the key being removed
       recompute-fn - a no-arg function that recomputes measure from children (called only if needed)

     Returns the updated measure.")

  (weight [this measure]
    "Extract the element weight (count) from a measure object.
     Used by rank-based navigation (get-nth) to traverse the tree
     by accumulated element counts rather than by key comparison.

     Must satisfy: weight(merge(a, b)) == weight(a) + weight(b)

     Returns the number of data elements represented by this measure."))
