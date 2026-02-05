(ns me.tonsky.persistent-sorted-set.impl.stats
  "Protocol for computing and maintaining statistics over tree nodes.
   Statistics form a monoid with identity and associative merge operation.")

(defprotocol IStats
  "Interface for computing and maintaining statistics over tree nodes.
   Statistics form a monoid with identity and associative merge operation."

  (identity-stats [this]
    "Returns the identity (empty) statistics.
     This is the monoid identity element.")

  (extract [this key]
    "Extract statistics from a single key.
     For a leaf with one element, this gives the stats for that element.")

  (merge-stats [this s1 s2]
    "Merge two statistics objects.
     This operation must be associative: merge(a, merge(b, c)) == merge(merge(a, b), c)")

  (remove-stats [this current key recompute-fn]
    "Remove a key's contribution from statistics.

     For invertible statistics (count, sum, sum-squared), this can be computed directly.
     For non-invertible statistics (min, max), this may need to recompute from children.

     Parameters:
       current - the current statistics
       key - the key being removed
       recompute-fn - a no-arg function that recomputes stats from children (called only if needed)

     Returns the updated statistics."))
