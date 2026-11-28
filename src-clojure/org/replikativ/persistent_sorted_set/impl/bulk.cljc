(ns org.replikativ.persistent-sorted-set.impl.bulk)

(defn streaming-split
  "Lazy seq of vectors, reproducing `split`'s distribution over a SEQ rather than
   an indexed collection, buffering at most `(* 2 avg)` elements.

   `split` needs the remaining count to choose a cut, which looks like it needs
   the whole collection — but its rule only ever asks whether at least `2*avg`
   remain, so a `2*avg` lookahead answers it exactly. That is what makes a
   streaming build possible at all, and it is why the output is IDENTICAL to
   `from-sorted-array`'s rather than merely similar: same cuts, same tree.

   The three cases are `split`'s, in order:
     >= 2*avg remaining  take exactly avg
     <= max remaining    take all of it as the last node
     otherwise           halve it, so the last two nodes are both >= min"
  [coll avg max]
  ;; avg >= 2 or the build never terminates: with avg = 1 every branch gets one
  ;; child, so a level with k nodes produces k nodes again and the tree grows
  ;; upward forever. avg = (min+max)/2 with min = bf>>>1, so this means bf >= 3.
  ;; Checked rather than left to hang — bf=2 spun until OOM.
  (assert (>= avg 2)
          (str "branching-factor must be >= 4 for a streaming build (got avg fanout "
               avg "); a fanout of 1 never reduces the level count"))
  (let [need (* 2 avg)
        fill (fn [buf s]
               (loop [buf buf s s]
                 (if (or (nil? s) (>= (count buf) need))
                   [buf s]
                   (recur (clojure.core/conj buf (first s)) (next s)))))]
    ((fn step [buf s]
       (lazy-seq
        (let [[buf s] (fill buf s)
              n (count buf)]
          (cond
            (zero? n) nil
            ;; `(into [] (subvec ...))` and NOT the bare subvec: a SubVector shares
            ;; its base, and `conj` on one does `base.assocN(end, o)` — so the base
            ;; grows without bound and every element ever consumed stays reachable.
            ;; That silently makes this O(n), which is the one thing the function
            ;; exists not to be. Measured: 4M elements OOM'd at -Xmx128m before
            ;; this copy, and complete comfortably after it.
            (>= n need) (cons (into [] (subvec buf 0 avg))
                              (step (into [] (subvec buf avg)) s))
            (<= n max) (list buf)
            :else (let [h (quot n 2)]
                    (list (into [] (subvec buf 0 h)) (into [] (subvec buf h))))))))
     [] (seq coll))))

(defn build-levels
  "Construct a count-based tree bottom-up from ordered keys. Each constructor
   returns either a resident node or a stored entry for the next level."
  [xs avg max-len make-leaf make-branch]
  (loop [level 1 entries (map make-leaf (streaming-split xs avg max-len))]
    (when-let [s (seq entries)]
      (if (next s)
        (recur (inc level)
               (map #(make-branch level %) (streaming-split s avg max-len)))
        (first s)))))
