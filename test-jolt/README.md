Run `test-jolt/check.sh` for the Jolt public API tests and the JVM comparisons. It downloads Jolt 0.8.30 on Linux x86-64. Set `JOLT` to an installed binary on other platforms.

Run `test-jolt/compare-leaves.sh` with `clojure` and `jolt` on PATH. Set `JOLT` to select a particular executable. The JVM run uses this checkout's compiled Java Leaf (run `clojure -T:build java` at the repository root first). Jolt loads the shared leaf from this checkout.

Each case inserts into a leaf, checks that its source still has the same keys and cached measure, and reports the resulting leaves in order. The script compares the two runtimes' reports. Cases include ascending and descending comparators, odd and even branching factors, duplicate inserts, and warm and cold measures. The ordered measure exposes an insertion folded at the wrong position.

Run `test-jolt/compare-branches.sh` to exercise insertion through root splits. The JVM oracle uses the public sorted set from this checkout. Jolt uses shared resident nodes and the same root promotion function as the ClojureScript sorted set. The test compares every retained root after all insertions, including separators, counts, ordered measures and lookup results. It uses branching factor four and disables diff buffering.

On Jolt, count-based trees support synchronous `store`, `restore` and `walk-delta`. The storage test reopens two roots with an empty cache, verifies that equal roots cost no reads, and checks that a one-key change visits exactly the newly stored addresses.
The CBOR comparison exchanges stored trees between Jolt and the JVM. Each reader re-encodes the detached root before reading any nodes, then checks the bytes of every restored node. Both runtimes use the same released Boring artifact.
