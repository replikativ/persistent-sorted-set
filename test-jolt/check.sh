#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
if [ -z "${JOLT:-}" ]; then
  archive=jolt-v0.8.30-x86_64-linux.tar.gz
  (cd "$out"
   curl -fsSL --retry 3 -O "https://github.com/jolt-lang/jolt/releases/download/v0.8.30/$archive"
   echo "c9eb00ce9b58e6ab19dcfc324edad2ad5127bab7339e8d018ce2c30876037bdd  $archive" | sha256sum -c -
   tar -xzf "$archive")
  JOLT="$out/${archive%.tar.gz}/jolt"
fi

export JOLT
clojure -T:build java
"$JOLT" -Sdeps '{:paths ["src-clojure" "test-clojure"]}' -e "
(require 'clojure.test 'org.replikativ.persistent-sorted-set.test.resident-api)
(let [r (clojure.test/run-tests 'org.replikativ.persistent-sorted-set.test.resident-api)]
  (assert (and (pos? (:test r)) (zero? (+ (:fail r) (:error r))))))"
test-jolt/compare-leaves.sh
test-jolt/compare-branches.sh
