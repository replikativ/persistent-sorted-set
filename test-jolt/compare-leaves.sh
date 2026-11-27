#!/bin/sh
set -eu
cd "$(dirname "$0")"
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
clojure -M:oracle -m leaf-cases > "$out/jvm.edn"
"${JOLT:-jolt}" -m leaf-cases > "$out/jolt.edn"
diff -u "$out/jvm.edn" "$out/jolt.edn"
printf 'Matched %s leaf insertion cases.\n' "$(wc -l < "$out/jvm.edn")"
