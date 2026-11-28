#!/bin/sh
set -eu
cd "$(dirname "$0")"
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
clojure -M:oracle -m branch-cases > "$out/jvm.edn"
"${JOLT:-jolt}" -m branch-cases > "$out/jolt.edn"
diff -u "$out/jvm.edn" "$out/jolt.edn"
printf 'Matched %s retained tree versions.\n' "$(wc -l < "$out/jvm.edn")"
