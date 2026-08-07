#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "$(dirname "$0")/.."

# The build is :node-tests, not :test — there has never been a :test build in
# shadow-cljs.edn, so this script exited non-zero on its first line. Both this and
# the watch script are the documented way to run the cljs suite (CLAUDE.md), which
# is how the breakage stayed invisible: everyone ran shadow-cljs directly.
#
# :node-stress is a SEPARATE build holding the stress namespace; run it too, or a
# green run here means less than it looks.
yarn shadow-cljs release node-tests
node target/pss/tests.min.js

yarn shadow-cljs release node-stress
node target/pss/stress.min.js