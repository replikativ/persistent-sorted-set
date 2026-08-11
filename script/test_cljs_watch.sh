#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "$(dirname "$0")/.."

yarn shadow-cljs watch node-tests --config-merge '{:autorun true}'