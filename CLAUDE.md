# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a B-tree based persistent sorted set implementation for Clojure and ClojureScript. It's designed as a drop-in replacement for `clojure.core/sorted-set` with enhanced performance and features.

## Build and Development Commands

### Building the Project

```bash
# Set Java 8 home first (required for building)
export JAVA8_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home"

# Build Java classes
lein javac
# or
./script/build.sh

# Build the JAR
lein jar
```

### Running Tests

```bash
# Clojure tests
lein test
# or
clj -M:test
# or
./script/test_clj.sh

# ClojureScript tests
yarn shadow-cljs release test
# or
./script/test_cljs.sh

# Watch mode for ClojureScript tests
yarn shadow-cljs watch test --config-merge '{:autorun true}'
# or
./script/test_cljs_watch.sh
```

### Running Benchmarks

```bash
# Clojure benchmarks
lein bench
# or
./script/bench_clj.sh

# ClojureScript benchmarks
yarn shadow-cljs release bench
# or
./script/bench_cljs.sh
```

### REPL Development

```bash
# Socket REPL on port 5555
./script/repl.sh

# nREPL
clj -M:dev -m nrepl.cmdline --interactive
# or
./script/nrepl.sh
```

### Clean Build Artifacts

```bash
rm -rf target
# or
./script/clean.sh
```

## Architecture and Code Structure

### Core Components

1. **Java Implementation** (`src-java/me/tonsky/persistent_sorted_set/`)
   - `APersistentSortedSet.java` - Abstract base class
   - `PersistentSortedSet.java` - Main implementation
   - `ANode.java`, `Branch.java`, `Leaf.java` - B-tree node structure
   - `IStorage.java` - Interface for persistent storage
   - `Settings.java` - Configuration (branching factor, reference types)

2. **Clojure/ClojureScript API** (`src-clojure/me/tonsky/persistent_sorted_set/`)
   - Public API functions: `sorted-set`, `sorted-set-by`, `conj`, `disj`, `slice`, `rslice`, `seek`
   - Storage operations: `store`, `restore`, `walk-addresses`
   - Utility namespace: `arrays.cljc` for cross-platform array operations

### Key Design Aspects

- **B-tree Implementation**: Uses a configurable branching factor (default 512) for efficient operations
- **Transient Support**: Provides efficient batch operations through transients
- **Custom Comparators**: Supports custom comparison functions
- **Slicing**: Efficient iteration over subsets of the data
- **Storage Interface**: Allows lazy loading and persistence through the `IStorage` interface
- **Reference Types**: Supports strong, soft, and weak references for memory management

### Build Configuration

- **Leiningen** (`project.clj`): Primary build tool, handles Java compilation
- **deps.edn**: Clojure CLI configuration for development and testing
- **shadow-cljs.edn**: ClojureScript build configuration
- **package.json**: Node.js dependencies for ClojureScript tooling

### Testing Structure

Tests are located in `test-clojure/me/tonsky/persistent_sorted_set/test/`:
- `core.cljc` - Main test suite
- `small.cljc` - Focused tests for specific cases
- `storage.clj` - Storage interface tests (Clojure only)
- `stress.cljc` - Performance and stress tests

### Important Notes

- Java 8 is required for building due to bootclasspath requirements
- The set cannot store `nil` values (unlike `clojure.core/sorted-set`)
- ClojureScript version does not yet support the storage/durability features