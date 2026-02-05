A B-tree based persistent sorted set for Clojure/Script.

PersistentSortedSet supports:

- transients,
- custom comparators,
- fast iteration,
- efficient slices (iterator over a part of the set)
- efficient `rseq` on slices.

Almost a drop-in replacement for `clojure.core/sorted-set`, the only difference being this one can’t store `nil`.

Implementations are provided for Clojure and ClojureScript.

## Building

```
export JAVA8_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home"
lein jar
```

## Support us

<a href="https://www.patreon.com/bePatron?u=4230547"><img src="./extras/become_a_patron_button@2x.png" alt="Become a Patron!" width="217" height="51"></a>

## Usage

Dependency:

```clj
[persistent-sorted-set "0.3.0"]
```

Code:

```clj
(require '[me.tonsky.persistent-sorted-set :as set])

(set/sorted-set 3 2 1)
;=> #{1 2 3}

(-> (set/sorted-set 1 2 3 4)
    (conj 2.5))
;=> #{1 2 2.5 3 4}

(-> (set/sorted-set 1 2 3 4)
    (disj 3))
;=> #{1 2 4}

(-> (set/sorted-set 1 2 3 4)
    (contains? 3))
;=> true

(-> (apply set/sorted-set (range 10000))
    (set/slice 5000 5010))
;=> (5000 5001 5002 5003 5004 5005 5006 5007 5008 5009 5010)

(-> (apply set/sorted-set (range 10000))
    (set/rslice 5010 5000))
;=> (5010 5009 5008 5007 5006 5005 5004 5003 5002 5001 5000)

(set/sorted-set-by > 1 2 3)
;=> #{3 2 1}
```
One can also efficiently seek on the iterators.

```clj
(-> (seq (into (set/sorted-set) (range 10)))
    (set/seek 5))
;; => (5 6 7 8 9)

(-> (into (set/sorted-set) (range 100))
    (set/rslice 75 25)
    (set/seek 60)
    (set/seek 30))
;; => (30 29 28 27 26 25)
```

## Durability

Clojure version allows efficient storage of Persistent Sorted Set on disk/DB/anywhere.

To do that, implement `IStorage` interface:

```clojure
(defrecord Storage [*storage]
  IStorage
  (store [_ node]
    (let [address (random-uuid)]
      (swap! *storage assoc address
        (pr-str
          (if (instance? Branch node)
            {:level     (.level ^Branch node)
             :keys      (.keys ^Branch node)
             :addresses (.addresses ^Branch node)}
            (.keys ^Leaf node))))
      address))
    
  (restore [_ address]
    (let [value (-> (get @*storage address)
                  (edn/read-string))]
      (if (map? value)
        (Branch. (int (:level value)) ^java.util.List (:keys value) ^java.util.List (:addresses value))
        (Leaf. ^java.util.List value)))))
```

Storing Persistent Sorted Set works per node. This will save each node once:

```clojure
(def set
  (into (set/sorted-set) (range 1000000)))

(def storage
  (Storage. (atom {})))

(def root
  (set/store set storage))
```

If you try to store once again, no store operations will be issued:

```clojure
(assert
  (= root
    (set/store set storage)))
```

If you modify set and store new one, only nodes that were changed will be stored. For a tree of depth 3, it’s usually just \~3 nodes. The root will be new, though:

```clojure
(def set2
  (into set [-1 -2 -3]))

(assert
  (not= root
    (set/store set2 storage)))
```

Finally, one can construct a new set from its stored snapshot. You’ll need address for that:

```clojure
(def set-lazy
  (set/restore root storage))
```

Restore operation is lazy. By default it won’t do anything, but when you start accessing returned set, `IStorage::restore` operations will be issued and part of the set will be reconstructed in memory. Only nodes needed for a particular operation will be loaded.

E.g. this will load \~3 nodes for a set of depth 3:

```clojure
(first set-lazy)
```

This will load \~50 nodes on default settings:

```clojure
(take 5000 set-lazy)
```

Internally Persistent Sorted Set does not caches returned nodes, so don’t be surprised if subsequent `first` loads the same nodes again. One must implement cache inside IStorage implementation for efficient retrieval of already loaded nodes. Also see `IStorage::accessed` for access stats, e.g. for LRU.

Any operation that can be done on in-memory PSS can be done on a lazy one, too. It will fetch required nodes when needed, completely transparently for the user. Lazy PSS can exist arbitrary long without ever being fully realized in memory:

```clojure
(def set3
  (conj set-lazy [-1 -2 -3]))

(def set4
  (disj set-lazy [4 5 6 7 8]))

(contains? set-lazy 5000)
```

Last piece of the puzzle: `set/walk-addresses`. Use it to check which nodes are actually in use by current PSS and optionally clean up garbage in your storage that is not referenced by it anymore:

```clojure
(let [*alive-addresses (volatile! [])]
  (set/walk-addresses set #(vswap! *alive-addresses conj %))
  @*alive-addresses)
```

See [test_storage.clj](test-clojure/me/tonsky/persistent_sorted_set/test_storage.clj) for more examples.

Durability for ClojureScript is not yet supported.

## Efficient Range Counting

Count elements in a range without iterating through them:

```clj
(def s (into (set/sorted-set) (range 10000)))

;; Count elements in [1000, 2000]
(set/count-slice s 1000 2000)
;=> 1001

;; Count from beginning to 500
(set/count-slice s nil 500)
;=> 501

;; Count from 9000 to end
(set/count-slice s 9000 nil)
;=> 1000

;; Use custom comparator
(set/count-slice s 1000 2000 my-comparator)
```

This uses O(log n) traversal by leveraging subtree counts stored in branch nodes.

## Aggregate Statistics

PersistentSortedSet can maintain aggregate statistics that update incrementally as elements are added or removed. This enables O(log n) queries for sum, count, min, max, variance, etc. over any range.

### Using Built-in Numeric Statistics

```clj
(import '[me.tonsky.persistent_sorted_set NumericStatsOps])

;; Create set with numeric stats tracking
(def s (into (set/sorted-set* {:stats (NumericStatsOps.)})
             [1 2 3 4 5]))

;; Get stats for entire set
(set/stats s)
;=> NumericStats{count=5, sum=15.0, min=1, max=5, ...}

;; Get stats for a range [2, 4]
(set/stats-slice s 2 4)
;=> NumericStats{count=3, sum=9.0, min=2, max=4, ...}
```

The `NumericStats` object provides `count`, `sum`, `sumSq`, `min`, `max`, plus derived `mean()`, `variance()`, and `stdDev()` methods.

### Implementing Custom Statistics

Statistics must form a **monoid** - they need an identity element and an associative merge operation. Implement the `IStats` interface:

```java
public interface IStats<Key, S> {
    // Identity element: merge(identity, x) == x
    S identity();

    // Extract stats from a single key
    S extract(Key key);

    // Associative merge: merge(a, merge(b, c)) == merge(merge(a, b), c)
    S merge(S s1, S s2);

    // Remove a key's contribution (see below)
    S remove(S current, Key key, Supplier<S> recompute);
}
```

**Example: Counting distinct categories**

```java
public class CategoryStats implements IStats<Item, Map<String, Long>> {
    public Map<String, Long> identity() {
        return Collections.emptyMap();
    }

    public Map<String, Long> extract(Item item) {
        return Map.of(item.category(), 1L);
    }

    public Map<String, Long> merge(Map<String, Long> a, Map<String, Long> b) {
        Map<String, Long> result = new HashMap<>(a);
        b.forEach((k, v) -> result.merge(k, v, Long::sum));
        return result;
    }

    public Map<String, Long> remove(Map<String, Long> current, Item item,
                                     Supplier<Map<String, Long>> recompute) {
        // For invertible stats, compute directly
        Map<String, Long> result = new HashMap<>(current);
        result.computeIfPresent(item.category(), (k, v) -> v > 1 ? v - 1 : null);
        return result;
    }
}
```

### Handling Non-Invertible Statistics

Some statistics like `min` and `max` can't be updated incrementally when removing elements - if you remove the minimum, you need to scan to find the new one. The `remove` method receives a `recompute` supplier for this:

```java
public S remove(S current, Key key, Supplier<S> recompute) {
    if (affectsResult(current, key)) {
        // Can't compute incrementally, recompute from children
        return recompute.get();
    }
    // Safe to compute incrementally
    return subtractKey(current, key);
}
```

The built-in `NumericStatsOps` handles this automatically for min/max.

### ClojureScript Statistics

For ClojureScript, implement the `IStats` protocol from `me.tonsky.persistent-sorted-set.impl.stats`:

```cljs
(require '[me.tonsky.persistent-sorted-set :as set])
(require '[me.tonsky.persistent-sorted-set.impl.stats :as stats])
(require '[me.tonsky.persistent-sorted-set.impl.numeric-stats :as numeric-stats])

;; Use built-in numeric stats
(def s (into (set/sorted-set* {:stats numeric-stats/numeric-stats-ops})
             [1 2 3 4 5]))

;; Or implement custom stats via the IStats protocol
(defrecord MyStats [count sum])

(def my-stats-ops
  (reify stats/IStats
    (identity-stats [_]
      (->MyStats 0 0))

    (extract [_ key]
      (->MyStats 1 key))

    (merge-stats [_ s1 s2]
      (->MyStats (+ (:count s1) (:count s2))
                 (+ (:sum s1) (:sum s2))))

    (remove-stats [_ current key recompute-fn]
      (->MyStats (dec (:count current))
                 (- (:sum current) key)))))

(def s (into (set/sorted-set* {:stats my-stats-ops}) [1 2 3 4 5]))
```

## Performance

To reproduce:

1. Install `[com.datomic/datomic-free "0.9.5703"]` locally.
2. Run `lein bench`.

`PersistentTreeSet` is Clojure’s Red-black tree based sorted-set.
`BTSet` is Datomic’s B-tree based sorted set (no transients, no disjoins).
`PersistentSortedSet` is this implementation.

Numbers I get on my 3.2 GHz i7-8700B:

### Conj 100k randomly sorted Integers

```
PersistentTreeSet                 143..165ms  
BTSet                             125..141ms  
PersistentSortedSet               105..121ms  
PersistentSortedSet (transient)   50..54ms    
```

### Call contains? 100k times with random Integer on a 100k Integers set

```
PersistentTreeSet     51..54ms    
BTSet                 45..47ms    
PersistentSortedSet   46..47ms    
```

### Iterate with java.util.Iterator over a set of 1M Integers

```
PersistentTreeSet     70..77ms    
PersistentSortedSet   10..11ms    
```

### Iterate with ISeq.first/ISeq.next over a set of 1M Integers

```
PersistentTreeSet     116..124ms  
BTSet                 92..105ms   
PersistentSortedSet   56..68ms    
```

### Iterate over a part of a set from 1 to 999999 in a set of 1M Integers

For `PersistentTreeSet` we use ISeq produced by `(take-while #(<= % 999999) (.seqFrom set 1 true))`.

For `PersistentSortedSet` we use `(.slice set 1 999999)`.

```
PersistentTreeSet     238..256ms  
PersistentSortedSet   70..91ms    
```

### Disj 100k elements in randomized order from a set of 100k Integers

```
PersistentTreeSet                 151..155ms  
PersistentSortedSet               91..98ms    
PersistentSortedSet (transient)   47..50ms    
```

## Projects using PersistentSortedSet

- [Datascript](https://github.com/tonsky/datascript), persistent in-memory database

## License

Copyright © 2019 Nikita Prokopov

Licensed under MIT (see [LICENSE](LICENSE)).
