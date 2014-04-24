#Accumulo Range Store Recipe

##What is a range store?

Often times, the big challenge in Accumulo is figuring out how to collapse multi-dimensional data down into a single dimension so that the data can be partitioned effectively across a cluster and then further queried back in parallel. Interval trees are a data structure that optimize the storage and search of ranges that overlap a given range. A good example are finding CIDR ranges that overlap a given CIDR range. 

The interval tree can be effectively be modeled in Accumulo by representing two things-

- The dual query of some starting interval's midpoint outwards in both directions.
- The inward traversal of the outermost interval possible from the starting midpoint until the outermost interval in the first bullet is found.

Sound complicated? It's really just a good example of how a 2-dimensional model like an interval (with a start and end point) can be collapsed into a single dimension (a range that can be scanned lexicographically forward from a starting point to an ending point).

##But why does this matter?

The entire CIDR address space is absolutely huge. Think about having a seperate interval for each combination of addresses in each mask bit that could be generated. It takes a store that could partition those into multiple nodes and allow the nodes to be queried in parallel when the input gets big enough. Read (interval tree)[http://en.wikipedia.org/wiki/Interval_tree] to find out more.

##Adding intervals and finding overlaps

###Saving intervals

```java
Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", "".getBytes());
AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(connector, new LongRangeHelper());
rangeStore.save(singleton(new ValueRange<Long>(5L, 10L)));
rangeStore.save(singleton(new ValueRange<Long>(90L, 95L)));
rangeStore.save(singleton(new ValueRange<Long>(2L, 98L)));
rangeStore.save(singleton(new ValueRange<Long>(20L, 80L)));
```

###Finding overlapping intervals

Overlapping intervals are returned by calling the query method on the store and specifying the lower and upper bound of interest. 

```java
Iterable<ValueRange<Long>> results = rangeStore.query(new ValueRange<Long>(49L, 51L), new Auths());
```

