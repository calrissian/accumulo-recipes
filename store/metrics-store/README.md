#Metrics Store Accumulo Recipe

Metrics store demonstrates Accumulo's ability to perform reduce functions on tablets "behind the scenes" in parallel. Really, the reduce function happening behind the scenes is permanently being applied to the data when it's either written to disk (minor compaction) or re-written to merge files making up tablets together (major compaction). A scan time scope makes sure that I'm always looking at the product of the reduce function no matter what.

The metrics store demonstrates aggregating counts for items that can be modeled in a hierarchy with a "group", a "type", and a "name". Realistically, either of these hierarchical index elements can be as complicated as they need to be. For instance, the "group" could represent a set of systems like "datacenter". The type could represent a specific system in the datacenter like "maryland|location1". The name generally represents the nature of the metric (i.e. latency, eventsReceived, exceptionsThrown). 

Generally, it makes sense to model metrics in a way where items in the same group will have metrics with the same names and the type is used to denote the component of interest. For example, all of my metrics in the  "datacenter" group will have the names latency and droppedPackets. With this design, the type would denote the actual datacenter which contains the metrics.

##Adding metrics to the store

A metric object can be created as follows:

```java
Metric metric = new Metric(System.currentTimeMillis(), "internal", "systemA", "itemsIngested", 100);
```

The metric above can be added to the store just as easily:

```java
Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", "".getBytes());
AccumuloMetricStore store = new AccumuloMetricStore(connector);
store.save(Collections.singleton(metric));
```

###Fetching metrics from the store

Metrics are pretty easy to fetch from the store. They can be queried back by specifying at least the group and the type. The name is optional. 

```java
Iterable<Metric> metrics = store.query(new Date(0), new Date(), "group", "type", "name", MetricTimeUnit.MINUTES, new Auths());
```

Easy Peesy.
