#What is the Last N Store?


##Description
Accumulo's Versioning iterator is great at keeping only some finite number of previous versions of a single column but it will not help with documents that have been partitioned over many columns so that cell-level security can be constrained. This store effectively demonstrates how a count-based eviction can still be implemented using filters that store state so that the underlying keys/values of documents can be filtered appropriately. 

Ultimately, this store maintains for you a window of the last events while Accumulo will automatically sort the events in descending order by time.

##Okay, but what's the purpose?

There are many use-cases when this could be an effective solution. Specifically, when passing events through a CEP engine and alerting on different correlations of those events, it's possible that you may want a cache to store the last n events that may have fired on an alerting stream so that they can see if there have been recent events, as well as which properties make up those events. Another use-case could be for a "news feed" where you may want to keep events that occurred in your system but you want to limit them by count instead of time- perhaps grouping them by user, or by event type, or both. 

##How do I use it?

###Add objects to the store

```java
Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", "secret".getBytes());

// create a last-n store that only keeps the last 100 events for each index.
// The only time the last n value is set is the first time the store is configured in Accumulo
AccumuloLastNStore lastNStore = new AccumuloLastNStore(connector, 100);

StoreEntry entry1 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
entry1.put(new Tuple("key1", "val1", ""));
entry1.put(new Tuple("key3", "val3", ""));

StoreEntry entry2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
entry2.put(new Tuple("key1", "val1", ""));
entry2.put(new Tuple("key3", "val3", ""));

lastNStore.put("index1", entry1);
lastNStore.put("index1", entry2);
```





