#What is the Last N Store?


##Description
Accumulo's Versioning iterator is great at keeping only some finite number of previous versions of a single column but it will not help with documents that have been partitioned over many columns so that cell-level security can be constrained. This store effectively demonstrates how a count-based eviction can still be implemented using filters that store state so that the underlying keys/values of documents can be filtered appropriately. Accumulo also allows an age-off filter to be configured which would keep only the last n for a period of time before they expire.

Ultimately, this store maintains for you a window of the last events while Accumulo will automatically sort the events in descending order by time.

##Okay, but what's the purpose?

There are many use-cases when this could be an effective solution. Specifically, when passing events through a CEP engine and alerting on different correlations of those events, it's possible that you may want a cache to store the last n events that may have fired on an alerting stream so that they can see if there have been recent events, as well as which properties make up those events. Another use-case could be for a "news feed" where you may want to keep events that occurred in your system but you want to limit them by count instead of time- perhaps grouping them by user, or by event type, or both. 

##How do I use it?

The "last n" value is determined when the store is first configured. This is passed into the store upon construction and stored with the table configuration in Accumulo.

###Add objects to the store

Let's set up a store that will keep only the last 100 entries for some index. We'll use the generic "index1" here but you could make this anything grouping you'd like. Thinking about windows, you could group by something special based on the events themselves. Or you could group by a user's id. You could also group by some category of updates- like "blog posts" or "status changes"

```java
Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", new PasswordToken("secret"));

// create a last-n store that only keeps the last 100 events for each index.
// The only time the last n value is set is the first time the store is configured in Accumulo
AccumuloLastNStore lastNStore = new AccumuloLastNStore(connector, 100);

Event entry1 = EventBuilder.create("eventType")
    .attr("key1", "val1")
    .attr("key3", "val3")
    .build()

Event entry2 = EventBuilder.create("eventType")
    .attr("key1", "val1")
    .attr("key3", "val3")
    .build()

lastNStore.put("index1", entry1);
lastNStore.put("index1", entry2);
```

###Retrieve the last n objects from the store

```java
Iterable<Event> lastN = lastNStore.get("index1", new Auths());
```

There you have it. Seem simple?


