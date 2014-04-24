#What is the Event Store Recipe?

We tend to define events today in enterprise systems as an immutable object that occurs at some well-defined time. Events in the Calrissian stack are composed of tuples- that is, a set of keys and values that also contain visiblity information for cell-level security. The event store is a document store. But it's not just any document store, it's a document store that can scale to extreme sizes. It supplies a rich query interface for selecting events from the store. Future versions will include MapReduce input formats that can query events directly.

##What does an event look like?

Many of the stores in the Calrissian stack deal with objects modeled like events. A "StoreEntry" object is provided for such a purpose. Each store entry object must have an id, a timestamp, and at least one tuple to define it.

```java
// create our event to contain the keys/values we plan to add to give it state
StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());

// add the state to the event
event.put(new Tuple("systemName", "system1", "USER|ADMIN"));
event.put(new Tuple("eventType", "status", "USER|ADMIN"));
event.put(new Tuple("healthOK", true, "USER|ADMIN"));
event.put(new Tuple("location", "Maryland", "ADMIN"));
```

Here we've constructed an event to model the health status of a system. In this scenario, assume we have systems at several locations throughout an enterprise fabric. Perhaps we have a couple clouds located throughout the country but only users with ADMIN privileges can see those locations. 

Also notice that tuple values aren't just limited to being strings. Most standard java primitives are supported (with the exception of arrays). When this event gets stored in Accumulo, the value of the tuple will keep datatype information with it so that queries that are performed will know exactly the datatype of each value in which to search. Ths is important as well because of the lexicographically sorted nature of Accumulo keys. When the value of a tuple is supposed to be representing a number, it's important that I don't put that number in a string because 10 will sort before 2 in the lexicographic order of the bytes. This may not seem like a big deal until I want to search for numbers 1-10 and find that only 1 and 10 were returned and not 2-9.

##How do I store events?

Like many of the other recipes, the first thing you need is a Connector object to an Accumulo instance
```java
Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", "secret".getBytes());
```

Next you will need to create an EventStore instance and save your event
```java
AccumuloEventStore store = new AccumuloEventStore(connector);
store.save(event);
```

Almost seemed too easy. What's really happening underneath is that a discrete shard has been created for the RowID of the underlying Accumulo table. The shard is created to guarantee even distribution of the event over the nodes in the Accumulo cluster. Aside from the shard, the key and value have been normalized so that they can be searched using the built-in query mechanism.

It's important, however, that you pre-split the Accumulo table. 

##Pre-splitting the Accumulo table

If the event table isn't pre-split, all shards will end up on the same physical tablet until it gets big enough and Accumulo decides to split it into two. This won't allow the store distribute its data randomly across the cluster. Let's go ahead and split the table. There's a class provided in org.calrissian.eventstore.cli called ShardSplitter. It's an executable class but you'll need to pass in as arguments your Accumulo configuration. Since events are all time-based, a time-range for which to the split the table needs to be passed in as well. This allows shards to be split every morning, for instance, or every hour.

```java
java -cp accumulo-recipes-<version>.jar org.calrissian.eventstore.cli.ShardSplitter <zookeepers> <instance> <username> <password> <tableName> <start day: yyyy-mm-dd> <stop day: yyyy-mm-dd>
```

##Querying events from the store

A store isn't any good if we can't get data back out. Because of the nature of the indexes in the EventStore, we are actually able to perform a fairly rich set of queries against the store in parallel across the many tablets that make up the store. The query is actually pushed down to each Accumulo tablet server that hosts the shards where our data is being stored for the period of time for which we are interested.

Let's say we want to query back the event we constructure earlier. Let's first query for the events that are located in Maryland:

```java
Node query = new QueryBuilder().eq("location", "Maryland").build();
Date startTime = new Date(System.currentTimeMillis() - (60 * 60 * 1000));
Date endTime = new Date(System.currentTimeMillis() + (60 * 60 * 1000));
CloseableIterable<StoreEntry> events = store.query(startTime, endTime, query, new Auths("ADMIN"));
```

This query will return all events that have a "location" field equal to "Maryland" for the given time range where any tuples visiblity to the "ADMIN" label will be returned.

Notice the returned object is a CloseableIterable. Accumulo opens up socket connections to the tablet servers in a threadpool to queue up the items that should be returned when iterating the result set. It's important that you close the results when the iteration is done.

```java
for(StoreEntry entry : events)
  System.out.println("Entry Returned: " + entry);
events.close();
```

What's intrigueing about the loose and schemaless model of the StoreEntry object is that many heterogenous models can live together in the same store and be queried/returned together, each object returned can have a completely different set of tuples. You could, in theory, have several different types of "status update" events from several types of systems that are located in Maryland and you could query all of them from the same location for a given time period.

Better yet, if you want separation of keys so that they don't collide with other events in the store, you have the freedom to namespace them as you wish (i.e. status.location instead of just location).

A query wouldn't be extremely useful, however, if all you could ever query are single keys equal to a single value. You can use OR and AND queries here as well:

```java
// find all the status update events that have healthOK equal to false
Node query = new QueryBuilder().and().eq("eventType", "status").eq("healthOK", false).endStatement().build();

// find all the event object from Maryland or Virginia
Node query = new QueryBuilder().or().eq("location", "Maryland").eq("location", "Virginia").endStatement().build());
```
