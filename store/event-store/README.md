#What is the Event Store Recipe?

We tend to define events today in enterprise systems as an immutable object that occurs at some well-defined time. Events in the Calrissian stack are composed of tuples- that is, a set of keys and values that also contain visiblity information for cell-level security. The event store is a document store. But it's not just any document store, it's a document store that can scale to extreme sizes. It supplies a rich query interface for selecting events from the store. Future versions will include MapReduce input formats that can query events directly.

##What does an event look like?

Many of the stores in the Calrissian stack deal with objects modeled like events. A "StoreEntry" object is provided for such a purpose. Each store entry object must have an id, a timestamp, and at least one tuple to define it (an object doesn't exist in a key/value store unless it has at least one key/value).

```java

// create our event to contain the keys/values we plan to add to give it state
StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());

// add the state to the event
event.put(new Tuple("systemName", "system1", "USER&ADMIN"));
event.put(new Tuple("eventType", "status", "USER&ADMIN"));
event.put(new Tuple("healthOK", true, "USER&ADMIN"));
event.put(new Tuple("location", "Maryland", "ADMIN"));
```

Here you can see, we've constructed an event to model the health status of a system. In this scenario, assume we have systems at several locations throughout an enterprise fabric. Perhaps we have a couple clouds located throughout the country but only users with ADMIN privileges can see those locations. 
