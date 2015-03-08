#Entity Store Accumulo Recipe

Entities are objects that can be modeled like things in the real world. They have a type, an id, and some number of attributes (key/value/visiblity) that describe their state. A person can be an entity. A system can be an entity. Entities can reference each other by creating first-class relationships to other entities (or, through the pluggable type system, to anything- events, metrics, etc...). The unique thing about the entity store vs. the event store is that, unlike events, entities are not assumed to have occurred at some discrete point in time. That is, where an Event is defined by its timestamp, an Entity is defined by its type.

##Using the Entity Store

First, you will need to create an entity store instance
```java
Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", "".getBytes());
EntityStore entityStore = new AccumuloEntityStore(connector);
```

###Modelling and saving

Entities can be modeled using the following:
```java
Entity entity = EntityBuilder.create("Person", "1")
    .attr("name", "John Smith")
    .attr("age", 36)
    .attr("location", "Maryland")
    .attr("brother", new EntityIdentifier("Person", "2")
    .build();
```

Saving an entity to the store is pretty simple:
```java
entityStore.save(Collections.singleton(entity));
entityStore.flush();
```

###Fetching and querying

There are a few different get and query options for entities. 

- You can stream a single or a bunch of entities in a batch by their types and ids:
```java
EntityIdentifier index = new EntityIdentifier("Person", "1");
CloseableIterable<Entity> entities = entityStore.get(Collections.singleton(index), null, new Auths());
```

- You can stream all the entities of a collection of types:
```java
CloseableIterable<Entity> entities = entityStore.getAllByType(Collections.singleton("Person"), null, new Auths());
```

- Or you can query for the entities of interest for a collection of types:
```java
Node query = new QueryBuilder().and().eq("age", 36).eq("name", "John Smith").end().build();
CloseableIterable<Entity> entities = entityStore.query(Collections.singleton("Person"), query, null, new Auths());
```


## Persisting and querying JSON

Mango provides a utility for flattening json into a collection of attributes that can be used to hydrate entity objects. The same utility can also be used to re-expand the flattened attributes back into json. This allows users to quickly get their data into the entity store without spending too much time worrying about object parsing and translation.

First thing you'll want to do is probably to turn your json into an entity. You'll need a Jackson ```ObjectMapper```:
```java
ObjectMapper objectMapper = new ObjectMapper();
String json = "{ \"locations\":[{\"name\":\"Office\", \"addresses\":[{\"number\":1234,\"street\":{\"name\":\"BlahBlah Lane\"}}]}]}}";
Entity entity = EntityBuilder.create("Person", "1")
    .attrs(JsonAttributeStore.fromJson(json, objectMapper))
    .build();
```

Now you can persist the entity, as it's just a bunch of key/value attributes.
```java
entityStore.save(Collections.singleton(entity));
entityStore.flush();
```

When you've performed a query from the store and gotten results back, you can re-expand them into JSON:
```java
String resultJson = JsonAttributeStore.toJsonString(event.getAttributes(), objectMapper);
```

## Concepts

### Entities vs Events

Really, the difference between the two comes down to the scheme that's used to shard them because they have very different expected query patterns. Because of the different query patterns, it makes sense to give each an optimized storage structure. While events are defined by their timestamp and sharded by time, Entities are sharded by their type. This minimizes the cross chatter from entities of varying types when specific types are being targeted for query. That is not to say that they couldn't be sharded differently- in fact, you could extend the EntityShardBuilder class and set it on the AccumuloEntityStore instance to provide whatever sharding scheme you'd like. 

By default, entities of each type are sharded into a predefined number of partitons (7). What differs from a time-sharded store is that if entities of any type get insanely large (several billion), then it's possible 7 shards may no longer be enough. The idea here is that it's not hard to shuffle data around in a cloud. In fact, Accumulo provides an extremely easy way to lift up the tablets for those entity types and re-ingest them into the new partition size with minimal downtime. You could even use a distributed locking mechanism like Zookeeper to make sure distributed clients know the entities of a specific type are in the process of being re-sharded. This paradigm is not uncommon. Elasticsearch follows a similar workflow- 5 shards are chosen by default, if more are needed, the data needs to be reindexed. This operation is expensive, but it's something that doesn't need to happen often.

Another defining factor for events vs. entities is that events tend to be immutable while entities can represent both immutable and user-generated mutable data. With the help of Zookeeper, you could also make sure mutually-exclusive locks are held down to an entity's type and id when being updated so that user's don't run the risk of having stale data at any point. Because distributed locking is an expensive operation, however, it's not something that many scenarios would choose to use as a default. Rather, what makes an entity store useful is that it follows the same shared-nothing paradigm as most distributed document stores today. That is, it guarantees nothing about referential integrity. An entity can point to another entity that doesn't exist. Sometimes it's more important to know about the link than it is to know about what's on the other side- even if it no longer or never has existed in the data store.


###The pains of forced referential integrity

The referential integrity bit is quite important to what makes this store useful. Many graph databases tend to want to control the identifier creation layer to guarantee referential integrity. This often forces users to make very bad design decisions as they need to query 2 vertices before they can link an edge to them. Further, if they know they may need those vertices further, they end up writing caching mechanisms to stick as many of the vertices in memory as possible. This breaks down with large graphs and eliminates the possiblity for bulk ingest in most cases (in fact, many graph databases like Titan and Neo4j allow you to turn off referential integrity so you can bulk ingest but you are still stuck using their identification scheme).

What if an id was just a string and it was up to the user to determine what it meant, how unique it should be, and even more importantly, how it was created? When we model data in a SQL database, the first thing we ask ourselves is "what makes each row unique?". We tend to model entities the same way. Where the attributes that make the entity unique from other entities are used to construct that entity's id. When an entity's id can be based on some natural key of properties that make it unique, it also becomes deterministic. Further, links to other entities also become deterministic. In this way, entities can point to other entities by knowing simple things about the entities in which they point. The nice part of this scheme is that it doesn't forbid you from still querying an entity to link to it when the id is not so straightforward, it just gives you the ability to link to ids without querying when it's possible.

