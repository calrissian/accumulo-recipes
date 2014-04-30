#Entity Store Accumulo Recipe

Entities are objects that can be modeled like things in the real world. They have a type, an id, and some number of tuples (key/value/visiblity) that describe their state. A person can be an entity. A system can be an entity. Entities can reference each other by creating first-class relationships to other entities (or, through the pluggable type system, to anything- events, metrics, etc...). The unique thing about the entity store vs. the event store is that, unlike events, Entities are assumed to have occurred at some discrete point in time. That is, where an Event is defined by its timestamp, an entity is defined by its type. 

Really, the difference between the two comes down to the scheme that's used to shard them because they have very different expected query patterns. Because of the different query patterns, it makes sense to give them an optimized storage structure.

