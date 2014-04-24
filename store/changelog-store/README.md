# Changelog Store

## What is the Changelog Store?

It's easy to use Accumulo to manage data changes that may need to be shared & further rectified on foreign clouds. With the lexicographically sorted property of Accumulo keys, reading items in descending order is easily achieved through reverse indexing the timestamps into static-length strings. By truncating those static-length string, we can truncate our timestamps (i.e. to every hour, half hour, fifteen minutes, etc...) to create buckets. Reverse sorted time-based buckets? That sounds like a wonderful changelog problem for a Merkle Tree to optimize. You can read all about merkle trees on [wikipedia](http://en.wikipedia.org/wiki/Hash_tree). The Changelog store uses the merkle tree implementation in [mango-hash](https://github.com/calrissian/mango/tree/master/mango-core/src/main/java/org/calrissian/mango/hash/tree).

## How to store and model changes

The Changelog effectively allows state to be summarized quickly so that the state can be compared against another system running another Changelog store where differences may need to be reconciled quickly. Just like many of the other stores in the Accumulo recipes, we have used the StoreEntry object to model our changes.

Take for a moment, an update that occurs on a system. Let's say a person's location was updated. We won't get into the specifics about how a person is modeled because that's outside of the scope of this recipe. Instead, let's dive into how I may model a person's location being changed:

```java
StoreEntry changeEvent = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
changeEvent.put(new Tuple("updateId", "1"));
changeEvent.put(new Tuple("updateType", "person"));
changeEvent.put(new Tuple("updateKey", "location"));
changeEvent.put(new Tuple("newValue", "Virginia"));
```

So as it appears, a person with id 1 had a location updated to Virginia.
