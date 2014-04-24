# Changelog Store

## What is the Changelog Store?

Many multi-datacenter systems today have to make trade-offs on how they handle their data. Which data needs to be kept synchronized across the systems? Which data should be local to the systems? Many times, it's possible the datacenters themselves may not be connected all the time. When they are, it's very possible they have very little bandwidth by which to communicate. Problems like this make it important to summarize changes between the systems so that as little as possible can be communicated back and forth for the systems to determine which data actually needs to be synchronized.

Merkle Trees work very well for summarizing changes between distributed systems. Git, ZFS, and many peer-to-peer network protocols use it for this very reason. Merkle trees work by breaking up the larger set of data into smaller buckets and then summarizing those buckets, further working up the tree summarizing the parents of the buckets until the root of the tree is summarized. The best-case scenario for this distributed structure is when two systems contain the same data and only the root of the tree was used for that determination. You can read more about Merkle trees [here](http://en.wikipedia.org/wiki/Hash_tree). 

Calrissian provides a MerkleTree implementation in our commons repo, Mango. We call it [mango-hash](https://github.com/calrissian/mango/tree/master/mango-core/src/main/java/org/calrissian/mango/hash/tree).

## How to store and model changes

The Changelog effectively allows state to be summarized quickly so that the state can be compared against another system running another Changelog store where differences may need to be reconciled quickly. Just like many of the other stores in the Accumulo recipes, we have used the StoreEntry object to model our changes.

###The model

Take for a moment, an update that occurs on a system. Let's say a person's location was updated. We won't get into the specifics about how a person is modeled because that's outside of the scope of this recipe. Instead, let's dive into how I may model a person's location being changed:

```java
StoreEntry changeEvent = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
changeEvent.put(new Tuple("id", "person#1"));
changeEvent.put(new Tuple("location", "Virginia"));
```

As it appears, a person with id 1 had their location updated to Virginia.

###Storing changes

First we need to make a couple considerations about the systems we are working with. How often are changes expected to occur? If a system goes down for some reason, could it be down for days or would it come back up in a couple of minutes? These questions will help you determine the bucket size for which to partition your results. The store will default to a bucket size of 5 minutes so we'll use that for our example.

Perhaps it's time to create a store and put some changes in it

```java
Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", "password".getBytes());
AccumuloChangelogStore store = new AccumuloChangelogStore(connector);

store.put(Collections.singletonList(changeEvent));
```

##Building the tree and synchronizing results

###Getting a merkle tree

The merkle tree is built by asking the store for a set of changes for a period of time. It's very important, when using this to reconcile changes between two distributed systems, that the trees are built for the same period of time. The amount of variance in time allowed between the systems depends on the bucket size. A bucket size of 5 minutes will only allow variations within 5 minutes of each other, no more. 

```java
MerkleTree changeTree = store.getChangeTree(new Date(System.currentTimeMillis() - (2 * 60 * 60 * 1000)), new Date());
```

The code above builds a change tree for the last 2 hours. This change tree could be serialized in different ways and sent over the wire to a source node so that it can determine if there will be any other transmissions necessary.

###Determining differences

The merkle tree data structure itself contains a ```diff(MerkleTree other)``` method that will propagate down a tree when changes are found to find those buckets which will need to be transmitted.

```java
/** 
  * what we care about here is the timestamp of each leaf that's different. 
  * This determines the buckets that need to be re-transmitted
 **/
List<BucketHashLeaf> diffLeaves = targetTree.diff(sourceTree);  
```
