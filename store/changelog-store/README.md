# Changelog Store

## What is the Changelog Store?

It's easy to use Accumulo to manage data changes that may need to be shared & further rectified on foreign clouds. With the lexicographically sorted property of Accumulo keys, reading items in descending order is easily achieved through reverse indexing the timestamps into static-length strings. By truncating those static-length string, we can truncate our timestamps (i.e. to every hour, half hour, fifteen minutes, etc...) to create buckets. Reverse sorted time-based buckets? That sounds like a wonderful changelog problem for a Merkle Tree to optimize. You can read all about merkle trees on [wikipedia](http://en.wikipedia.org/wiki/Hash_tree). The Changelog store uses the merkle tree implementation in [mango-hash](https://github.com/calrissian/mango/tree/master/hash).
