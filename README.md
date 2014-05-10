#What are Accumulo Recipes?

These recipes and stores have been created as a starting point for using Accumulo to implement various different use-cases. The projects contained in this repository could be used either directly or modified entirely to fit differing needs. The code is meant to provide different scenarios and exemplify:

- How effective Accumulo can be at munging data in parallel across a cluster of machines. 
- How great Accumulo can index and process data using lexicographically sorted keys making that data immediately available.
- How well Accumulo is integrated into the Hadoop stack where data partitioning across the cluster can be fine-tuned to take advantage of locality when doing bulk operations.


Be sure to check the README files at the root of each store's folder to get detailed instructions and explanations. If you've got a recipe that you don't see here, we'd love to have it. 

##Stores

- Blob Store: This store demonstrates how to effectively stream bytes into and out of Accumulo tables. Large streams can be chunked up over several columns so that they don't need to fit into memory.
- Changelog Store: This store is for distributed systems that need to be able to summarize data for determining how it may differ from other data.
- Event Store: This is a document store for time-based events that shards the data to make it very scalable to store. It provides a query language for finding events of interest.
- Geospatial Store: This store indexes events under geohashes. The data is partitioned in a way where even geopoints that are geographically close to each other can be spread evenly across the cluster. The events can be queried back using rectangular "bounding boxes" representing a space on the earth.
- Graph Store: This store indexes edges of a graph so that they can be easily traversed by their vertices. It allows for breadth-first traversal and filtering.
- Last N store: This store is essentially a window that allows events to be grouped together and evicted by count.
- Metrics Store: Useful for aggregating counts and other statistical algorithms that can be applied associatively over units of time (minutes, hours, days, months, etc...).
- Range Store: Allows intervals (start and stop ranges) to be indexed so that overlapping intervals can be queried back easily.
- Temporal Last N Store: Similar to the last-n store, this does't evict based on count, but rather allows a customizable window into a custom grouping for datasets for some point in time.
