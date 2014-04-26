#Accumulo GeoSpatial Store Recipe

It's often necessary to collapse multi-dimensional data down into a single dimension in Accumulo so that they can be scanned forward using some pre-determined range with an Accumulo scanner. The GeoSpatial store uses a space-filling  z-curve based on quad trees to generate a geo-hash at a predermined depth. It indexes events using this geohash so that the events themselves can be reconstructed when queried.

The purpose of the store is to find all of the possible events that are associated with 2-dimensional geo-points that lie within a given bounding box. 

##Adding entries

You can specify mutliple entires to be added at a single location. That location is a 2-dimensional geo-coordinate represented with an x and a y value (longitude and latitude respectively). Let's make an example store entry and add it.

```java
StoreEntry entry = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
entry.put(new Tuple("name", "John Doe", ""));
entry.put(new Tuple("age", 35, ""));
entry.put(new Tuple("affiliation", "republican", ""));

Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", "".getBytes());
AccumuloGeoSpatialStore store = new AccumuloGeoSpatialStore(connector);
store.put(Collections.singleton(entry), new Point2D.Double(76.7000, 39.0000));
```

##Querying Entries

So now that we've indexed our entry, let's fetch it back out

```java
CloseableIterable<StoreEntry> entries = store.get(new Rectangle2D.Double(74.0, 37, 5, 9), new Auths());
```

