#Graph Store Accumulo Recipe

Graphs are popular methods for modelling the connectivity of things. How are systems in a computer network communicating? How are a community of people related? How are human gene pools mapped to their respective inheritance trees? Graphs are becoming more and more popular for solving problems of anomaly detection, machine learning, recommendation systems, and other complex algorithms utilizing detection of related clusters and communities, changes in paths, and eigenvector centrality of nodes and communities (importance based on being the most likely to be traversed in a subgraph).

Specifically, the Calrissian graph store implementation allows the modelling of property graphs. Property graphs are simple to comprehend and conceptualize because they give both vertices and edges their own set of defining properties which can be queried and traversed. What this also does for edges is creates multiple possible values that can be used as weights. One weight could be a cardinality while another weight on the same edge could represent proximal geographic distance between the two vertices in which it is connecting. Property graphs model Entities very well. Because of this, the Entity store is used as the backing store while an optimized edge index is used for breadth-first traversal of edges through the graph from any set of vertices.

##Using the Graph Store

First, create a graph store:
```java
Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", "".getBytes());
GraphStore graphStore = new AccumuloEntityGraphStore(connector);
```


###Modelling and saving vertices and edges

As described above, modelling isn't too complicated at all. Any entity can be a vertex. Edges are just entities with 3 necessary properties describing the vertices they are connecting and the nature of the connection. I will elaborate on the model from the Calrissian entity store implementation and show how the same brother relationship would be modelled using the graph store.

```java
Entity vertex1 = new BaseEntity("Person", "1");
vertex1.put(new Tuple("name", "John Smith"));
vertex1.put(new Tuple("age", 34));
vertex1.put(new Tuple("location", "Maryland"));

Entity vertex2 = new BaseEntity("Person", "2");
vertex2.put(new Tuple("name", "James Smith"));
vertex2.put(new Tuple("age", 30));
vertex2.put(new Tuple("location", "Virginia"));

EdgeEntity edge = new EdgeEntity("Relative", "1:2", vertex1, "", vertex2, "", "brother");
edge.put(new Tuple("biological", true));
```
What's convenient about this way of modelling entities is that the vertices are only associated with each other through an edge. This means when an edge is added or removed, the vertices do not need to be updated. This allows edges to be "enriched" at later times via system-level analytics and user enrichment.

We add the entities to the graph store in the same way we'd add them to the entity store:
```java
graphStore.save(Arrays.asList(new Entity[] { vertex1, edge, vertex2 }));
```





