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

###Traversing the graph

Graph traversals start at some number of vertices and propagate through edges to the vertices on the opposite sides. Sometimes it's important to make several jumps through the graph and sometimes it's just important to know which edges sit directly adjacent. Either way, we generally start a traversal by querying for vertices of interest.

```java
Node query = new QueryBuilder().eq("name", "John Smith").build();
CloseableIterable<Entity> vertices = graphStore.query(Collections.singleton("Person"), query, null, new Auths());
```

After we've found the vertices of interest, we can find the adjacent out edges connected to those vertices with the following:
```java
Collection<EntityIndex> indexes = CloseableIterables.transform(vertices, TransformUtils.entityToEntityIndex);
CloseableIterable<Entity> edges = graphStore.adjacentEdges(indexes, null, Direction.OUT, new Auths());
```

##Tinkerpop

Tinkerpop is a very well thought out framework for using graphs to solve problems. At the base of the framework is Tinkerpop Blueprints, a set of interfaces for modeling a graph. Tinkerpop Gremlin is a traversal language that's used to do discovery and solve graph-related problems. Things like eigenvectors and shortest path algorithms can be implemented fairly easily. You can read more on the Tinkerpop framework [here](https://github.com/tinkerpop/).

The current Tinkerpop Blueprints graph is read-only. We are working to supply a mutable graph in the future but for now it's best and most performant to use the ```AccumuloEntityGraphStore``` to write entities.

###Creating an EntityGraph

Let's use the same model from above to traverse our graph. Since we are modelling our graph based on entity objects, we will need to know which entity types we plan to query. First,  we will create an ```EntityGraph``` instance:

```java
Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", "".getBytes());
GraphStore graphStore = new AccumuloEntityGraphStore(connector);
Graph g = new EntityGraph(graphStore, Sets.newHashSet("Person"), Sets.newHashSet("Relative"), new Auths());
```

That is, we only want to traverse over the vertices that include Person entities and we only want to traverse through edges that are Relative types. This typing makes it possible to traverse through the subgraphs we care about for a specific question.

###Using Gremlin to traverse the graph

Now, let's demonstrate using Tinkerpop's Gremlin to traverse the graph above. Gremlin is nice in that it can be written in both native Java and Groovy formats. Let's query for the vertex like we did in the ```AccumuloEntityGraphStore``` example above:
```java
Vertex v = g.getVertex(new EntityIndex("Person", "1"));
```

Now, let's set up a Gremlin query using Groovy to find the brothers of the vertex:

```java
List results = new ArrayList();
Bindings bindings = engine.createBindings();
bindings.put("g", graph);
bindings.put("v", v); 
bindings.put("results", results);

engine.eval("v.out('brother').fill(results)", bindings);
```

Or, let's say we had a parent relationship as a relative: 

``` java
Entity vertex3 = new BaseEntity("Person", "3");
vertex2.put(new Tuple("name", "James Smith Sr."));
vertex2.put(new Tuple("age", 65));
vertex2.put(new Tuple("location", "Maryland"));

EdgeEntity edge2 = new EdgeEntity("Relative", "2:3", vertex2, "", vertex3, "", "child");
edge2.put(new Tuple("biological", true));

EdgeEntity edge3 = new EdgeEntity("Relative", "1:3", vertex1, "", vertex3, "", "child");
edge3.put(new Tuple("biological", false));

graphStore.save(Arrays.asList(new Entity[] { edge2, edge3 }));
```

Now, we can traverse the parents from either of the children and figure out which one is biological:

```java
v.in('child').has('biological', true).fill(results);
```
