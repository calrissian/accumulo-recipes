#Graph Store Accumulo Recipe

Graphs are popular structures for modelling the connectivity of things. How are systems in a computer network communicating? How are a community of people related? How are human gene pools mapped to their respective inheritance trees? Graphs are becoming more and more popular for solving problems of anomaly detection, machine learning, recommendation systems, and other complex algorithms utilizing detection of related clusters and communities, changes in paths, and eigenvector centrality of nodes and communities (importance based on being the most likely to be traversed in a subgraph).

Specifically, the Calrissian graph store implementation allows the modelling of property graphs. Property graphs are simple to comprehend and conceptualize because they give both vertices and edges their own set of defining properties which can be queried and traversed. What this also does for edges is creates multiple possible values that can be used as weights. One weight could be a cardinality while another weight on the same edge could represent proximal geographic distance between the two vertices in which it is connecting. Entities model property graphs very well. Because of this, the entity store is used as the backing store with the addition of an edge index which allows very quick breadth-first traversal of edges through the graph from any set of vertices.

##Using the Graph Store

First, create a graph store:
```java
Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", new PasswordToken(""));
GraphStore graphStore = new AccumuloEntityGraphStore(connector);
```

###Modelling and saving vertices and edges

As described above, modelling isn't too complicated at all. Any entity can be a vertex. Edges are just a specialized entity with a few required properties describing the vertices they are connecting and the nature of the connection (these properties are HEAD, TAIL, and LABEL). I will elaborate on the example model from the Calrissian entity store documentation and show how the same brother relationship would be modelled using the graph store.

```java
Entity vertex1 = new EntityBuilder("Person", "1")
    .attr("name", "John Smith")
    .attr("age", 34)
    .attr("location", "Maryland")
    .build();

Entity vertex2 = new EntityBuilder("Person", "2")
    .attr("name", "James Smith")
    .attr("age", 30)
    .attr("location", "Virginia")
    .build();

EdgeEntity edge = new EdgeEntityBuilder("Relative", "1:2", vertex1, vertex2, "brother")
    .attr("biological", true)
    .build();
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
CloseableIterable<EdgeEntity> edges = graphStore.adjacentEdges(indexes, null, Direction.OUT, new Auths());
```

Or we can propagate right to the set of vertices on the other side of the edges:
```java
Collection<EntityIndex> indexes = CloseableIterables.transform(vertices, TransformUtils.entityToEntityIndex);
CloseableIterable<Entity> newVertices = graphStore.adjacencies(indexes, null, Direction.OUT, new Auths());
```

With this, it's possible to perform a breadth-first traversal through the graph. Let's calculate the 3-hop of a graph:
```java

for(int i = 0; i < 3; i++) {
  Collection<EntityIndex> indexes = CloseableIterables.transform(vertices, TransformUtils.entityToEntityIndex);
  vertices.close()  // don't forget to close!
  vertices = graphStore.adjacencies(indexes, null, Direction.OUT, new Auths());
}
```

Now, if you were to iterate through ```vertices```, you would find that it contains the vertices 3 hops away from the initial set of vertices. 

It's important to note that the graph will return duplicate vertices and edges. For instance, if I were to have three edges that all linked ```vertex1``` to ```vertex3``` and I traversed through the ```adjacencies()``` from ```vertex1``` to ```vertex3``` without any further filtering of the edges upon which I traversed, I should expect to have ```vertex3``` show up three times in the resulting set of vertices. If the graph did not return these duplicates, then algorithms trying to quantify the number of times things were traversed would not be accurate.

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
ScriptEngine engine = new GremlinGroovyScriptEngine();
List results = new ArrayList();
Bindings bindings = engine.createBindings();
bindings.put("g", graph);
bindings.put("v", v); 
bindings.put("results", results);

engine.eval("v.out('brother').fill(results)", bindings);
```

Or, let's say we had a parent relationship as a relative: 

``` java
Entity vertex3 = new EntityBuilder("Person", "3")
    .attr("name", "James Smith Sr.")
    .attr("age", 65)
    .attr("location", "Maryland")
    .build();

EdgeEntity edge2 = new EdgeEntityBuilder("Relative", "2:3", vertex2, vertex3, "child")
    .attr("biological", true)
    .build();

EdgeEntity edge3 = new EdgeEntityBuilder("Relative", "1:3", vertex1, vertex3, "child")
    .attr("biological", false)
    .build();

graphStore.save(Arrays.asList(new Entity[] { edge2, vertex3, edge3 }));
```

Now, we can traverse the parents from either of the children and figure out which one is biological:

```groovy
v.in('child').has('biological', true).fill(results);
```

We can also loop a traversal and define a termination point, like the part between the as('x') and the loop('x') in the example below. Let's find the great great grandparent of all people named John Smith:

```groovy
v = g.V('name', 'John Smith');
v.as('x').in('child').loop('x'){it.loops < 3}.path
```

This is Tinkerpop's equivalent of the 3-hop we did using the ```AccumuloEntityGraphStore``` above.
