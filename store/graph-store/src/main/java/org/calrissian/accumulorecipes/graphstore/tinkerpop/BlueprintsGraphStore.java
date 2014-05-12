package org.calrissian.accumulorecipes.graphstore.tinkerpop;

import com.google.common.base.*;
import com.tinkerpop.blueprints.*;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.entitystore.model.EntityRelationship;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityEdge;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityElement;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.model.EntityVertex;
import org.calrissian.accumulorecipes.graphstore.tinkerpop.query.EntityGraphQuery;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.criteria.Criteria;
import org.calrissian.mango.domain.Entity;

import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.singleton;
import static org.calrissian.accumulorecipes.graphstore.model.EdgeEntity.HEAD;
import static org.calrissian.accumulorecipes.graphstore.model.EdgeEntity.TAIL;
import static org.calrissian.mango.collect.CloseableIterables.transform;

public class BlueprintsGraphStore implements Graph {

  protected GraphStore graphStore;
  protected Set<String> vertexTypes;
  protected Set<String> edgeTypes;

  protected Auths auths;

  public BlueprintsGraphStore(GraphStore graphStore, Set<String> vertexTypes, Set<String> edgeTypes, Auths auths) {
    this.graphStore = graphStore;
    this.vertexTypes = vertexTypes;
    this.edgeTypes = edgeTypes;
    this.auths = auths;
  }

  @Override
  public Features getFeatures() {
    Features features = new Features();
    features.supportsTransactions = false;
    features.supportsDuplicateEdges = false;
    features.supportsSelfLoops = true;
    features.supportsSerializableObjectProperty = true;
    features.supportsBooleanProperty = true;
    features.supportsDoubleProperty = true;
    features.supportsFloatProperty = true;
    features.supportsIntegerProperty = true;
    features.supportsPrimitiveArrayProperty = false;
    features.supportsUniformListProperty = false;
    features.supportsMixedListProperty = false;
    features.supportsLongProperty = true;
    features.supportsMapProperty = false;
    features.supportsStringProperty = true;
    features.ignoresSuppliedIds = false;
    features.isPersistent = true;
    features.isWrapper = true;
    features.supportsIndices = true;
    features.supportsVertexIndex = true;
    features.supportsEdgeIndex = true;
    features.supportsKeyIndices = true;
    features.supportsVertexKeyIndex = true;
    features.supportsEdgeKeyIndex = true;
    features.supportsEdgeIteration = true;
    features.supportsVertexIteration = true;
    features.supportsEdgeRetrieval = true;
    features.supportsVertexProperties = true;
    features.supportsEdgeProperties = true;
    features.supportsThreadedTransactions = false;
    features.supportsTransactions = false;

    return features;

  }

  @Override
  public Vertex addVertex(Object o) {
    throw new UnsupportedOperationException("The Calrissian EntityGraph is immutable. Use the GraphStore API to modify the graph.");
  }

  @Override
  public Vertex getVertex(Object o) {
    checkArgument(o instanceof EntityIndex);
    if (vertexTypes.contains(((EntityIndex) o).getType())) ;
    CloseableIterable<Entity> entities = graphStore.get(singleton((EntityIndex) o), null, auths);
    Iterator<Entity> itr = entities.iterator();
    if (itr.hasNext()) {
      EntityVertex toReturn = new EntityVertex(itr.next(), graphStore, auths);
      entities.closeQuietly();
      return toReturn;
    }
    return null;
  }

  @Override
  public void removeVertex(Vertex vertex) {
    throw new UnsupportedOperationException("The Calrissian EntityGraph is immutable. Use the GraphStore API to modify the graph.");
  }

  @Override
  public CloseableIterable<Vertex> getVertices() {
    CloseableIterable<Entity> entities = graphStore.getAllByType(vertexTypes, null, auths);
    return transform(entities, new VertexEntityXform(graphStore, auths));
  }

  @Override
  public CloseableIterable<Vertex> getVertices(String s, Object o) {
    CloseableIterable<Entity> entities = graphStore.query(vertexTypes, new QueryBuilder().eq(s, o).build(), null, auths);
    return transform(entities, new VertexEntityXform(graphStore, auths));
  }

  @Override
  public Edge addEdge(Object o, Vertex vertex, Vertex vertex2, String s) {
    throw new UnsupportedOperationException("The Calrissian EntityGraph is immutable. Use the GraphStore API to modify the graph.");
  }

  @Override
  public Edge getEdge(Object o) {
    checkArgument(o instanceof EntityIndex);
    if (edgeTypes.contains(((EntityIndex) o).getType())) ;
    CloseableIterable<Entity> entities = graphStore.get(singleton((EntityIndex) o), null, auths);
    Iterator<Entity> itr = entities.iterator();
    if (itr.hasNext()) {
      EntityEdge toReturn = new EntityEdge(itr.next(), graphStore, auths);
      entities.closeQuietly();
      return toReturn;
    }
    return null;
  }

  @Override
  public void removeEdge(Edge edge) {
    throw new UnsupportedOperationException("The Calrissian EntityGraph is immutable. Use the GraphStore API to modify the graph.");
  }

  @Override
  public CloseableIterable<Edge> getEdges() {
    CloseableIterable<Entity> entities = graphStore.getAllByType(edgeTypes, null, auths);
    return transform(entities, new EdgeEntityXform(graphStore, auths));
  }

  @Override
  public CloseableIterable<Edge> getEdges(String s, Object o) {
    CloseableIterable<Entity> entities = graphStore.query(edgeTypes, new QueryBuilder().eq(s, o).build(), null, auths);
    return transform(entities, new EdgeEntityXform(graphStore, auths));
  }

  @Override
  public GraphQuery query() {
    return new EntityGraphQuery(graphStore, vertexTypes, edgeTypes, auths);
  }

  @Override
  public void shutdown() {

  }

  public static class EdgeEntityXform implements Function<Entity, Edge> {

    private GraphStore graphStore;
    private Auths auths;

    public EdgeEntityXform(GraphStore graphStore, Auths auths) {
      this.graphStore = graphStore;
      this.auths = auths;
    }

    @Override
    public Edge apply(Entity entity) {
      return new EntityEdge(entity, graphStore, auths);
    }
  }


  public static class VertexEntityXform implements Function<Entity, Vertex> {

    private GraphStore graphStore;
    private Auths auths;

    public VertexEntityXform(GraphStore graphStore, Auths auths) {
      this.graphStore = graphStore;
      this.auths = auths;
    }

    @Override
    public Vertex apply(Entity entity) {
      return new EntityVertex(entity, graphStore, auths);
    }
  }

  public static class EntityIndexXform implements Function<Element, EntityIndex> {
    @Override
    public EntityIndex apply(Element element) {
      return new EntityIndex(((EntityElement) element).getEntity());
    }
  }

  public static class EdgeToVertexIndexXform implements Function<Edge, EntityIndex> {

    private EntityVertex v;

    public EdgeToVertexIndexXform(EntityVertex v) {
      this.v = v;
    }

    @Override
    public EntityIndex apply(Edge element) {
      EntityRelationship tail = (((EntityEdge)element).getEntity().<EntityRelationship>get(TAIL)).getValue();
      EntityRelationship head = (((EntityEdge)element).getEntity().<EntityRelationship>get(HEAD)).getValue();
      EntityRelationship finalRelationship = (tail.getTargetType().equals(v.getEntity().getType()) &&
              tail.getTargetId().equals(v.getEntity().getId())) ? head : tail;
      return new EntityIndex(finalRelationship.getTargetType(), finalRelationship.getTargetId());
    }
  }

  public static class EntityFilterPredicate implements com.google.common.base.Predicate<Element> {

    Criteria criteria;

    public EntityFilterPredicate(Criteria criteria) {
      this.criteria = criteria;
    }

    @Override
    public boolean apply(Element element) {
      return criteria.matches(((EntityElement) element).getEntity());
    }
  }


  @Override
  public String toString() {
    return getClass().getSimpleName().toLowerCase() + "{" +
            "graphStore=" + graphStore +
            ", vertexTypes=" + vertexTypes +
            ", edgeTypes=" + edgeTypes +
            ", auths=" + auths +
            '}';
  }
}
