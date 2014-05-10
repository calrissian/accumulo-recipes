package org.calrissian.accumulorecipes.graphstore.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.model.Entity;
import org.calrissian.accumulorecipes.entitystore.model.EntityRelationship;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.model.Edge;
import org.calrissian.accumulorecipes.graphstore.model.Vertex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;

import java.util.Set;

import static org.calrissian.accumulorecipes.commons.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.commons.support.Constants.EMPTY_VALUE;
import static org.calrissian.accumulorecipes.graphstore.model.Direction.IN;
import static org.calrissian.accumulorecipes.graphstore.model.Direction.OUT;
import static org.calrissian.accumulorecipes.graphstore.model.Edge.*;

public class AccumuloEntityGraphStore extends AccumuloEntityStore implements GraphStore {

  public static final String DEFAULT_TABLE_NAME = "entityStore_graph";

  protected String table;

  protected BatchWriter writer;

  public AccumuloEntityGraphStore(Connector connector)
          throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
    super(connector);
    table = DEFAULT_TABLE_NAME;
    init();
  }

  public AccumuloEntityGraphStore(Connector connector, String indexTable, String shardTable, String edgeTable, StoreConfig config)
          throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
    super(connector, indexTable, shardTable, config);
    table = edgeTable;
    init();
  }

  private void init() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
    if(!getConnector().tableOperations().exists(table))
      getConnector().tableOperations().create(table);

    writer = getConnector().createBatchWriter(table, getConfig().getMaxMemory(), getConfig().getMaxLatency(),
            getConfig().getMaxWriteThreads());
  }

  @Override
  public CloseableIterable<Edge> adjacentEdges(Iterable<Vertex> fromVertices,
                                               Node query,
                                               Direction direction,
                                               Set<String> labels,
                                               Auths auths) {

    // this one is fairly easy- return the adjacent edges\
    return null;
  }

  @Override
  public CloseableIterable<Edge> adjacentEdges(Iterable<Vertex> fromVertices, Node query, Direction direction, Auths auths) {
    return null;
  }

  @Override
  public CloseableIterable<Vertex> adjacencies(Iterable<Vertex> fromVertices,
                                               Node query, Direction direction,
                                               Set<String> labels,
                                               Auths auths) {
    return null;
  }

  @Override
  public CloseableIterable<Vertex> adjacencies(Iterable<Vertex> fromVertices, Node query, Direction direction, Auths auths) {
    return null;
  }

  @Override
  public void save(Iterable<Entity> entities) {
    super.save(entities);

    // here is where we want to store the edge index everytime an entity is saved
    for(Entity entity : entities) {
      if(isEdge(entity)) {

        EntityRelationship edgeRelationship = new EntityRelationship(entity);
        EntityRelationship toVertex = entity.<EntityRelationship>get(HEAD).getValue();
        EntityRelationship fromVertex = entity.<EntityRelationship>get(TAIL).getValue();
        String label = entity.<String>get(LABEL).getValue();
        try {
          String fromEncoded = ENTITY_TYPES.encode(fromVertex);
          String toEncoded = ENTITY_TYPES.encode(toVertex);
          String edgeEncoded = ENTITY_TYPES.encode(edgeRelationship);
          Mutation forward = new Mutation(fromEncoded);
          Mutation reverse = new Mutation(toEncoded);


          // todo: the visibility tree needs to be combined for the LABEL, TAIL, and HEAD

          forward.put(new Text(OUT.toString() + DELIM + label), new Text(edgeEncoded + DELIM + toEncoded), EMPTY_VALUE);
          reverse.put(new Text(IN.toString() + DELIM + label), new Text(edgeEncoded + DELIM + fromEncoded), EMPTY_VALUE);

          writer.addMutation(forward);
          writer.addMutation(reverse);

        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    try {
      writer.flush();
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isEdge(Entity entity) {
    return entity.get(HEAD) != null &&
           entity.get(TAIL) != null &&
           entity.get(LABEL) != null;
  }
}
