package org.calrissian.accumulorecipes.graphstore.impl;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.entitystore.model.EntityRelationship;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.model.EdgeEntity;
import org.calrissian.accumulorecipes.graphstore.support.TupleCollectionCriteriaPredicate;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.types.exception.TypeDecodingException;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang.StringUtils.defaultString;
import static org.calrissian.accumulorecipes.commons.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.commons.support.Constants.EMPTY_VALUE;
import static org.calrissian.accumulorecipes.entitystore.model.RelationshipTypeEncoder.ALIAS;
import static org.calrissian.accumulorecipes.graphstore.model.Direction.IN;
import static org.calrissian.accumulorecipes.graphstore.model.Direction.OUT;
import static org.calrissian.accumulorecipes.graphstore.model.EdgeEntity.*;
import static org.calrissian.mango.accumulo.Scanners.closeableIterable;
import static org.calrissian.mango.collect.CloseableIterables.*;
import static org.calrissian.mango.criteria.support.NodeUtils.criteriaFromNode;

public class AccumuloEntityGraphStore extends AccumuloEntityStore implements GraphStore {

  public static final String DEFAULT_TABLE_NAME = "entityStore_graph";

  public static final int DEFAULT_BUFFER_SIZE = 50;

  protected String table;
  protected int bufferSize = DEFAULT_BUFFER_SIZE;

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

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  @Override
  public CloseableIterable<EdgeEntity> adjacentEdges(Iterable<EntityIndex> fromVertices,
                                                 Node query,
                                                 Direction direction,
                                                 Set<String> labels,
                                                 final Auths auths) {
    checkNotNull(labels);
    final CloseableIterable<Entity> entities = adjacencies(fromVertices, query, direction, labels, auths, true);
    return transform(entities, new Function<Entity, EdgeEntity>() {
      @Override
      public EdgeEntity apply(Entity entity) {
        return new EdgeEntity(entity);
      }
    });
  }

  @Override
  public CloseableIterable<EdgeEntity> adjacentEdges(Iterable<EntityIndex> fromVertices, Node query, Direction direction, final Auths auths) {
    final CloseableIterable<Entity> entities = adjacencies(fromVertices, query, direction, null, auths, true);
    return transform(entities, new Function<Entity, EdgeEntity>() {
      @Override
      public EdgeEntity apply(Entity entity) {
        return new EdgeEntity(entity);
      }
    });
  }

  @Override
  public CloseableIterable<Entity> adjacencies(Iterable<EntityIndex> fromVertices,
                                               Node query, Direction direction,
                                               Set<String> labels,
                                               final Auths auths) {
    checkNotNull(labels);
    return adjacencies(fromVertices, query, direction, labels, auths, false);
  }

  @Override
  public CloseableIterable<Entity> adjacencies(Iterable<EntityIndex> fromVertices, Node query, Direction direction, final Auths auths) {
    return adjacencies(fromVertices, query, direction, null, auths, false);
  }

  private CloseableIterable<Entity> adjacencies(Iterable<EntityIndex> fromVertices,
                                                Node query, Direction direction,
                                                Set<String> labels,
                                                final Auths auths, final boolean edges) {
    checkNotNull(fromVertices);
    checkNotNull(auths);

    TupleCollectionCriteriaPredicate filter =
            query != null ? new TupleCollectionCriteriaPredicate(criteriaFromNode(query)) : null;

    // this one is fairly easy- return the adjacent edges that match the given query
    try {
      BatchScanner scanner = getConnector().createBatchScanner(table, auths.getAuths(), getConfig().getMaxQueryThreads());

      Collection<Range> ranges = new ArrayList<Range>();
      for(EntityIndex entity : fromVertices) {
        String row = ENTITY_TYPES.encode(new EntityRelationship(entity.getType(), entity.getId()));
        if(labels != null) {
          for(String label : labels)
            populateRange(ranges, row, direction, label);
        } else
          populateRange(ranges, row, direction, null);
      }

      scanner.setRanges(ranges);

      /**
       * This partitions the initial Accumulo rows in the scanner into buffers of <bufferSize> so that the full entities
       * can be grabbed from the server in batches instead of one at a time.
       */
      CloseableIterable<Entity> entities = concat(transform(partition(closeableIterable(scanner), bufferSize),
          new Function<List<Map.Entry<Key, Value>>, CloseableIterable<Entity>>() {
            @Override
            public CloseableIterable<Entity> apply(List<Map.Entry<Key, Value>> entries) {
              CloseableIterable<Entity> entites = get(Iterables.transform(entries, new EdgeRowXform(edges)), null, auths);
              return entites;
            }
          }
        )
      );

      if(filter != null)
        return filter(entities, filter);
      else
        return entities;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void populateRange(Collection<Range> ranges, String row, Direction direction, String label) {
    if(direction == Direction.IN || direction == Direction.BOTH)
      ranges.add(Range.prefix(row, Direction.IN.toString() + DELIM + defaultString(label)));
    if(direction == Direction.OUT || direction == Direction.BOTH) {
      ranges.add(Range.prefix(row, Direction.OUT.toString() + DELIM + defaultString(label)));
    }
  }

  /**
   * Extracts an edge/vertex (depending on what is requested) on the far side of a given vertex
   */
  private class EdgeRowXform implements Function<Map.Entry<Key, Value>, EntityIndex> {

    private boolean edges;

    private EdgeRowXform(boolean edges) {
      this.edges = edges;
    }

    @Override
    public EntityIndex apply(Map.Entry<Key, Value> keyValueEntry) {
      String cq = keyValueEntry.getKey().getColumnQualifier().toString();
      int startIdx = edges ? 0 : cq.indexOf(DELIM)+1;
      int endIdx = edges ? cq.indexOf(DELIM) : cq.length();
      String edge = cq.substring(startIdx, endIdx);
      try {
        EntityRelationship rel = (EntityRelationship) ENTITY_TYPES.decode(ALIAS, edge);
        return new EntityIndex(rel.getTargetType(), rel.getTargetId());
      } catch (TypeDecodingException e) {
        throw new RuntimeException(e);
      }
    }
  };

  @Override
  public void save(Iterable<Entity> entities) {
    super.save(entities);

    // here is where we want to store the edge index everytime an entity is saved
    for(Entity entity : entities) {
      if(isEdge(entity)) {

        EntityRelationship edgeRelationship = new EntityRelationship(entity);
        EntityRelationship toVertex = entity.<EntityRelationship>get(TAIL).getValue();
        EntityRelationship fromVertex = entity.<EntityRelationship>get(HEAD).getValue();

        String toVertexVis = entity.get(TAIL).getVisibility();
        String fromVertexVis = entity.get(HEAD).getVisibility();

        String label = entity.<String>get(LABEL).getValue();
        try {
          String fromEncoded = ENTITY_TYPES.encode(fromVertex);
          String toEncoded = ENTITY_TYPES.encode(toVertex);
          String edgeEncoded = ENTITY_TYPES.encode(edgeRelationship);
          Mutation forward = new Mutation(fromEncoded);
          Mutation reverse = new Mutation(toEncoded);

          forward.put(new Text(OUT.toString() + DELIM + label), new Text(edgeEncoded + DELIM + toEncoded),
                  new ColumnVisibility(toVertexVis), EMPTY_VALUE);
          reverse.put(new Text(IN.toString() + DELIM + label), new Text(edgeEncoded + DELIM + fromEncoded),
                  new ColumnVisibility(fromVertexVis), EMPTY_VALUE);

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

  @Override
  public void shutdown() throws MutationsRejectedException {
    super.shutdown();
    writer.close();
  }

  private boolean isEdge(Entity entity) {
    return entity.get(HEAD) != null &&
           entity.get(TAIL) != null &&
           entity.get(LABEL) != null;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName().toLowerCase() + "{" +
            "table='" + table + '\'' +
            ", bufferSize=" + bufferSize +
            ", writer=" + writer +
            '}';
  }
}
