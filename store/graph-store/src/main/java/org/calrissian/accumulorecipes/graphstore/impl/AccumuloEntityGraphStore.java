package org.calrissian.accumulorecipes.graphstore.impl;

import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.accumulorecipes.entitystore.model.EntityRelationship;
import org.calrissian.accumulorecipes.graphstore.GraphStore;
import org.calrissian.accumulorecipes.graphstore.model.Direction;
import org.calrissian.accumulorecipes.graphstore.support.TupleCollectionCriteriaPredicate;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.CloseableIterables;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.types.exception.TypeDecodingException;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Iterables.transform;
import static org.calrissian.accumulorecipes.commons.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.commons.support.Constants.EMPTY_VALUE;
import static org.calrissian.accumulorecipes.entitystore.model.RelationshipTypeEncoder.ALIAS;
import static org.calrissian.accumulorecipes.graphstore.model.Direction.IN;
import static org.calrissian.accumulorecipes.graphstore.model.Direction.OUT;
import static org.calrissian.accumulorecipes.graphstore.model.EdgeEntity.*;
import static org.calrissian.mango.accumulo.Scanners.closeableIterable;
import static org.calrissian.mango.collect.CloseableIterables.*;
import static org.calrissian.mango.criteria.utils.NodeUtils.criteriaFromNode;

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
  public CloseableIterable<Entity> adjacentEdges(Iterable<Entity> fromVertices,
                                                 Node query,
                                                 Direction direction,
                                                 Set<String> labels,
                                                 final Auths auths) {

    checkNotNull(labels);
    checkNotNull(fromVertices);
    checkNotNull(auths);

    TupleCollectionCriteriaPredicate filter =
            query != null ? new TupleCollectionCriteriaPredicate(criteriaFromNode(query)) : null;

    // this one is fairly easy- return the adjacent edges that match the given query
    try {
      BatchScanner scanner = getConnector().createBatchScanner(table, auths.getAuths(), getConfig().getMaxQueryThreads());

      Collection<Range> ranges = new ArrayList<Range>();
      for(Entity entity : fromVertices) {

        String row = ENTITY_TYPES.encode(new EntityRelationship(entity));
        for(String label : labels) {
          if(direction == Direction.IN || direction == Direction.BOTH)
            ranges.add(Range.prefix(row, Direction.IN.toString() + DELIM + label));
          if(direction == Direction.OUT || direction == Direction.BOTH)
            ranges.add(Range.prefix(row, Direction.OUT.toString() + DELIM + label));
        }
      }

      scanner.setRanges(ranges);

      CloseableIterable<Entity> entities = concat(wrap(transform(partition(closeableIterable(scanner), 50),
          new Function<List<Map.Entry<Key, Value>>, CloseableIterable<Entity>>() {

            @Override
            public CloseableIterable<Entity> apply(List<Map.Entry<Key, Value>> entries) {
              return get(transform(entries, new Function<Map.Entry<Key, Value>, EntityIndex>() {
                @Override
                public EntityIndex apply(Map.Entry<Key, Value> keyValueEntry) {
                  String cq = keyValueEntry.getKey().getColumnQualifier().toString();
                  String edge = cq.substring(0, cq.indexOf(DELIM));
                  try {
                    EntityRelationship rel = (EntityRelationship) ENTITY_TYPES.decode(ALIAS, edge);
                    return new EntityIndex(rel.getTargetType(), rel.getTargetId());
                  } catch (TypeDecodingException e) {
                    throw new RuntimeException(e);
                  }
                }
              }), null, auths);
            }
          }
        )
      ));

      if(filter != null)
        return CloseableIterables.filter(entities, filter);
      else
        return entities;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CloseableIterable<Entity> adjacentEdges(Iterable<Entity> fromVertices, Node query, Direction direction, final Auths auths) {
    checkNotNull(fromVertices);
    checkNotNull(auths);

    TupleCollectionCriteriaPredicate filter =
            query != null ? new TupleCollectionCriteriaPredicate(criteriaFromNode(query)) : null;

    // this one is fairly easy- return the adjacent edges that match the given query
    try {
      BatchScanner scanner = getConnector().createBatchScanner(table, auths.getAuths(), getConfig().getMaxQueryThreads());

      Collection<Range> ranges = new ArrayList<Range>();
      for(Entity entity : fromVertices) {

        String row = ENTITY_TYPES.encode(new EntityRelationship(entity));
        if(direction == Direction.IN || direction == Direction.BOTH)
          ranges.add(Range.prefix(row, Direction.IN.toString()));
        if(direction == Direction.OUT || direction == Direction.BOTH)
          ranges.add(Range.prefix(row, Direction.OUT.toString()));
      }

      scanner.setRanges(ranges);

      CloseableIterable<Entity> entities = concat(wrap(transform(partition(closeableIterable(scanner), 50),
                      new Function<List<Map.Entry<Key, Value>>, CloseableIterable<Entity>>() {

                        @Override
                        public CloseableIterable<Entity> apply(List<Map.Entry<Key, Value>> entries) {
                          return get(transform(entries, new Function<Map.Entry<Key, Value>, EntityIndex>() {
                            @Override
                            public EntityIndex apply(Map.Entry<Key, Value> keyValueEntry) {
                              String cq = keyValueEntry.getKey().getColumnQualifier().toString();
                              String edge = cq.substring(0, cq.indexOf(DELIM));
                              try {
                                EntityRelationship rel = (EntityRelationship) ENTITY_TYPES.decode(ALIAS, edge);
                                return new EntityIndex(rel.getTargetType(), rel.getTargetId());
                              } catch (TypeDecodingException e) {
                                throw new RuntimeException(e);
                              }
                            }
                          }), null, auths);
                        }
                      }
              )
      ));

      if(filter != null)
        return CloseableIterables.filter(entities, filter);
      else
        return entities;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CloseableIterable<Entity> adjacencies(Iterable<Entity> fromVertices,
                                               Node query, Direction direction,
                                               Set<String> labels,
                                               Auths auths) {
    return null;
  }

  @Override
  public CloseableIterable<Entity> adjacencies(Iterable<Entity> fromVertices, Node query, Direction direction, Auths auths) {
    return null;
  }

  @Override
  public void save(Iterable<Entity> entities) {
    super.save(entities);

    // here is where we want to store the edge index everytime an entity is saved
    for(Entity entity : entities) {
      if(isEdge(entity)) {

        EntityRelationship edgeRelationship = new EntityRelationship(entity);
        EntityRelationship toVertex = entity.<EntityRelationship>get(TAIL).getValue();
        EntityRelationship fromVertex = entity.<EntityRelationship>get(HEAD).getValue();
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
