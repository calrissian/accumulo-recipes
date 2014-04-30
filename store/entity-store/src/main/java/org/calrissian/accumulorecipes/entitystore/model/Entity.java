package org.calrissian.accumulorecipes.entitystore.model;

import com.google.common.collect.Iterables;
import org.calrissian.accumulorecipes.commons.domain.TupleCollection;
import org.calrissian.mango.domain.Tuple;

import java.util.*;

import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.unmodifiableCollection;

public class Entity implements TupleCollection{

  private String id;
  private String type;

  private Map<String,Set<Tuple>> tuples;

  public Entity(String type, String id) {
    this.id = id;
    this.type = type;
    this.tuples = new HashMap<String, Set<Tuple>>();
  }

  public String getId() {
    return id;
  }

  public String getType() {
    return type;
  }

  public void put(Tuple tuple) {
    Set<Tuple> keyedTuples = tuples.get(tuple.getKey());
    if(keyedTuples == null) {
      keyedTuples = new HashSet<Tuple>();
      tuples.put(tuple.getKey(), keyedTuples);
    }
    keyedTuples.add(tuple);
  }

  public void putAll(Iterable<Tuple> tuples) {
    for(Tuple tuple : tuples)
      put(tuple);
  }


  /**
   * Returns all the getTuples set on the current entity
   */
  public Collection<Tuple> getTuples() {
    Collection<Tuple> tupleCollection = new LinkedList<Tuple>();
    addAll(tupleCollection, concat(tuples.values()));
    return unmodifiableCollection(tupleCollection);
  }

  /**
   * A get operation for multi-valued keys
   */
  public Collection<Tuple> getAll(String key) {
    return tuples.get(key);
  }

  /**
   * A get operation for single-valued keys
   */
  public Tuple get(String key) {
    return tuples.get(key) != null ? tuples.get(key).iterator().next() : null;
  }
 }
