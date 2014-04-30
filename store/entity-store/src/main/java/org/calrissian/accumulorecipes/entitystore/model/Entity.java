package org.calrissian.accumulorecipes.entitystore.model;

import org.calrissian.mango.domain.Tuple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.concat;

public class Entity {

  private String id;
  private String type;

  private Map<String,Set<Tuple>> tuples;

  public Entity(String id, String type) {
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
   * Returns all the tuples set on the current entity
   */
  public Iterable<Tuple> tuples() {
    return concat(tuples.values());
  }

  /**
   * A get operation for multi-valued keys
   */
  public Iterable<Tuple> getAll(String key) {
    return tuples.get(key);
  }

  /**
   * A get operation for single-valued keys
   */
  public Tuple get(String key) {
    return tuples.get(key) != null ? tuples.get(key).iterator().next() : null;
  }
 }
