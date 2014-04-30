package org.calrissian.accumulorecipes.commons.domain;

import org.calrissian.mango.domain.Tuple;

public interface TupleCollection {

  public void put(Tuple tuple);

  public void putAll(Iterable<Tuple> tuples);

  public Iterable<Tuple> tuples();

  public Iterable<Tuple> getAll(String key);

  public Tuple get(String key);
}