package org.calrissian.accumulorecipes.commons.domain;

import org.calrissian.mango.domain.Tuple;

import java.util.Collection;

public interface TupleCollection {

  public void put(Tuple tuple);

  public void putAll(Iterable<Tuple> tuples);

  public Collection<Tuple> getTuples();

  public Collection<Tuple> getAll(String key);

  public Tuple get(String key);
}