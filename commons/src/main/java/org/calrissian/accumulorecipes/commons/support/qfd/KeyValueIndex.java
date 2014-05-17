package org.calrissian.accumulorecipes.commons.support.qfd;

import org.calrissian.mango.domain.TupleCollection;

public interface KeyValueIndex<T extends TupleCollection> {

  void indexKeyValues(Iterable<T> items);

  void commit() throws Exception;
}
