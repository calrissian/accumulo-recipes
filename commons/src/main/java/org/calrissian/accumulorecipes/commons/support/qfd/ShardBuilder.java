package org.calrissian.accumulorecipes.commons.support.qfd;

import org.calrissian.mango.domain.TupleCollection;

public interface ShardBuilder<T extends TupleCollection> {

  String buildShard(T item);
}
