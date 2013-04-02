package org.calrissian.accumulorecipes.rangestore;


import org.calrissian.mango.types.range.ValueRange;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.Iterator;

/**
 * A range store is a 1-dimensional NoSQL key/value version of the common range tree data structure.
 */
public interface RangeStore {

    void initialize();

    void insert(Collection<ValueRange<Long>> ranges);

    Iterator<ValueRange<Long>> query(ValueRange<Long> range, Authorizations auths);

    void shutdown();
}
