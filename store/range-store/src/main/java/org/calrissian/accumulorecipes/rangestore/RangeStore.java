package org.calrissian.accumulorecipes.rangestore;


import org.calrissian.mango.types.range.ValueRange;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.Iterator;

/**
 * A range store is a 1-dimensional NoSQL key/value version of the common range tree data structure.
 */
public interface RangeStore {

    /**
     * Inserts a collection of ranges into the store.
     * @param ranges
     */
    void insert(Collection<ValueRange<Long>> ranges);

    /**
     * Given a range, queries for any ranges that overlap
     * @param range
     * @param auths
     * @return
     */
    Iterator<ValueRange<Long>> query(ValueRange<Long> range, Authorizations auths);

    /**
     * Releases any resources being held
     */
    void shutdown();
}
