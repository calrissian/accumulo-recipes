package org.calrissian.accumulorecipes.rangestore;


import com.google.common.collect.Range;
import org.apache.accumulo.core.security.Authorizations;

/**
 * A range store is a 1-dimensional NoSQL key/value version of the common interval tree data structure.
 */
public interface RangeStore {

    /**
     * Inserts ranges into the store.
     * @param ranges
     */
    void save(Iterable<Range<Long>> ranges);

    /**
     * Deletes ranges from the store.
     * @param ranges
     */
    void delete(Iterable<Range<Long>> ranges);

    /**
     * Queries for any ranges that intersect, overlap, or are contained by the given range.
     * @param range
     * @param auths
     * @return
     */
    Iterable<Range<Long>> query(Range<Long> range, Authorizations auths);

}
