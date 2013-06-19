package org.calrissian.accumulorecipes.rangestore;


import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.mango.types.range.ValueRange;

/**
 * A range store is a 1-dimensional NoSQL key/value version of the common interval tree data structure.
 */
public interface RangeStore<T extends Comparable<T>> {

    /**
     * Inserts ranges into the store.
     * @param ranges
     */
    void save(Iterable<ValueRange<T>> ranges);

    /**
     * Deletes ranges from the store.
     * @param ranges
     */
    void delete(Iterable<ValueRange<T>> ranges);

    /**
     * Queries for any ranges that intersect, overlap, or are contained by the given range.
     * @param range
     * @param auths
     * @return
     */
    Iterable<ValueRange<T>> query(ValueRange<T> range, Authorizations auths);

}
