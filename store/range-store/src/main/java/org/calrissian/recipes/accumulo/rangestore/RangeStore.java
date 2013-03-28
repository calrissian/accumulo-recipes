package org.calrissian.recipes.accumulo.rangestore;


import mango.types.range.ValueRange;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.Iterator;

public interface RangeStore {

    void initialize();

    void insert(Collection<ValueRange<Long>> ranges);

    Iterator<ValueRange<Long>> query(ValueRange<Long> range, Authorizations auths);

    void shutdown();
}
