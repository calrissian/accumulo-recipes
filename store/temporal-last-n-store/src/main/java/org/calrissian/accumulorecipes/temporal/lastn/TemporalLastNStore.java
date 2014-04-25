package org.calrissian.accumulorecipes.temporal.lastn;

import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.Date;
import java.util.Set;

public interface TemporalLastNStore {

    void put(String group, StoreEntry entry);

    CloseableIterable<StoreEntry> get(Date start, Date stop, Set<String> groups);
}
