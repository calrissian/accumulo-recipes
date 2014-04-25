package org.calrissian.accumulorecipes.temporal.lastn;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.Collection;
import java.util.Date;

public interface TemporalLastNStore {

    void put(String group, StoreEntry entry);

    CloseableIterable<StoreEntry> get(Date start, Date stop, Collection<String> groups, int n, Auths auths);
}
