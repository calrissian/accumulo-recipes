package org.calrissian.accumulorecipes.lastn;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.collect.CloseableIterator;

public interface LastNStore {

    void put(String index, StoreEntry entry);

    CloseableIterator<StoreEntry> get(String index, Authorizations auths);
}
