package org.calrissian.accumulorecipes.lastn;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;

import java.util.Iterator;

public interface LastNStore {

    /**
     * Puts a StoreEntry into the Last N store under the specified index. The Last N items returned are all grouped
     * underneath the index.
     * @param index
     * @param entry
     */
    void put(String index, StoreEntry entry);

    /**
     * Returns the last N store entries under the specified index- starting with the most recent.
     * @param index
     * @param auths
     * @return
     */
    Iterator<StoreEntry> get(String index, Authorizations auths);
}
