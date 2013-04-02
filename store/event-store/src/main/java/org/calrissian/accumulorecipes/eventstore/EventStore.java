package org.calrissian.accumulorecipes.eventstore;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.criteria.domain.Node;
import org.calrissian.mango.collect.CloseableIterator;

import java.util.Collection;
import java.util.Date;

/**
 * An event store generally holds temporal keys/values.
 */
public interface EventStore {

    /**
     * Persists a collection of StoreEntry objects into the event store
     * @param events
     * @throws Exception
     */
    void put(Collection<StoreEntry> events) throws Exception;

    /**
     * Shut down the store and cleanup any resources being held
     * @throws Exception
     */
    void shutdown() throws Exception;

    /**
     * Query the store using criteria specified
     * @param start
     * @param end
     * @param node
     * @param auths
     * @return
     */
    CloseableIterator<StoreEntry> query(Date start, Date end, Node node, Authorizations auths);

    /**
     * Get a specific StoreEntry with the given ID
     * @param uuid
     * @param auths
     * @return
     */
    StoreEntry get(String uuid, Authorizations auths);
}
