package org.calrissian.accumulorecipes.eventstore;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.criteria.domain.Node;
import org.calrissian.mango.collect.CloseableIterator;

import java.util.Collection;
import java.util.Date;

public interface EventStore {

    void put(Collection<StoreEntry> events) throws Exception;

    void shutdown() throws Exception;

    CloseableIterator<StoreEntry> query(Date start, Date end, Node node, Authorizations auths);

    StoreEntry get(String uuid, Authorizations auths);
}
