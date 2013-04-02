package org.calrissian.accumulorecipes.eventstore;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.eventstore.domain.Event;
import org.calrissian.criteria.domain.Node;
import org.calrissian.mango.collect.CloseableIterator;

import java.util.Collection;
import java.util.Date;

public interface EventStore {

    void put(Collection<Event> events) throws Exception;

    void shutdown() throws Exception;

    CloseableIterator<Event> query(Date start, Date end, Node node, Authorizations auths);

    Event get(String uuid, Authorizations auths);
}
