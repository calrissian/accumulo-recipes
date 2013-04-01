package org.calrissian.accumulorecipes.eventstore;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.eventstore.domain.Event;
import org.calrissian.criteria.domain.Node;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.Collection;

public interface EventStore {

    void put(Collection<Event> events) throws Exception;

    //TODO: Replace Authorizations with some intermediary Auths class
    CloseableIterable<Event> query(Node node, Authorizations auths);
}
