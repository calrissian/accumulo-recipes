package org.calrissian.accumulorecipes.commons.support.qfd;

import java.util.List;
import java.util.Set;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.entity.Entity;

public interface QfdStore<ET,EIT> {

    void save(Iterable<ET> items);

    void flush() throws Exception;

    void shutdown() throws Exception;

    CloseableIterable<Entity> get(List<EIT> typesAndIds, Set<String> selectFields, Auths auths);

    CloseableIterable<Entity> get(List<EIT> typesAndIds, Auths auths);
}
