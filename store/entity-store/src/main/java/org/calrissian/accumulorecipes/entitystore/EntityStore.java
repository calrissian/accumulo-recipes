package org.calrissian.accumulorecipes.entitystore;


import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.model.Entity;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;

import java.util.Set;

public interface EntityStore {

  void save(Iterable<Entity> entities);

  CloseableIterable<Entity> get(Iterable<EntityIndex> typesAndIds, Set<String> selectFields, Auths auths);

  CloseableIterable<Entity> getAllByType(Set<String> types, Set<String> selectFields, Auths auths);

  CloseableIterable<Entity> query(Set<String> types, Node query, Set<String> selectFields, Auths auths);

  void shutdown();
}
