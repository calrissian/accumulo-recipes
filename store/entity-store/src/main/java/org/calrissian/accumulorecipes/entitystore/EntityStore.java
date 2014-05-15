package org.calrissian.accumulorecipes.entitystore;


import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.entitystore.model.EntityIndex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Pair;

import java.util.List;
import java.util.Set;

public interface EntityStore {

  void save(Iterable<Entity> entities);

  CloseableIterable<Entity> get(List<EntityIndex> typesAndIds, Set<String> selectFields, Auths auths);

  CloseableIterable<Entity> getAllByType(Set<String> types, Set<String> selectFields, Auths auths);

  CloseableIterable<Entity> query(Set<String> types, Node query, Set<String> selectFields, Auths auths);

  CloseableIterable<Pair<String,String>> keys(String type, Auths auths);

  void delete(Iterable<EntityIndex> typesAndIds, Auths auths);

  void shutdown() throws Exception;
}
