package org.calrissian.accumulorecipes.graphstore.support;

import com.google.common.base.Predicate;
import org.calrissian.mango.criteria.domain.criteria.Criteria;
import org.calrissian.mango.domain.TupleCollection;

public class TupleCollectionCriteriaPredicate implements Predicate<TupleCollection> {

  private Criteria criteria;

  public TupleCollectionCriteriaPredicate(Criteria criteria) {
    this.criteria = criteria;
  }

  @Override
  public boolean apply(TupleCollection storeEntry) {
    return criteria.apply(storeEntry);
  }
}
