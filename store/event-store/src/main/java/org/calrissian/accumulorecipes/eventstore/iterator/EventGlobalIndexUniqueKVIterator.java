package org.calrissian.accumulorecipes.eventstore.iterator;

import org.calrissian.accumulorecipes.commons.iterators.FirstEntryInPrefixedRowIterator;

import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;

/**
 * Created by cjnolet on 11/9/14.
 */
public class EventGlobalIndexUniqueKVIterator extends FirstEntryInPrefixedRowIterator {

  @Override protected String getPrefix(String rowStr) {

    int idx = rowStr.lastIndexOf(NULL_BYTE);
    String substr = rowStr.substring(0, idx);
    return substr;
  }
}
