package org.calrissian.accumulorecipes.eventstore.support.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.iterators.MetadataExpirationFilter;

import static org.calrissian.accumulorecipes.eventstore.support.EventQfdHelper.parseTimestampFromKey;

public class EventExpirationFilter extends MetadataExpirationFilter {
  @Override
  protected long parseTimestamp(Key k, Value v) {
    return parseTimestampFromKey(k);
  }
}
