package org.calrissian.accumulorecipes.eventstore.support;

import org.apache.accumulo.core.data.Key;
import org.calrissian.accumulorecipes.commons.support.criteria.BaseCardinalityKey;

import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_K;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_V;

public class EventCardinalityKey extends BaseCardinalityKey {

  public EventCardinalityKey(Key key) {

    String row = key.getRow().toString();
    if(row.startsWith(INDEX_V)) {

      this.alias = row.substring(row.indexOf("_")+1, row.indexOf("__"));
      this.normalizedValue = row.substring(row.indexOf("__")+2, row.length());
      this.key = key.getColumnFamily().toString();
    } else if(row.startsWith(INDEX_K)) {

      this.key = row.substring(row.indexOf("_"), row.length());
      this.alias = key.getColumnFamily().toString();
    }
  }
}
