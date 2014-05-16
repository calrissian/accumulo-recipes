package org.calrissian.accumulorecipes.entitystore.support;

import org.apache.accumulo.core.data.Key;
import org.calrissian.accumulorecipes.commons.support.criteria.BaseCardinalityKey;

import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_K;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_V;

public class EntityCardinalityKey extends BaseCardinalityKey {

  public EntityCardinalityKey(String key, String value, String alias) {
    super(key, value, alias);
  }

  public EntityCardinalityKey(Key key) {

    String row = key.getRow().toString();
    if(row.indexOf("_" + INDEX_V + "_") != -1) {
      this.alias = row.substring(row.indexOf("_" + INDEX_V + "_")+3, row.indexOf("__"));
      this.normalizedValue = row.substring(row.indexOf("__")+2, row.length());
      this.key = key.getColumnFamily().toString();
    } else if(row.indexOf("_" + INDEX_K + "_") != -1) {
      this.key = row.substring(row.indexOf("_" + INDEX_K + "_")+3, row.length());
      this.alias = key.getColumnFamily().toString();
    }
  }
}
