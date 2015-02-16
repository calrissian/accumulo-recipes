/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.commons.support.qfd;

import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_K;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_V;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex.INDEX_SEP;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.StringUtils;

/**
 * A tuple index key represents an entry in an index table. The reason a separate object is used
 * instead of just representing this information in a {@link LeafNode} is because it's possible
 * several different leaf nodes could benefit from sharing state of the same index key in a
 * query. A good example could be this:
 *
 * (x != 1 and x < 5) or (x != 1 and x > 5)
 *
 * In the query above, we should be able to perform only one index from the index table- that is,
 * we know we can just find all the shards that have a key x. We represent that information in
 * the implementers of this interface so that we wouldn't need to fetch/store cardinality and other
 * index information more times than is necessary.
 */
public class TupleIndexKey {

    protected String key;
    protected String normalizedValue;
    protected String alias;
    protected String shard;

    protected TupleIndexKey() {
    }

    public TupleIndexKey(String key, String value, String alias) {
      this.key = key;
      this.normalizedValue = value;
      this.alias = alias;
    }

    public TupleIndexKey(Key key) {

        String row = key.getRow().toString();
        String parts[] = StringUtils.splitByWholeSeparatorPreserveAllTokens(row, INDEX_SEP);
        int firstNBIdx = parts[3].indexOf(NULL_BYTE);

        if (row.startsWith(INDEX_V)) {
            int lastNBIdx = parts[3].lastIndexOf(NULL_BYTE);
            this.alias = parts[2];
            this.key = parts[3].substring(0, firstNBIdx);
            this.normalizedValue = parts[3].substring(firstNBIdx + 1, lastNBIdx);
            this.shard = parts[3].substring(lastNBIdx + 1, parts[3].length());
        } else if (row.startsWith(INDEX_K)) {
            this.key = parts[2];
            this.alias = parts[3].substring(0, firstNBIdx);
            this.shard = parts[3].substring(firstNBIdx + 1, parts[3].length());
        }
    }

    public String getKey() {
        return key;
    }

    public String getNormalizedValue() {
        return normalizedValue;
    }

    public String getAlias() {
        return alias;
    }

    public String getShard() {
      return shard;
    }

    /**
     * It's important for subclasses that override this method to be aware that shard has
     * been purposefully left out of the comparison.
     * @return
     */
    @Override
    public boolean equals(Object o) {

      if (this == o) {
        return true;
      } else if(o == null)
        return false;

      TupleIndexKey that = (TupleIndexKey) o;

      if (alias != null ? !alias.equals(that.alias) : that.alias != null) {
        return false;
      }
      if (key != null ? !key.equals(that.key) : that.key != null) {
        return false;
      }
      if (normalizedValue != null ? !normalizedValue.equals(that.normalizedValue) : that.normalizedValue != null) {
        return false;
      }

      return true;
    }

    /**
     * It's important for subclasses that override this method to be aware that
     * the shard is purposefully left out of the calculation.
     * @return
     */
    @Override
    public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (normalizedValue != null ? normalizedValue.hashCode() : 0);
      result = 31 * result + (alias != null ? alias.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
        return "BaseCardinalityKey{" +
            "key='" + key + '\'' +
            ", normalizedValue='" + normalizedValue + '\'' +
            ", alias='" + alias + '\'' +
            '}';
  }
}
