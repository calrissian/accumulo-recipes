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
package org.calrissian.accumulorecipes.eventstore.support;

import com.esotericsoftware.kryo.Kryo;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.commons.support.qfd.QfdHelper;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionQueryXform;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionWholeColFXform;
import org.calrissian.mango.domain.BaseEvent;
import org.calrissian.mango.domain.Event;
import org.calrissian.mango.types.TypeRegistry;

import java.util.Set;

import static org.calrissian.accumulorecipes.commons.support.Constants.EMPTY_VALUE;


public class EventQfdHelper extends QfdHelper<Event> {

  public EventQfdHelper(Connector connector, String indexTable, String shardTable, StoreConfig config,
                        ShardBuilder<Event> shardBuilder, TypeRegistry<String> typeRegistry, KeyValueIndex<Event> keyValueIndex)
          throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
    super(connector, indexTable, shardTable, config, shardBuilder, typeRegistry, keyValueIndex);
  }

  @Override
  protected String buildId(Event item) {
    return item.getId();
  }

  @Override
  protected Value buildValue(Event item) {
    return EMPTY_VALUE; // placeholder for things like dynamic age-off
  }

  @Override
  protected long buildTimestamp(Event item) {
    return item.getTimestamp();
  }

  public QueryXform buildQueryXform(Set<String> selectFields) {
    return new QueryXform(getKryo(), getTypeRegistry(), selectFields);
  }

  public WholeColFXForm buildWholeColFXform() {
    return new WholeColFXForm(getKryo(), getTypeRegistry());
  }


  @Override
  protected void configureIndexTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {}

  @Override
  protected void configureShardTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {}

  public static class QueryXform extends KeyToTupleCollectionQueryXform<Event> {

    public QueryXform(Kryo kryo, TypeRegistry<String> typeRegistry, Set<String> selectFields) {
      super(kryo, typeRegistry, selectFields);
    }

    @Override
    protected Event buildTupleCollectionFromKey(Key k) {
      return new BaseEvent(k.getColumnFamily().toString(), k.getTimestamp());
    }
  }

  public static class WholeColFXForm extends KeyToTupleCollectionWholeColFXform<Event> {
    public WholeColFXForm(Kryo kryo, TypeRegistry<String> typeRegistry) {
      super(kryo, typeRegistry, null);
    }

    @Override
    protected Event buildEntryFromKey(Key k) {
      return new BaseEvent(k.getColumnFamily().toString(), k.getTimestamp());
    }
  }
}
