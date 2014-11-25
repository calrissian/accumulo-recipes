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

import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.commons.support.qfd.QfdHelper;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionQueryXform;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionWholeColFXform;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.PREFIX_E;

public class EventQfdHelper extends QfdHelper<Event> {

    public EventQfdHelper(Connector connector, String indexTable, String shardTable, StoreConfig config,
                          ShardBuilder<Event> shardBuilder, TypeRegistry<String> typeRegistry, KeyValueIndex<Event> keyValueIndex)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super(connector, indexTable, shardTable, config, shardBuilder, typeRegistry, keyValueIndex);
    }

    @Override
    protected String buildId(Event item) {
        return PREFIX_E + ONE_BYTE + item.getId() + ONE_BYTE + item.getTimestamp();
    }

    public QueryXform buildQueryXform(Set<String> selectFields) {
        return new QueryXform(getKryo(), getTypeRegistry(), selectFields, getMetadataSerDe());
    }

    public WholeColFXForm buildWholeColFXform() {
        return new WholeColFXForm(getKryo(), getTypeRegistry(), null, getMetadataSerDe());
    }


    @Override
    protected void configureIndexTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    }


    @Override
    protected void configureShardTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    }

    public static class QueryXform extends KeyToTupleCollectionQueryXform<Event> {

        public QueryXform(Kryo kryo, TypeRegistry<String> typeRegistry, Set<String> selectFields, MetadataSerDe metadataSerDe) {
            super(kryo, typeRegistry, selectFields, metadataSerDe);
        }

        @Override
        protected Event buildTupleCollectionFromKey(Key k) {
            return createEventFromkey(k);
        }
    }

    public static class WholeColFXForm extends KeyToTupleCollectionWholeColFXform<Event> {
        public WholeColFXForm(Kryo kryo, TypeRegistry<String> typeRegistry, Set<String> selectFields, MetadataSerDe metadataSerDe) {
            super(kryo, typeRegistry, selectFields, metadataSerDe);
        }

        @Override
        protected Event buildEntryFromKey(Key k) {
            return createEventFromkey(k);
        }
    }

    private static final Event createEventFromkey(Key key) {
      String cf = key.getColumnFamily().toString();
      int uuidIdx = cf.indexOf(ONE_BYTE);
      int tsIdx = cf.lastIndexOf(ONE_BYTE);
      String uuid =  cf.substring(uuidIdx+1, tsIdx);
      long ts = Long.parseLong(cf.substring(tsIdx+1, cf.length()));
      return new BaseEvent(uuid, ts);
    }

    public static final Long parseTimestampFromKey(Key k) {

      String cf = k.getColumnFamily().toString();
      String cq = k.getColumnQualifier().toString();

      String toParse = cq;
      if(cf.startsWith(PREFIX_E))
        toParse = cf;

      int idx = toParse.lastIndexOf(ONE_BYTE);
      return Long.parseLong(toParse.substring(idx+1, toParse.length()));
    }

}
