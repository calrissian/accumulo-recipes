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

import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.PREFIX_E;

import com.esotericsoftware.kryo.Kryo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.commons.support.qfd.QfdHelper;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionQueryXform;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionWholeColFXform;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

public class EventQfdHelper extends QfdHelper<Event> {

    public static final String FI_TYPE_KEY_SEP = "__$";

    public EventQfdHelper(Connector connector, String indexTable, String shardTable, StoreConfig config,
        ShardBuilder<Event> shardBuilder, TypeRegistry<String> typeRegistry, KeyValueIndex<Event> keyValueIndex)
        throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super(connector, indexTable, shardTable, config, shardBuilder, typeRegistry, keyValueIndex, new EventNodeToJexl(typeRegistry));
    }

    public static class EventNodeToJexl extends NodeToJexl {

        public EventNodeToJexl(TypeRegistry<String> registry) {
            super(registry);
        }

        @Override
        protected String buildKey(String type, String key) {
            return buildTupleKey(type, key);
        }
    }

    public QueryXform buildQueryXform() {
        return new QueryXform(getKryo(), getTypeRegistry(), getMetadataSerDe());
    }

    public WholeColFXForm buildWholeColFXform() {
        return new WholeColFXForm(getKryo(), getTypeRegistry(), getMetadataSerDe());
    }

    @Override
    protected String buildId(Event item) {
        return PREFIX_E + ONE_BYTE + item.getType() + ONE_BYTE + item.getId() + ONE_BYTE + item.getTimestamp();
    }

    @Override
    protected String buildTupleKey(Event item, String key) {
        return buildTupleKey(item.getType(), key);
    }

    private static final String buildTupleKey(String type, String key) {
        return type + FI_TYPE_KEY_SEP + key;
    }


    @Override
    protected void configureShardTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    }
    @Override
    protected void configureIndexTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    }


    public static class QueryXform extends KeyToTupleCollectionQueryXform<Event> {

        public QueryXform(Kryo kryo, TypeRegistry<String> typeRegistry, MetadataSerDe metadataSerDe) {
            super(kryo, typeRegistry, metadataSerDe);
        }

        @Override
        protected Event buildTupleCollectionFromKey(Key k) {
            return createEventFromkey(k);
        }
    }

    public static class WholeColFXForm extends KeyToTupleCollectionWholeColFXform<Event> {
        public WholeColFXForm(Kryo kryo, TypeRegistry<String> typeRegistry, MetadataSerDe metadataSerDe) {
            super(kryo, typeRegistry, metadataSerDe);
        }

        @Override
        protected Event buildEntryFromKey(Key k) {
            return createEventFromkey(k);
        }
    }

    @Override
    protected Class<? extends OptimizedQueryIterator> getOptimizedQueryIteratorClass() {
        return EventOptimizedQueryIterator.class;
    }

    private static final Event createEventFromkey(Key key) {
        String cf = key.getColumnFamily().toString();
        String cfParts[] = splitPreserveAllTokens(cf, ONE_BYTE);

        String type = cfParts[1];
        String uuid =  cfParts[2];
        long ts = Long.parseLong(cfParts[3]);
        return new BaseEvent(type, uuid, ts);
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
