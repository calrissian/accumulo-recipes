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
package org.calrissian.accumulorecipes.entitystore.support;

import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.commons.lang.StringUtils;
import org.calrissian.accumulorecipes.commons.iterators.MetadataExpirationFilter;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.commons.support.qfd.QfdHelper;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionQueryXform;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionWholeColFXform;
import org.calrissian.mango.domain.entity.BaseEntity;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.types.TypeRegistry;

import static java.util.EnumSet.allOf;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope.majc;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;


public class EntityQfdHelper extends QfdHelper<Entity> {

    public EntityQfdHelper(Connector connector, String indexTable, String shardTable, StoreConfig config,
                           ShardBuilder<Entity> shardBuilder, TypeRegistry<String> typeRegistry, KeyValueIndex<Entity> keyValueIndex)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super(connector, indexTable, shardTable, config, shardBuilder, typeRegistry, keyValueIndex);
    }

    @Override
    protected String buildId(Entity item) {
        return item.getType() + ONE_BYTE + item.getId();
    }

    public QueryXform buildQueryXform(Set<String> selectFields) {
        return new QueryXform(getKryo(), getTypeRegistry(), selectFields, getMetadataSerDe());
    }

    public WholeColFXform buildWholeColFXform(Set<String> selectFields) {
        return new WholeColFXform(getKryo(), getTypeRegistry(), selectFields, getMetadataSerDe());
    }

    @Override
    protected void configureIndexTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    }

    @Override
    protected void configureShardTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        if(connector.tableOperations().getIteratorSetting(tableName, "expiration", majc) == null) {
            IteratorSetting expirationFilter = new IteratorSetting(10, "expiration", MetadataExpirationFilter.class);
            MetadataExpirationFilter.setMetadataSerdeFactory(expirationFilter, getMetadataSerdeFactory().getClass());
            connector.tableOperations().attachIterator(tableName, expirationFilter, allOf(IteratorUtil.IteratorScope.class));
        }
    }

    public static class QueryXform extends KeyToTupleCollectionQueryXform<Entity> {

        public QueryXform(Kryo kryo, TypeRegistry<String> typeRegistry, Set<String> selectFields, MetadataSerDe metadataSerDe) {
            super(kryo, typeRegistry, selectFields, metadataSerDe);
        }

        @Override
        protected Entity buildTupleCollectionFromKey(Key k) {
            String[] typeId = StringUtils.splitPreserveAllTokens(k.getColumnFamily().toString(), ONE_BYTE);
            return new BaseEntity(typeId[0], typeId[1]);
        }
    }

    public static class WholeColFXform extends KeyToTupleCollectionWholeColFXform<Entity> {

        public WholeColFXform(Kryo kryo, TypeRegistry<String> typeRegistry, Set<String> selectFields, MetadataSerDe metadataSerDe) {
            super(kryo, typeRegistry, selectFields, metadataSerDe);
        }

        @Override
        protected Entity buildEntryFromKey(Key k) {
            String[] typeId = StringUtils.splitPreserveAllTokens(k.getColumnFamily().toString(), ONE_BYTE);
            return new BaseEntity(typeId[0], typeId[1]);
        }

    }
}
