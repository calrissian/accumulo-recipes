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

import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.PREFIX_E;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.commons.lang.StringUtils;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.iterators.MetadataExpirationFilter;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.attribute.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyToAttributeStoreQueryXform;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyToAttributeStoreWholeColFXform;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.commons.support.qfd.QfdHelper;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.calrissian.mango.types.TypeRegistry;


public class EntityQfdHelper extends QfdHelper<Entity> {

    public EntityQfdHelper(Connector connector, String indexTable, String shardTable, StoreConfig config,
        ShardBuilder<Entity> shardBuilder, TypeRegistry<String> typeRegistry, KeyValueIndex<Entity> keyValueIndex)
        throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super(connector, indexTable, shardTable, config, shardBuilder, typeRegistry, keyValueIndex, new NodeToJexl(typeRegistry));
    }

    @Override
    protected String buildId(Entity item) {
        return PREFIX_E + ONE_BYTE + item.getType() + ONE_BYTE + item.getId();
    }

    public QueryXform buildQueryXform() {
        return new QueryXform(getKryo(), getTypeRegistry(), getMetadataSerDe());
    }

    @Override
    protected String buildAttributeKey(Entity item, String key) {
        return key;
    }

    @Override
    protected long buildAttributeTimestampForEntity(Entity e) {
        return System.currentTimeMillis();
    }

    public WholeColFXform buildWholeColFXform() {
        return new WholeColFXform(getKryo(), getTypeRegistry(), getMetadataSerDe());
    }

    @Override
    protected void configureIndexTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    }

    @Override
    protected void configureShardTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Set<IteratorUtil.IteratorScope> scopes = Sets.newHashSet(IteratorUtil.IteratorScope.majc, IteratorUtil.IteratorScope.minc);
        IteratorSetting expirationFilter = new IteratorSetting(7, "metaExpiration", MetadataExpirationFilter.class);
        connector.tableOperations().attachIterator(tableName, expirationFilter, Sets.newEnumSet(scopes, IteratorUtil.IteratorScope.class));

    }

    public static class QueryXform extends KeyToAttributeStoreQueryXform<Entity,EntityBuilder> {

        public QueryXform(Kryo kryo, TypeRegistry<String> typeRegistry, MetadataSerDe metadataSerDe) {
            super(kryo, typeRegistry, metadataSerDe);
        }

        @Override
        protected EntityBuilder buildAttributeCollectionFromKey(Key k) {
            String[] typeId = StringUtils.splitPreserveAllTokens(k.getColumnFamily().toString(), ONE_BYTE);
            return new EntityBuilder(typeId[1], typeId[2]);
        }
    }

    public static class WholeColFXform extends KeyToAttributeStoreWholeColFXform<Entity, EntityBuilder> {

        public WholeColFXform(Kryo kryo, TypeRegistry<String> typeRegistry, MetadataSerDe metadataSerDe) {
            super(kryo, typeRegistry, metadataSerDe);
        }

        @Override
        protected EntityBuilder buildEntryFromKey(Key k) {
            String[] typeId = StringUtils.splitPreserveAllTokens(k.getColumnFamily().toString(), ONE_BYTE);
            return new EntityBuilder(typeId[1], typeId[2]);
        }

    }
}
