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

import com.esotericsoftware.kryo.Kryo;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.StringUtils;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.commons.support.qfd.QfdHelper;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionQueryXform;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionWholeColFXform;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.BaseEntity;
import org.calrissian.mango.types.TypeRegistry;

import java.util.Set;

import static java.lang.System.currentTimeMillis;
import static org.calrissian.accumulorecipes.commons.support.Constants.EMPTY_VALUE;
import static org.calrissian.accumulorecipes.commons.support.Constants.INNER_DELIM;


public class EntityQfdHelper extends QfdHelper<Entity> {

    public EntityQfdHelper(Connector connector, String indexTable, String shardTable, StoreConfig config,
                           ShardBuilder<Entity> shardBuilder, TypeRegistry<String> typeRegistry, KeyValueIndex<Entity> keyValueIndex)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super(connector, indexTable, shardTable, config, shardBuilder, typeRegistry, keyValueIndex);
    }

    @Override
    protected String buildId(Entity item) {
        return item.getType() + INNER_DELIM + item.getId();
    }

    @Override
    protected Value buildValue(Entity item) {
        return EMPTY_VALUE; // placeholder for things like dynamic age-off
    }

    @Override
    protected long buildTimestamp(Entity item) {
        return currentTimeMillis();
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
    }

    public static class QueryXform extends KeyToTupleCollectionQueryXform<Entity> {

        public QueryXform(Kryo kryo, TypeRegistry<String> typeRegistry, Set<String> selectFields, MetadataSerDe metadataSerDe) {
            super(kryo, typeRegistry, selectFields, metadataSerDe);
        }

        @Override
        protected Entity buildTupleCollectionFromKey(Key k) {
            String[] typeId = StringUtils.splitPreserveAllTokens(k.getColumnFamily().toString(), INNER_DELIM);
            return new BaseEntity(typeId[0], typeId[1]);
        }
    }

    public static class WholeColFXform extends KeyToTupleCollectionWholeColFXform<Entity> {

        public WholeColFXform(Kryo kryo, TypeRegistry<String> typeRegistry, Set<String> selectFields, MetadataSerDe metadataSerDe) {
            super(kryo, typeRegistry, selectFields, metadataSerDe);
        }

        @Override
        protected Entity buildEntryFromKey(Key k) {
            String[] typeId = StringUtils.splitPreserveAllTokens(k.getColumnFamily().toString(), INNER_DELIM);
            return new BaseEntity(typeId[0], typeId[1]);
        }

    }
}
