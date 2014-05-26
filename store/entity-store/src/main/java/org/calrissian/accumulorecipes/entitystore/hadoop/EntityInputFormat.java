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
package org.calrissian.accumulorecipes.entitystore.hadoop;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.calrissian.accumulorecipes.commons.hadoop.BaseQfdInputFormat;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable;
import org.calrissian.accumulorecipes.entitystore.support.EntityGlobalIndexVisitor;
import org.calrissian.accumulorecipes.entitystore.support.EntityQfdHelper;
import org.calrissian.accumulorecipes.entitystore.support.EntityShardBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.entity.Entity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.*;

public class EntityInputFormat extends BaseQfdInputFormat<Entity, EntityWritable> {

    public static void setInputInfo(Configuration config, String username, byte[] password, Authorizations auths) {
        setInputInfo(config, username, password, DEFAULT_SHARD_TABLE_NAME, auths);
    }

    public static void setQueryInfo(Configuration config, Set<String> entityTypes, Node query, Set<String> selectFields) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        setQueryInfo(config, entityTypes, query, selectFields, DEFAULT_SHARD_BUILDER);
    }

    public static void setQueryInfo(Configuration config, Set<String> entityTypes, Node query, Set<String> selectFields, EntityShardBuilder shardBuilder) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

        validateOptions(config);

        Instance instance = getInstance(config);
        Connector connector = instance.getConnector(getUsername(config), getPassword(config));
        BatchScanner scanner = connector.createBatchScanner(DEFAULT_IDX_TABLE_NAME, getAuthorizations(config), 5);
        GlobalIndexVisitor globalIndexVisitor = new EntityGlobalIndexVisitor(scanner, shardBuilder, entityTypes);

        configureScanner(config, query, globalIndexVisitor);
    }

    @Override
    protected Function<Map.Entry<Key, Value>, Entity> getTransform(Configuration configuration) {
        final String[] selectFields = configuration.getStrings("selectFields");

        Kryo kryo = new Kryo();
        initializeKryo(kryo);

        return new EntityQfdHelper.QueryXform(kryo, TYPES, selectFields != null ?
                new HashSet<String>(asList(selectFields)) : null);

    }

    @Override
    protected EntityWritable getWritable() {
        return new EntityWritable();
    }
}
