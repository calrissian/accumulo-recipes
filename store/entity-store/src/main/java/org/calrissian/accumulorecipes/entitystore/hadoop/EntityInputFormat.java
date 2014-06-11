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
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.hadoop.BaseQfdInputFormat;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.metadata.SimpleMetadataSerDe;
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable;
import org.calrissian.accumulorecipes.entitystore.support.EntityGlobalIndexVisitor;
import org.calrissian.accumulorecipes.entitystore.support.EntityQfdHelper;
import org.calrissian.accumulorecipes.entitystore.support.EntityShardBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.types.TypeRegistry;

import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.accumulo.core.data.Range.prefix;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.*;
import static org.calrissian.mango.io.Serializables.fromBase64;
import static org.calrissian.mango.io.Serializables.toBase64;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class EntityInputFormat extends BaseQfdInputFormat<Entity, EntityWritable> {

    private static final String QUERY = "query";
    private static final String TYPE_REGISTRY = "typeRegistry";

    public static void setInputInfo(Configuration config, String username, byte[] password, Authorizations auths) {
        setInputInfo(config, username, password, DEFAULT_SHARD_TABLE_NAME, auths);
    }

    public static void setQueryInfo(Configuration config, Set<String> entityTypes, Node query) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        setQueryInfo(config, entityTypes, query, DEFAULT_SHARD_BUILDER, LEXI_TYPES);
    }

    public static void setQueryInfo(Configuration config, Set<String> entityTypes, Node query, EntityShardBuilder shardBuilder, TypeRegistry<String> typeRegistry) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

        validateOptions(config);

        Instance instance = getInstance(config);
        Connector connector = instance.getConnector(getUsername(config), getPassword(config));
        BatchScanner scanner = connector.createBatchScanner(DEFAULT_IDX_TABLE_NAME, getAuthorizations(config), 5);
        GlobalIndexVisitor globalIndexVisitor = new EntityGlobalIndexVisitor(scanner, shardBuilder, entityTypes);

        configureScanner(config, query, globalIndexVisitor, typeRegistry);

        config.setBoolean(QUERY, true);
        config.set(TYPE_REGISTRY, new String(toBase64(typeRegistry)));
    }

    public static void setMetadataSerDe(Configuration configuration, MetadataSerDe metadataSerDe) throws IOException {
        configuration.set("metadataSerDe", new String(toBase64(metadataSerDe)));
    }

    public static void setQueryInfo(Configuration config, Set<String> entityTypes) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        setQueryInfo(config, entityTypes, DEFAULT_SHARD_BUILDER, LEXI_TYPES);
    }


    public static void setQueryInfo(Configuration config, Set<String> entityTypes, EntityShardBuilder shardBuilder, TypeRegistry<String> typeRegistry) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

        validateOptions(config);

        Collection<Range> ranges = new LinkedList<Range>();
        for (String type : entityTypes) {
            Set<Text> shards = shardBuilder.buildShardsForTypes(singleton(type));
            for (Text shard : shards)
                ranges.add(prefix(shard.toString(), type));
        }

        setRanges(config, ranges);

        IteratorSetting iteratorSetting = new IteratorSetting(16, "wholeColumnFamilyIterator", WholeColumnFamilyIterator.class);
        addIterator(config, iteratorSetting);

        config.setBoolean(QUERY, false);
        config.set(TYPE_REGISTRY, new String(toBase64(typeRegistry)));
    }

    /**
     * Sets selection fields on the current configuration.
     */
    public static void setSelectFields(Configuration config, Set<String> selectFields) {

        if(selectFields != null)
            config.setStrings("selectFields", selectFields.toArray(new String[] {}));
    }

    @Override
    protected Function<Map.Entry<Key, Value>, Entity> getTransform(Configuration configuration) {


        final String[] selectFields = configuration.getStrings("selectFields");


        Set<String> finalSelectFields = selectFields != null ?
                new HashSet<String>(asList(selectFields)) : null;

        try {
            TypeRegistry<String> typeRegistry = fromBase64(configuration.get(TYPE_REGISTRY).getBytes());

            MetadataSerDe metadataSerDe;
            if(configuration.get("metadataSerDe") != null)
                metadataSerDe = fromBase64(configuration.get("metadataSerDe").getBytes());
            else
                metadataSerDe = new SimpleMetadataSerDe(typeRegistry);

            Kryo kryo = new Kryo();
            initializeKryo(kryo);

            if(configuration.getBoolean(QUERY, false))
                return new EntityQfdHelper.QueryXform(kryo, typeRegistry, finalSelectFields, metadataSerDe);
            else
                return new EntityQfdHelper.WholeColFXform(kryo, typeRegistry, finalSelectFields, metadataSerDe);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected EntityWritable getWritable() {
        return new EntityWritable();
    }
}
