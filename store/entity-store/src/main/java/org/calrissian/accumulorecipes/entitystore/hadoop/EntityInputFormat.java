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

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.accumulo.core.data.Range.prefix;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_IDX_TABLE_NAME;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_SHARD_BUILDER;
import static org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore.DEFAULT_SHARD_TABLE_NAME;
import static org.calrissian.mango.io.Serializables.fromBase64;
import static org.calrissian.mango.io.Serializables.toBase64;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.calrissian.accumulorecipes.commons.hadoop.BaseQfdInputFormat;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.commons.support.tuple.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.tuple.metadata.SimpleMetadataSerDe;
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable;
import org.calrissian.accumulorecipes.entitystore.support.EntityGlobalIndexVisitor;
import org.calrissian.accumulorecipes.entitystore.support.EntityQfdHelper;
import org.calrissian.accumulorecipes.entitystore.support.EntityShardBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.types.TypeRegistry;

public class EntityInputFormat extends BaseQfdInputFormat<Entity, EntityWritable> {

    private static final String QUERY = "query";
    private static final String TYPE_REGISTRY = "typeRegistry";

    public static void setInputInfo(Job job, String username, byte[] password, Authorizations auths) throws AccumuloSecurityException {
        setConnectorInfo(job, username, new PasswordToken(password));
        setInputTableName(job, DEFAULT_SHARD_TABLE_NAME);
        setScanAuthorizations(job, auths);
    }

    public static void setZooKeeperInstanceInfo(Job job, String inst, String zk) {
        setZooKeeperInstance(job, inst, zk);
    }


    public static void setQueryInfo(Job job, Set<String> entityTypes, Node query) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        setQueryInfo(job, entityTypes, query, DEFAULT_SHARD_BUILDER, LEXI_TYPES);
    }

    public static void setQueryInfo(Job job, Set<String> entityTypes, Node query, EntityShardBuilder shardBuilder, TypeRegistry<String> typeRegistry) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

        validateOptions(job);

        Instance instance = getInstance(job);
        Connector connector = instance.getConnector(getPrincipal(job), getAuthenticationToken(job));
        BatchScanner scanner = connector.createBatchScanner(DEFAULT_IDX_TABLE_NAME, getScanAuthorizations(job), 5);
        GlobalIndexVisitor globalIndexVisitor = new EntityGlobalIndexVisitor(scanner, shardBuilder, entityTypes);

        configureScanner(job, entityTypes, query, new NodeToJexl(typeRegistry), globalIndexVisitor, typeRegistry, OptimizedQueryIterator.class);

        job.getConfiguration().setBoolean(QUERY, true);
        job.getConfiguration().set(TYPE_REGISTRY, new String(toBase64(typeRegistry)));
    }

    public static void setMetadataSerDe(Configuration configuration, MetadataSerDe metadataSerDe) throws IOException {
        configuration.set("metadataSerDe", new String(toBase64(metadataSerDe)));
    }

    public static void setQueryInfo(Job job, Set<String> entityTypes) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        setQueryInfo(job, entityTypes, DEFAULT_SHARD_BUILDER, LEXI_TYPES);
    }


    public static void setQueryInfo(Job job, Set<String> entityTypes, EntityShardBuilder shardBuilder, TypeRegistry<String> typeRegistry) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

        validateOptions(job);

        Collection<Range> ranges = new LinkedList<Range>();
        for (String type : entityTypes) {
            Set<Text> shards = shardBuilder.buildShardsForTypes(singleton(type));
            for (Text shard : shards)
                ranges.add(prefix(shard.toString(), "e" + ONE_BYTE + type));
        }

        setRanges(job, ranges);

        addIterator(job, new IteratorSetting(18, WholeColumnFamilyIterator.class));


        job.getConfiguration().setBoolean(QUERY, false);
        job.getConfiguration().set(TYPE_REGISTRY, new String(toBase64(typeRegistry)));
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
                return new EntityQfdHelper.QueryXform(kryo, typeRegistry, metadataSerDe);
            else
                return new EntityQfdHelper.WholeColFXform(kryo, typeRegistry, metadataSerDe);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected EntityWritable getWritable() {
        return new EntityWritable();
    }
}
