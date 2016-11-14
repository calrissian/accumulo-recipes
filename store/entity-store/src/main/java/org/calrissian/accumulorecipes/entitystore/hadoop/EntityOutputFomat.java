/*
 * Copyright (C) 2016 The Calrissian Authors
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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore;
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable;
import org.calrissian.accumulorecipes.entitystore.support.EntityShardBuilder;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.io.Serializables;
import org.calrissian.mango.types.TypeRegistry;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import static java.util.Collections.singleton;

public class EntityOutputFomat extends OutputFormat {

    private static final Class CLASS = EntityOutputFomat.class;

    public enum AccumuloEntityStoreTableOptions {
        INDEX_TABLE, SHARD_TABLE
    }

    public enum TypeRegistryInfo {
        TYPE_REGISTRY
    }

    public enum AccumuloEntityStoreStoreConfigOptions {
        MAX_QUERY_THREADS, MAX_MEMORY, MAX_LATENCY, MAX_WRITE_THREADS
    }

    public enum AccumuloEntityStoreShardBuilderConfigOptions {
        PARTITION_SIZE
    }

    public static void setConnectorInfo(Job job, String principal, AuthenticationToken token) throws AccumuloSecurityException {
        OutputConfigurator.setConnectorInfo(CLASS, job.getConfiguration(), principal, token);
    }

    public static void setTables(Job job, String indexTable, String shardTable) throws AccumuloSecurityException {
        ArgumentChecker.notNull(indexTable, shardTable);
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEntityStoreTableOptions.INDEX_TABLE), indexTable);
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEntityStoreTableOptions.SHARD_TABLE), shardTable);
    }

    protected static String getIndexTable(JobContext context) {
        return context.getConfiguration().get(enumToConfKey(CLASS, AccumuloEntityStoreTableOptions.INDEX_TABLE));
    }

    protected static String getShardTable(JobContext context) {
        return context.getConfiguration().get(enumToConfKey(CLASS, AccumuloEntityStoreTableOptions.SHARD_TABLE));
    }

    public static <U> void setShardPartitionSize(Job job, int shardPartitionSize) throws AccumuloSecurityException, IOException {
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEntityStoreShardBuilderConfigOptions.PARTITION_SIZE), shardPartitionSize + "", "UTF-8");
    }

    protected static EntityShardBuilder getEntityShardBuilder(JobContext context) throws IOException, ClassNotFoundException {
        String value = context.getConfiguration().get(enumToConfKey(CLASS, AccumuloEntityStoreShardBuilderConfigOptions.PARTITION_SIZE));
        if (value == null) {
            return null;
        }
        return new EntityShardBuilder(Integer.valueOf(value));
    }

    public static <U> void setTypeRegistry(Job job, TypeRegistry<U> typeRegistry) throws AccumuloSecurityException, IOException {
        ArgumentChecker.notNull(typeRegistry);
        job.getConfiguration().set(enumToConfKey(CLASS, TypeRegistryInfo.TYPE_REGISTRY), new String(Serializables.toBase64(typeRegistry), "UTF-8"));
    }

    protected static <U> TypeRegistry<U> getTypeRegistry(JobContext context) throws IOException, ClassNotFoundException {
        String value = context.getConfiguration().get(enumToConfKey(CLASS, TypeRegistryInfo.TYPE_REGISTRY));
        if (value == null) {
            return null;
        }
        return Serializables.fromBase64(value.getBytes());
    }

    public static void setStoreConfig(Job job, StoreConfig storeConfig) throws AccumuloSecurityException {
        ArgumentChecker.notNull(storeConfig);
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEntityStoreStoreConfigOptions.MAX_QUERY_THREADS), storeConfig.getMaxQueryThreads() + "");
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEntityStoreStoreConfigOptions.MAX_MEMORY), storeConfig.getMaxMemory() + "");
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEntityStoreStoreConfigOptions.MAX_LATENCY), storeConfig.getMaxLatency() + "");
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEntityStoreStoreConfigOptions.MAX_WRITE_THREADS), storeConfig.getMaxWriteThreads() + "");
    }

    protected static StoreConfig getStoreConfig(JobContext context) {
        try {
            final int maxQueryThreads = Integer.parseInt(context.getConfiguration().get(enumToConfKey(CLASS, AccumuloEntityStoreStoreConfigOptions.MAX_QUERY_THREADS)));
            final long maxMemory = Long.parseLong(context.getConfiguration().get(enumToConfKey(CLASS, AccumuloEntityStoreStoreConfigOptions.MAX_MEMORY)));
            final long maxLatency = Long.parseLong(context.getConfiguration().get(enumToConfKey(CLASS, AccumuloEntityStoreStoreConfigOptions.MAX_LATENCY)));
            final int maxWriteThreads = Integer.parseInt(context.getConfiguration().get(enumToConfKey(CLASS, AccumuloEntityStoreStoreConfigOptions.MAX_WRITE_THREADS)));
            StoreConfig storeConfig = new StoreConfig(maxQueryThreads, maxMemory, maxLatency, maxWriteThreads);
            return storeConfig;
        } catch (Throwable t) {
            return null;
        }
    }

    public static void setZooKeeperInstance(Job job, ClientConfiguration clientConfig) {
        OutputConfigurator.setZooKeeperInstance(CLASS, job.getConfiguration(), clientConfig);
    }

    public static void setMockInstance(Job job, String instanceName) {
        OutputConfigurator.setMockInstance(CLASS, job.getConfiguration(), instanceName);
    }

    protected static Instance getInstance(JobContext context) {
        return OutputConfigurator.getInstance(CLASS, context.getConfiguration());
    }

    protected static Boolean isConnectorInfoSet(JobContext context) {
        return OutputConfigurator.isConnectorInfoSet(CLASS, context.getConfiguration());
    }

    protected static String getPrincipal(JobContext context) {
        return OutputConfigurator.getPrincipal(CLASS, context.getConfiguration());
    }

    protected static AuthenticationToken getAuthenticationToken(JobContext context) {
        return OutputConfigurator.getAuthenticationToken(CLASS, context.getConfiguration());
    }


    protected static String enumToConfKey(Class<?> implementingClass, Enum<?> e) {
        return implementingClass.getSimpleName() + "." + e.getDeclaringClass().getSimpleName() + "." + StringUtils.camelize(e.name().toLowerCase());
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        if (!isConnectorInfoSet(context))
            throw new IOException("Connector info has not been set.");
        try {
            // if the instance isn't configured, it will complain here
            String principal = getPrincipal(context);
            AuthenticationToken token = getAuthenticationToken(context);
            Connector c = getInstance(context).getConnector(principal, token);
            if (!c.securityOperations().authenticateUser(principal, token))
                throw new IOException("Unable to authenticate user");
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IOException(e);
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new NullOutputFormat<Text, EntityWritable>().getOutputCommitter(context);
    }


    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        final AccumuloEntityStore entityService = getEntityStore(context);

        return new RecordWriter<Text, EntityWritable>() {

            @Override
            public void write(Text text, EntityWritable entity) throws IOException {
                try {
                    entityService.save(singleton(entity.get()));
                } catch (Exception e) {
                    throw new IOException("An error occurred storing entity. [entity=<" + entity.get() + ">]", e);
                }
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                try {
                    entityService.flush();
                    entityService.shutdown();
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        };
    }

    private Iterable<Entity> getEntitiesFromBuffer(BlockingQueue<EntityWritable> entityBuffer) {
        return Iterables.transform(entityBuffer, new Function<EntityWritable, Entity>() {
            @Nullable
            @Override
            public Entity apply(@Nullable EntityWritable input) {
                return input.get();
            }
        });
    }

    private AccumuloEntityStore getEntityStore(TaskAttemptContext job) {

        try {
            Connector connector = getInstance(job)
                    .getConnector(getPrincipal(job),
                            getAuthenticationToken(job));

            String indexTable = getIndexTable(job);
            String shardTable = getShardTable(job);
            StoreConfig storeConfig = getStoreConfig(job);
            TypeRegistry<String> typeRegistry = getTypeRegistry(job);
            EntityShardBuilder entityShardBuilder = getEntityShardBuilder(job);

            AccumuloEntityStore.Builder builder = new AccumuloEntityStore.Builder(connector);

            if (indexTable!=null) {
                builder.setIndexTable(indexTable);
            }
            if (shardTable!=null) {
                builder.setShardTable(shardTable);
            }
            if (storeConfig!=null) {
                builder.setStoreConfig(storeConfig);
            }
            if (typeRegistry!=null) {
                builder.setTypeRegistry(typeRegistry);
            }
            if (entityShardBuilder!=null) {
                builder.setShardBuilder(entityShardBuilder);
            }

            return builder.build();
        } catch (IOException | AccumuloSecurityException | AccumuloException | ClassNotFoundException | TableNotFoundException | TableExistsException e) {
            throw new RuntimeException(e);
        }

    }
}
