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
package org.calrissian.accumulorecipes.eventstore.hadoop;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.lib.impl.OutputConfigurator;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore;
import org.calrissian.mango.io.Serializables;
import org.calrissian.mango.types.TypeRegistry;

import java.io.IOException;

import static java.util.Collections.singleton;

public class EventOutputFormat extends OutputFormat<Text, EventWritable> {

    private static final Class CLASS = EventOutputFormat.class;

    public enum TypeRegistryInfo {
        TYPE_REGISTRY
    }

    public enum AccumuloEventStoreTableOptions {
        INDEX_TABLE, SHARD_TABLE,
    }

    public enum AccumuloEventStoreStoreConfigOptions {
        MAX_QUERY_THREADS, MAX_MEMORY, MAX_LATENCY, MAX_WRITE_THREADS
    }


    public static void setConnectorInfo(Job job, String principal, AuthenticationToken token) throws AccumuloSecurityException {
        OutputConfigurator.setConnectorInfo(CLASS, job.getConfiguration(), principal, token);
    }

    public static void setTables(Job job, String indexTable, String shardTable) throws AccumuloSecurityException {
        ArgumentChecker.notNull(indexTable, shardTable);
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEventStoreTableOptions.INDEX_TABLE), indexTable);
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEventStoreTableOptions.SHARD_TABLE), shardTable);
    }

    protected static String getIndexTable(JobContext context) {
        return context.getConfiguration().get(enumToConfKey(CLASS,AccumuloEventStoreTableOptions.INDEX_TABLE));
    }

    protected static String getShardTable(JobContext context) {
        return context.getConfiguration().get(enumToConfKey(CLASS,AccumuloEventStoreTableOptions.SHARD_TABLE));
    }

    public static void setStoreConfig(Job job, StoreConfig storeConfig) throws AccumuloSecurityException {
        ArgumentChecker.notNull(storeConfig);
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEventStoreStoreConfigOptions.MAX_QUERY_THREADS), storeConfig.getMaxQueryThreads()+"");
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEventStoreStoreConfigOptions.MAX_MEMORY), storeConfig.getMaxMemory()+"");
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEventStoreStoreConfigOptions.MAX_LATENCY), storeConfig.getMaxLatency()+"");
        job.getConfiguration().set(enumToConfKey(CLASS, AccumuloEventStoreStoreConfigOptions.MAX_WRITE_THREADS), storeConfig.getMaxWriteThreads()+"");
    }

    public static <U> void setTypeRegistry(Job job, TypeRegistry<U> typeRegistry) throws AccumuloSecurityException, IOException {
        ArgumentChecker.notNull(typeRegistry);
        job.getConfiguration().set(enumToConfKey(CLASS, TypeRegistryInfo.TYPE_REGISTRY),  new String(Serializables.toBase64(typeRegistry),"UTF-8"));
    }

    protected static <U> TypeRegistry<U> getTypeRegistry(JobContext context) throws IOException, ClassNotFoundException {
        String value = context.getConfiguration().get(enumToConfKey(CLASS, TypeRegistryInfo.TYPE_REGISTRY));
        if (value==null) {
            return null;
        }
        return Serializables.fromBase64(value.getBytes());
    }

    protected static StoreConfig getStoreConfig(JobContext context) {
        try {
            final int maxQueryThreads = Integer.parseInt(context.getConfiguration().get(enumToConfKey(CLASS, AccumuloEventStoreStoreConfigOptions.MAX_QUERY_THREADS)));
            final long maxMemory = Long.parseLong(context.getConfiguration().get(enumToConfKey(CLASS, AccumuloEventStoreStoreConfigOptions.MAX_MEMORY)));
            final long maxLatency = Long.parseLong(context.getConfiguration().get(enumToConfKey(CLASS, AccumuloEventStoreStoreConfigOptions.MAX_LATENCY)));
            final int maxWriteThreads = Integer.parseInt(context.getConfiguration().get(enumToConfKey(CLASS, AccumuloEventStoreStoreConfigOptions.MAX_WRITE_THREADS)));
            StoreConfig storeConfig = new StoreConfig(maxQueryThreads, maxMemory, maxLatency, maxWriteThreads);
            return storeConfig;
        } catch (Throwable t) {
            return null;
        }
    }

    protected static String enumToConfKey(Class<?> implementingClass, Enum<?> e) {
        return implementingClass.getSimpleName() + "." + e.getDeclaringClass().getSimpleName() + "." + StringUtils.camelize(e.name().toLowerCase());
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

    protected static AccumuloEventStore getAccumuloEventStore(JobContext job) {
        try {
            Connector connector = getInstance(job)
                    .getConnector(getPrincipal(job),
                            getAuthenticationToken(job));

            String indexTable = getIndexTable(job);
            String shardTable = getShardTable(job);
            StoreConfig storeConfig = getStoreConfig(job);
            TypeRegistry<String> typeRegistry = getTypeRegistry(job);

            AccumuloEventStore.Builder builder = new AccumuloEventStore.Builder(connector);
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

           return builder.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public RecordWriter<Text, EventWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        final AccumuloEventStore accumuloEventStore = getAccumuloEventStore(taskAttemptContext);

        return new RecordWriter<Text, EventWritable>() {
            @Override
            public void write(Text text, EventWritable event) throws IOException, InterruptedException {
                accumuloEventStore.save(singleton(event.get()));
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                try {
                    accumuloEventStore.flush();
                    accumuloEventStore.shutdown();
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }


        };
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        if (!isConnectorInfoSet(jobContext))
            throw new IOException("Connector info has not been set.");
        try {
            // if the instance isn't configured, it will complain here
            String principal = getPrincipal(jobContext);
            AuthenticationToken token = getAuthenticationToken(jobContext);
            Connector c = getInstance(jobContext).getConnector(principal, token);
            if (!c.securityOperations().authenticateUser(principal, token))
                throw new IOException("Unable to authenticate user");
        } catch (AccumuloException e) {
            throw new IOException(e);
        } catch (AccumuloSecurityException e) {
            throw new IOException(e);
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new NullOutputFormat<Text, EventWritable>().getOutputCommitter(taskAttemptContext);
    }
}
