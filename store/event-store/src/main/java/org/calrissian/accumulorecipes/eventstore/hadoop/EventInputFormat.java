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
package org.calrissian.accumulorecipes.eventstore.hadoop;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
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
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.metadata.SimpleMetadataSerDe;
import org.calrissian.accumulorecipes.eventstore.support.EventGlobalIndexVisitor;
import org.calrissian.accumulorecipes.eventstore.support.iterators.EventTimeLimitingFilter;
import org.calrissian.accumulorecipes.eventstore.support.shard.EventShardBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

import static java.util.Arrays.asList;
import static org.apache.accumulo.core.data.Range.prefix;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.commons.support.Constants.PREFIX_E;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_IDX_TABLE_NAME;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_SHARD_BUILDER;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_SHARD_TABLE_NAME;
import static org.calrissian.accumulorecipes.eventstore.support.EventQfdHelper.QueryXform;
import static org.calrissian.accumulorecipes.eventstore.support.EventQfdHelper.WholeColFXForm;
import static org.calrissian.mango.io.Serializables.fromBase64;
import static org.calrissian.mango.io.Serializables.toBase64;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

/**
 * A Hadoop {@link InputFormat} for streaming events from Accumulo tablet servers directly into mapreduce jobs
 * (preserving data locality if possible).
 */
public class EventInputFormat extends BaseQfdInputFormat<Event, EventWritable> {

    private static final String XFORM_KEY = "xform";
    private static final String QUERY_XFORM = "queryXform";
    private static final String CF_XFORM = "cfXform";

    /**
     * Sets basic input info on the job- username, password, and authorizations
     * @param job
     * @param username
     * @param password
     * @param auths
     * @throws AccumuloSecurityException
     */
    public static void setInputInfo(Job job, String username, byte[] password, Authorizations auths) throws AccumuloSecurityException {
        setConnectorInfo(job, username, new PasswordToken(password));
        setInputTableName(job, DEFAULT_SHARD_TABLE_NAME);
        setScanAuthorizations(job, auths);
    }

    /**
     * Sets up the job to stream all events between the start and end times
     * @param job
     * @param start
     * @param end
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     * @throws IOException
     */
    public static void setQueryInfo(Job job, Date start, Date end) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
       setQueryInfo(job, start, end, null, DEFAULT_SHARD_BUILDER, LEXI_TYPES);
    }

    /**
     * Sets up the job to stream all events between the start and end times. This method will allow a custom
     * shard builder and type registry to be used.
     * @param job
     * @param start
     * @param end
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     * @throws IOException
     */
    public static void setQueryInfo(Job job, Date start, Date end, EventShardBuilder shardBuilder, TypeRegistry<String> typeRegistry) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        setQueryInfo(job, start, end, null, shardBuilder, typeRegistry);
    }

    /**
     * Sets up the job to stream all events between the given start and end times that match the given query.
     * The query is propagated down to the tablet servers through iterators so that data can be scanned
     * close to the disks.
     * @param job
     * @param start
     * @param end
     * @param query
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     * @throws IOException
     */
    public static void setQueryInfo(Job job, Date start, Date end, Node query) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        setQueryInfo(job, start, end, query, DEFAULT_SHARD_BUILDER, LEXI_TYPES);
    }

    /**
     * Sets up the job to stream all events between the given start and end times that match the given query.
     * The query is propagated down to the tablet servers through iterators so that data can be scanned
     * close to the disks. This method will allow a custom shard builder and type registry to be used.
     * @param job
     * @param start
     * @param end
     * @param query
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     * @throws IOException
     */
    public static void setQueryInfo(Job job, Date start, Date end, Node query, EventShardBuilder shardBuilder, TypeRegistry<String> typeRegistry) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

      validateOptions(job);

      job.getConfiguration().set("typeRegistry", new String(toBase64(typeRegistry)));

      if(query == null) {

          //TODO: This could be dangerous- so it may be reasonable to limit the possible number of shards
          Set<Text> shards = shardBuilder.buildShardsInRange(start, end);
          Set<Range> ranges = new HashSet<Range>();
          for(Text shard : shards)
            ranges.add(prefix(shard.toString(), PREFIX_E));


          IteratorSetting iteratorSetting = new IteratorSetting(16, "wholeColumnFamilyIterator", WholeColumnFamilyIterator.class);
          addIterator(job, iteratorSetting);

          job.getConfiguration().set(XFORM_KEY, CF_XFORM);

          setRanges(job, ranges);

        } else {

          Instance instance = getInstance(job);
          Connector connector = instance.getConnector(getPrincipal(job), getAuthenticationToken(job));
          BatchScanner scanner = connector.createBatchScanner(DEFAULT_IDX_TABLE_NAME, getScanAuthorizations(job), 5);
          GlobalIndexVisitor globalIndexVisitor = new EventGlobalIndexVisitor(start, end, scanner, shardBuilder);

          IteratorSetting timeSetting = new IteratorSetting(14, EventTimeLimitingFilter.class);
          EventTimeLimitingFilter.setCurrentTime(timeSetting, end.getTime());
          EventTimeLimitingFilter.setTTL(timeSetting, end.getTime() - start.getTime());
          addIterator(job, timeSetting);

          job.getConfiguration().set(XFORM_KEY, QUERY_XFORM);
          configureScanner(job, query, globalIndexVisitor, typeRegistry);
        }

    }

    /**
     * Sets a custom {@link MetadataSerDe} to be used on the job. Only use this if a custom {@link MetadataSerD«e} was
     * used to persist the events.
     * @param configuration
     * @param metadataSerDe
     * @throws IOException
     */
    public static void setMetadataSerDe(Configuration configuration, MetadataSerDe metadataSerDe) throws IOException {
        configuration.set("metadataSerDe", new String(toBase64(metadataSerDe)));
    }

    /**
     * Sets selection fields on the current configuration. Only fields in the given set of select fields will be included
     * in the resulting events.
     */
    public static void setSelectFields(Configuration config, Set<String> selectFields) {

        if(selectFields != null)
            config.setStrings("selectFields", selectFields.toArray(new String[] {}));
    }


    @Override
    protected Function<Map.Entry<Key, Value>, Event> getTransform(Configuration configuration) {

        try {
            final String[] selectFields = configuration.getStrings("selectFields");

            TypeRegistry<String> typeRegistry = fromBase64(configuration.get("typeRegistry").getBytes());

            MetadataSerDe metadataSerDe;
            if(configuration.get("metadataSerDe") != null)
                metadataSerDe = fromBase64(configuration.get("metadataSerDe").getBytes());
            else
                metadataSerDe = new SimpleMetadataSerDe(typeRegistry);

            final Kryo kryo = new Kryo();
            initializeKryo(kryo);

            if(configuration.get(XFORM_KEY).equals(QUERY_XFORM))
              return new QueryXform(kryo, typeRegistry, selectFields != null ? new HashSet<String>(asList(selectFields)) : null, metadataSerDe);
            else
              return new WholeColFXForm(kryo, typeRegistry, selectFields != null ? new HashSet<String>(asList(selectFields)) : null, metadataSerDe);

        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected EventWritable getWritable() {
        return new EventWritable();
    }
}
