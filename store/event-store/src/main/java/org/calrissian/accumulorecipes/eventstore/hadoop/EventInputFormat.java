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

import static org.apache.accumulo.core.data.Range.prefix;
import static org.apache.commons.lang.StringUtils.splitByWholeSeparatorPreserveAllTokens;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.PREFIX_E;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_IDX_TABLE_NAME;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_SHARD_BUILDER;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_SHARD_TABLE_NAME;
import static org.calrissian.accumulorecipes.eventstore.support.EventQfdHelper.QueryXform;
import static org.calrissian.accumulorecipes.eventstore.support.EventQfdHelper.WholeColFXForm;
import static org.calrissian.mango.io.Serializables.fromBase64;
import static org.calrissian.mango.io.Serializables.toBase64;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
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
import org.apache.hadoop.mapreduce.Job;
import org.calrissian.accumulorecipes.commons.hadoop.BaseQfdInputFormat;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator;
import org.calrissian.accumulorecipes.commons.support.attribute.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.attribute.metadata.SimpleMetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.eventstore.support.EventGlobalIndexVisitor;
import org.calrissian.accumulorecipes.eventstore.support.EventOptimizedQueryIterator;
import org.calrissian.accumulorecipes.eventstore.support.EventQfdHelper;
import org.calrissian.accumulorecipes.eventstore.support.EventTimeLimitingFilter;
import org.calrissian.accumulorecipes.eventstore.support.shard.EventShardBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

/**
 * A Hadoop InputFormat for streaming events from Accumulo tablet servers directly into mapreduce jobs
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

    public static void setZooKeeperInstanceInfo(Job job, String inst, String zk) {
        setZooKeeperInstance(job, inst, zk);
    }

    /**
     * Sets up the job to stream all events between the start and end times for the types given
     * @param job
     * @param start
     * @param end
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     * @throws IOException
     */
    public static void setQueryInfo(Job job, Date start, Date end, Set<String> types) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
       setQueryInfo(job, start, end, types, null, DEFAULT_SHARD_BUILDER, LEXI_TYPES);
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
    public static void setQueryInfo(Job job, Date start, Date end, Set<String> types, EventShardBuilder shardBuilder, TypeRegistry<String> typeRegistry) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        setQueryInfo(job, start, end, types, null, shardBuilder, typeRegistry);
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
    public static void setQueryInfo(Job job, Date start, Date end, Set<String> types, Node query) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        setQueryInfo(job, start, end, types, query, DEFAULT_SHARD_BUILDER, LEXI_TYPES);
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
    public static void setQueryInfo(Job job, Date start, Date end, Set<String> types, Node query, EventShardBuilder shardBuilder, TypeRegistry<String> typeRegistry) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

      validateOptions(job);

      job.getConfiguration().set("typeRegistry", new String(toBase64(typeRegistry)));

        Instance instance = getInstance(job);
        Connector connector = instance.getConnector(getPrincipal(job), getAuthenticationToken(job));
        BatchScanner scanner = connector.createBatchScanner(DEFAULT_IDX_TABLE_NAME, getScanAuthorizations(job), 5);

        if(query == null) {


          Set<Range> typeIndexRanges = new HashSet<Range>();
          for(String type : types) {

              Key typeStartKey = new Key("t__" + type + NULL_BYTE + shardBuilder.buildShard(start.getTime(), 0));
              Key typeStopKey = new Key("t__" + type + NULL_BYTE + shardBuilder.buildShard(end.getTime(), shardBuilder.numPartitions()));
              typeIndexRanges.add(new Range(typeStartKey, typeStopKey));
          }
          scanner.setRanges(typeIndexRanges);

          Collection<Range> ranges = new LinkedList<Range>();
          for(Map.Entry<Key,Value> entry : scanner) {
              String[] parts = splitPreserveAllTokens(entry.getKey().getRow().toString(), NULL_BYTE);
              String[] typeParts = splitByWholeSeparatorPreserveAllTokens(parts[0], "__");
              ranges.add(prefix(parts[1], PREFIX_E + ONE_BYTE + typeParts[1] + ONE_BYTE));
          }
          scanner.close();

          job.getConfiguration().set(XFORM_KEY, CF_XFORM);

          setRanges(job, ranges);
          addIterator(job, new IteratorSetting(18, WholeColumnFamilyIterator.class));

        } else {

          GlobalIndexVisitor globalIndexVisitor = new EventGlobalIndexVisitor(start, end, types, scanner, shardBuilder);

          IteratorSetting timeSetting = new IteratorSetting(14, EventTimeLimitingFilter.class);
            EventTimeLimitingFilter.setCurrentTime(timeSetting, end.getTime());
            EventTimeLimitingFilter.setTTL(timeSetting, end.getTime() - start.getTime());
          addIterator(job, timeSetting);


          job.getConfiguration().set(XFORM_KEY, QUERY_XFORM);
          configureScanner(job, types, query, new EventQfdHelper.EventNodeToJexl(typeRegistry), globalIndexVisitor, typeRegistry,
              EventOptimizedQueryIterator.class);
        }

    }

    @Override
    protected Function<Map.Entry<Key, Value>, Event> getTransform(Configuration configuration) {

        try {

            TypeRegistry<String> typeRegistry = fromBase64(configuration.get("typeRegistry").getBytes());

            MetadataSerDe metadataSerDe = new SimpleMetadataSerDe();
            final Kryo kryo = new Kryo();
            initializeKryo(kryo);

            if(configuration.get(XFORM_KEY).equals(QUERY_XFORM))
              return new QueryXform(kryo, typeRegistry, metadataSerDe);
            else
              return new WholeColFXForm(kryo, typeRegistry,metadataSerDe);

        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected EventWritable getWritable() {
        return new EventWritable();
    }
}
