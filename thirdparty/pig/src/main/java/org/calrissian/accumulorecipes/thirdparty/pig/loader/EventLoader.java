/*
* Copyright (C) 2014 The Calrissian Authors
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
package org.calrissian.accumulorecipes.thirdparty.pig.loader;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.calrissian.accumulorecipes.thirdparty.pig.support.TupleStoreIterator;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.accumulorecipes.commons.hadoop.RecordReaderValueIterator;
import org.calrissian.accumulorecipes.commons.support.GettableTransform;
import org.calrissian.accumulorecipes.eventstore.hadoop.EventInputFormat;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.SimpleTypeEncoders;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.uri.support.UriUtils;
import org.joda.time.DateTime;

/**
 * Allows events to be streamed from the event store into Pig tuples. This class is really a proxy
 * for the {@link EventInputFormat} which also decomposes the attributes of an event into quads
 * (id, timestamp, key, value) which can be manipulated easily through pig statements.
 */
public class EventLoader extends LoadFunc implements Serializable {

    public static final String USAGE = "Usage: event://indexTable/shardTable?user=&pass=&inst=&zk=&start=&end=&auths=[&fields=]";

    protected transient TupleStoreIterator<Event> itr;
    protected final TypeRegistry<String> registry = SimpleTypeEncoders.SIMPLE_TYPES;

    protected final QueryBuilder qb;

    public EventLoader(String query) {
        Preconditions.checkNotNull(query);
        Preconditions.checkArgument(!query.equals(""));

        try {
            // call groovy expressions from Java code
            Binding binding = new Binding();
            binding.setVariable("q", new QueryBuilder());
            GroovyShell shell = new GroovyShell(binding);
            qb = (QueryBuilder) shell.evaluate(query);
        } catch(Exception e) {
            throw new RuntimeException("There was an error parsing the groovy query string. ");
        }
    }

    @Override
    public void setLocation(String uri, Job job) throws IOException {

        Configuration conf = job.getConfiguration();
        if(!ConfiguratorBase.isConnectorInfoSet(AccumuloInputFormat.class, job.getConfiguration())) {
            String path = uri.substring(uri.indexOf("://")+3, uri.indexOf("?"));

            String[] indexAndShardTable = StringUtils.splitPreserveAllTokens(path, "/");
            if(indexAndShardTable.length != 2)
                throw new IOException("Path portion of URI must contain the index and shard tables. " + USAGE);

            if(uri.startsWith("event")) {
                String queryPortion = uri.substring(uri.indexOf("?")+1, uri.length());
                Multimap<String, String> queryParams = UriUtils.splitQuery(queryPortion);

                String accumuloUser = getProp(queryParams, "user");
                String accumuloPass = getProp(queryParams, "pass");
                String accumuloInst = getProp(queryParams, "inst");
                String zookeepers = getProp(queryParams, "zk");
                if(accumuloUser == null || accumuloPass == null || accumuloInst == null || zookeepers == null)
                    throw new IOException("Some Accumulo connection information is missing. Must supply username, password, instance, and zookeepers. " + USAGE);

                String startTime = getProp(queryParams, "start");
                String endTime = getProp(queryParams, "end");
                if(startTime == null || endTime == null)
                    throw new IOException("Start and end times are required. " + USAGE);

                String auths = getProp(queryParams, "auths");
                if(auths == null)
                    auths = "";     // default auths to empty
                String selectFields = getProp(queryParams, "fields");

                String types = getProp(queryParams, "types");

                Set<String> fields = selectFields != null ? Sets.newHashSet(StringUtils.splitPreserveAllTokens(selectFields, ",")) : null;
                Set<String> finalTypes = types != null ? Sets.newHashSet(StringUtils.splitPreserveAllTokens(types, ",")) : null;

                DateTime startDT = new DateTime(startTime);
                DateTime endDT = new DateTime(endTime);

                AbstractInputFormat.setZooKeeperInstance(job, accumuloInst, zookeepers);
                try {
                    EventInputFormat.setInputInfo(job, accumuloUser, accumuloPass.getBytes(), new Authorizations(auths.getBytes()));
                } catch (AccumuloSecurityException e) {
                    throw new RuntimeException(e);
                }
                try {
                    EventInputFormat.setQueryInfo(job, startDT.toDate(), endDT.toDate(), finalTypes, qb.build());
                    if(fields != null)
                        EventInputFormat.setSelectFields(conf, fields);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new IOException("Location uri must begin with event://");
            }
        }
    }

    private String getProp(Multimap<String, String> queryParams, String propKey) {
        Collection<String> props = queryParams.get(propKey);
        if (props.size() > 0)
            return props.iterator().next();
        return null;
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new EventInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        RecordReaderValueIterator<Key, EventWritable> rri = new RecordReaderValueIterator<Key, EventWritable>(recordReader);
        Iterator<Event> xformed = Iterators.transform(rri, new GettableTransform<Event>());
        itr = new TupleStoreIterator<Event>(xformed);
    }


    @Override
    public Tuple getNext() throws IOException {

        if(!itr.hasNext())
            return null;

        org.calrissian.mango.domain.Tuple eventTuple = itr.next();

        /**
         * Create the pig tuple and hydrate with event details. The format of the tuple is as follows:
         * (id, timestamp, key, datatype, value, visiblity)
         */
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(itr.getTopStore().getId());
        t.append(itr.getTopStore().getTimestamp());
        t.append(eventTuple.getKey());
        t.append(registry.getAlias(eventTuple.getValue()));
        t.append(registry.encode(eventTuple.getValue()));

        return t;
    }
}
