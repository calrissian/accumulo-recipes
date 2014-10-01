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

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.calrissian.accumulorecipes.commons.hadoop.BaseQfdInputFormat;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.metadata.SimpleMetadataSerDe;
import org.calrissian.accumulorecipes.eventstore.support.EventGlobalIndexVisitor;
import org.calrissian.accumulorecipes.eventstore.support.shard.EventShardBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.*;
import static org.calrissian.accumulorecipes.eventstore.support.EventQfdHelper.QueryXform;
import static org.calrissian.mango.io.Serializables.fromBase64;
import static org.calrissian.mango.io.Serializables.toBase64;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class EventInputFormat extends BaseQfdInputFormat<Event, EventWritable> {

    public static void setInputInfo(Job job, String username, byte[] password, Authorizations auths) throws AccumuloSecurityException {
        setConnectorInfo(job, username, new PasswordToken(password));
        setInputTableName(job, DEFAULT_SHARD_TABLE_NAME);
        setScanAuthorizations(job, auths);
    }

    public static void setQueryInfo(Job job, Date start, Date end, Node query) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        setQueryInfo(job, start, end, query, DEFAULT_SHARD_BUILDER, LEXI_TYPES);
    }

    public static void setQueryInfo(Job job, Date start, Date end, Node query, EventShardBuilder shardBuilder, TypeRegistry<String> typeRegistry) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {

        validateOptions(job);

        Instance instance = getInstance(job);
        Connector connector = instance.getConnector(getPrincipal(job), getAuthenticationToken(job));
        BatchScanner scanner = connector.createBatchScanner(DEFAULT_IDX_TABLE_NAME, getScanAuthorizations(job), 5);
        GlobalIndexVisitor globalIndexVisitor = new EventGlobalIndexVisitor(start, end, scanner, shardBuilder);

        job.getConfiguration().set("typeRegistry", new String(toBase64(typeRegistry)));

        configureScanner(job, query, globalIndexVisitor, typeRegistry);
    }

    public static void setMetadataSerDe(Configuration configuration, MetadataSerDe metadataSerDe) throws IOException {
        configuration.set("metadataSerDe", new String(toBase64(metadataSerDe)));
    }

    /**
     * Sets selection fields on the current configuration.
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

            return new QueryXform(kryo, typeRegistry, selectFields != null ? new HashSet<String>(asList(selectFields)) : null, metadataSerDe);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected EventWritable getWritable() {
        return new EventWritable();
    }
}
