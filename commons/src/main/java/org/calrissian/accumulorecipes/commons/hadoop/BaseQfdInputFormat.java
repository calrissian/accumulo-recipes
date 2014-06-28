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
package org.calrissian.accumulorecipes.commons.hadoop;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.calrissian.accumulorecipes.commons.domain.Settable;
import org.calrissian.accumulorecipes.commons.iterators.BooleanLogicIterator;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.criteria.QueryOptimizer;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.TupleStore;
import org.calrissian.mango.types.TypeRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.apache.accumulo.core.util.format.DefaultFormatter.formatEntry;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.commons.support.Constants.END_BYTE;

public abstract class BaseQfdInputFormat<T extends TupleStore, W extends Settable> extends InputFormatBase<Key, W> {

    protected static void configureScanner(Configuration config, Node query, GlobalIndexVisitor globalInexVisitor, TypeRegistry<String> typeRegistry) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        QueryOptimizer optimizer = new QueryOptimizer(query, globalInexVisitor, typeRegistry);
        NodeToJexl nodeToJexl = new NodeToJexl(typeRegistry);
        String jexl = nodeToJexl.transform(optimizer.getOptimizedQuery());
        String originalJexl = nodeToJexl.transform(query);

        Collection<Range> ranges = new ArrayList<Range>();
        if(jexl.equals("()") || jexl.equals("")) {
            ranges.add(new Range(END_BYTE));
        } else {
            for (String shard : optimizer.getShards())
                ranges.add(new Range(shard));
        }

        setRanges(config, ranges);

        IteratorSetting setting = new IteratorSetting(16, OptimizedQueryIterator.class);
        setting.addOption(BooleanLogicIterator.QUERY_OPTION, originalJexl);
        setting.addOption(BooleanLogicIterator.FIELD_INDEX_QUERY, jexl);

        addIterator(config, setting);
    }

    protected abstract Function<Map.Entry<Key, Value>, T> getTransform(Configuration configuration);

    protected abstract W getWritable();

    @Override
    public RecordReader<Key, W> createRecordReader(InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {

        final W sharedWritable = getWritable();

        Kryo kryo = new Kryo();
        initializeKryo(kryo);

        final Function<Map.Entry<Key, Value>, T> xform = getTransform(context.getConfiguration());

        return new RecordReaderBase<Key, W>() {
            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (scannerIterator.hasNext()) {
                    ++numKeysRead;
                    Map.Entry<Key, Value> entry = scannerIterator.next();
                    currentK = currentKey = entry.getKey();
                    sharedWritable.set(xform.apply(entry));
                    currentV = sharedWritable;

                    if (log.isTraceEnabled())
                        log.trace("Processing key/value pair: " + formatEntry(entry, true));
                    return true;
                }
                return false;
            }
        };
    }
}
