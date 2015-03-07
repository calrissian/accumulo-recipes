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
package org.calrissian.accumulorecipes.lastn.iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.calrissian.accumulorecipes.commons.support.Constants.END_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.attribute.Metadata.Visiblity.setVisibility;
import static org.calrissian.accumulorecipes.commons.util.WritableUtils2.serialize;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.event.EventBuilder;
import org.calrissian.mango.io.Serializables;
import org.calrissian.mango.types.TypeRegistry;

/**
 * An iterator to return StoreEntry objects serialized to JSON so that grouping can be done server side instead of
 * client side.
 */
public class EntryIterator extends WrappingIterator {

    public static final String TYPE_REGISTRY = "typeRegistry";

    private TypeRegistry<String> typeRegistry;
    private SortedKeyValueIterator<Key, Value> sourceItr;
    private EventWritable writable;

    public void init(SortedKeyValueIterator<Key, Value> source, java.util.Map<String, String> options,
                     IteratorEnvironment env) throws IOException {

        super.init(source, options, env);
        sourceItr = source.deepCopy(env);
        this.typeRegistry = getTypeRegistry(options);
        this.writable = new EventWritable();
    }

    public static void setTypeRegistry(IteratorSetting setting, TypeRegistry<String> registry) {
        checkNotNull(registry);
        try {
            setting.addOption(TYPE_REGISTRY, new String(Serializables.toBase64(registry)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TypeRegistry<String> getTypeRegistry(Map<String,String> options) {
        if(options.containsKey(TYPE_REGISTRY)) {
            try {
                return Serializables.fromBase64(options.get(TYPE_REGISTRY).getBytes());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        else
            throw new RuntimeException("Error: Type registry was not configured on iterator.");
    }

    /**
     * For each index row in the lastN store, grab the associated getAttributes (further down in the tablet) and construct
     * the entry to be returned.
     *
     * @return
     */
    @Override
    public Value getTopValue() {

        if (hasTop()) {

            Key topKey = getTopKey();
            Value topVal = super.getTopValue();
            String entryId = new String(topVal.get());

            Key startRangeKey = new Key(topKey.getRow(), new Text(END_BYTE + entryId));
            Key stopRangeKey = new Key(topKey.getRow(), new Text(END_BYTE + entryId + END_BYTE));

            Range range = new Range(startRangeKey, stopRangeKey);

            long timestamp = 0;

            try {
                sourceItr.seek(range, Collections.<ByteSequence>emptyList(), false);

                Collection<Attribute> attributes = new ArrayList<Attribute>();
                while (sourceItr.hasTop()) {

                    Key nextKey = sourceItr.getTopKey();
                    sourceItr.next();

                    if (!nextKey.getColumnFamily().toString().endsWith(entryId)) {
                        break;
                    }

                    String[] keyValueDatatype = nextKey.getColumnQualifier().toString().split(NULL_BYTE);

                    if (keyValueDatatype.length == 3) {

                        String vis = nextKey.getColumnVisibility().toString();

                        Attribute attribute = new Attribute(
                                keyValueDatatype[0],
                                typeRegistry.decode(keyValueDatatype[2], keyValueDatatype[1]),
                                setVisibility(new HashMap<String, String>(1), vis)
                        );


                        attributes.add(attribute);
                        timestamp = nextKey.getTimestamp();
                    }
                }

                int oneByte = entryId.indexOf(ONE_BYTE);
                String finalType = entryId.substring(0, oneByte);
                String finalId = entryId.substring(oneByte+1, entryId.length());

                EventBuilder entry = new EventBuilder(finalType, finalId, timestamp);

                if (attributes.size() > 0)
                    entry.attrs(attributes);

                writable.set(entry.build());
                return new Value(serialize(writable));

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return new Value("".getBytes());
    }


}
