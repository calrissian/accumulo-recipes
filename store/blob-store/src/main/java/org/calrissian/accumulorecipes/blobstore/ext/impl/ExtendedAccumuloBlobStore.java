/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.calrissian.accumulorecipes.blobstore.ext.impl;


import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.blobstore.ext.ExtendedBlobStore;
import org.calrissian.accumulorecipes.blobstore.impl.AccumuloBlobStore;
import org.calrissian.mango.types.exception.TypeNormalizationException;

import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.EnumSet.allOf;
import static java.util.Map.Entry;
import static org.apache.accumulo.core.client.IteratorSetting.Column;
import static org.apache.commons.lang.StringUtils.defaultString;
import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang.Validate.notNull;

/**
 * This implementation is an extension of the {@link AccumuloBlobStore} which stores additional data
 * including the storage size (in bytes) and the properties for the data.
 *
 * Data Row format is as follows:
 *
 * RowId:               key\u0000type
 * Column Family:       DATA
 * Column Qualifier:    sequence#
 * Value:               byte[]
 *
 * Row format is as follows:
 *
 * RowId:               key\u0000type
 * Column Family:       SIZE
 * Column Qualifier:
 * Value:               chunksize
 *
 * Property format is as follows:
 *
 * RowId:               key\u0000type
 * Column Family:       PROP
 * Column Qualifier:    \u0000propKey\u0000propValue
 * Value:
 */
public class ExtendedAccumuloBlobStore extends AccumuloBlobStore implements ExtendedBlobStore{

    private static final String PROP_CF = "PROP";
    private static final String SIZE_CF = "SIZE";

    public ExtendedAccumuloBlobStore(Connector connector) throws TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector);
    }

    public ExtendedAccumuloBlobStore(Connector connector, String tableName) throws TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector, tableName);
    }

    public ExtendedAccumuloBlobStore(Connector connector, int bufferSize) throws TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector, bufferSize);
    }

    public ExtendedAccumuloBlobStore(Connector connector, String tableName, int bufferSize) throws TableExistsException, AccumuloSecurityException, AccumuloException {
        super(connector, tableName, bufferSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void createTable() throws TableExistsException, AccumuloSecurityException, AccumuloException {
        super.createTable();

        //Set up the default summing iterator with a priority of 5
        IteratorSetting setting = new IteratorSetting(5, "blob-size", SummingCombiner.class);
        SummingCombiner.setColumns(setting, asList(new Column(SIZE_CF, "")));
        SummingCombiner.setEncodingType(setting, LongCombiner.Type.STRING);
        try {
            connector.tableOperations().attachIterator(tableName, setting, allOf(IteratorUtil.IteratorScope.class));
        } catch (TableNotFoundException e) {
            //TODO should never happen, but atleast log if there is a problem
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Mutation generateMutation(String key, String type, byte[] data, int sequenceNum, long timestamp, ColumnVisibility visibility) throws TypeNormalizationException {
        Mutation m = super.generateMutation(key, type, data, sequenceNum, timestamp, visibility);

        //add a size value to the mutation
        m.put(SIZE_CF, "", visibility, Integer.toString(data.length));

        return m;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int blobSize(String key, String type, Authorizations auths) {
        notNull(auths, "Null authorizations");

        try {
            String rowId = generateRowId(key, type);
            //Scan over the range for the key, but only include the size column family
            Scanner scanner = connector.createScanner(tableName, auths);
            scanner.setRange(new Range(
                    new Key(rowId, SIZE_CF),
                    new Key(rowId, SIZE_CF + "\u0000")
            ));
            scanner.fetchColumnFamily(new Text(SIZE_CF));
            scanner.setBatchSize(1);

            Iterator<Entry<Key,Value>> iterator = scanner.iterator();
            if (iterator.hasNext())
                return parseInt(iterator.next().getValue().toString());

            return 0;

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getProperties(String key, String type, Authorizations auths) {
        notNull(auths, "Null authorizations");

        try {
            String rowId = generateRowId(key, type);
            //Scan for a range including only the data
            Scanner scanner = connector.createScanner(tableName, auths);
            scanner.setRange(new Range(
                    new Key(rowId, PROP_CF, "\u0000"),
                    new Key(rowId, PROP_CF, "\uFFFF")
            ));
            scanner.fetchColumnFamily(new Text(PROP_CF));

            Iterator<Entry<Key,Value>> iterator = scanner.iterator();
            if (!iterator.hasNext())
                return emptyMap();

            Map<String, String> properties = new LinkedHashMap<String, String>();
            while (iterator.hasNext()) {
                String[] keyVal = iterator.next().getKey().getColumnQualifier().toString().replaceFirst("\u0000", "")
                        .split("\u0000");

                if(keyVal.length == 2)
                    properties.put(keyVal[0], keyVal[1]);
            }

            return properties;

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream store(String key, String type, Map<String, String> properties, long timestamp, String visibility) {

        //Use of table user's auths instead of callers auths means information is leaked about
        //key and type.  Therefore they should now contain protected information, or this check
        //can not be done.
        isTrue(!checkExists(key, type), String.format("Data with %s type and %s key already exists.", type, key));

        try {

            ColumnVisibility colVis = new ColumnVisibility(defaultString(visibility));
            BatchWriter writer = getWriter();

            //If there are properties write these first
            if (properties != null && !properties.isEmpty()) {

                Mutation m = new Mutation(generateRowId(key, type));
                for (Entry<String, String> prop : properties.entrySet()) {
                    m.put(PROP_CF, "\u0000" + defaultString(prop.getKey()) + "\u0000" + defaultString(prop.getValue()),
                            colVis, timestamp, new Value(new byte[]{}));
                }
                writer.addMutation(m);
            }

            return generateWriteStream(writer, key, type, timestamp, visibility);

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
