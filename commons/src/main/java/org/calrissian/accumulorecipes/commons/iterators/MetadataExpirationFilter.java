/*
 * Copyright (C) 2015 The Calrissian Authors
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
package org.calrissian.accumulorecipes.commons.iterators;

import static java.lang.Math.min;
import static org.calrissian.accumulorecipes.commons.util.RowEncoderUtil.decodeRow;
import static org.calrissian.accumulorecipes.commons.util.RowEncoderUtil.encodeRow;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.calrissian.accumulorecipes.commons.support.Constants;

public class MetadataExpirationFilter extends WrappingIterator {

    public Value extractExpiredAttributes(Key k, Value v) {

        if(k.getColumnFamily().toString().startsWith(Constants.PREFIX_E)) {

            ByteArrayInputStream bais = new ByteArrayInputStream(v.get());
            DataInputStream dis = new DataInputStream(bais);
            try {
                dis.readInt();
                long expiration = dis.readLong();
                if(shouldExpire(expiration, parseTimestampFromKey(k))) {
                    long newMinExpiration = Long.MAX_VALUE;
                    List<Map.Entry<Key,Value>> finalKeyValList = new ArrayList();
                    for(Map.Entry<Key,Value> curEntry : decodeRow(k, bais)) {
                        long curExpiration = Long.parseLong(curEntry.getKey().getColumnFamily().toString());
                        if(!shouldExpire(curExpiration, curEntry.getKey().getTimestamp())) {
                            min(curExpiration, newMinExpiration);
                            finalKeyValList.add(curEntry);
                        }
                    }

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(baos);
                    dos.writeInt(finalKeyValList.size());
                    dos.writeLong(newMinExpiration != Long.MAX_VALUE ? newMinExpiration : -1);
                    dos.flush();
                    encodeRow(finalKeyValList, baos);
                    baos.flush();

                    return new Value(baos.toByteArray());
                }
            } catch (IOException e) {
                return v;
            }
        }

        return v;
    }

    /**
     * This method has been broken out for situations where logical may be used and the timestamp
     * has been placed somewhere else in the key.
     * @param k
     * @return
     */
    protected long parseTimestampFromKey(Key k) {
        return k.getTimestamp();
    }

    /**
     * Utility method used both internally and externally to determine when a key should expire based
     * on a dynamic expiration in the metadata of a attribute.
     * @param expiration
     * @param timestamp
     * @return
     */
    public static boolean shouldExpire(long expiration, long timestamp) {
        return (expiration > -1 && System.currentTimeMillis() - timestamp > expiration);
    }

    @Override
    public Value getTopValue() {
        // apply expiration
        return extractExpiredAttributes(getTopKey(), super.getTopValue());
    }

}

