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

import static org.calrissian.accumulorecipes.commons.support.Constants.PREFIX_E;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;

public class MetadataExpirationFilter extends Filter {


    protected long parseTimestampFromValue(Value v) {
        ByteArrayInputStream bais = new ByteArrayInputStream(v.get());
        DataInputStream dis = new DataInputStream(bais);
        try {
            dis.readLong();
            long returnVal = dis.readLong();
            return returnVal;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
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
    public boolean accept(Key k, Value v) {
         if(k.getColumnFamily().toString().startsWith(PREFIX_E)) {

            ByteArrayInputStream bais = new ByteArrayInputStream(v.get());
            DataInputStream dis = new DataInputStream(bais);
            try {
                long expiration = dis.readLong();
                boolean should = shouldExpire(expiration, parseTimestampFromValue(v));
                return !should;
            } catch(Exception e) {
                return true;
            }
        }

        return true;
    }


}

