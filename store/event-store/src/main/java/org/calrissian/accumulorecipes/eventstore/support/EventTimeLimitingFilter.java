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
package org.calrissian.accumulorecipes.eventstore.support;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.iterators.TimeLimitingFilter;
import org.calrissian.accumulorecipes.commons.support.Constants;

public class EventTimeLimitingFilter extends TimeLimitingFilter {

    @Override
    protected long parseTimestamp(Key k, Value v) {

        if(k.getColumnFamily().toString().startsWith(Constants.PREFIX_FI))
            return k.getTimestamp();

        ByteArrayInputStream bais = new ByteArrayInputStream(v.get());
        DataInputStream dis = new DataInputStream(bais);

        long ret = -1;
        try {
            dis.readLong();
            ret = dis.readLong();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                dis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return ret;
    }
}
