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

import static org.calrissian.accumulorecipes.commons.support.Constants.PREFIX_FI;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;

public class FieldIndexExpirationFilter extends Filter {
    @Override
    public boolean accept(Key k, Value v) {
        String cf = k.getColumnFamily().toString();
        if(cf.startsWith(PREFIX_FI)) {
            long expiration = Long.parseLong(new String(v.get()));
            return !MetadataExpirationFilter.shouldExpire(expiration, k.getTimestamp());
        }

        return true;
    }
}
