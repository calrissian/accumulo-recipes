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
package org.calrissian.accumulorecipes.commons.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.support.qfd.GlobalIndexValue;

/**
 * Allows Accumulo to expire keys/values based on an expiration threshold in the value of the global index table.
 */
public class GlobalIndexExpirationFilter extends ExpirationFilter {

    protected long parseExpiration(long timestamp, Key k,  Value v) {
        GlobalIndexValue val = new GlobalIndexValue(v);
        return val.getExpiration();
    }
}
