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
package org.calrissian.accumulorecipes.commons.support.qfd;

import org.apache.accumulo.core.data.Value;

public class GlobalIndexValue {

    private final long cardinatlity;
    private final long expiration;

    public GlobalIndexValue(Value value) {

        String str = new String(value.get());

        int idx = str.indexOf(",");

        if(idx == -1) {
            cardinatlity = Long.parseLong(str);
            expiration = -1;
        } else {
            cardinatlity = Long.parseLong(str.substring(0, idx));
            expiration = Long.parseLong(str.substring(idx + 1, str.length()));
        }
    }

    public GlobalIndexValue(long cardinality, long expiration) {

        this.cardinatlity = cardinality;
        this.expiration = expiration;
    }

    public long getCardinatlity() {
        return cardinatlity;
    }

    public long getExpiration() {
        return expiration;
    }

    public Value toValue() {
        return new Value((Long.toString(cardinatlity) + "," + Long.toString(expiration)).getBytes());
    }
}
