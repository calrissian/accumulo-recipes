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
package org.calrissian.accumulorecipes.commons.support.tuple;


import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

import static java.util.Collections.unmodifiableMap;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity;

public class MetadataBuilder {

    protected final Map<String, Object> metadata;

    public MetadataBuilder() {
        this(new HashMap<String, Object>());
    }

    public MetadataBuilder(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public MetadataBuilder setVisibility(String visibility) {
        Visiblity.setVisibility(metadata, visibility);
        return this;
    }

    public MetadataBuilder setExpiration(long expiration) {
        Metadata.Expiration.setExpiration(metadata, expiration);
        return this;
    }

    public MetadataBuilder setTimestamp(long timestamp) {
        Metadata.Timestamp.setTimestamp(metadata, timestamp);
        return this;
    }

    public MetadataBuilder setCustom(String key, Object value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkArgument(key.length() > 0, "Not allowed to use an empty Metadata key");

        if (value != null)
            metadata.put(key, value);

        return this;
    }

    public Map<String, Object> build() {
        return unmodifiableMap(metadata);
    }
}
