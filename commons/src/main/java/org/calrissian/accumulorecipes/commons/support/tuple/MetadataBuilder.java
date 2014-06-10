package org.calrissian.accumulorecipes.commons.support.tuple;


import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

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
