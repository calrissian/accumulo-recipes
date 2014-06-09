package org.calrissian.accumulorecipes.commons.support.qfd;

import org.calrissian.accumulorecipes.commons.support.tuple.MetadataBuilder;

import java.util.Map;

import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Expiration;

public class QfdMetadataBuilder extends MetadataBuilder{

    public QfdMetadataBuilder() {
    }

    public QfdMetadataBuilder(Map<String, Object> metadata) {
        super(metadata);
    }

    public QfdMetadataBuilder setExpiration(long expiration) {
        Expiration.setExpiration(metadata, expiration);
        return this;
    }
}
