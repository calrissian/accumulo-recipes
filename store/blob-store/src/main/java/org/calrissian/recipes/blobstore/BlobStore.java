package org.calrissian.recipes.blobstore;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.recipes.blobstore.support.HeaderInputStream;

import java.io.InputStream;
import java.util.Map;

public interface BlobStore<T extends HeaderInputStream> {

    String put(InputStream blob, String type, long timestamp, String visibility);

    String put(InputStream blob, String type, long timestamp, String visibility, Map<String,String> headers);

    void put(String key, InputStream blob, String type, long timestamp, String visibility);

    void put(String key, InputStream blob, String type, long timestamp, String visibility, Map<String,String> headers);

    T get(String key, String type, Authorizations auths);

    void shutdown();
}
