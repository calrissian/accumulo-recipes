package org.calrissian.accumulorecipes.blobstore;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.blobstore.support.HeaderInputStream;

import java.io.InputStream;
import java.util.Map;

/**
 * A storage facility for streaming content in/out. Content is anything that can be represented as bytes.
 * @param <T>
 */
public interface BlobStore<T extends HeaderInputStream> {

    /**
     * Puts an input stream into the store.
     * @param blob
     * @param type some grouped name
     * @param timestamp
     * @param visibility
     * @return a uuid for the key that was generated
     */
    String put(InputStream blob, String type, long timestamp, String visibility);

    /**
     * Puts an input stream into the store along with specified headers
     * @param blob
     * @param type
     * @param timestamp
     * @param visibility
     * @param headers
     * @return a uuid for the key that was generated
     */
    String put(InputStream blob, String type, long timestamp, String visibility, Map<String,String> headers);

    /**
     * Puts an input stream into the store at the given key (needs to be unique)
     * @param key
     * @param blob
     * @param type
     * @param timestamp
     * @param visibility
     */
    void put(String key, InputStream blob, String type, long timestamp, String visibility);

    /**
     * Puts an input stream into the store at the given key with the specified headers
     * @param key
     * @param blob
     * @param type
     * @param timestamp
     * @param visibility
     * @param headers
     */
    void put(String key, InputStream blob, String type, long timestamp, String visibility, Map<String,String> headers);

    /**
     * Gets an inputstream from the store
     * @param key
     * @param type
     * @param auths
     * @return
     */
    T get(String key, String type, Authorizations auths);

    /**
     * Release any resources being held
     */
    void shutdown();
}
