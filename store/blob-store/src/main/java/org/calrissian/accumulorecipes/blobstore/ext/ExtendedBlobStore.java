package org.calrissian.accumulorecipes.blobstore.ext;


import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.blobstore.BlobStore;

import java.io.OutputStream;
import java.util.Map;

public interface ExtendedBlobStore extends BlobStore {

    /**
     * Returns the size of the data stored with the given key and type.
     * @param key
     * @param type
     * @param auths
     * @return
     */
    int blobSize(String key, String type, Authorizations auths);

    /**
     * Returns the properties stored with the data for the given key and type.
     * @param key
     * @param type
     * @param auths
     * @return
     */
    Map<String, String> getProperties(String key, String type, Authorizations auths);

    /**
     * Provides an {@link java.io.OutputStream} to allow storage of the data into the underlying store.
     * @param key
     * @param type
     * @param visibility
     * @return
     */
    OutputStream store(String key, String type, Map<String, String> properties, long timestamp, String visibility);
}
