/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.calrissian.accumulorecipes.blobstore.ext;


import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.blobstore.BlobStore;

import java.io.OutputStream;
import java.util.Map;

/**
 * A blob store with the ability to store and retrieve additional metadata about the blobs stored in the store.
 */
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
     * Provides an {@link java.io.OutputStream} to allow storage of the data into the store along with some properties.
     * @param key
     * @param type
     * @param visibility
     * @return
     */
    OutputStream store(String key, String type, Map<String, String> properties, long timestamp, String visibility);
}
