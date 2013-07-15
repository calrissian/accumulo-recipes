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
package org.calrissian.accumulorecipes.blobstore;

import org.calrissian.accumulorecipes.commons.domain.Auths;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * A storage facility for streaming content in/out. Content is anything that can be represented as bytes.
 */
public interface BlobStore {

    /**
     * Provides an {@link java.io.OutputStream} to allow storage of the data into the store.
     */
    OutputStream store(String key, String type, long timestamp, String visibility);

    /**
     * Provides an {@link InputStream} to retrieve the data from the store.
     */
    InputStream get(String key, String type, Auths auths);

}
