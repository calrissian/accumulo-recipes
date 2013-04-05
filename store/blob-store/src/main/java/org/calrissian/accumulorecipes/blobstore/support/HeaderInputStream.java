/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.blobstore.support;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public abstract class HeaderInputStream extends InputStream {

    protected Map<String,String> headers = new HashMap<String,String>();
    protected String name;

    public Map<String,String> getHeaders() {
        return headers;
    }

    public String getName() {
        return name;
    }

    protected void setHeaders(Map<String,String> headers) {
        this.headers = headers;
    }

    protected void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "HeaderInputStream{" +
                "name='" + name + '\'' +
                ", headers=" + headers +
                '}';
    }
}
