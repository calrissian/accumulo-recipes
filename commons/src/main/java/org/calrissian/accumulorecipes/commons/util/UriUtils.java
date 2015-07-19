/*
 * Copyright (C) 2015 The Calrissian Authors
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
package org.calrissian.accumulorecipes.commons.util;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;

public class UriUtils {

    private UriUtils() {}

    public static Multimap<String, String> splitQuery(String query) throws UnsupportedEncodingException {
        Multimap<String, String> query_pairs = LinkedListMultimap.create();
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
        }
        return query_pairs;
    }

    public static Multimap<String,String> splitQuery(URI uri) throws UnsupportedEncodingException {
        return splitQuery(uri.getQuery());
    }
}
