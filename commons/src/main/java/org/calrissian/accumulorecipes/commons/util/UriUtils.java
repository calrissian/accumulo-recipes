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
