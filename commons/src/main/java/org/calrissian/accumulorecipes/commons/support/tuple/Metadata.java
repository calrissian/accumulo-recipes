package org.calrissian.accumulorecipes.commons.support.tuple;


import org.calrissian.mango.domain.Tuple;

import java.util.Map;

public class Metadata {

    public static class Expiration {

        public static final String EXPIRATION = "expiration";

        private Expiration(){}

        public static Map<String, Object> setExpiration(Map<String, Object> metadata, long expiration) {
            metadata.put(EXPIRATION, expiration);
            return metadata;
        }

        public static Long getExpiration(Tuple tuple, long defaultExpiration) {
            return getExpiration(tuple.getMetadata(), defaultExpiration);
        }

        public static Long getExpiration(Map<String,Object> metadata, long defaultExpiration) {
            if(!metadata.containsKey(EXPIRATION))
                return defaultExpiration;
            else
                return (Long)metadata.get(EXPIRATION);
        }

    }

    public static class Visiblity {

        public static final String VISIBILITY = "visibility";

        private Visiblity(){}

        public static Map<String, Object> setVisibility(Map<String, Object> metadata, String visibility) {
            if (visibility != null && !visibility.isEmpty())
                metadata.put(VISIBILITY, visibility);

            return metadata;
        }

        public static String getVisibility(Tuple tuple, String defaultVisibility) {
            if(tuple.getMetadataValue(VISIBILITY) == null)
                return defaultVisibility;
            else
                return (String)tuple.getMetadataValue(VISIBILITY);
        }
    }
}
