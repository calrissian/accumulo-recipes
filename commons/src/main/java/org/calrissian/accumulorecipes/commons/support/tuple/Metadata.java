package org.calrissian.accumulorecipes.commons.support.tuple;


import org.calrissian.mango.domain.Tuple;

import java.util.Map;

public class Metadata {

    public static class Expiration {

        public static final String EXPIRATION = "expiration";

        private Expiration(){}

        public static Map<String, Object> addExpiration(Map<String, Object> metadata, long expiration) {
            metadata.put(EXPIRATION, expiration);
            return metadata;
        }

        public static Long getExpiration(Tuple tuple, long defaultExpiration) {
            if(tuple.getMetadataValue(EXPIRATION) == null)
                return defaultExpiration;
            else
                return (Long)tuple.getMetadataValue(EXPIRATION);
        }
    }

    public static class Visiblity {

        public static final String VISIBILITY = "visibility";

        private Visiblity(){}

        public static Map<String, Object> addVisibility(Map<String, Object> metadata, String visibility) {
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
