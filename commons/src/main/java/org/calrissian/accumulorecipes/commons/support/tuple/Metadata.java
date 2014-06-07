package org.calrissian.accumulorecipes.commons.support.tuple;


import org.calrissian.mango.domain.Tuple;

public class Metadata {

    public static class Expiration {

        public static final String EXPIRATION = "expiration";

        private Expiration(){}

        public static void setExpiration(Tuple tuple, long expiration) {
            tuple.setMetadataValue(EXPIRATION, expiration);
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

        public static void setVisibility(Tuple tuple, String visibility) {
            tuple.setMetadataValue(VISIBILITY, visibility);
        }

        public static String getVisibility(Tuple tuple, String defaultVisibility) {
            if(tuple.getMetadataValue(VISIBILITY) == null)
                return defaultVisibility;
            else
                return (String)tuple.getMetadataValue(VISIBILITY);
        }
    }
}
