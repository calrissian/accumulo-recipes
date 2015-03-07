/*
* Copyright (C) 2014 The Calrissian Authors
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
package org.calrissian.accumulorecipes.commons.support.attribute;


import java.util.Map;

import org.calrissian.mango.domain.Attribute;

public class Metadata {

    public static class Expiration {

        public static final String EXPIRATION = "expiration";

        private Expiration(){}

        public static Map<String, String> setExpiration(Map<String, String> metadata, long expiration) {
            metadata.put(EXPIRATION, Long.toString(expiration));
            return metadata;
        }

        public static Long getExpiration(Attribute attribute, long defaultExpiration) {
            return getExpiration(attribute.getMetadata(), defaultExpiration);
        }

        public static Long getExpiration(Map<String,String> metadata, long defaultExpiration) {
            if(!metadata.containsKey(EXPIRATION))
                return defaultExpiration;
            else
                return Long.parseLong(metadata.get(EXPIRATION));
        }

    }

    public static class Visiblity {

        public static final String VISIBILITY = "visibility";

        private Visiblity(){}

        public static Map<String, String> setVisibility(Map<String, String> metadata, String visibility) {
            if (visibility != null && !visibility.isEmpty())
                metadata.put(VISIBILITY, visibility);

            return metadata;
        }

        public static String getVisibility(Attribute attribute, String defaultVisibility) {
            if(attribute.getMetadataValue(VISIBILITY) == null)
                return defaultVisibility;
            else
                return attribute.getMetadataValue(VISIBILITY);
        }


      public static String getVisibility(Map<String,String> attribute, String defaultVisibility) {
        if(!attribute.containsKey(VISIBILITY))
          return defaultVisibility;
        else
          return attribute.get(VISIBILITY);
      }

    }

    public static class Timestamp {

      public static final String TIMESTAMP = "timestamp";

      private Timestamp(){}

      public static Map<String, String> setTimestamp(Map<String, String> metadata, long timestamp) {
        metadata.put(TIMESTAMP, Long.toString(timestamp));

        return metadata;
      }

      public static long getTimestamp(Map<String,String> metadata, long defaultTimestamp) {
        if(!metadata.containsKey(TIMESTAMP))
          return defaultTimestamp;
        else
          return Long.parseLong(metadata.get(TIMESTAMP));
      }

    }

}
