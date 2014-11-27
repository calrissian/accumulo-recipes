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
package org.calrissian.accumulorecipes.commons.support.tuple;


import java.util.Map;

import org.calrissian.mango.domain.Tuple;

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


      public static String getVisibility(Map<String,Object> tuple, String defaultVisibility) {
        if(!tuple.containsKey(VISIBILITY))
          return defaultVisibility;
        else
          return (String)tuple.get(VISIBILITY);
      }

    }
}
