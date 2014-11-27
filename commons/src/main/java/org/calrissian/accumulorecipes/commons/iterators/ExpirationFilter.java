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
package org.calrissian.accumulorecipes.commons.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;

import static java.lang.System.currentTimeMillis;

/**
 * Allows Accumulo to expire keys/values based on an expiration in the value.
 */
public abstract class ExpirationFilter extends Filter {


   /**
     * Accepts entries whose timestamps are less than currentTime - threshold.
     */
    @Override
    public boolean accept(Key k, Value v) {
       if (v.get().length > 0) {
            long timestamp = parseTimestamp(k, v);
            long threshold = parseExpiration(timestamp, k, v);
            return !shouldExpire(threshold, timestamp);
        }
        return true;
    }



    public static boolean shouldExpire(long threshold, long timestamp) {
      if (threshold > -1) {
        long currentTime = currentTimeMillis();
        if (currentTime - timestamp > threshold)
          return true;
      }
      return false;
    }

    protected abstract long parseExpiration(long timestamp, Key k, Value v);

    protected long parseTimestamp(Key k, Value v) {
      return k.getTimestamp();
    }
}
