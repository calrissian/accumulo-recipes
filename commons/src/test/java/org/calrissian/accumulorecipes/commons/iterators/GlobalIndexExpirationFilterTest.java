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
import org.calrissian.accumulorecipes.commons.support.qfd.GlobalIndexValue;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GlobalIndexExpirationFilterTest {

    @Test
    public void testExpiration() {

        GlobalIndexExpirationFilter expirationFilter = new GlobalIndexExpirationFilter();

        Key key = new Key();
        key.setTimestamp(System.currentTimeMillis() - 1000);
        assertTrue(expirationFilter.accept(key, new GlobalIndexValue(1, 100000).toValue()));
        assertFalse(expirationFilter.accept(key, new GlobalIndexValue(1, 1).toValue()));
        assertTrue(expirationFilter.accept(key, new GlobalIndexValue(1, -1).toValue()));

        assertTrue(expirationFilter.accept(key, new Value("1".getBytes())));        // test backwards compatibility
    }
}
