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
