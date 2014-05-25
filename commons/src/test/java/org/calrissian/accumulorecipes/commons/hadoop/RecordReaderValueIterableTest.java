package org.calrissian.accumulorecipes.commons.hadoop;

import com.google.common.collect.Iterables;
import org.calrissian.accumulorecipes.commons.mock.MockRecordReader;
import org.junit.Test;

import static java.util.Collections.EMPTY_LIST;
import static org.junit.Assert.assertEquals;

public class RecordReaderValueIterableTest {

    @Test
    public void testEmpty_returnsNothing() {
        MockRecordReader<String, String> mockRecordReader = new MockRecordReader<String, String>(EMPTY_LIST);
        RecordReaderValueIterable<String,String> rrvi = new RecordReaderValueIterable<String, String>(mockRecordReader);
        assertEquals(0, Iterables.size(rrvi));
    }

    
}
