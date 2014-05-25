package org.calrissian.accumulorecipes.commons.hadoop;

import com.google.common.collect.Iterators;
import org.calrissian.accumulorecipes.commons.mock.MockRecordReader;
import org.calrissian.mango.domain.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.EMPTY_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RecordReaderValueIteratorTest {

    Pair<String,String> pair1 =  new Pair<String, String>("1", "2");
    Pair<String,String> pair2 = new Pair<String, String>("3", "4");

    @Test
    public void testEmpty_returnsNothing() {
        MockRecordReader<String, String> mockRecordReader = new MockRecordReader<String, String>(EMPTY_LIST);
        RecordReaderValueIterator<String,String> rrvi = new RecordReaderValueIterator<String, String>(mockRecordReader);
        assertEquals(0, Iterators.size(rrvi));
    }

    @Test
    public void testNonEmpty_returnsData() {

        List<Pair<String,String>> pairs = new ArrayList<Pair<String, String>>();
        pairs.add(pair1);
        pairs.add(pair2);

        MockRecordReader<String, String> mockRecordReader = new MockRecordReader<String, String>(pairs);
        RecordReaderValueIterator<String,String> rrvi = new RecordReaderValueIterator<String, String>(mockRecordReader);

        int count = 0;
        while(rrvi.hasNext()) {
            if(count == 0) {
                assertEquals(pair1.getTwo(), rrvi.next());
            } else if(count == 1)
                assertEquals(pair2.getTwo(), rrvi.next());
            else
                fail();

            count++;
        }

        assertEquals(2, count);

    }

}
