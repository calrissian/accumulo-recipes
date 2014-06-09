package org.calrissian.accumulorecipes.commons.hadoop;


import com.google.common.collect.ImmutableMap;
import org.calrissian.mango.domain.Tuple;
import org.junit.Test;

import java.io.IOException;

import static org.calrissian.accumulorecipes.commons.support.WritableUtils2.asWritable;
import static org.calrissian.accumulorecipes.commons.support.WritableUtils2.serialize;
import static org.junit.Assert.assertEquals;

public class TupleWritableTest {

    @Test
    public void testSerializesAndDeserializes() throws IOException {

        Tuple tuple = new Tuple("key", "val", ImmutableMap.of("metaKey", "metaVal"));

        byte[] serialized = serialize(new TupleWritable(tuple));

        Tuple actual = asWritable(serialized, TupleWritable.class).get();
        assertEquals(tuple, actual);
    }


}
