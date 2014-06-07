package org.calrissian.accumulorecipes.commons.hadoop;


import org.calrissian.accumulorecipes.commons.support.WritableUtils2;
import org.calrissian.mango.domain.Tuple;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TupleWritableTest {

    @Test
    public void testSerializesAndDeserializes() throws IOException {

        Tuple tuple = new Tuple("key", "val");
        tuple.setMetadataValue("metaKey", "metaVal");

        byte[] serialized = WritableUtils2.serialize(new TupleWritable(tuple));

        Tuple actual = WritableUtils2.asWritable(serialized, TupleWritable.class).get();
        assertEquals(tuple, actual);
    }


}
