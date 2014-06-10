package org.calrissian.accumulorecipes.commons.iterators;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.support.metadata.BaseMetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.tuple.Metadata;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.System.currentTimeMillis;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.junit.Assert.assertEquals;

public class MetadataExpirationFilterTest {

    @Test
    public void test() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "".getBytes());
        connector.tableOperations().create("test");

        MetadataSerDe metadataSerDe = new BaseMetadataSerDe(LEXI_TYPES);

        IteratorSetting setting = new IteratorSetting(10, "filter", MetadataExpirationFilter.class);
        MetadataExpirationFilter.setMetadataSerde(setting, metadataSerDe);


        Map<String, Object> metadataMap = new HashMap<String, Object>();
        Metadata.Expiration.setExpiration(metadataMap, 1);

        BatchWriter writer = connector.createBatchWriter("test", 1000, 1000l, 10);
        Mutation m = new Mutation("a");
        m.put(new Text("b"), new Text(), currentTimeMillis() - 500, new Value(metadataSerDe.serialize(metadataMap)));
        m.put(new Text("c"), new Text(), currentTimeMillis() - 500, new Value("".getBytes()));

        writer.addMutation(m);
        writer.flush();

        Scanner scanner = connector.createScanner("test", new Authorizations());
        scanner.setRange(new Range("a"));

        assertEquals(2, Iterables.size(scanner));

        connector.tableOperations().attachIterator("test", setting);

        assertEquals(1, Iterables.size(scanner));

        assertEquals("c", Iterables.get(scanner, 0).getKey().getColumnFamily().toString());
    }


}
