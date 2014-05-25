package org.calrissian.accumulorecipes.eventstore.pig;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.accumulorecipes.eventstore.hadoop.EventInputFormat;
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore;
import org.calrissian.accumulorecipes.test.MockRecordReader;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.singleton;

/**
 * Created by cjnolet on 5/24/14.
 */
public class EventLoaderTest {

    Event event;
    Configuration conf;

    @Before
    public void setup() throws IOException {
        conf = new Configuration();
    }

    @Test
    public void test() throws AccumuloException, TableExistsException, TableNotFoundException, AccumuloSecurityException, IOException, InterruptedException {
        setUpJob();

        List<Pair<String, EventWritable>> mocks = new ArrayList<Pair<String, EventWritable>>();
        mocks.add(new Pair<String, EventWritable>("", new EventWritable(event)));

        MockRecordReader<String, EventWritable> mockRecordReader = new MockRecordReader<String, EventWritable>(mocks);

        EventLoader loader = new EventLoader();
        loader.prepareToRead(mockRecordReader, new PigSplit());

        org.apache.pig.data.Tuple t;
        while((t = loader.getNext()) != null) {
            System.out.println(t);
        }


    }

    private void setUpJob() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException {
        Instance instance = new MockInstance("instName");
        Connector connector = instance.getConnector("root", "".getBytes());
        AccumuloEventStore store = new AccumuloEventStore(connector);
        event = new BaseEvent(UUID.randomUUID().toString());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", false, ""));
        store.save(singleton(event));

        EventInputFormat.setInputInfo(conf, "root", "".getBytes(), new Authorizations());
        EventInputFormat.setMockInstance(conf, "instName");
        EventInputFormat.setQueryInfo(conf, new Date(System.currentTimeMillis() - 50000), new Date(),
                new QueryBuilder().eq("key1", "val1").build(), null);

    }
}
