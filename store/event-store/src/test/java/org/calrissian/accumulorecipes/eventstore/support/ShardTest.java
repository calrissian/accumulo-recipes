package org.calrissian.accumulorecipes.eventstore.support;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.Set;

import static org.calrissian.accumulorecipes.eventstore.support.Constants.DEFAULT_PARTITION_SIZE;
import static org.junit.Assert.assertEquals;

public class ShardTest {

    ShardBuilder shardBuilder;

    @Before
    public void setUp() {
        this.shardBuilder = new ShardBuilder(DEFAULT_PARTITION_SIZE);
    }

    @Test
    public void testBuildShardsInRange_multipleHours() {
        Set<Text> ranges = shardBuilder.buildShardsInRange(new Date(), new Date(System.currentTimeMillis() + (60 * 1000 * 60 * 4)));
        assertEquals(Constants.DEFAULT_PARTITION_SIZE * 4, ranges.size());
    }

    @Test
    public void testBuildShardsInRange_noHours() {
        Set<Text> ranges = shardBuilder.buildShardsInRange(new Date(), new Date());
        assertEquals(Constants.DEFAULT_PARTITION_SIZE * 1, ranges.size());
    }

}
