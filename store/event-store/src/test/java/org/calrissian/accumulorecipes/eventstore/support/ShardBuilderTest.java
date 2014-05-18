/*
 * Copyright (C) 2013 The Calrissian Authors
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
package org.calrissian.accumulorecipes.eventstore.support;

import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.support.Constants;
import org.calrissian.accumulorecipes.eventstore.support.shard.HourlyShardBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.Set;

import static org.calrissian.accumulorecipes.commons.support.Constants.DEFAULT_PARTITION_SIZE;
import static org.junit.Assert.assertEquals;

public class ShardBuilderTest {

    HourlyShardBuilder shardBuilder;

    @Before
    public void setUp() {
        this.shardBuilder = new HourlyShardBuilder(DEFAULT_PARTITION_SIZE);
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
