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
package org.calrissian.accumulorecipes.eventstore.support.shard;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.mango.domain.event.Event;

public class HourlyShardBuilder implements ShardBuilder<Event>, EventShardBuilder {

    protected final Integer numPartitions;

    protected String delimiter = "_";

    protected String dateFormat = "yyyyMMddHH";

    public HourlyShardBuilder(Integer numPartitions) {
        this.numPartitions = numPartitions;
    }

    public String buildShard(Event event) {
        return buildShard(event.getTimestamp(), (Math.abs(event.getId().hashCode()) % numPartitions));
    }

    public String buildShard(long timestamp, int partition) {
        int partitionWidth = String.valueOf(numPartitions).length();
        Date date = new Date(timestamp);
        return String.format("%s%s%0" + partitionWidth + "d", new SimpleDateFormat(dateFormat).format(date),
                delimiter, partition);
    }

    @Override
    public SortedSet<Text> buildShardsInRange(Date start, Date stop) {

        SortedSet<Text> shards = new TreeSet<Text>();

        int hours = (int) ((stop.getTime() - start.getTime()) / (60 * 60 * 1000));
        hours = hours > 0 ? hours : 1;

        for (int i = 0; i < hours; i++) {
            for (int j = 0; j < numPartitions; j++)
                shards.add(new Text(buildShard(start.getTime(), j)));
            start.setTime(start.getTime() + (60 * 60 * 1000));
        }

        return shards;
    }
}
