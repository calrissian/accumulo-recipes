/*
 * Copyright (C) 2015 The Calrissian Authors
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

import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.mango.domain.event.Event;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public abstract class TimeBasedShardBuilder implements ShardBuilder<Event>, EventShardBuilder {

  protected final Integer numPartitions;

  protected String delimiter = "_";

  protected DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(getDateFormat());

  public TimeBasedShardBuilder(Integer numPartitions) {
    this.numPartitions = numPartitions;
  }

  protected abstract String getDateFormat();

  @Override public int numPartitions() {
    return numPartitions;
  }

  public String buildShard(Event event) {
    return buildShard(event.getTimestamp(), (Math.abs(event.getId().hashCode()) % numPartitions));
  }

  public String buildShard(long timestamp, int partition) {
    int partitionWidth = String.valueOf(numPartitions).length();
    return String.format("%s%s%0" + partitionWidth + "d", dateTimeFormatter.print(timestamp),
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
