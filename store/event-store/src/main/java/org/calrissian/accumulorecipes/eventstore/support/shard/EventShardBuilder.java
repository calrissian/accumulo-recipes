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

import java.util.Date;
import java.util.SortedSet;

import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.mango.domain.event.Event;

/**
 * This specifies how to build a single shard for a given partition
 * and how to build a set of shards within a given time range. With
 * this class, different shard strategies can be built pretty easily.
 * The main constraint on the shard scheme is that it needs to be
 * able to be scanned lexicographically by time.
 */
public interface EventShardBuilder extends ShardBuilder<Event> {

    String buildShard(long timestamp, int partition);

    SortedSet<Text> buildShardsInRange(Date start, Date stop);
}
