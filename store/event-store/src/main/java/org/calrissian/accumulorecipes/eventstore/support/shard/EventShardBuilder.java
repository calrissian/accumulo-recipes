package org.calrissian.accumulorecipes.eventstore.support.shard;

import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.mango.domain.Event;

public interface EventShardBuilder extends ShardBuilder<Event> {

  String buildShard(long timestamp, int partition);
}
