package org.calrissian.accumulorecipes.eventstore.support.shard;

import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.mango.domain.Event;

/**
 * Created by cjnolet on 5/16/14.
 */
public interface EventShardBuilder extends ShardBuilder<Event> {

  String buildShard(long timestamp, int partition);
}
