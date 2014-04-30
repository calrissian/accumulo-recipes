package org.calrissian.accumulorecipes.eventstore.support.shard;


import org.apache.hadoop.io.Text;

import java.util.Date;
import java.util.SortedSet;

public interface ShardBuilder {

  public String buildShard(long timestamp, String uuid);

  public String buildShard(long timestamp, int partition);

  public SortedSet<Text> buildShardsInRange(Date start, Date stop);
}
