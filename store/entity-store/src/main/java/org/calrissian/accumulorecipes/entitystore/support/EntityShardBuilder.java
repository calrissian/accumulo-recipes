package org.calrissian.accumulorecipes.entitystore.support;

import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.Set;

import static java.lang.String.format;

public class EntityShardBuilder {

  private int partitionSize;

  public EntityShardBuilder(int partitionSize) {
    this.partitionSize = partitionSize;
  }

  public String buildShard(String entityType, String entityId) {
    int partition = Math.abs(entityId.hashCode() % partitionSize);
    return format("%s_%" + Integer.toString(partitionSize).length() + "d", entityType, partition);
  }

  public Set<Text> buildShardsForTypes(Collection<String> types) {
    return null;
  }
}
