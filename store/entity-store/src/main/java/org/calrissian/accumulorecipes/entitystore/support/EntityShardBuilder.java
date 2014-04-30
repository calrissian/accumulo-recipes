package org.calrissian.accumulorecipes.entitystore.support;

import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;

public class EntityShardBuilder {

  private int partitionSize;

  public EntityShardBuilder(int partitionSize) {
    this.partitionSize = partitionSize;
  }

  public String buildShard(String entityType, String entityId) {
    int partition = Math.abs(entityId.hashCode() % partitionSize);
    return buildShard(entityType, partition);
  }

  public String buildShard(String entityType, int partition) {
    return format("%s_%" + Integer.toString(partitionSize).length() + "d", entityType, partition);
  }

  public Set<Text> buildShardsForTypes(Collection<String> types) {
    Set<Text> ret = new HashSet<Text>();
    for(int i = 0; i < partitionSize; i++) {
      for(String type : types)
        ret.add(new Text(buildShard(type, i)));
    }
    return ret;
  }
}
