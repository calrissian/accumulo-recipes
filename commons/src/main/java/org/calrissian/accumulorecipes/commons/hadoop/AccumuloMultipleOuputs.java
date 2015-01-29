package org.calrissian.accumulorecipes.commons.hadoop;

import java.io.IOException;

import org.apache.accumulo.core.client.mapred.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * A wrapper around the {@link org.apache.accumulo.core.client.mapred.AccumuloFileOutputFormat} that will output
 * each group into a separate folder in HDFS. This enables keys/values for multiple tables to be partitioned, sorted,
 * and output within the same mapreduce job.`
 */
public class AccumuloMultipleOuputs extends MultipleOutputFormat<GroupedKey,Value> {

  @Override
  protected RecordWriter<GroupedKey,Value> getBaseRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
    return new GroupedKeyRecordWriter(fileSystem, jobConf, s, progressable);
  }

  /**
   * Makes sure that a file for a specific grouped key is always placed in the folder of that group
   * @param key
   * @param value
   * @param name
   * @return
   */
  @Override
  protected String generateFileNameForKeyValue(GroupedKey key, Value value, String name) {
    return key.getGroup() + "/" + key.getGroup() + "-" + name;
  }

  protected static class GroupedKeyRecordWriter implements RecordWriter<GroupedKey,Value> {

    private final RecordWriter<Key,Value> internalRecordWriter;

    public GroupedKeyRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
      this.internalRecordWriter = new AccumuloFileOutputFormat().getRecordWriter(fileSystem, jobConf, s, progressable);
    }

    @Override
    public void write(GroupedKey key, Value value) throws IOException {
      internalRecordWriter.write(key.getKey(), value);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      internalRecordWriter.close(reporter);
    }
  }
}
