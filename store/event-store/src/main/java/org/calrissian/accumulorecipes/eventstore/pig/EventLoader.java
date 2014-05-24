package org.calrissian.accumulorecipes.eventstore.pig;

import com.google.common.collect.Multimap;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.calrissian.accumulorecipes.commons.hadoop.StoreEntryWritable;
import org.calrissian.accumulorecipes.eventstore.hadoop.EventInputFormat;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.uri.support.UriUtils;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;

public class EventLoader extends LoadFunc {

  private RecordReader<Key, StoreEntryWritable> reader;

  @Override
  public void setLocation(String uri, Job job) throws IOException {

    Configuration conf = job.getConfiguration();

    String queryPortion = uri.substring(uri.indexOf("?"), uri.length());
    Multimap<String,String> queryParams = UriUtils.splitQuery(queryPortion);

    String accumuloUser = getProp(queryParams, "accumuloUser");
    String accumuloPass = getProp(queryParams, "accumuloPass");
    String accumuloInst = getProp(queryParams, "accumuloInst");
    String zookeepers = getProp(queryParams, "zookeepers");
    String query = getProp(queryParams, "query");
    String shardTable = getProp(queryParams, "shardTable");
    String indexTable = getProp(queryParams, "indexTable");
    String startTime = getProp(queryParams, "startTime");
    String endTime = getProp(queryParams, "endTime");
    String auths = getProp(queryParams, "auths");
    String selectFields = getProp(queryParams, "selectFields");

    Set<String> fields = selectFields != null ? newHashSet(asList(splitPreserveAllTokens(selectFields, ","))) : null;

    DateTime startDT = DateTime.parse(startTime);
    DateTime endDT = DateTime.parse(endTime);


// call groovy expressions from Java code
    Binding binding = new Binding();
    binding.setVariable("q", new QueryBuilder());
    GroovyShell shell = new GroovyShell(binding);
    QueryBuilder qb = (QueryBuilder) shell.evaluate(query);

    EventInputFormat.setZooKeeperInstance(conf, accumuloInst, zookeepers);
    EventInputFormat.setInputInfo(conf, accumuloUser, accumuloPass.getBytes(), new Authorizations(auths.getBytes()));
    try {
      EventInputFormat.setQueryInfo(conf, startDT.toDate(), endDT.toDate(), qb.build(), fields);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getProp(Multimap<String,String> queryParams, String propKey) {
    Collection<String> props = queryParams.get(propKey);
    if(props.size() > 0)
      return props.iterator().next();
    return null;
  }

  @Override
  public InputFormat getInputFormat() throws IOException {
    return new EventInputFormat();
  }

  @Override
  public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
    this.reader = recordReader;
  }

  @Override
  public Tuple getNext() throws IOException {

    try {
      if(!reader.nextKeyValue())
        return null;
      else {

      }


    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
