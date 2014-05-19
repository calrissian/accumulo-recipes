package org.calrissian.accumulorecipes.eventstore.pig;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class EventLoader extends LoadFunc {
  @Override
  public void setLocation(String s, Job job) throws IOException {

  }

  @Override
  public InputFormat getInputFormat() throws IOException {
    return null;
  }

  @Override
  public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
  }

  @Override
  public Tuple getNext() throws IOException {
    return null;
  }
}
