package org.calrissian.accumulorecipes.test;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.calrissian.mango.domain.Pair;

import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class MockRecordReader<K, V> extends RecordReader<K, V> {

    Iterator<Pair<K, V>> itrPairs;
    Pair<K, V> curPair;

    public MockRecordReader(Iterable<Pair<K, V>> pairs) {
        checkNotNull(pairs);
        this.itrPairs = pairs.iterator();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(itrPairs.hasNext()) {
            curPair = itrPairs.next();
            return true;
        }
        return false;
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return curPair.getOne();
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return curPair.getTwo();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return itrPairs.hasNext() ? 0 : 1;
    }

    @Override
    public void close() throws IOException {

    }
}
