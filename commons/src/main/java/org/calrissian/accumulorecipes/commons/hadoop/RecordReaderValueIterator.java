package org.calrissian.accumulorecipes.commons.hadoop;

import org.apache.hadoop.mapreduce.RecordReader;

import java.util.Iterator;

public class RecordReaderValueIterator<K,V> implements Iterator<V>{

    private RecordReader<K,V> recordReader;
    public RecordReaderValueIterator(RecordReader<K, V> recordReader) {
        this.recordReader = recordReader;
    }

    V curVal;

    @Override
    public boolean hasNext() {
        if(curVal != null)
            return true;
        else {
            try {
                if(recordReader.nextKeyValue()) {
                    curVal = recordReader.getCurrentValue();
                    System.out.println(curVal);
                    return true;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return false;
    }

    @Override
    public V next() {

        if(hasNext()) {

            V tmpVal = curVal;
            curVal = null;
            return tmpVal;
        }
        else
            throw new RuntimeException("No more items to iterate");
    }

    @Override
    public void remove() {

    }
}
