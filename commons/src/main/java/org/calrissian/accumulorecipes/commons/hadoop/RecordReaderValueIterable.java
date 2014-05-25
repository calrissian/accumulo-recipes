package org.calrissian.accumulorecipes.commons.hadoop;

import org.apache.hadoop.mapreduce.RecordReader;

import java.util.Iterator;

public class RecordReaderValueIterable<K,V> implements Iterable<V>{

    private RecordReader<K,V> recordReader;
    public RecordReaderValueIterable(RecordReader<K, V> recordReader) {
        this.recordReader = recordReader;
    }

    @Override
    public Iterator<V> iterator() {

        return new Iterator<V>() {

            V curVal;

            @Override
            public boolean hasNext() {
                if(curVal != null)
                    return true;
                else {
                    try {
                        if(recordReader.nextKeyValue()) {
                            curVal = recordReader.getCurrentValue();
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

                if(hasNext())
                    return curVal;
                else
                    throw new RuntimeException("No more items to iterate");
            }

            @Override
            public void remove() {

            }
        };
    }
}
