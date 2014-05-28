package org.calrissian.accumulorecipes.commons.hadoop;

import com.google.common.collect.AbstractIterator;
import org.apache.hadoop.mapreduce.RecordReader;

public class RecordReaderValueIterator<K,V> extends AbstractIterator<V> {

    private RecordReader<K,V> recordReader;
    public RecordReaderValueIterator(RecordReader<K, V> recordReader) {
        this.recordReader = recordReader;
    }

    @Override
    protected V computeNext() {
        try {
            if(recordReader.nextKeyValue())
                return recordReader.getCurrentValue();
            else
                endOfData();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
