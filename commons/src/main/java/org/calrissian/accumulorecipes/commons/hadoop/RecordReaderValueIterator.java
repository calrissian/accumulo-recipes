/*
* Copyright (C) 2014 The Calrissian Authors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
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
