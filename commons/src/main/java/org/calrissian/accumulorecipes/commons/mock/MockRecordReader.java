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
package org.calrissian.accumulorecipes.commons.mock;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.calrissian.mango.domain.Pair;

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
