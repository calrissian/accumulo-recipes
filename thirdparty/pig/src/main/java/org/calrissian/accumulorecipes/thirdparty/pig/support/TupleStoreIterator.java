/*
 * Copyright (C) 2013 The Calrissian Authors
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
package org.calrissian.accumulorecipes.thirdparty.pig.support;

import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.TupleStore;

import static com.google.common.base.Preconditions.checkNotNull;

public class TupleStoreIterator<T extends TupleStore> extends AbstractIterator<Tuple> {

    Iterator<T> tupleCollections;
    T curTupleCollection;
    Iterator<Tuple> tuples;

    public TupleStoreIterator(Iterator<T> tupleCollections) {
        checkNotNull(tupleCollections);
        this.tupleCollections = tupleCollections;
    }

    @Override
    protected Tuple computeNext() {

        while((tuples == null || !tuples.hasNext()) &&
            tupleCollections.hasNext()) {
            curTupleCollection = tupleCollections.next();
            tuples = curTupleCollection.getTuples().iterator();
        }

        if(tuples != null && tuples.hasNext())
            return tuples.next();

        return endOfData();
    }


    public T getTopStore() {
        return curTupleCollection;
    }
}
