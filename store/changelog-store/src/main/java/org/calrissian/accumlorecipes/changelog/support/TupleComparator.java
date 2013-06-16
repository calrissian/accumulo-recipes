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
package org.calrissian.accumlorecipes.changelog.support;

import org.calrissian.mango.domain.Tuple;

import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple> {


    @Override
    public int compare(Tuple tuple, Tuple tuple1) {
        try {
            String tupleString = Utils.tupleToString(tuple);
            String tuple1String = Utils.tupleToString(tuple1);

            return tupleString.compareTo(tuple1String);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
