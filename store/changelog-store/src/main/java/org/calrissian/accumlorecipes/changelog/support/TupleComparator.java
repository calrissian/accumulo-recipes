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

import com.google.common.collect.ComparisonChain;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeContext;
import org.calrissian.mango.types.exception.TypeNormalizationException;

import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple> {

    private final TypeContext typeContext;

    public TupleComparator(TypeContext typeContext) {
        this.typeContext = typeContext;
    }

    @Override
    public int compare(Tuple tuple, Tuple tuple1) {
        try {
            return ComparisonChain.start()
                    .compare(tuple.getKey(), tuple1.getKey())
                    .compare(typeContext.normalize(tuple.getValue()), typeContext.normalize(tuple.getValue()))
                    .compare(tuple.getVisibility(), tuple.getVisibility())
                    .result();

        } catch (TypeNormalizationException e) {
            throw new RuntimeException(e);
        }
    }
}
