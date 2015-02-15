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


import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.entity.BaseEntity;
import org.calrissian.mango.domain.entity.Entity;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TupleStoreIteratorTest {

    Entity entity = new BaseEntity("type", "id1");
    Entity entity2 = new BaseEntity("type", "id2");

    Tuple e1T1 = new Tuple("key", "val");
    Tuple e1T2 = new Tuple("key3", "val3");

    Tuple e2T1 = new Tuple("key2", "val2");
    Tuple e2T2 = new Tuple("key1", "val1");

    @Before
    public void setup() {

        entity.put(e1T1);
        entity.put(e1T2);

        entity2.put(e2T1);
        entity2.put(e2T2);
    }

    @Test
    public void test() {

        TupleStoreIterator<Entity> entityTupleStoreIterator = new TupleStoreIterator<Entity>(asList(new Entity[]{entity, entity2}).iterator());
        int count = 0;
        while (entityTupleStoreIterator.hasNext()) {
            Tuple curTuple = entityTupleStoreIterator.next();
            System.out.println(curTuple);
            if (count == 0) {
                assertEquals(entity.getType(), entityTupleStoreIterator.getTopStore().getType());
                assertEquals(entity.getId(), entityTupleStoreIterator.getTopStore().getId());
                assertEquals(e1T2, curTuple);
            } else if (count == 1) {
                assertEquals(entity.getType(), entityTupleStoreIterator.getTopStore().getType());
                assertEquals(entity.getId(), entityTupleStoreIterator.getTopStore().getId());
                assertEquals(e1T1, curTuple);
            } else if (count == 2) {
                assertEquals(entity2.getType(), entityTupleStoreIterator.getTopStore().getType());
                assertEquals(entity2.getId(), entityTupleStoreIterator.getTopStore().getId());
                assertEquals(e2T1, curTuple);
            } else if (count == 3) {
                assertEquals(entity2.getType(), entityTupleStoreIterator.getTopStore().getType());
                assertEquals(entity2.getId(), entityTupleStoreIterator.getTopStore().getId());
                assertEquals(e2T2, curTuple);
            } else {
                fail();
            }

            count++;
        }

        assertEquals(4, count);

    }

}
