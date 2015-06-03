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


import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AttributeStoreIteratorTest {

    Entity entity;
    Entity entity2;

    Attribute e1T1 = new Attribute("key", "val");
    Attribute e1T2 = new Attribute("key3", "val3");

    Attribute e2T1 = new Attribute("key2", "val2");
    Attribute e2T2 = new Attribute("key1", "val1");

    @Before
    public void setup() {
        entity = EntityBuilder.create("type", "id1")
                .attr(e1T1)
                .attr(e1T2)
                .build();

        entity2 = EntityBuilder.create("type", "id2")
                .attr(e2T1)
                .attr(e2T2)
                .build();
    }

    @Test
    public void test() {

        AttributeStoreIterator<Entity> entityAttributeStoreIterator = new AttributeStoreIterator<Entity>(asList(new Entity[]{entity, entity2}).iterator());
        int count = 0;
        while (entityAttributeStoreIterator.hasNext()) {
            Attribute curAttribute = entityAttributeStoreIterator.next();
            System.out.println(curAttribute);
            if (count == 0) {
                assertEquals(entity.getType(), entityAttributeStoreIterator.getTopStore().getType());
                assertEquals(entity.getId(), entityAttributeStoreIterator.getTopStore().getId());
                assertEquals(e1T2, curAttribute);
            } else if (count == 1) {
                assertEquals(entity.getType(), entityAttributeStoreIterator.getTopStore().getType());
                assertEquals(entity.getId(), entityAttributeStoreIterator.getTopStore().getId());
                assertEquals(e1T1, curAttribute);
            } else if (count == 2) {
                assertEquals(entity2.getType(), entityAttributeStoreIterator.getTopStore().getType());
                assertEquals(entity2.getId(), entityAttributeStoreIterator.getTopStore().getId());
                assertEquals(e2T1, curAttribute);
            } else if (count == 3) {
                assertEquals(entity2.getType(), entityAttributeStoreIterator.getTopStore().getType());
                assertEquals(entity2.getId(), entityAttributeStoreIterator.getTopStore().getId());
                assertEquals(e2T2, curAttribute);
            } else {
                fail();
            }

            count++;
        }

        assertEquals(4, count);

    }

}
