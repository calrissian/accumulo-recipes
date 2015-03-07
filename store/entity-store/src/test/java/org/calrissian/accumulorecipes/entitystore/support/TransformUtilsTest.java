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
package org.calrissian.accumulorecipes.entitystore.support;

import static org.junit.Assert.assertEquals;

import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityBuilder;
import org.calrissian.mango.domain.entity.EntityIndex;
import org.junit.Test;

public class TransformUtilsTest {

    @Test
    public void test() {

        Entity entity = new EntityBuilder("type", "id").build();

        EntityIndex index = TransformUtils.entityToEntityIndex.apply(entity);
        assertEquals(entity.getType(), index.getType());
        assertEquals(entity.getId(), index.getId());
    }
}
