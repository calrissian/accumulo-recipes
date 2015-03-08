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

import com.google.common.base.Function;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityIdentifier;

public class TransformUtils {
    private TransformUtils() {/**private constructor**/}

    public static Function<Entity, EntityIdentifier> entityToEntityIdentifier = new Function<Entity, EntityIdentifier>() {
        @Override
        public EntityIdentifier apply(Entity entity) {
            return new EntityIdentifier(entity.getType(), entity.getId());
        }
    };
}
