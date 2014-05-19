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
package org.calrissian.accumulorecipes.entitystore.model;

import org.apache.hadoop.io.WritableComparable;
import org.calrissian.accumulorecipes.commons.domain.Settable;
import org.calrissian.mango.domain.BaseEntity;
import org.calrissian.mango.domain.Entity;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeDecodingException;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;


public class EntityWritable implements WritableComparable, Settable<Entity> {

    private static TypeRegistry<String> typeRegistry = LEXI_TYPES;
    Entity entity;

    public EntityWritable() {
    }

    public EntityWritable(Entity entity) {
        checkNotNull(entity);
        this.entity = entity;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(entity.getType());
        dataOutput.writeUTF(entity.getId());
        dataOutput.writeInt(entity.getTuples() != null ? entity.getTuples().size() : 0);
        for (Tuple tuple : entity.getTuples()) {
            dataOutput.writeUTF(tuple.getKey());
            dataOutput.writeUTF(typeRegistry.getAlias(tuple.getValue()));
            try {
                dataOutput.writeUTF(typeRegistry.encode(tuple.getValue()));
                dataOutput.writeUTF(tuple.getVisibility());
            } catch (TypeEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String entityType = dataInput.readUTF();
        String id = dataInput.readUTF();

        entity = new BaseEntity(entityType, id);
        for (int i = 0; i < dataInput.readInt(); i++) {
            String key = dataInput.readUTF();
            String type = dataInput.readUTF();
            String val = dataInput.readUTF();
            String vis = dataInput.readUTF();
            try {
                entity.put(new Tuple(key, typeRegistry.decode(type, val), vis));
            } catch (TypeDecodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void set(Entity entity) {
        this.entity = entity;
    }

    public Entity get() {
        return entity;
    }

    @Override
    public int compareTo(Object o) {
        Entity entity2 = (Entity) o;

        int res = entity.getType().compareTo(entity2.getType());
        if (res == 0)
            res = entity.getId().compareTo(entity2.getId());

        return res;
    }
}
