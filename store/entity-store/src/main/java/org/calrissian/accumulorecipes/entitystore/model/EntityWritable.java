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
import org.calrissian.accumulorecipes.commons.domain.Gettable;
import org.calrissian.accumulorecipes.commons.domain.Settable;
import org.calrissian.accumulorecipes.commons.hadoop.AttributeWritable;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.entity.BaseEntity;
import org.calrissian.mango.domain.entity.Entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;


public class EntityWritable implements WritableComparable, Settable<Entity>, Gettable<Entity> {

    private AttributeWritable attributeWritable = new AttributeWritable();
    private Entity entity;

    public EntityWritable() {
    }

    public EntityWritable(Entity entity) {
        checkNotNull(entity);
        this.entity = entity;
    }

    public void setAttributeWritable(AttributeWritable sharedAttributeWritable) {
        this.attributeWritable = sharedAttributeWritable;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(entity.getType());
        dataOutput.writeUTF(entity.getId());
        dataOutput.writeInt(entity.getAttributes() != null ? entity.getAttributes().size() : 0);
        for (Attribute attribute : entity.getAttributes()) {
            attributeWritable.set(attribute);
            attributeWritable.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String entityType = dataInput.readUTF();
        String id = dataInput.readUTF();

        entity = new BaseEntity(entityType, id);
        int attributeSize = dataInput.readInt();
        for (int i = 0; i < attributeSize; i++) {
            attributeWritable.readFields(dataInput);
            entity.put(attributeWritable.get());
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
