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
package org.calrissian.accumulorecipes.commons.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.calrissian.accumulorecipes.commons.domain.Gettable;
import org.calrissian.accumulorecipes.commons.domain.Settable;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.event.EventBuilder;

public class EventWritable implements Writable, Settable<Event>, Gettable<Event> {

    private AttributeWritable attributeWritable = new AttributeWritable();

    Event entry;

    public EventWritable() {
    }

    public EventWritable(Event entry) {
        this.entry = entry;
    }

    public void setAttributeWritable(AttributeWritable sharedAttributeWritable) {
        this.attributeWritable = sharedAttributeWritable;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(entry.getType());
        dataOutput.writeUTF(entry.getId());
        dataOutput.writeLong(entry.getTimestamp());

        dataOutput.writeInt(entry.getAttributes() != null ? entry.getAttributes().size() : 0);
        for (Attribute attribute : entry.getAttributes()) {
            attributeWritable.set(attribute);
            attributeWritable.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String type = dataInput.readUTF();
        String uuid = dataInput.readUTF();
        long timestamp = dataInput.readLong();
        EventBuilder builder = EventBuilder.create(type, uuid, timestamp);

        int count = dataInput.readInt();
        for (int i = 0; i < count; i++) {
            attributeWritable.readFields(dataInput);
            builder = builder.attr(attributeWritable.get());
        }

        entry = builder.build();
    }

    public void set(Event entry) {
        this.entry = entry;
    }

    public Event get() {
        return entry;
    }
}
