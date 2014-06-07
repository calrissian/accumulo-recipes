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
package org.calrissian.accumulorecipes.commons.hadoop;

import org.apache.hadoop.io.Writable;
import org.calrissian.accumulorecipes.commons.domain.Gettable;
import org.calrissian.accumulorecipes.commons.domain.Settable;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class EventWritable implements Writable, Settable<Event>, Gettable<Event> {

    private TupleWritable tupleWritable = new TupleWritable();

    Event entry;

    public EventWritable() {
    }

    public EventWritable(Event entry) {
        this.entry = entry;
    }

    public void setTupleWritable(TupleWritable sharedTupleWritable) {
        this.tupleWritable = sharedTupleWritable;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(entry.getId());
        dataOutput.writeLong(entry.getTimestamp());

        dataOutput.writeInt(entry.getTuples() != null ? entry.getTuples().size() : 0);
        for (Tuple tuple : entry.getTuples()) {
            tupleWritable.set(tuple);
            tupleWritable.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String uuid = dataInput.readUTF();
        long timestamp = dataInput.readLong();
        entry = new BaseEvent(uuid, timestamp);

        int count = dataInput.readInt();
        for (int i = 0; i < count; i++) {
            tupleWritable.readFields(dataInput);
            entry.put(tupleWritable.get());
        }
    }

    public void set(Event entry) {
        this.entry = entry;
    }

    public Event get() {
        return entry;
    }
}
