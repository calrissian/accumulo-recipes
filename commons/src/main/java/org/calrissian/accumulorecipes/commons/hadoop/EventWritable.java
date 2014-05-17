package org.calrissian.accumulorecipes.commons.hadoop;

import org.apache.hadoop.io.Writable;
import org.calrissian.accumulorecipes.commons.domain.Settable;
import org.calrissian.mango.domain.BaseEvent;
import org.calrissian.mango.domain.Event;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeDecodingException;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;


public class EventWritable implements Writable, Settable<Event> {

  private static TypeRegistry<String> typeRegistry = LEXI_TYPES;

  public EventWritable() {
  }

  Event entry;

  public EventWritable(Event entry) {
    this.entry = entry;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(entry.getId());
    dataOutput.writeLong(entry.getTimestamp());

    dataOutput.writeInt(entry.getTuples() != null ? entry.getTuples().size() : 0);
    for (Tuple tuple : entry.getTuples()) {
      dataOutput.writeUTF(tuple.getKey());
      dataOutput.writeUTF(typeRegistry.getAlias(tuple.getValue()));
      try {
        dataOutput.writeUTF(typeRegistry.encode(tuple.getValue()));
      } catch (TypeEncodingException e) {
        throw new RuntimeException(e);
      }
      dataOutput.writeUTF(tuple.getVisibility());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    String uuid = dataInput.readUTF();
    long timestamp = dataInput.readLong();
    entry = new BaseEvent(uuid, timestamp);

    int count = dataInput.readInt();
    for (int i = 0; i < count; i++) {
      String key = dataInput.readUTF();
      String type = dataInput.readUTF();
      String val = dataInput.readUTF();
      String vis = dataInput.readUTF();
      try {
        entry.put(new Tuple(key, typeRegistry.decode(type, val), vis));
      } catch (TypeDecodingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void set(Event entry) {
    this.entry = entry;
  }

  public Event get() {
    return entry;
  }
}
