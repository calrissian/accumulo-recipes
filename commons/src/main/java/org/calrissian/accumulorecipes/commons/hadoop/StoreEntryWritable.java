package org.calrissian.accumulorecipes.commons.hadoop;

import org.apache.hadoop.io.Writable;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeDecodingException;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;


public class StoreEntryWritable implements Writable {

  private static TypeRegistry<String> typeRegistry = ACCUMULO_TYPES;

  public StoreEntryWritable() {
  }

  StoreEntry entry;
  public StoreEntryWritable(StoreEntry entry) {
    this.entry = entry;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(entry.getTimestamp());
    dataOutput.writeUTF(entry.getId());
    dataOutput.writeInt(entry.getTuples() != null ? entry.getTuples().size() : 0);
    for(Tuple tuple : entry.getTuples()) {
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
    String uuid = dataInput.readUTF();
    long timestamp = dataInput.readLong();

    entry = new StoreEntry(uuid, timestamp);
    for(int i = 0; i < dataInput.readInt(); i++) {
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

  public void set(StoreEntry entry) {
    this.entry = entry;
  }

  public StoreEntry get() {
    return entry;
  }
}
