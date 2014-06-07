package org.calrissian.accumulorecipes.commons.hadoop;


import org.apache.hadoop.io.Writable;
import org.calrissian.accumulorecipes.commons.domain.Gettable;
import org.calrissian.accumulorecipes.commons.domain.Settable;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class TupleWritable implements Writable, Gettable<Tuple>, Settable<Tuple>{

    private Tuple tuple;
    private TypeRegistry<String> typeRegistry = LEXI_TYPES;

    public TupleWritable() {
    }

    public TupleWritable(Tuple tuple) {
        this.tuple = tuple;
    }


    public void setTypeRegistry(TypeRegistry<String> registry) {
        this.typeRegistry = registry;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tuple.getKey());
        dataOutput.writeUTF(typeRegistry.getAlias(tuple.getValue()));
        dataOutput.writeUTF(typeRegistry.encode(tuple.getValue()));


        Set<Map.Entry<String, Object>> metaMap = tuple.getMetadata().entrySet();
        int finalMeta = 0;
        for(Map.Entry<String,Object> meta : metaMap) {
            if(meta.getValue() != null)
                finalMeta++;
        }

        dataOutput.writeInt(finalMeta);
        for(Map.Entry<String,Object> meta : metaMap) {
            if(meta.getValue() != null) {
                dataOutput.writeUTF(meta.getKey());
                dataOutput.writeUTF(typeRegistry.getAlias(meta.getValue()));
                dataOutput.writeUTF(typeRegistry.encode(meta.getValue()));
            }
        }



    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String key = dataInput.readUTF();
        String type = dataInput.readUTF();
        String val = dataInput.readUTF();

        tuple = new Tuple(key, typeRegistry.decode(type, val));


        int count = dataInput.readInt();
        for (int i = 0; i < count; i++) {
            String metaKey = dataInput.readUTF();
            String metaType = dataInput.readUTF();
            String metaVal = dataInput.readUTF();
            tuple.setMetadataValue(metaKey, typeRegistry.decode(metaType, metaVal));
        }
    }

    @Override
    public Tuple get() {
        return tuple;
    }

    @Override
    public void set(Tuple item) {
        this.tuple = item;
    }
}
