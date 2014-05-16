package org.calrissian.accumulorecipes.entitystore.model;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.WritableComparable;
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


public class EntityWritable implements WritableComparable {

    private static TypeRegistry<String> typeRegistry = LEXI_TYPES;

    public EntityWritable() {
    }

    Entity entity;
    public EntityWritable(Entity entity) {
        checkNotNull(entity);
        this.entity = entity;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(entity.getType());
        dataOutput.writeUTF(entity.getId());
        dataOutput.writeInt(entity.getTuples() != null ? entity.getTuples().size() : 0);
        for(Tuple tuple : entity.getTuples()) {
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
        for(int i = 0; i < dataInput.readInt(); i++) {
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
        Entity entity2 = (Entity)o;

        int res = entity.getType().compareTo(entity2.getType());
        if (res == 0)
            res = entity.getId().compareTo(entity2.getId());

        return res;
    }
}
