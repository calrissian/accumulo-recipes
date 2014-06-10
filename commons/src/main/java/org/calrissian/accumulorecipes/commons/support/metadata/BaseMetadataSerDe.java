package org.calrissian.accumulorecipes.commons.support.metadata;

import org.calrissian.mango.types.TypeRegistry;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * A base serializer/deserializer for a hashmap of metadata. This will encode the data into and from a byte array.
 */
public class BaseMetadataSerDe implements MetadataSerDe {

    private TypeRegistry<String> typeRegistry;


    @Override
    public byte[] serialize(Map<String, Object> metadata) {


        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput dataOutput = new DataOutputStream(baos);

            int count = 0;
            for(Map.Entry<String, Object> entry : metadata.entrySet()) {
                if(entry.getValue() != null)
                    count++;
            }

            dataOutput.writeInt(count);

            for(Map.Entry<String, Object> entry : metadata.entrySet()) {
                if(entry.getValue() != null)
                    count++;
            }

            for(Map.Entry<String, Object> entry : metadata.entrySet()) {
                dataOutput.writeUTF(entry.getKey());
                dataOutput.writeUTF(typeRegistry.getAlias(entry.getValue()));
                dataOutput.writeUTF(typeRegistry.encode(entry.getValue()));
            }

            return baos.toByteArray();

        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setTypeRegistry(TypeRegistry<String> registry) {

        this.typeRegistry = registry;
    }

    @Override
    public Map<String, Object> deserialize(byte[] bytes) {

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            DataInputStream dis = new DataInputStream(bais);

            int count = dis.readInt();

            Map<String, Object> metadata = new HashMap<String, Object>();
            for(int i = 0; i < count; i++) {

                String key = dis.readUTF();
                String alias = dis.readUTF();
                String encodedVal = dis.readUTF();

                metadata.put(key, typeRegistry.decode(alias, encodedVal));
            }

            return metadata;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
