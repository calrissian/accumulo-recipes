package org.calrissian.accumulorecipes.commons.support.metadata;

import org.calrissian.mango.types.TypeRegistry;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * A base serializer/deserializer for a hashmap of metadata. This will encode the data into and from a simple byte array.
 */
public class SimpleMetadataSerDe implements MetadataSerDe {

    private TypeRegistry<String> typeRegistry;

    public SimpleMetadataSerDe(TypeRegistry<String> typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

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
                dataOutput.writeUTF(entry.getKey());
                dataOutput.writeUTF(typeRegistry.getAlias(entry.getValue()));
                dataOutput.writeUTF(typeRegistry.encode(entry.getValue()));
            }

            byte[] bytes =  baos.toByteArray();

            baos.flush();
            baos.close();

            return bytes;

        } catch(Exception e) {
            throw new RuntimeException(e);
        }
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

            bais.close();
            dis.close();

            return metadata;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
