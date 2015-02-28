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
package org.calrissian.accumulorecipes.commons.support.tuple.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.calrissian.mango.types.TypeRegistry;

/**
 * A base serializer/deserializer for a hashmap of metadata. This will encode the data into and from a simple byte array.
 */
public class SimpleMetadataSerDe implements MetadataSerDe {

    private TypeRegistry<String> typeRegistry;

    public SimpleMetadataSerDe(TypeRegistry<String> typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public byte[] serialize(Map<String, String> metadata) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(baos);

        try {

            int count = 0;
            for(Map.Entry<String, String> entry : metadata.entrySet()) {
                if(entry.getValue() != null)
                    count++;
            }

            dataOutput.writeInt(count);

            for(Map.Entry<String, String> entry : metadata.entrySet()) {
                dataOutput.writeUTF(entry.getKey());
                dataOutput.writeUTF(entry.getValue());
            }


        } catch(Exception e) {
        } finally {
            try {
                baos.close();
                baos.flush();
            } catch (IOException e) {
            }
        }

        byte[] bytes =  baos.toByteArray();
        return bytes;
    }

    @Override
    public Map<String, String> deserialize(byte[] bytes) {

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bais);


        Map<String, String> map = new HashMap<String,String>();
        try {

            int count = dis.readInt();

            for(int j = 0; j < count; j++) {
                String key = dis.readUTF();
                String encodedVal = dis.readUTF();

                map.put(key, encodedVal);
            }

        } catch(Exception e) {
        } finally {
            try {
                bais.close();
                dis.close();
            } catch (IOException e) {
            }
        }

        return map;
    }
}
