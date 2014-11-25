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
package org.calrissian.accumulorecipes.commons.support.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    public byte[] serialize(List<Map<String, Object>> metadata) {

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutput dataOutput = new DataOutputStream(baos);

        try {

          dataOutput.writeInt(metadata.size());

          for(Map<String,Object> map : metadata) {
            int count = 0;
            for(Map.Entry<String, Object> entry : map.entrySet()) {
              if(entry.getValue() != null)
                count++;
            }

            dataOutput.writeInt(count);

            for(Map.Entry<String, Object> entry : map.entrySet()) {
              dataOutput.writeUTF(entry.getKey());
              dataOutput.writeUTF(typeRegistry.getAlias(entry.getValue()));
              dataOutput.writeUTF(typeRegistry.encode(entry.getValue()));
            }
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
    public List<Map<String, Object>> deserialize(byte[] bytes) {

      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      DataInputStream dis = new DataInputStream(bais);


      List<Map<String, Object>> metadata = new ArrayList<Map<String,Object>>();


      try {
        int listCount = dis.readInt();

        for(int i = 0; i < listCount; i++) {
          Map<String, Object> map = new HashMap<String,Object>();
          int count = dis.readInt();

          for(int j = 0; j < count; j++) {
            String key = dis.readUTF();
            String alias = dis.readUTF();
            String encodedVal = dis.readUTF();

            map.put(key, typeRegistry.decode(alias, encodedVal));
          }

          metadata.add(map);
        }

      } catch(Exception e) {
      } finally {
        try {
          bais.close();
          dis.close();
        } catch (IOException e) {
        }
      }
      return metadata;
    }
}
