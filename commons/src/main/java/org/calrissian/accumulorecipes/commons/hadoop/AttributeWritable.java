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


import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.calrissian.accumulorecipes.commons.domain.Gettable;
import org.calrissian.accumulorecipes.commons.domain.Settable;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.types.TypeRegistry;

public class AttributeWritable implements Writable, Gettable<Attribute>, Settable<Attribute>{

    private Attribute attribute;
    private TypeRegistry<String> typeRegistry = LEXI_TYPES;

    public AttributeWritable() {
    }

    public AttributeWritable(Attribute attribute) {
        this.attribute = attribute;
    }


    public void setTypeRegistry(TypeRegistry<String> registry) {
        this.typeRegistry = registry;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(attribute.getKey());
        dataOutput.writeUTF(typeRegistry.getAlias(attribute.getValue()));
        dataOutput.writeUTF(typeRegistry.encode(attribute.getValue()));


        Set<Map.Entry<String, Object>> metaMap = attribute.getMetadata().entrySet();
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

        int count = dataInput.readInt();
        Map<String, Object> metadata = new HashMap<String, Object>(count);
        for (int i = 0; i < count; i++) {
            String metaKey = dataInput.readUTF();
            String metaType = dataInput.readUTF();
            String metaVal = dataInput.readUTF();
            metadata.put(metaKey, typeRegistry.decode(metaType, metaVal));
        }

        attribute = new Attribute(key, typeRegistry.decode(type, val), metadata);
    }

    @Override
    public Attribute get() {
        return attribute;
    }

    @Override
    public void set(Attribute item) {
        this.attribute = item;
    }
}
