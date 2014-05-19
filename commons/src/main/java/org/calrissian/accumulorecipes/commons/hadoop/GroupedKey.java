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

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GroupedKey implements WritableComparable<GroupedKey> {

    private String group;
    private Key key;

    public String getGroup() {
        return group;
    }

    public Key getKey() {
        return key;
    }

    @Override
    public int compareTo(GroupedKey groupedKey) {
        int result = getGroup().compareTo(groupedKey.getGroup());
        if (result == 0)
            result = getKey().compareTo(groupedKey.getKey());
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(group);
        key.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        group = dataInput.readUTF();

        key = new Key();
        key.readFields(dataInput);
    }
}
