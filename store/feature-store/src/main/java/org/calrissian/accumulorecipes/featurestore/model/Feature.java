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
package org.calrissian.accumulorecipes.featurestore.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A feature models defining properties of a thing. These defining properties generally take on the form of
 * some mathematical representation that can be used for machine learning, trending, and other statistical
 * analysis. This model of a feature provides some common identification information as well as a vector
 * that houses the actual mathematical summary data.
 */
public abstract class Feature<T extends Writable> implements Writable {

    protected long timestamp;
    protected String group;
    protected String type;
    protected String name;
    protected String visibility;
    protected T vector;

    protected Feature(long timestamp, String group, String type, String name, String visibility, T vector) {
        this.timestamp = timestamp;
        this.group = group;
        this.type = type;
        this.name = name;
        this.visibility = visibility;
        this.vector = vector;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getGroup() {
        return group;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getVisibility() {
        return visibility;
    }

    public T getVector() {
        return vector;
    }

    protected abstract  T buildVector(DataInput input);

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(getTimestamp());
        dataOutput.writeUTF(getGroup());
        dataOutput.writeUTF(getType());
        dataOutput.writeUTF(getName());
        dataOutput.writeUTF(getVisibility());
        getVector().write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        timestamp = dataInput.readLong();
        group = dataInput.readUTF();
        type = dataInput.readUTF();
        name = dataInput.readUTF();
        visibility = dataInput.readUTF();
        vector = buildVector(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Feature that = (Feature) o;

        if (timestamp != that.timestamp) return false;
        if (group != null ? !group.equals(that.group) : that.group != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (vector != null ? !vector.equals(that.vector) : that.vector != null) return false;
        if (visibility != null ? !visibility.equals(that.visibility) : that.visibility != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (group != null ? group.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (visibility != null ? visibility.hashCode() : 0);
        result = 31 * result + (vector != null ? vector.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Feature{" +
                "timestamp=" + timestamp +
                ", group='" + group + '\'' +
                ", type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", visibility='" + visibility + '\'' +
                ", vector=" + vector +
                '}';
    }
}
