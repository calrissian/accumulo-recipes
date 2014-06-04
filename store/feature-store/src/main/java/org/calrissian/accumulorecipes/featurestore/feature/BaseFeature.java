package org.calrissian.accumulorecipes.featurestore.feature;

import org.apache.hadoop.io.Writable;
import org.calrissian.accumulorecipes.featurestore.feature.vector.FeatureVector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class BaseFeature<T extends FeatureVector> implements Writable {

    protected long timestamp;
    protected String group;
    protected String type;
    protected String name;
    protected String visibility;
    protected T vector;

    protected BaseFeature(long timestamp, String group, String type, String name, String visibility, T vector) {
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

        BaseFeature that = (BaseFeature) o;

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
        return "BaseFeature{" +
                "timestamp=" + timestamp +
                ", group='" + group + '\'' +
                ", type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", visibility='" + visibility + '\'' +
                ", vector=" + vector +
                '}';
    }
}
