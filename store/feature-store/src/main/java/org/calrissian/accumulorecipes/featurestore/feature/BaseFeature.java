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
}
