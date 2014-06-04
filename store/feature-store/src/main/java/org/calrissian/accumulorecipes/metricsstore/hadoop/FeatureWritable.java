package org.calrissian.accumulorecipes.metricsstore.hadoop;

import org.apache.hadoop.io.Writable;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.feature.BaseFeature;
import org.calrissian.accumulorecipes.metricsstore.feature.transform.AccumuloFeatureConfig;
import org.calrissian.accumulorecipes.metricsstore.feature.vector.FeatureVector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FeatureWritable implements Writable {

    private BaseFeature feature;

    public FeatureWritable() {}

    public FeatureWritable(BaseFeature feature) {
        this.feature = feature;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(feature.getTimestamp());
        dataOutput.writeUTF(feature.getGroup());
        dataOutput.writeUTF(feature.getType());
        dataOutput.writeUTF(feature.getName());
        dataOutput.writeUTF(feature.getVisibility());
        feature.getVector().write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        long timestamp = dataInput.readLong();
        String group = dataInput.readUTF();
        String type = dataInput.readUTF();
        String name = dataInput.readUTF();
        String visibility = dataInput.readUTF();

        FeatureVector vector =
    }

    public BaseFeature get() {
        return feature;
    }

    public void set(BaseFeature metric) {
        this.feature = metric;
    }
}
