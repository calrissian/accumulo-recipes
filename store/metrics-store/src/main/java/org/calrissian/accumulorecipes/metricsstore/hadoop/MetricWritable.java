package org.calrissian.accumulorecipes.metricsstore.hadoop;

import org.apache.hadoop.io.Writable;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetricWritable implements Writable {

    private Metric metric;

    public MetricWritable() {

    }

    public MetricWritable(Metric metric) {
        this.metric = metric;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(metric.getTimestamp());
        dataOutput.writeUTF(metric.getGroup());
        dataOutput.writeUTF(metric.getType());
        dataOutput.writeUTF(metric.getName());
        dataOutput.writeUTF(metric.getVisibility());
        dataOutput.writeLong(metric.getValue());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        long timestamp = dataInput.readLong();
        String group = dataInput.readUTF();
        String type = dataInput.readUTF();
        String name = dataInput.readUTF();
        String visibility = dataInput.readUTF();
        long value = dataInput.readLong();

        metric = new Metric(timestamp, group, type, name, visibility, value);
    }

    public Metric get() {
        return metric;
    }

    public void set(Metric metric) {
        this.metric = metric;
    }
}
