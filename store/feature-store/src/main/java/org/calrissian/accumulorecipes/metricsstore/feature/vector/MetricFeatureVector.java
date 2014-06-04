package org.calrissian.accumulorecipes.metricsstore.feature.vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

import static java.lang.Math.sqrt;

public class MetricFeatureVector implements FeatureVector {

    private long min;
    private long max;
    private long sum;
    private long count;
    private BigInteger sumSquare;

    public MetricFeatureVector() {
    }

    public MetricFeatureVector(long min, long max, long sum, long count, BigInteger sumSquare) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
        this.sumSquare = sumSquare;
    }

    public long getMin() {
        return min;
    }

    public long getMax() {
        return max;
    }

    public long getSum() {
        return sum;
    }

    public long getCount() {
        return count;
    }

    public BigInteger getSumSquare() {
        return sumSquare;
    }

    /**
     * The mean/average of the values encountered for the time range
     */
    public double getMean() {
        if (count < 1)
            return 0;

        return ((double) sum) / count;
    }

    /**
     * The population variance for the values encountered for the time range
     */
    public double getVariance() {
        return getVariance(false);
    }

    /**
     * The variance of the values encountered for the time range.  The asSample option
     * allows the user to get the variance of the data as if the data was a sample population.
     *
     * @see https://statistics.laerd.com/statistical-guides/measures-of-spread-standard-deviation.php
     */
    public double getVariance(boolean asSample) {
        return 0.0;     //TODO: FIX THIS FOR BIGDECIMAL
    }

    /**
     * The population standard deviation for the values encountered for the time range.
     */
    public double getStdDev() {
        return getStdDev(false);
    }

    /**
     * The standard deviation for the values encountered for the time range.  The asSample option
     * allows the user to get the standard deviation of the data as if the data was a sample population.
     *
     * @see https://statistics.laerd.com/statistical-guides/measures-of-spread-standard-deviation.php
     */
    public double getStdDev(boolean asSample) {
        return sqrt(getVariance(asSample));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(min);
        dataOutput.writeLong(max);
        dataOutput.writeLong(sum);
        dataOutput.writeLong(count);
        dataOutput.writeUTF(sumSquare.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        min = dataInput.readLong();
        max = dataInput.readLong();
        sum = dataInput.readLong();
        count = dataInput.readLong();
        sumSquare = new BigInteger(dataInput.readUTF());
    }
}
