package org.calrissian.accumulorecipes.featurestore.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import static java.lang.Math.sqrt;
import static java.math.BigDecimal.valueOf;
import static java.math.RoundingMode.FLOOR;

/**
 * Contains statistical summary information (min, max, sum, count, std dev, etc...).
 */
public class Metric implements Writable {

    private long min;
    private long max;
    private long sum;
    private long count;
    private BigInteger sumSquare;

    public Metric() {
    }

    public Metric(long value) {
        this.min = value;
        this.max = value;
        this.sum = value;
        this.count = 1;
        this.sumSquare = BigInteger.valueOf(value).multiply(BigInteger.valueOf(value));
    }

    public Metric(long min, long max, long sum, long count, BigInteger sumSquare) {
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
        BigDecimal sumSquare = new BigDecimal(this.sumSquare);

        if(count < 2)
            return 0;

        BigDecimal sumSquareDivideByCount = sumSquare.divide(valueOf(count), 15, FLOOR);
        return (sumSquareDivideByCount.subtract(new BigDecimal(getMean() * getMean()))).doubleValue();
    }


    /**
     * The population standard deviation
     */
    public double getStdDev() {
        return sqrt(getVariance());
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Metric that = (Metric) o;

        if (count != that.count) return false;
        if (max != that.max) return false;
        if (min != that.min) return false;
        if (sum != that.sum) return false;
        if (sumSquare != null ? !sumSquare.equals(that.sumSquare) : that.sumSquare != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (min ^ (min >>> 32));
        result = 31 * result + (int) (max ^ (max >>> 32));
        result = 31 * result + (int) (sum ^ (sum >>> 32));
        result = 31 * result + (int) (count ^ (count >>> 32));
        result = 31 * result + (sumSquare != null ? sumSquare.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Metric{" +
                "min=" + min +
                ", max=" + max +
                ", sum=" + sum +
                ", count=" + count +
                ", sumSquare=" + sumSquare +
                '}';
    }
}
