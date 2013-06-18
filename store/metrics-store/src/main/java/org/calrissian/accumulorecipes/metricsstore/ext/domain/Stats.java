package org.calrissian.accumulorecipes.metricsstore.ext.domain;

import static com.google.common.base.Preconditions.checkNotNull;

public class Stats {
    private final long timestamp;
    private final String group;
    private final String type;
    private final String name;
    private final String visibility;
    private final long min;
    private final long max;
    private final long sum;
    private final long count;


    public Stats(long timestamp, String group, String type, String name, String visibility,
                 long min, long max, long sum, long count) {
        checkNotNull(group);
        checkNotNull(type);
        checkNotNull(name);
        checkNotNull(visibility);

        this.timestamp = timestamp;
        this.group = group;
        this.type = type;
        this.name = name;
        this.visibility = visibility;
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Stats)) return false;

        Stats that = (Stats) o;

        if (count != that.count) return false;
        if (max != that.max) return false;
        if (min != that.min) return false;
        if (sum != that.sum) return false;
        if (timestamp != that.timestamp) return false;
        if (!group.equals(that.group)) return false;
        if (!name.equals(that.name)) return false;
        if (!type.equals(that.type)) return false;
        if (!visibility.equals(that.visibility)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + group.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + visibility.hashCode();
        result = 31 * result + (int) (min ^ (min >>> 32));
        result = 31 * result + (int) (max ^ (max >>> 32));
        result = 31 * result + (int) (sum ^ (sum >>> 32));
        result = 31 * result + (int) (count ^ (count >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Stats{" +
                "timestamp=" + timestamp +
                ", group='" + group + '\'' +
                ", type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", visibility='" + visibility + '\'' +
                ", min=" + min +
                ", max=" + max +
                ", sum=" + sum +
                ", count=" + count +
                '}';
    }
}
