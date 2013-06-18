package org.calrissian.accumulorecipes.metricsstore.ext.custom.domain;


import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Data object to hold the value of custom metric functions.
 * @param <T>
 */
public class CustomMetric<T> {


    private final long timestamp;
    private final String group;
    private final String type;
    private final String name;
    private final String visibility;
    private final T value;

    public CustomMetric(long timestamp, String group, String type, String name, String visibility, T value) {
        checkNotNull(group);
        checkNotNull(type);
        checkNotNull(name);
        checkNotNull(visibility);
        checkNotNull(value);

        this.timestamp = timestamp;
        this.group = group;
        this.type = type;
        this.name = name;
        this.visibility = visibility;
        this.value = value;
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

    public T getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CustomMetric that = (CustomMetric) o;

        if (timestamp != that.timestamp) return false;
        if (!group.equals(that.group)) return false;
        if (!name.equals(that.name)) return false;
        if (!type.equals(that.type)) return false;
        if (!value.equals(that.value)) return false;
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
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CustomMetric{" +
                "timestamp=" + timestamp +
                ", group='" + group + '\'' +
                ", type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", visibility='" + visibility + '\'' +
                ", value=" + value +
                '}';
    }
}
