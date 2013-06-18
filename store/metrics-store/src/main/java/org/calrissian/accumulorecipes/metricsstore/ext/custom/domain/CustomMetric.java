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


}
