package org.calrissian.accumulorecipes.metricsstore.feature;

import org.calrissian.accumulorecipes.metricsstore.feature.vector.FeatureVector;

public abstract class BaseFeature<T extends FeatureVector> {

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
}
