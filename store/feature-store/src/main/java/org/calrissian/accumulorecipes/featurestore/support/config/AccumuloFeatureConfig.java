package org.calrissian.accumulorecipes.featurestore.support.config;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.featurestore.model.Feature;

import java.io.Serializable;
import java.util.List;

public interface AccumuloFeatureConfig<T extends Feature> extends Serializable {

    Class<T> transforms();

    Value buildValue(T feature);

    <T>T buildFeatureFromValue(long timestamp, String group, String type, String name, String visibility, Value value);

    String featureName();

    List<IteratorSetting> buildIterators();
}
