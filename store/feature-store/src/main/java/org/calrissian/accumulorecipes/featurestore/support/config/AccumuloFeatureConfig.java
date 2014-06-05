package org.calrissian.accumulorecipes.featurestore.support.config;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.featurestore.model.Feature;

import java.io.Serializable;
import java.util.List;

/**
 * Represents the methods necessary for pluggin in a new feature type
 */
public interface AccumuloFeatureConfig<T extends Feature> extends Serializable {

    Class<T> transforms();

    Value buildValue(T feature);

    <T>T buildFeatureFromValue(long timestamp, String group, String type, String name, String visibility, Value value);

    String featureName();

    /**
     * Called for iterators to be built and returned. The priority MUST be used as the beginning priority. It is
     * expected that, after the iterators are returned, the priority will increment by 1 for reach iterator.
     * Therefore, the caller knows the increment the priority for each call to buildIterators() by the number
     * of items in the resulting list.
     */
    List<IteratorSetting> buildIterators(int priority);
}
