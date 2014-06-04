package org.calrissian.accumulorecipes.featurestore.feature.transform;


import com.google.common.base.Function;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.StringUtils;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.featurestore.feature.MetricFeature;
import org.calrissian.accumulorecipes.featurestore.feature.vector.MetricFeatureVector;
import org.calrissian.accumulorecipes.featurestore.support.StatsCombiner;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.lang.Long.parseLong;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.featurestore.impl.AccumuloFeatureStore.combine;

public class MetricFeatureTransform implements AccumuloFeatureConfig<MetricFeature> {

    @Override
    public Class<MetricFeature> transforms() {
        return MetricFeature.class;
    }

    @Override
    public Value buildValue(MetricFeature feature) {
        return vectorToValue.apply(feature.getVector());
    }

    @Override
    public MetricFeature buildFeatureFromValue(long timestamp, String group, String type, String name, String visibility, Value value) {
        MetricFeatureVector metricFeatureVector = valueToVector.apply(value);
        return new MetricFeature(timestamp, group, type, name, visibility, metricFeatureVector);
    }

    @Override
    public String featureName() {
        return "metric";
    }

    @Override
    public List<IteratorSetting> buildIterators() {
        List<IteratorSetting.Column> columns = new ArrayList<IteratorSetting.Column>();
        for (MetricTimeUnit timeUnit : MetricTimeUnit.values())
            columns.add(new IteratorSetting.Column(combine(featureName(), timeUnit.toString())));

        IteratorSetting setting = new IteratorSetting(14, "stats", StatsCombiner.class);
        StatsCombiner.setColumns(setting, columns);

        return Collections.singletonList(setting);
    }

    public static final Function<Value, MetricFeatureVector> valueToVector = new Function<Value, MetricFeatureVector>() {
        @Override
        public MetricFeatureVector apply(Value value) {
            String[] vals = splitPreserveAllTokens(new String(value.get()), ",");
            return new MetricFeatureVector(
                parseLong(vals[0]),
                parseLong(vals[1]),
                parseLong(vals[2]),
                parseLong(vals[3]),
                new BigInteger(vals[4])
            );
        }
    };

    public static final Function<MetricFeatureVector, Value> vectorToValue = new Function<MetricFeatureVector, Value>() {
        @Override
        public Value apply(MetricFeatureVector metricFeatureVector) {
            return new Value(StringUtils.join(Arrays.asList(
                metricFeatureVector.getMin(),
                metricFeatureVector.getMax(),
                metricFeatureVector.getSum(),
                metricFeatureVector.getCount(),
                metricFeatureVector.getSumSquare().toString()),
                ",").getBytes());
        }
    };
}
