package org.calrissian.accumulorecipes.metricsstore.ext.custom.function;


/**
 * Custom function for defining the aggregation behavior of metrics for a query.
 * @param <T>
 */
public interface MetricFunction<T> {

    /**
     * Provides an initial value for metric value
     * @return
     */
    T intitialValue();

    /**
     * Updates the original metric with the given value.
     * @param orig
     * @param value
     * @return
     */
    T update(T orig, long value);

    /**
     * Merges the two metrics together.
     * @param orig
     * @param value
     * @return
     */
    T merge(T orig, T value);

    /**
     * Serialize the given metric
     * @param value
     * @return
     */
    String serialize(T value);

    /**
     * Deserialize the given metric.
     * @param data
     * @return
     */
    T deserialize(String data);

}
