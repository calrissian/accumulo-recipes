package org.calrissian.accumulorecipes.metricsstore.iterator;

import com.google.common.base.Function;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.calrissian.mango.serialization.ObjectMapperContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Aggregate the values and return the normal distribution assuming the values are doubles.
 * The output is a json map of the normal distribution.
 */
public class NormalDistFunction implements Function<Iterator<byte[]>, byte[]> {
    private static final Logger logger = LoggerFactory.getLogger(NormalDistFunction.class);
    private final ObjectMapper objectMapper = ObjectMapperContext.getInstance().getObjectMapper();

    @Override
    public byte[] apply(Iterator<byte[]> iterator) {
        SummaryStatistics statistics = new SummaryStatistics();
        while (iterator.hasNext()) {
            statistics.addValue(Double.parseDouble(new String(iterator.next())));
        }
        Map<String, Number> map = new HashMap<String, Number>();
        map.put("max", statistics.getMax());
        map.put("min", statistics.getMin());
        map.put("mean", statistics.getMean());
        map.put("var", statistics.getVariance());
        map.put("n", statistics.getN());
        try {
            return objectMapper.writeValueAsBytes(map);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
