package org.calrissian.accumulorecipes.eventstore.support;


import com.google.common.base.Function;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.serialization.ObjectMapperContext;
import org.codehaus.jackson.map.ObjectMapper;

import static java.util.Map.Entry;

public class EventScannerTransform implements Function<Entry<Key, Value>, StoreEntry> {

    ObjectMapper objectMapper = ObjectMapperContext.getInstance().getObjectMapper();

    @Override
    public StoreEntry apply(Entry<Key, Value> input) {

        try {
            String nextEvent = new String(input.getValue().get());
            return objectMapper.readValue(nextEvent, StoreEntry.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
