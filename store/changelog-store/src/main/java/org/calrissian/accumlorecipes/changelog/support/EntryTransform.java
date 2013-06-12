package org.calrissian.accumlorecipes.changelog.support;

import com.google.common.base.Function;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.serialization.ObjectMapperContext;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

import static java.util.Map.Entry;

public class EntryTransform implements Function<Entry<Key, Value>, StoreEntry> {

    ObjectMapper objectMapper = ObjectMapperContext.getInstance().getObjectMapper();

    @Override
    public StoreEntry apply(Entry<Key, Value> input) {
        try {
            return objectMapper.readValue(new String(input.getValue().get()), StoreEntry.class);
        } catch (IOException e) {
            throw new IterationInterruptedException("There was a problem deserializing from the row");
        }
    }
}
