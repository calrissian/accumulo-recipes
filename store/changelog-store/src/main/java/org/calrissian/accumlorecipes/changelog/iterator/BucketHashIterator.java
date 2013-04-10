package org.calrissian.accumlorecipes.changelog.iterator;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.calrissian.accumlorecipes.changelog.support.Utils;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.serialization.ObjectMapperContext;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.calrissian.mango.hash.support.HashUtils.hashString;

public class BucketHashIterator extends WrappingIterator {

    ObjectMapper objectMapper = ObjectMapperContext.getInstance().getObjectMapper();

    String currentBucket;
    List<String> hashes;

    protected Key retKey;
    protected Value val;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        hashes = new ArrayList<String>();
    }

    @Override
    public boolean hasTop() {
        return val != null || super.hasTop();
    }


    @Override
    public void next() throws IOException {
        primeVal();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);
        primeVal();
    }

    @Override
    public Key getTopKey() {
        return retKey;
    }

    @Override
    public Value getTopValue() {
        return val;
    }

    public void primeVal() {

        val = null;
        hashes = new ArrayList<String>();

        String nowBucket = currentBucket;
        try {

            while(super.hasTop()) {

                Key topKey = super.getTopKey();
                Value value = super.getTopValue();

                if(currentBucket == null) {
                    currentBucket = topKey.getRow().toString();
                    nowBucket = currentBucket;
                }
                if(!topKey.getRow().toString().equals(currentBucket)) {
                    currentBucket = topKey.getRow().toString();
                    break;
                }

                super.next();

                StoreEntry entry = objectMapper.readValue(new String(value.get()), StoreEntry.class);
                hashes.add(new String(Utils.hashEntry(entry)));
            }

            if(hashes.size() > 0) {

                val = new Value(hashString(objectMapper.writeValueAsString(hashes)).getBytes());
                retKey = new Key(new Text(nowBucket));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
