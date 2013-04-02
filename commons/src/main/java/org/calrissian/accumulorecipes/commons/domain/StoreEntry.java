package org.calrissian.accumulorecipes.commons.domain;

import org.calrissian.commons.domain.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

public class StoreEntry {

    protected final String id;
    protected final long timestamp;

    protected Collection<Tuple> tuples;

    public StoreEntry() {
        this.id = UUID.randomUUID().toString();
        this.timestamp = System.currentTimeMillis();
    }

    public StoreEntry(String id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;

        this.tuples = new ArrayList<Tuple>();
    }

    public void putAll(Collection<Tuple> tuples) {

        if(tuples != null) {
            this.tuples.addAll(tuples);
        }
    }

    public void put(Tuple tuple) {
        this.tuples.add(tuple);
    }

    public String getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Collection<Tuple> getTuples() {
        return tuples;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StoreEntry)) return false;

        StoreEntry event = (StoreEntry) o;

        if (timestamp != event.timestamp) return false;
        if (id != null ? !id.equals(event.id) : event.id != null) return false;
        if (tuples != null ? !tuples.equals(event.tuples) : event.tuples != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (tuples != null ? tuples.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "StoreEntry{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", tuples=" + tuples +
                '}';
    }
}
