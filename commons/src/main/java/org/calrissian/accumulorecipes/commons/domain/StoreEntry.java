/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.commons.domain;


import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.TupleCollection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static java.lang.System.currentTimeMillis;

/**
 * A store entry acts as a useful common business object for representing different types of models. An optional time
 * dimension can be set directly or left untouched (defaulting in current time).
 */
public class StoreEntry implements TupleCollection {

    protected final String id;
    protected final long timestamp; // in Millis

    protected Collection<Tuple> tuples;

    /**
     * New store entry with random UUID and timestamp defaulted to current time
     */
    public StoreEntry() {
        this(UUID.randomUUID().toString());
    }

    /**
     * New store entry with ID. Timestamp defaults to current time.
     * @param id
     */
    public StoreEntry(String id) {
        this(id, currentTimeMillis());
    }

    /**
     * New store entry with ID and a timestamp
     * @param id
     * @param timestamp
     */
    public StoreEntry(String id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;

        this.tuples = new ArrayList<Tuple>();
    }

    /**
     * Put multiple getTuples at the same time
     * @param tuples
     */
    public void putAll(Collection<Tuple> tuples) {

        if(tuples != null) {
            this.tuples.addAll(tuples);
        }
    }

    /**
     * Put a single tuple
     * @param tuple
     */
    public void put(Tuple tuple) {
        this.tuples.add(tuple);
    }

  @Override
  public void putAll(Iterable<Tuple> tuples) {
    for(Tuple tuple : tuples)
      put(tuple);
  }

  /**
     * Used for single valued getTuples. Returns the first tuple with the specified key
     * @param key
     * @return null if tuple with key does not exist
     */
    public Tuple get(String key) {

        for(Tuple tuple : tuples) {
            if(tuple.getKey().equals(key)) {
                return tuple;
            }
        }

        return null;
    }

    /**
     * Used for multi-valued getTuples. Returns all getTuples with the specified key
     * @param key
     * @return empty collection if no getTuples with key exist
     */
    public Collection<Tuple> getAll(String key) {
        Collection<Tuple> retTuples = new ArrayList<Tuple>();
        for(Tuple tuple : tuples) {
            if(tuple.getKey().equals(key)) {
                retTuples.add(tuple);
            }
        }

        return retTuples;
    }

    /**
     * Accessor for Id
     * @return
     */
    public String getId() {
        return id;
    }

    /**
     * Accessor for timestamp
     * @return
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Accessor for getTuples
     * @return
     */
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
                ", getTuples=" + tuples +
                '}';
    }
}
