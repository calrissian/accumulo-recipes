package org.calrissian.accumlorecipes.changelog.domain;

import org.calrissian.commons.domain.Tuple;

import java.io.Serializable;
import java.util.Collection;

public class ChangeSet implements Serializable {

    String uuid;

    Long timestamp;

    Collection<Tuple> oldTuples;
    Collection<Tuple> newTuples;

    public ChangeSet(String uuid, Long timestamp, Collection<Tuple> oldTuples, Collection<Tuple> newTuples) {
        this.uuid = uuid;
        this.timestamp = timestamp;
        this.oldTuples = oldTuples;
        this.newTuples = newTuples;
    }

    public String getUuid() {
        return uuid;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Collection<Tuple> getOldTuples() {
        return oldTuples;
    }

    public Collection<Tuple> getNewTuples() {
        return newTuples;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChangeSet)) return false;

        ChangeSet changeSet = (ChangeSet) o;

        if (newTuples != null ? !newTuples.equals(changeSet.newTuples) : changeSet.newTuples != null) return false;
        if (oldTuples != null ? !oldTuples.equals(changeSet.oldTuples) : changeSet.oldTuples != null) return false;
        if (timestamp != null ? !timestamp.equals(changeSet.timestamp) : changeSet.timestamp != null) return false;
        if (uuid != null ? !uuid.equals(changeSet.uuid) : changeSet.uuid != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (oldTuples != null ? oldTuples.hashCode() : 0);
        result = 31 * result + (newTuples != null ? newTuples.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ChangeSet{" +
                "uuid='" + uuid + '\'' +
                ", timestamp=" + timestamp +
                ", oldTuples=" + oldTuples +
                ", newTuples=" + newTuples +
                '}';
    }
}