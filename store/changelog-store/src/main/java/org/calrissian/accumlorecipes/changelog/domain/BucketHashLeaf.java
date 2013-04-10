package org.calrissian.accumlorecipes.changelog.domain;


import org.calrissian.mango.hash.tree.HashLeaf;

/**
 * Represents a hashed bucket that sorts by timestamp (in descending order).
 */
public class BucketHashLeaf extends HashLeaf {

    protected long timestamp;

    public BucketHashLeaf() {}

    public BucketHashLeaf(String hash, long timestamp) {
        super(hash);
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "BucketHashLeaf{" +
                "timestamp='" + timestamp + '\'' +
                ", hash='" + hash + "'" +
                '}';
    }
}
