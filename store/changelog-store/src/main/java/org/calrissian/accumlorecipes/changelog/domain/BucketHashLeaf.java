package org.calrissian.accumlorecipes.changelog.domain;


import org.calrissian.mango.hash.tree.HashLeaf;
import org.calrissian.mango.types.exception.TypeNormalizationException;
import org.calrissian.mango.types.normalizers.LongNormalizer;

/**
 * Represents a hashed bucket that sorts by timestamp (in descending order).
 */
public class BucketHashLeaf extends HashLeaf implements Comparable<BucketHashLeaf> {

    protected Long timestamp;

    public BucketHashLeaf() {}

    public BucketHashLeaf(long timestamp) {
        super();
        this.timestamp = timestamp;
        try {
            this.hash = new LongNormalizer().normalize(timestamp);
        } catch (TypeNormalizationException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "BucketHashLeaf{" +
                "hash=" + hash +
                ", timestamp=" + timestamp +
                '}';
    }

    /**
     * Timestamp should be sorted in descending order
     * @param bucketHashLeaf
     * @return
     */
    @Override
    public int compareTo(BucketHashLeaf bucketHashLeaf) {
        return bucketHashLeaf.timestamp.compareTo(timestamp);
    }
}
