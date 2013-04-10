package org.calrissian.accumlorecipes.changelog;

import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.collect.CloseableIterator;
import org.calrissian.mango.hash.tree.MerkleTree;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.Collection;
import java.util.Date;

/**
 * A Changelog store to represent a bucketed set of hashes representing change sets that can be shared between two
 * distributed environment. Of a large number of buckets representing changes, the comparing two merkle trees should
 * allow systems to figure out exactly 'which' buckets are different.
 */
public interface ChangelogStore {

    /**
     * Put a changeset into the changeset store.
     * @param changes
     */
    void put(Collection<StoreEntry> changes);

    /**
     * Get a Merkle tree containing hashes of each of the buckets
     * @param start
     * @param stop
     * @return
     */
    MerkleTree getChangeTree(Date start, Date stop);

    /**
     * Get changesets living inside of the given buckets
     *
     *
     * @param buckets dates representing time increments (i.e. 15 minutes)
     * @return
     */
    CloseableIterator<StoreEntry> getChanges(Collection<Date> buckets);
}
