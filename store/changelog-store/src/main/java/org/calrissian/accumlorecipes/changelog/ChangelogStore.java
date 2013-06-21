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
package org.calrissian.accumlorecipes.changelog;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.hash.tree.MerkleTree;

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
    void put(Iterable<StoreEntry> changes);

    /**
     * Get a Merkle tree containing hashes of each of the buckets
     * @param start
     * @param stop
     * @return
     */
    MerkleTree getChangeTree(Date start, Date stop, Authorizations auths);

    /**
     * Get a Merkle tree containing hashes of each of the buckets with the given dimensions
     * @param start
     * @param stop
     * @return
     */
    MerkleTree getChangeTree(Date start, Date stop, int dimensions, Authorizations auths);

    /**
     * Get changesets living inside of the given buckets
     *
     *
     * @param buckets dates representing time increments (i.e. 15 minutes)
     * @return
     */
    CloseableIterable<StoreEntry> getChanges(Iterable<Date> buckets, Authorizations auths);
}
