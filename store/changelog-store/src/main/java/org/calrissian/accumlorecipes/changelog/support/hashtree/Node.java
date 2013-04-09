package org.calrissian.accumlorecipes.changelog.support.hashtree;

import java.io.Serializable;
import java.util.List;

/**
 * A node in a merkle tree representing a hash
 */
public interface Node extends Serializable {

    /**
     * Accessor for the children that this Node's hash is comprised of
     * @return
     */
    List<Node> getChildren();

    /**
     * If this node has children, aggregates the hashes of the children. If this node is a leaf, represents the hash
     * of the data.
     * @return
     */
    String getHash();
}
