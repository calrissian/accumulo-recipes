package org.calrissian.accumlorecipes.changelog.support.hashtree;

import java.util.List;

/**
 * A node in a merkle tree representing a hash
 */
public interface Node {

    /**
     * Accessor for the parent whose hash will include this node
     * @return
     */
    Node getParent();

    /**
     * Mutation function to set parent of current node
     */
    void setParent(Node node);

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
