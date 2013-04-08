package org.calrissian.accumlorecipes.changelog.support.hashtree;

import org.calrissian.accumlorecipes.changelog.support.HashUtils;

import java.util.List;

/**
 * A node in a merkle tree representing a hash
 */
public class HashNode implements Node {

    protected final String hash;
    protected Node parent;
    protected final List<Node> children;

    public HashNode(List<Node> children) {
        this.children = children;

        String hashes = "";
        for(Node node : children) {

            node.setParent(this);
            hashes += node.getHash() + "\u0000";
        }

        try {

            hash = HashUtils.hashString(hashes);
            System.out.println("HASH of: " + hashes + "=" + hash);

        } catch (Exception e) {
            throw new IllegalStateException("Hash could not be derived from children: " + children);
        }
    }

    /**
     * Accessor for the parent whose hash will include this node
     * @return
     */
    @Override
    public Node getParent() {
        return parent;
    }

    /**
     * Accessor for the children that this Node's hash is comprised of
     * @return
     */
    @Override
    public List<Node> getChildren() {
        return children;
    }

    @Override
    public void setParent(Node node) {
        this.parent = node;
    }

    /**
     * If this node has children, aggregates the hashes of the children. If this node is a leaf, represents the hash
     * of the data.
     * @return
     */
    @Override
    public String getHash() {

        return hash;
    }

    @Override
    public String toString() {
        return "Node{" +
                "hash=" + hash +
                ", children=" + children +
                '}';
    }
}
