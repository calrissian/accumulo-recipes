package org.calrissian.accumlorecipes.changelog.support.hashtree;

import org.calrissian.accumlorecipes.changelog.support.HashUtils;

import java.util.List;

/**
 * An internal hash node in a merkle tree
 */
public class HashNode implements Node {

    protected String hash;
    protected List<Node> children;

    public HashNode() {

    }

    public HashNode(List<Node> children) {
        this.children = children;

        String hashes = "";
        for(Node node : children) {

            hashes += node.getHash() + "\u0000";
        }

        try {

            hash = HashUtils.hashString(hashes);
        } catch (Exception e) {
            throw new IllegalStateException("Hash could not be derived from children: " + children);
        }
    }

    /**
     * Accessor for the children that this Node's hash is comprised of
     * @return
     */
    @Override
    public List<Node> getChildren() {
        return children;
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
        return "HashNode{" +
                "hash=" + hash +
                ", children=" + children +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HashNode)) return false;

        HashNode hashNode = (HashNode) o;

        if (hash != null ? !hash.equals(hashNode.hash) : hashNode.hash != null) return false;
        if (children != null ? !children.equals(hashNode.children) : hashNode.children != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = hash != null ? hash.hashCode() : 0;
        result = 31 * result + (children != null ? children.hashCode() : 0);
        return result;
    }
}
