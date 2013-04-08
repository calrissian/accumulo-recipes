package org.calrissian.accumlorecipes.changelog.support.hashtree;


import java.util.List;

public abstract class Leaf implements Node, Comparable {

    protected Node parent;
    protected String hash;

    public Leaf(String hash) {
        this.hash = hash;
    }

    @Override
    public Node getParent() {
        return parent;
    }

    @Override
    public void setParent(Node node) {
        this.parent = node;
    }

    @Override
    public List<Node> getChildren() {
        return null;
    }

    @Override
    public String getHash() {
        return hash;
    }

    @Override
    public String toString() {
        return "Leaf{" +
                "hash='" + hash + '\'' +
                '}';
    }
}
