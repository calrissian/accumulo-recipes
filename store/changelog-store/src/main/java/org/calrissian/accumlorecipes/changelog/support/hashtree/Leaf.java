package org.calrissian.accumlorecipes.changelog.support.hashtree;


import java.util.List;

/**
 * A leaf represents a single "bucket" of data- it needs to be sortable.
 */
public abstract class Leaf implements Node, Comparable {

    protected String hash;

    public Leaf() {

    }

    public Leaf(String hash) {
        this.hash = hash;
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
        return getClass().getSimpleName() +  "{" +
                "hash='" + hash + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Leaf)) return false;

        Leaf leaf = (Leaf) o;

        if (hash != null ? !hash.equals(leaf.hash) : leaf.hash != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return hash != null ? hash.hashCode() : 0;
    }
}
