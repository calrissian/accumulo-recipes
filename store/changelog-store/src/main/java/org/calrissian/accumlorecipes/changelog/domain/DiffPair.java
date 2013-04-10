package org.calrissian.accumlorecipes.changelog.domain;

import org.calrissian.mango.hash.tree.HashLeaf;

public class DiffPair {

    HashLeaf minus;
    HashLeaf plus;

    public DiffPair(HashLeaf minus, HashLeaf plus) {

        this.minus = minus;
        this.plus = plus;
    }

    @Override
    public String toString() {
        return "{-" + minus + ", +" + plus + "}";
    }
}
