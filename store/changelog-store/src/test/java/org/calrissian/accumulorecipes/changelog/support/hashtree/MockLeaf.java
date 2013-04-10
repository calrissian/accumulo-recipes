package org.calrissian.accumulorecipes.changelog.support.hashtree;

import org.calrissian.mango.hash.tree.HashLeaf;

public class MockLeaf extends HashLeaf {

    public MockLeaf() {
        super();
    }

    public MockLeaf(String hash) {

        super(hash);
    }


    public int compareTo(Object o) {

        MockLeaf obj = (MockLeaf)o;
        return hash.compareTo(obj.hash);
    }
}
