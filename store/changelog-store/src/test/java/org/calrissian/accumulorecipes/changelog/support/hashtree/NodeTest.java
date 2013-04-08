package org.calrissian.accumulorecipes.changelog.support.hashtree;

import org.calrissian.accumlorecipes.changelog.support.HashUtils;
import org.calrissian.accumlorecipes.changelog.support.hashtree.Leaf;
import org.calrissian.accumlorecipes.changelog.support.hashtree.MerkleTree;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

public class NodeTest {

    @Test
    public void testNodeHashes() throws NoSuchAlgorithmException, UnsupportedEncodingException {

        MockLeaf leaf1 = new MockLeaf("4");
        MockLeaf leaf2 = new MockLeaf("2");
        MockLeaf leaf3 = new MockLeaf("8");

        List<MockLeaf> leaves = Arrays.asList(new MockLeaf[]{ leaf1, leaf2, leaf3});

        MerkleTree tree = new MerkleTree(leaves);

        System.out.println(tree.getTopHash());
        System.out.println(HashUtils.hashString("1\u00002\u0000"));
    }

    private class MockLeaf extends Leaf {

        public MockLeaf(String hash) {
            super(hash);
        }

        @Override
        public int compareTo(Object o) {

            MockLeaf obj = (MockLeaf)o;
            return hash.compareTo(obj.hash);
        }
    }

}
