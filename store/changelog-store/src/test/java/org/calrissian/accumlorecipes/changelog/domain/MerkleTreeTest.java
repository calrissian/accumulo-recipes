/*
 * Copyright (C) 2014 The Calrissian Authors
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
package org.calrissian.accumlorecipes.changelog.domain;

import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class MerkleTreeTest {

    MockLeaf leaf1 = new MockLeaf("4");
    MockLeaf leaf2 = new MockLeaf("2");
    MockLeaf leaf3 = new MockLeaf("8");
    MockLeaf leaf4 = new MockLeaf("99");
    MockLeaf leaf5 = new MockLeaf("77");
    MockLeaf leaf6 = new MockLeaf("56");
    MockLeaf leaf7 = new MockLeaf("9");
    MockLeaf leaf8 = new MockLeaf("0");

    @Test
    public void testTreeBuilds() throws NoSuchAlgorithmException, IOException, ClassNotFoundException {


        List<MockLeaf> leaves = asList(leaf1, leaf2, leaf8, leaf7, leaf4);

        MerkleTree<MockLeaf> tree = new MerkleTree<>(leaves, 2);

        assertEquals("17a19db32d969668fb08f9a5491eb4fe", tree.getTopHash().getHash());
        assertEquals(2, tree.getTopHash().getChildren().size());
    }

    @Test
    public void testDiff_differentDimensionsFails() {

        List<MockLeaf> leaves = asList(leaf1, leaf2);

        MerkleTree<MockLeaf> tree = new MerkleTree<>(leaves, 2);
        MerkleTree<MockLeaf> tree2 = new MerkleTree<>(leaves, 4);

        try {
            tree.diff(tree2);
            fail("Should have thrown exception");
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void testDiff_differentSizesFails() {

        List<MockLeaf> leaves = asList(leaf1, leaf2);
        List<MockLeaf> leaves2 = asList(leaf1, leaf2, leaf3);

        MerkleTree<MockLeaf> tree = new MerkleTree<>(leaves, 2);
        MerkleTree<MockLeaf> tree2 = new MerkleTree<>(leaves2, 2);

        try {
            tree.diff(tree2);
            fail("Should have thrown exception");
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void testEquals_false() {

        List<MockLeaf> leaves = asList(leaf1, leaf2);
        List<MockLeaf> leaves2 = asList(leaf1, leaf3);


        MerkleTree<MockLeaf> tree = new MerkleTree<>(leaves, 2);
        MerkleTree<MockLeaf> tree2 = new MerkleTree<>(leaves2, 2);

        assertFalse(tree.equals(tree2));
        assertFalse(tree2.equals(tree));
    }

    @Test
    public void testEquals_true() {

        List<MockLeaf> leaves = asList(leaf1, leaf2);
        List<MockLeaf> leaves2 = asList(leaf1, leaf2);

        MerkleTree<MockLeaf> tree = new MerkleTree<>(leaves, 2);
        MerkleTree<MockLeaf> tree2 = new MerkleTree<>(leaves2, 2);

        assertTrue(tree.equals(tree2));
        assertTrue(tree2.equals(tree));
    }

    @Test
    public void testDiff() {

        List<MockLeaf> leaves = asList(leaf1, leaf2);
        List<MockLeaf> leaves2 = asList(leaf1, leaf3);

        MerkleTree<MockLeaf> tree = new MerkleTree<>(leaves, 2);
        MerkleTree<MockLeaf> tree2 = new MerkleTree<>(leaves2, 2);

        List<MockLeaf> diffs = tree.diff(tree2);

        assertEquals(leaf2, diffs.get(0));
        assertEquals(1, diffs.size());

        diffs = tree2.diff(tree);

        assertEquals(leaf3, diffs.get(0));
        assertEquals(1, diffs.size());
    }

    private static class MockLeaf extends HashLeaf {
        private static final long serialVersionUID = 1L;

        public MockLeaf(String hash) {
            super(hash);
        }
    }
}
