package org.calrissian.accumlorecipes.changelog.support.hashtree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A standard MerkleTree that takes a collection of @link{Hashable} objects and creates a tree, aggregating the hashes as
 * it moves up the levels of the tree. The root represents the hashes of the entire tree.
 */
public class MerkleTree {

    public static final Integer DEFAULT_DIMENSIONS = 2;  // default to a binary tree

    protected Node topHash;

    public MerkleTree(List<? extends Leaf> leaves) throws IllegalStateException{

        this.topHash = build(leaves, DEFAULT_DIMENSIONS);
    }

    public Node getTopHash() {
        return topHash;
    }

    private Node build(List<? extends Leaf> leaves, int dimensions) {

        // first sort the colleection so we can construct our tree
        Collections.sort(leaves);

        List<HashNode> hashNodes = new ArrayList<HashNode>();
        List<Node> curLeaves = new ArrayList<Node>();
        for(int i = 0; i < leaves.size(); i++) {

            Leaf leaf = leaves.get(i);
            curLeaves.add(leaf);

            if(i > 0 && i - 1 % dimensions == 0 || i == leaves.size() - 1) {

                HashNode node = new HashNode(new ArrayList<Node>(curLeaves));
                hashNodes.add(node);

                curLeaves = new ArrayList<Node>();
            }
        }

        List<HashNode> finalTree = build(hashNodes, dimensions);

        if(finalTree != null && finalTree.size() > 0) {
            return finalTree.get(0);
        }

        else {
            throw new IllegalStateException("Final tree cannot have 0 root nodes.");
        }
    }

    private List<HashNode> build(List<HashNode> nodes, int dimensions) {

        List<HashNode> hashNodes = new ArrayList<HashNode>();
        List<Node> curNodes = new ArrayList<Node>();
        for(int i = 0; i < nodes.size(); i++) {

            HashNode node = nodes.get(i);
            curNodes.add(node);

            if(i > 0 && i - 1 % dimensions == 0 || i == nodes.size()) {

                HashNode hashNode = new HashNode(new ArrayList<Node>(curNodes));
                hashNodes.add(hashNode);

                curNodes = new ArrayList<Node>();
            }
        }

        if(hashNodes.size() > 1) {
            hashNodes = build(nodes, dimensions);
        }

        return hashNodes;
    }
}