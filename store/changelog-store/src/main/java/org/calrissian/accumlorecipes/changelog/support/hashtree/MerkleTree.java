package org.calrissian.accumlorecipes.changelog.support.hashtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A standard MerkleTree that takes a collection of @link{Hashable} objects and creates a tree, aggregating the hashes as
 * it moves up the levels of the tree. The root represents the hashes of the entire tree.
 */
public class MerkleTree<T extends Leaf> implements Serializable {

    protected Integer dimensions = 2;  // default to a binary tree
    protected Node topHash;

    public MerkleTree() {
    }

    public MerkleTree(List<T> leaves) throws IllegalStateException{
        this.topHash = build(leaves);
    }

    public MerkleTree(List<T> leaves, int dimensions) throws IllegalStateException{
        this.dimensions = dimensions;
        this.topHash = build(leaves);
    }

    /**
     * Accessor for the root of the tree
     * @return
     */
    public Node getTopHash() {
        return topHash;
    }

    /**
     * The merkle tree is constructed from the bottom up. That is, the parents of the leaves are constructed recursively
     * capped by the dimension size until the root node is encountered.
     * @param leaves
     * @return
     */
    private Node build(List<T> leaves) {

        // first sort the collection so we can deterministically construct our tree
        Collections.sort(leaves);

        List<Node> hashNodes = new ArrayList<Node>();
        List<T> curLeaves;
        for(int i = 0; i < leaves.size(); i+=dimensions) {
            int idx = i + dimensions > leaves.size() ? leaves.size() : i + dimensions;
            curLeaves = leaves.subList(i, idx);
            hashNodes.add(curLeaves.size() == 1 ? curLeaves.get(0) : new HashNode(new ArrayList<Node>(curLeaves)));
        }

        List<Node> finalTree = build(hashNodes);

        if(finalTree != null && finalTree.size() > 0) {
            return finalTree.get(0);
        }

        else {
            throw new IllegalStateException("Final tree cannot have 0 root nodes.");
        }
    }

    /**
     * Resursive method for hashing children and constructing parents until the top hash (root node) is encountered
     * @param nodes
     * @return
     */
    private List<Node> build(List<Node> nodes) {

        List<Node> hashNodes = new ArrayList<Node>();
        List<Node> curNodes;
        for(int i = 0; i < nodes.size(); i+=dimensions) {

            int idx = i + dimensions > nodes.size() ? nodes.size() : i + dimensions;
            curNodes = nodes.subList(i, idx);
            hashNodes.add(curNodes.size() == 1 ? curNodes.get(0) : new HashNode(new ArrayList<Node>(curNodes)));
        }

        if(hashNodes.size() > 1) {
            hashNodes = build(hashNodes);
        }

        return hashNodes;
    }

    /**
     * Diff current tree against another. The resulting list contains nodes in the current tree that differ from the
     * other tree.
     * @param other
     * @return
     */
    public List<T> diff(MerkleTree other) {

        List<T> differences = new ArrayList<T>();

        if(!other.getTopHash().getHash().equals(getTopHash().getHash())) {

            List<Node> nodes1 = topHash.getChildren();
            List<Node> nodes2 = other.getTopHash().getChildren();

            if(nodes1 == null) {
                return differences;
            }

            else if(nodes1 != null && nodes2 == null) {
                differences.addAll(getLeaves(nodes2));
            }

            else {
                for(int i = 0; i < nodes1.size(); i++) {

                    if(i < nodes1.size() && nodes2.size() == i) {
                        differences.addAll(getLeaves(nodes1.get(i).getChildren()));
                    }

                    else {
                        differences.addAll(diff(nodes1.get(i), nodes2.get(i)));
                    }
                }
            }
        }

        return differences;
    }

    /**
     * Recursive method for diffing two subtrees against each other
     * @param one
     * @param two
     * @return
     */
    private  List<T> diff(Node one, Node two) {

        List<T> differences = new ArrayList<T>();

        if(!one.getHash().equals(two.getHash())) {

            if(one instanceof Leaf) {
                differences.add((T)one);

            } else if(one.getChildren() != null && two.getChildren() == null) {
                differences.addAll(getLeaves(one.getChildren()));

            } else {

                for(int i = 0; i < one.getChildren().size(); i++) {

                    Node node1 = one.getChildren().get(i);
                    Node noe2 = two.getChildren().get(i);

                    differences.addAll(diff(node1, noe2));
                }
            }
        }

        return differences;
    }


    /**
     * Visits every subtree in a list of nodes until leaves are encountered and returns them.
     * @param nodes
     * @return
     */
    private List<T> getLeaves(List<Node> nodes) {

        List<T> leaves = new ArrayList<T>();
        for(Node child : nodes) {
            if(child instanceof Leaf) {
                leaves.add((T)child);
            }

            else {
                leaves.addAll(getLeaves(child.getChildren()));
            }
        }

        return leaves;
    }

    @Override
    public String toString() {
        return "MerkleTree{" +
                "dimensions=" + dimensions +
                ", topHash=" + topHash +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MerkleTree)) return false;

        MerkleTree that = (MerkleTree) o;

        if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) return false;
        if (topHash != null ? !topHash.equals(that.topHash) : that.topHash != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = dimensions != null ? dimensions.hashCode() : 0;
        result = 31 * result + (topHash != null ? topHash.hashCode() : 0);
        return result;
    }
}