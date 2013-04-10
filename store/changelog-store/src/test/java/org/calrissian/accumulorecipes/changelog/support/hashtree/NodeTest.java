package org.calrissian.accumulorecipes.changelog.support.hashtree;

import org.codehaus.jackson.JsonGenerationException;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class NodeTest {

    @Test
    public void testNodeHashes() throws NoSuchAlgorithmException, IOException, JsonGenerationException, ClassNotFoundException {

        MockLeaf leaf1 = new MockLeaf("4");
        MockLeaf leaf2 = new MockLeaf("2");
        MockLeaf leaf3 = new MockLeaf("8");
        MockLeaf leaf4 = new MockLeaf("99");
        MockLeaf leaf5 = new MockLeaf("77");
        MockLeaf leaf6 = new MockLeaf("56");
        MockLeaf leaf7 = new MockLeaf("9");
        MockLeaf leaf8 = new MockLeaf("0");

//        List<MockLeaf> leaves = Arrays.asList(new MockLeaf[]{leaf1, leaf2, leaf8, leaf7, leaf4});
//
//        MerkleTree<MockLeaf> tree = new MerkleTree<MockLeaf>(leaves, 4);
//
//        List<MockLeaf> leaves2 = Arrays.asList(new MockLeaf[]{ leaf4, leaf5, leaf6, leaf7, leaf8});
//
//        MerkleTree<MockLeaf> tree2 = new MerkleTree<MockLeaf>(leaves2, 4);
//
//        System.out.println("DIFFS 1 on 2: " + tree.diff(tree2));
//        System.out.println("DIFFS 2 on 1: " + tree2.diff(tree));
//
//        System.out.println("TREE 1: " + tree);
//        System.out.println("TREE 2: " + tree2);
//
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        ObjectOutputStream oos = new ObjectOutputStream(baos);
//        oos.writeObject(tree);
//        oos.flush();
//        oos.close();
//
//        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
//        ObjectInputStream ois = new ObjectInputStream(bais);
//
//        MerkleTree<MockLeaf> newTree = (MerkleTree<MockLeaf>) ois.readObject();
//
//        System.out.println(newTree);
//
//        Kryo kryo = new Kryo();
//
//        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
//        Output output = new Output(new GZIPOutputStream(baos2));
//        kryo.writeObject(output, tree);
//        output.close();
//
//        System.out.println(new String(baos2.toByteArray()));
//
//        Input input = new Input(new GZIPInputStream(new ByteArrayInputStream(baos2.toByteArray())));
//        MerkleTree someObject = kryo.readObject(input, MerkleTree.class);
//        input.close();
//
//        System.out.println(someObject.equals(tree));
//
//        BucketHashLeaf tbl1 = new BucketHashLeaf(System.currentTimeMillis());
//        BucketHashLeaf tbl5 = new BucketHashLeaf(System.currentTimeMillis() - 5000);
//        BucketHashLeaf tbl6 = new BucketHashLeaf(System.currentTimeMillis() - 10000);
//
//        MerkleTree<BucketHashLeaf> mt = new MerkleTree<BucketHashLeaf>(Arrays.asList(new BucketHashLeaf[]{tbl1,  tbl5,  tbl6}));
//
//        System.out.println(mt);
    }
}
