/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyten ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.MerkleTree.Hashable;
import org.apache.cassandra.utils.MerkleTree.RowHash;
import org.apache.cassandra.utils.MerkleTree.TreeRange;
import org.apache.cassandra.utils.MerkleTree.TreeRangeIterator;

import static org.apache.cassandra.utils.MerkleTree.RECOMMENDED_DEPTH;
import static org.junit.Assert.*;

public class MerkleTreeTest
{
    public static byte[] DUMMY = "blah".getBytes();

    /**
     * If a test assumes that the tree is 8 units wide, then it should set this value
     * to 8.
     */
    public static BigInteger TOKEN_SCALE = new BigInteger("8");

    protected IPartitioner partitioner;
    protected MerkleTree mt;

    private Range<Token> fullRange()
    {
        return new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
    }

    @Before
    public void setup()
    {
        TOKEN_SCALE = new BigInteger("8");
        partitioner = RandomPartitioner.instance;
        // TODO need to trickle TokenSerializer
        DatabaseDescriptor.setPartitionerUnsafe(partitioner);
        mt = new MerkleTree(partitioner, fullRange(), RECOMMENDED_DEPTH, Integer.MAX_VALUE);
    }

    public static void assertHashEquals(final byte[] left, final byte[] right)
    {
        assertHashEquals("", left, right);
    }

    public static void assertHashEquals(String message, final byte[] left, final byte[] right)
    {
        String lstring = left == null ? "null" : Hex.bytesToHex(left);
        String rstring = right == null ? "null" : Hex.bytesToHex(right);
        assertEquals(message, lstring, rstring);
    }

    /**
     * The value returned by this method is affected by TOKEN_SCALE: setting TOKEN_SCALE
     * to 8 means that passing -1 through 8 for this method will return values mapped
     * between -1 and Token.MAX_VALUE.
     */
    public static Token tok(int i)
    {
        if (i == -1)
            return new BigIntegerToken(new BigInteger("-1"));
        BigInteger bint = RandomPartitioner.MAXIMUM.divide(TOKEN_SCALE).multiply(new BigInteger("" + i));
        return new BigIntegerToken(bint);
    }

    @Test
    public void testSplit()
    {
        // split the range  (zero, zero] into:
        //  (zero,four], (four,six], (six,seven] and (seven, zero]
        mt.split(tok(4));
        mt.split(tok(6));
        mt.split(tok(7));

        assertEquals(4, mt.size());
        assertEquals(new Range<>(tok(7), tok(-1)), mt.get(tok(-1)));
        assertEquals(new Range<>(tok(-1), tok(4)), mt.get(tok(3)));
        assertEquals(new Range<>(tok(-1), tok(4)), mt.get(tok(4)));
        assertEquals(new Range<>(tok(4), tok(6)), mt.get(tok(6)));
        assertEquals(new Range<>(tok(6), tok(7)), mt.get(tok(7)));

        // check depths
        assertEquals((byte) 1, mt.get(tok(4)).depth);
        assertEquals((byte) 2, mt.get(tok(6)).depth);
        assertEquals((byte) 3, mt.get(tok(7)).depth);
        assertEquals((byte) 3, mt.get(tok(-1)).depth);

        try
        {
            mt.split(tok(-1));
            fail("Shouldn't be able to split outside the initial range.");
        }
        catch (AssertionError e)
        {
            // pass
        }
    }

    @Test
    public void testSplitLimitDepth()
    {
        mt = new MerkleTree(partitioner, fullRange(), (byte) 2, Integer.MAX_VALUE);

        assertTrue(mt.split(tok(4)));
        assertTrue(mt.split(tok(2)));
        assertEquals(3, mt.size());

        // should fail to split below hashdepth
        assertFalse(mt.split(tok(1)));
        assertEquals(3, mt.size());
        assertEquals(new Range<>(tok(4), tok(-1)), mt.get(tok(-1)));
        assertEquals(new Range<>(tok(-1), tok(2)), mt.get(tok(2)));
        assertEquals(new Range<>(tok(2), tok(4)), mt.get(tok(4)));
    }

    @Test
    public void testSplitLimitSize()
    {
        mt = new MerkleTree(partitioner, fullRange(), RECOMMENDED_DEPTH, 2);

        assertTrue(mt.split(tok(4)));
        assertEquals(2, mt.size());

        // should fail to split above maxsize
        assertFalse(mt.split(tok(2)));
        assertEquals(2, mt.size());
        assertEquals(new Range<>(tok(4), tok(-1)), mt.get(tok(-1)));
        assertEquals(new Range<>(tok(-1), tok(4)), mt.get(tok(4)));
    }

    @Test
    public void testInvalids()
    {
        Iterator<TreeRange> ranges;

        // (zero, zero]
        ranges = mt.invalids();
        assertEquals(new Range<>(tok(-1), tok(-1)), ranges.next());
        assertFalse(ranges.hasNext());

        // all invalid
        mt.split(tok(4));
        mt.split(tok(2));
        mt.split(tok(6));
        mt.split(tok(3));
        mt.split(tok(5));
        ranges = mt.invalids();
        assertEquals(new Range<>(tok(6), tok(-1)), ranges.next());
        assertEquals(new Range<>(tok(-1), tok(2)), ranges.next());
        assertEquals(new Range<>(tok(2), tok(3)), ranges.next());
        assertEquals(new Range<>(tok(3), tok(4)), ranges.next());
        assertEquals(new Range<>(tok(4), tok(5)), ranges.next());
        assertEquals(new Range<>(tok(5), tok(6)), ranges.next());
        assertEquals(new Range<>(tok(6), tok(-1)), ranges.next());
        assertFalse(ranges.hasNext());
    }


    @Test
    public void testHashFull()
    {
        byte[] val = DUMMY;
        Range<Token> range = new Range<>(tok(-1), tok(-1));

        // (zero, zero]
        assertNull(mt.hash(range));

        // validate the range
        mt.get(tok(-1)).hash(val);

        assertHashEquals(val, mt.hash(range));
    }

    @Test
    public void testHashPartial()
    {
        byte[] val = DUMMY;
        byte[] leftval = hashed(val, 1, 1);
        byte[] partialval = hashed(val, 1);
        Range<Token> left = new Range<>(tok(-1), tok(4));
        Range<Token> partial = new Range<>(tok(2), tok(4));
        Range<Token> right = new Range<>(tok(4), tok(-1));
        Range<Token> linvalid = new Range<>(tok(1), tok(4));
        Range<Token> rinvalid = new Range<>(tok(4), tok(6));

        // (zero,two] (two,four] (four, zero]
        mt.split(tok(4));
        mt.split(tok(2));
        assertNull(mt.hash(left));
        assertNull(mt.hash(partial));
        assertNull(mt.hash(right));
        assertNull(mt.hash(linvalid));
        assertNull(mt.hash(rinvalid));

        // validate the range
        mt.get(tok(2)).hash(val);
        mt.get(tok(4)).hash(val);
        mt.get(tok(-1)).hash(val);

        assertHashEquals(leftval, mt.hash(left));
        assertHashEquals(partialval, mt.hash(partial));
        assertHashEquals(val, mt.hash(right));
        assertNull(mt.hash(linvalid));
        assertNull(mt.hash(rinvalid));
    }

    @Test
    public void testHashInner()
    {
        byte[] val = DUMMY;
        byte[] lchildval = hashed(val, 3, 3, 2);
        byte[] rchildval = hashed(val, 2, 2);
        byte[] fullval = hashed(val, 3, 3, 2, 2, 2);
        Range<Token> full = new Range<>(tok(-1), tok(-1));
        Range<Token> lchild = new Range<>(tok(-1), tok(4));
        Range<Token> rchild = new Range<>(tok(4), tok(-1));
        Range<Token> invalid = new Range<>(tok(1), tok(-1));

        // (zero,one] (one, two] (two,four] (four, six] (six, zero]
        mt.split(tok(4));
        mt.split(tok(2));
        mt.split(tok(6));
        mt.split(tok(1));
        assertNull(mt.hash(full));
        assertNull(mt.hash(lchild));
        assertNull(mt.hash(rchild));
        assertNull(mt.hash(invalid));

        // validate the range
        mt.get(tok(1)).hash(val);
        mt.get(tok(2)).hash(val);
        mt.get(tok(4)).hash(val);
        mt.get(tok(6)).hash(val);
        mt.get(tok(-1)).hash(val);

        assertHashEquals(fullval, mt.hash(full));
        assertHashEquals(lchildval, mt.hash(lchild));
        assertHashEquals(rchildval, mt.hash(rchild));
        assertNull(mt.hash(invalid));
    }

    @Test
    public void testHashDegenerate()
    {
        TOKEN_SCALE = new BigInteger("32");

        byte[] val = DUMMY;
        byte[] childfullval = hashed(val, 5, 5, 4);
        byte[] fullval = hashed(val, 5, 5, 4, 3, 2, 1);
        Range<Token> childfull = new Range<>(tok(-1), tok(4));
        Range<Token> full = new Range<>(tok(-1), tok(-1));
        Range<Token> invalid = new Range<>(tok(4), tok(-1));

        mt = new MerkleTree(partitioner, fullRange(), RECOMMENDED_DEPTH, Integer.MAX_VALUE);
        mt.split(tok(16));
        mt.split(tok(8));
        mt.split(tok(4));
        mt.split(tok(2));
        mt.split(tok(1));
        assertNull(mt.hash(full));
        assertNull(mt.hash(childfull));
        assertNull(mt.hash(invalid));

        // validate the range
        mt.get(tok(1)).hash(val);
        mt.get(tok(2)).hash(val);
        mt.get(tok(4)).hash(val);
        mt.get(tok(8)).hash(val);
        mt.get(tok(16)).hash(val);
        mt.get(tok(-1)).hash(val);

        assertHashEquals(fullval, mt.hash(full));
        assertHashEquals(childfullval, mt.hash(childfull));
        assertNull(mt.hash(invalid));
    }

    @Test
    public void testHashRandom()
    {
        int max = 1000000;
        TOKEN_SCALE = new BigInteger("" + max);

        mt = new MerkleTree(partitioner, fullRange(), RECOMMENDED_DEPTH, 32);
        Random random = new Random();
        while (true)
        {
            if (!mt.split(tok(random.nextInt(max))))
                break;
        }

        // validate the tree
        TreeRangeIterator ranges = mt.invalids();
        for (TreeRange range : ranges)
            range.addHash(new RowHash(range.right, new byte[0], 0));

        assert mt.hash(new Range<>(tok(-1), tok(-1))) != null :
            "Could not hash tree " + mt;
    }

    /**
     * Generate two trees with different splits, but containing the same keys, and
     * check that they compare equally.
     *
     * The set of keys used in this test is: #{2,4,6,8,12,14,0}
     */
    @Test
    public void testValidateTree()
    {
        TOKEN_SCALE = new BigInteger("16"); // this test needs slightly more resolution

        Range<Token> full = new Range<>(tok(-1), tok(-1));
        Iterator<TreeRange> ranges;
        MerkleTree mt2 = new MerkleTree(partitioner, fullRange(), RECOMMENDED_DEPTH, Integer.MAX_VALUE);

        mt.split(tok(8));
        mt.split(tok(4));
        mt.split(tok(12));
        mt.split(tok(6));
        mt.split(tok(10));

        ranges = mt.invalids();
        ranges.next().addAll(new HIterator(2, 4)); // (-1,4]: depth 2
        ranges.next().addAll(new HIterator(6)); // (4,6]
        ranges.next().addAll(new HIterator(8)); // (6,8]
        ranges.next().addAll(new HIterator(/*empty*/ new int[0])); // (8,10]
        ranges.next().addAll(new HIterator(12)); // (10,12]
        ranges.next().addAll(new HIterator(14, -1)); // (12,-1]: depth 2


        mt2.split(tok(8));
        mt2.split(tok(4));
        mt2.split(tok(12));
        mt2.split(tok(2));
        mt2.split(tok(10));
        mt2.split(tok(9));
        mt2.split(tok(11));

        ranges = mt2.invalids();
        ranges.next().addAll(new HIterator(2)); // (-1,2]
        ranges.next().addAll(new HIterator(4)); // (2,4]
        ranges.next().addAll(new HIterator(6, 8)); // (4,8]: depth 2
        ranges.next().addAll(new HIterator(/*empty*/ new int[0])); // (8,9]
        ranges.next().addAll(new HIterator(/*empty*/ new int[0])); // (9,10]
        ranges.next().addAll(new HIterator(/*empty*/ new int[0])); // (10,11]: depth 4
        ranges.next().addAll(new HIterator(12)); // (11,12]: depth 4
        ranges.next().addAll(new HIterator(14, -1)); // (12,-1]: depth 2

        byte[] mthash = mt.hash(full);
        byte[] mt2hash = mt2.hash(full);
        assertHashEquals("Tree hashes did not match: " + mt + " && " + mt2, mthash, mt2hash);
    }

    @Test
    public void testSerialization() throws Exception
    {
        Range<Token> full = new Range<>(tok(-1), tok(-1));

        // populate and validate the tree
        mt.maxsize(256);
        mt.init();
        for (TreeRange range : mt.invalids())
            range.addAll(new HIterator(range.right));

        byte[] initialhash = mt.hash(full);

        DataOutputBuffer out = new DataOutputBuffer();
        MerkleTree.serializer.serialize(mt, out, MessagingService.current_version);
        byte[] serialized = out.toByteArray();

        DataInputPlus in = new DataInputBuffer(serialized);
        MerkleTree restored = MerkleTree.serializer.deserialize(in, MessagingService.current_version);

        assertHashEquals(initialhash, restored.hash(full));
    }

    @Test
    public void testDifference()
    {
        int maxsize = 16;
        mt.maxsize(maxsize);
        MerkleTree mt2 = new MerkleTree(partitioner, fullRange(), RECOMMENDED_DEPTH, maxsize);
        mt.init();
        mt2.init();

        // add dummy hashes to both trees
        for (TreeRange range : mt.invalids())
            range.addAll(new HIterator(range.right));
        for (TreeRange range : mt2.invalids())
            range.addAll(new HIterator(range.right));

        TreeRange leftmost = null;
        TreeRange middle = null;

        mt.maxsize(maxsize + 2); // give some room for splitting

        // split the leftmost
        Iterator<TreeRange> ranges = mt.invalids();
        leftmost = ranges.next();
        mt.split(leftmost.right);

        // set the hashes for the leaf of the created split
        middle = mt.get(leftmost.right);
        middle.hash("arbitrary!".getBytes());
        mt.get(partitioner.midpoint(leftmost.left, leftmost.right)).hash("even more arbitrary!".getBytes());

        // trees should disagree for (leftmost.left, middle.right]
        List<TreeRange> diffs = MerkleTree.difference(mt, mt2);
        assertEquals(diffs + " contains wrong number of differences:", 1, diffs.size());
        assertTrue(diffs.contains(new Range<>(leftmost.left, middle.right)));
    }

    /**
     * difference should behave as expected, even with extremely small ranges
     */
    @Test
    public void differenceSmallRange()
    {
        Token start = new BigIntegerToken("9");
        Token end = new BigIntegerToken("10");
        Range<Token> range = new Range<>(start, end);

        MerkleTree ltree = new MerkleTree(partitioner, range, RECOMMENDED_DEPTH, 16);
        ltree.init();
        MerkleTree rtree = new MerkleTree(partitioner, range, RECOMMENDED_DEPTH, 16);
        rtree.init();

        byte[] h1 = "asdf".getBytes();
        byte[] h2 = "hjkl".getBytes();

        // add dummy hashes to both trees
        for (TreeRange tree : ltree.invalids())
        {
            tree.addHash(new RowHash(range.right, h1, h1.length));
        }
        for (TreeRange tree : rtree.invalids())
        {
            tree.addHash(new RowHash(range.right, h2, h2.length));
        }

        List<TreeRange> diffs = MerkleTree.difference(ltree, rtree);
        assertEquals(Lists.newArrayList(range), diffs);
        assertEquals(MerkleTree.FULLY_INCONSISTENT, MerkleTree.differenceHelper(ltree, rtree, new ArrayList<>(), new MerkleTree.TreeDifference(ltree.fullRange.left, ltree.fullRange.right, (byte) 0)));
    }

    /**
     * matching should behave as expected, even with extremely small ranges
     */
    @Test
    public void matchingSmallRange()
    {
        Token start = new BigIntegerToken("9");
        Token end = new BigIntegerToken("10");
        Range<Token> range = new Range<>(start, end);

        MerkleTree ltree = new MerkleTree(partitioner, range, RECOMMENDED_DEPTH, 16);
        ltree.init();
        MerkleTree rtree = new MerkleTree(partitioner, range, RECOMMENDED_DEPTH, 16);
        rtree.init();

        byte[] h1 = "asdf".getBytes();
        byte[] h2 = "asdf".getBytes();


        // add dummy hashes to both trees
        for (TreeRange tree : ltree.invalids())
        {
            tree.addHash(new RowHash(range.right, h1, h1.length));
        }
        for (TreeRange tree : rtree.invalids())
        {
            tree.addHash(new RowHash(range.right, h2, h2.length));
        }

        // top level difference() should show no differences
        assertEquals(MerkleTree.difference(ltree, rtree), Lists.newArrayList());
    }

    /**
     * Return the root hash of a binary tree with leaves at the given depths
     * and with the given hash val in each leaf.
     */
    byte[] hashed(byte[] val, Integer... depths)
    {
        ArrayDeque<Integer> dstack = new ArrayDeque<Integer>();
        ArrayDeque<byte[]> hstack = new ArrayDeque<byte[]>();
        Iterator<Integer> depthiter = Arrays.asList(depths).iterator();
        if (depthiter.hasNext())
        {
            dstack.push(depthiter.next());
            hstack.push(val);
        }
        while (depthiter.hasNext())
        {
            Integer depth = depthiter.next();
            byte[] hash = val;
            while (depth.equals(dstack.peek()))
            {
                // consume the stack
                hash = Hashable.binaryHash(hstack.pop(), hash);
                depth = dstack.pop() - 1;
            }
            dstack.push(depth);
            hstack.push(hash);
        }
        assert hstack.size() == 1;
        return hstack.pop();
    }

    static class HIterator extends AbstractIterator<RowHash>
    {
        private Iterator<Token> tokens;

        public HIterator(int... tokens)
        {
            List<Token> tlist = new LinkedList<Token>();
            for (int token : tokens)
                tlist.add(tok(token));
            this.tokens = tlist.iterator();
        }

        public HIterator(Token... tokens)
        {
            this.tokens = Arrays.asList(tokens).iterator();
        }

        public RowHash computeNext()
        {
            if (tokens.hasNext())
                return new RowHash(tokens.next(), DUMMY, DUMMY.length);
            return endOfData();
        }
    }

    @Test
    public void testEstimatedSizes()
    {
        // With no or negative allowed space we should still get a depth of 1
        Assert.assertEquals(1, MerkleTree.estimatedMaxDepthForBytes(Murmur3Partitioner.instance, -20, 32));
        Assert.assertEquals(1, MerkleTree.estimatedMaxDepthForBytes(Murmur3Partitioner.instance, 0, 32));
        Assert.assertEquals(1, MerkleTree.estimatedMaxDepthForBytes(Murmur3Partitioner.instance, 1, 32));

        // The minimum of 1 megabyte split between RF=3 should yield trees of around 10
        Assert.assertEquals(10, MerkleTree.estimatedMaxDepthForBytes(Murmur3Partitioner.instance,
                                                                     1048576 / 3, 32));

        // With a single megabyte of space we should get 12
        Assert.assertEquals(12, MerkleTree.estimatedMaxDepthForBytes(Murmur3Partitioner.instance,
                                                                     1048576, 32));

        // With 100 megabytes we should get a limit of 19
        Assert.assertEquals(19, MerkleTree.estimatedMaxDepthForBytes(Murmur3Partitioner.instance,
                                                                     100 * 1048576, 32));

        // With 300 megabytes we should get the old limit of 20
        Assert.assertEquals(20, MerkleTree.estimatedMaxDepthForBytes(Murmur3Partitioner.instance,
                                                                     300 * 1048576, 32));
        Assert.assertEquals(20, MerkleTree.estimatedMaxDepthForBytes(RandomPartitioner.instance,
                                                                     300 * 1048576, 32));
        Assert.assertEquals(20, MerkleTree.estimatedMaxDepthForBytes(ByteOrderedPartitioner.instance,
                                                                     300 * 1048576, 32));
    }

    @Test
    public void testEstimatedSizesRealMeasurement()
    {
        // Use a fixed source of randomness so that the test does not flake.
        Random random = new Random(1);
        checkEstimatedSizes(RandomPartitioner.instance, random);
        checkEstimatedSizes(Murmur3Partitioner.instance, random);
    }

    private void checkEstimatedSizes(IPartitioner partitioner, Random random)
    {
        Range<Token> fullRange = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        MerkleTree tree = new MerkleTree(partitioner, fullRange, RECOMMENDED_DEPTH, 0);

        // Test 16 kilobyte -> 16 megabytes
        for (int i = 14; i < 24; i ++)
        {
            long numBytes = 1 << i;
            int maxDepth = MerkleTree.estimatedMaxDepthForBytes(partitioner, numBytes, 32);
            long realSizeOfMerkleTree = measureTree(tree, fullRange, maxDepth, random);
            long biggerTreeSize = measureTree(tree, fullRange, maxDepth + 1, random);

            Assert.assertTrue(realSizeOfMerkleTree < numBytes);
            Assert.assertTrue(biggerTreeSize > numBytes);
        }
    }

    private long measureTree(MerkleTree tree, Range<Token> fullRange, int depth, Random random)
    {
        tree = new MerkleTree(tree.partitioner(), fullRange, RECOMMENDED_DEPTH, (long) Math.pow(2, depth));
        // Initializes it as a fully balanced tree.
        tree.init();

        byte[] key = new byte[128];
        // Try to actually allocate some hashes. Note that this is not guaranteed to actually populate the tree,
        // but we re-use the source of randomness to try to make it reproducible.
        for (int i = 0; i < tree.maxsize() * 8; i++)
        {
            random.nextBytes(key);
            Token token = tree.partitioner().getToken(ByteBuffer.wrap(key));
            tree.get(token).addHash(new RowHash(token, new byte[32], 32));
        }

        tree.hash(fullRange);
        return ObjectSizes.measureDeep(tree);
    }
}
