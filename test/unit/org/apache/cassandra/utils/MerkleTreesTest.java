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
import java.util.*;

import com.google.common.collect.AbstractIterator;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.MerkleTree.RowHash;
import org.apache.cassandra.utils.MerkleTree.TreeRange;
import org.apache.cassandra.utils.MerkleTrees.TreeRangeIterator;

import static org.apache.cassandra.utils.MerkleTreeTest.digest;
import static org.junit.Assert.*;

public class MerkleTreesTest
{
    private static final byte[] DUMMY = digest("dummy");

    /**
     * If a test assumes that the tree is 8 units wide, then it should set this value
     * to 8.
     */
    public static BigInteger TOKEN_SCALE = new BigInteger("8");

    protected static final IPartitioner partitioner = RandomPartitioner.instance;
    protected MerkleTrees mts;

    private Range<Token> fullRange()
    {
        return new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
    }

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(partitioner);
    }
    @Before
    public void clear()
    {
        TOKEN_SCALE = new BigInteger("8");
        mts = new MerkleTrees(partitioner);
        mts.addMerkleTree(Integer.MAX_VALUE, fullRange());
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
        BigInteger bint = RandomPartitioner.MAXIMUM.divide(TOKEN_SCALE).multiply(new BigInteger(""+i));
        return new BigIntegerToken(bint);
    }

    @Test
    public void testIntersectingRanges()
    {
        mts = new MerkleTrees(partitioner);

        boolean failure = true;
        mts.addMerkleTree(1, new Range<>(tok(1), tok(3)));

        try
        {
            mts.addMerkleTree(1, new Range<>(tok(2), tok(4)));
        }
        catch (AssertionError e)
        {
            failure = false;
        }

        assertFalse(failure);
    }

    @Test
    public void testSplit()
    {
        // split the range  (zero, zero] into:
        //  (zero,four], (four,six], (six,seven] and (seven, zero]
        mts.split(tok(4));
        mts.split(tok(6));
        mts.split(tok(7));

        assertEquals(4, mts.size());
        assertEquals(new Range<>(tok(7), tok(-1)), mts.get(tok(-1)));
        assertEquals(new Range<>(tok(-1), tok(4)), mts.get(tok(3)));
        assertEquals(new Range<>(tok(-1), tok(4)), mts.get(tok(4)));
        assertEquals(new Range<>(tok(4), tok(6)), mts.get(tok(6)));
        assertEquals(new Range<>(tok(6), tok(7)), mts.get(tok(7)));

        // check depths
        assertEquals((byte) 1, mts.get(tok(4)).depth);
        assertEquals((byte) 2, mts.get(tok(6)).depth);
        assertEquals((byte) 3, mts.get(tok(7)).depth);
        assertEquals((byte) 3, mts.get(tok(-1)).depth);

        try
        {
            mts.split(tok(-1));
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
        mts = new MerkleTrees(partitioner);

        mts.addMerkleTree(Integer.MAX_VALUE, (byte) 2, fullRange());

        assertTrue(mts.split(tok(4)));
        assertTrue(mts.split(tok(2)));
        assertEquals(3, mts.size());

        // should fail to split below hashdepth
        assertFalse(mts.split(tok(1)));
        assertEquals(3, mts.size());
        assertEquals(new Range<>(tok(4), tok(-1)), mts.get(tok(-1)));
        assertEquals(new Range<>(tok(-1), tok(2)), mts.get(tok(2)));
        assertEquals(new Range<>(tok(2), tok(4)), mts.get(tok(4)));
    }

    @Test
    public void testSplitLimitSize()
    {
        mts = new MerkleTrees(partitioner);

        mts.addMerkleTree(2, fullRange());

        assertTrue(mts.split(tok(4)));
        assertEquals(2, mts.size());

        // should fail to split above maxsize
        assertFalse(mts.split(tok(2)));
        assertEquals(2, mts.size());
        assertEquals(new Range<>(tok(4), tok(-1)), mts.get(tok(-1)));
        assertEquals(new Range<>(tok(-1), tok(4)), mts.get(tok(4)));
    }

    @Test
    public void testInvalids()
    {
        Iterator<TreeRange> ranges;

        // (zero, zero]
        ranges = mts.rangeIterator();
        assertEquals(new Range<>(tok(-1), tok(-1)), ranges.next());
        assertFalse(ranges.hasNext());

        // all invalid
        mts.split(tok(4));
        mts.split(tok(2));
        mts.split(tok(6));
        mts.split(tok(3));
        mts.split(tok(5));
        ranges = mts.rangeIterator();
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
        assertNull(mts.hash(range));

        // validate the range
        mts.get(tok(-1)).hash(val);

        assertHashEquals(val, mts.hash(range));
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
        mts.split(tok(4));
        mts.split(tok(2));

        // validate the range
        mts.get(tok(2)).hash(val);
        mts.get(tok(4)).hash(val);
        mts.get(tok(-1)).hash(val);

        assertHashEquals(leftval, mts.hash(left));
        assertHashEquals(partialval, mts.hash(partial));
        assertHashEquals(val, mts.hash(right));
        assertNull(mts.hash(linvalid));
        assertNull(mts.hash(rinvalid));
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
        mts.split(tok(4));
        mts.split(tok(2));
        mts.split(tok(6));
        mts.split(tok(1));

        // validate the range
        mts.get(tok(1)).hash(val);
        mts.get(tok(2)).hash(val);
        mts.get(tok(4)).hash(val);
        mts.get(tok(6)).hash(val);
        mts.get(tok(-1)).hash(val);

        assertHashEquals(fullval, mts.hash(full));
        assertHashEquals(lchildval, mts.hash(lchild));
        assertHashEquals(rchildval, mts.hash(rchild));
        assertNull(mts.hash(invalid));
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

        mts.split(tok(16));
        mts.split(tok(8));
        mts.split(tok(4));
        mts.split(tok(2));
        mts.split(tok(1));

        // validate the range
        mts.get(tok(1)).hash(val);
        mts.get(tok(2)).hash(val);
        mts.get(tok(4)).hash(val);
        mts.get(tok(8)).hash(val);
        mts.get(tok(16)).hash(val);
        mts.get(tok(-1)).hash(val);

        assertHashEquals(fullval, mts.hash(full));
        assertHashEquals(childfullval, mts.hash(childfull));
        assertNull(mts.hash(invalid));
    }

    @Test
    public void testHashRandom()
    {
        int max = 1000000;
        TOKEN_SCALE = new BigInteger("" + max);

        mts = new MerkleTrees(partitioner);
        mts.addMerkleTree(32, fullRange());

        Random random = new Random();
        while (true)
        {
            if (!mts.split(tok(random.nextInt(max))))
                break;
        }

        // validate the tree
        TreeRangeIterator ranges = mts.rangeIterator();
        for (TreeRange range : ranges)
            range.addHash(new RowHash(range.right, new byte[0], 0));

        assert mts.hash(new Range<>(tok(-1), tok(-1))) != null : "Could not hash tree " + mts;
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
        MerkleTrees mts2 = new MerkleTrees(partitioner);
        mts2.addMerkleTree(Integer.MAX_VALUE, fullRange());

        mts.split(tok(8));
        mts.split(tok(4));
        mts.split(tok(12));
        mts.split(tok(6));
        mts.split(tok(10));

        int seed = 123456789;

        Random random1 = new Random(seed);
        ranges = mts.rangeIterator();
        ranges.next().addAll(new HIterator(random1, 2, 4)); // (-1,4]: depth 2
        ranges.next().addAll(new HIterator(random1, 6)); // (4,6]
        ranges.next().addAll(new HIterator(random1, 8)); // (6,8]
        ranges.next().addAll(new HIterator(/*empty*/ new int[0])); // (8,10]
        ranges.next().addAll(new HIterator(random1, 12)); // (10,12]
        ranges.next().addAll(new HIterator(random1, 14, -1)); // (12,-1]: depth 2


        mts2.split(tok(8));
        mts2.split(tok(4));
        mts2.split(tok(12));
        mts2.split(tok(2));
        mts2.split(tok(10));
        mts2.split(tok(9));
        mts2.split(tok(11));

        Random random2 = new Random(seed);
        ranges = mts2.rangeIterator();
        ranges.next().addAll(new HIterator(random2, 2)); // (-1,2]
        ranges.next().addAll(new HIterator(random2, 4)); // (2,4]
        ranges.next().addAll(new HIterator(random2, 6, 8)); // (4,8]: depth 2
        ranges.next().addAll(new HIterator(/*empty*/ new int[0])); // (8,9]
        ranges.next().addAll(new HIterator(/*empty*/ new int[0])); // (9,10]
        ranges.next().addAll(new HIterator(/*empty*/ new int[0])); // (10,11]: depth 4
        ranges.next().addAll(new HIterator(random2, 12)); // (11,12]: depth 4
        ranges.next().addAll(new HIterator(random2, 14, -1)); // (12,-1]: depth 2

        byte[] mthash = mts.hash(full);
        byte[] mt2hash = mts2.hash(full);
        assertHashEquals("Tree hashes did not match: " + mts + " && " + mts2, mthash, mt2hash);
    }

    @Test
    public void testSerialization() throws Exception
    {
        Range<Token> first = new Range<>(tok(3), tok(4));

        Collection<Range<Token>> ranges = new ArrayList<>();

        ranges.add(first);
        ranges.add(new Range<Token>(tok(5), tok(2)));

        mts = new MerkleTrees(partitioner);
        mts.addMerkleTrees(256, ranges);

        // populate and validate the tree
        mts.init();
        for (TreeRange range : mts.rangeIterator())
            range.addAll(new HIterator(range.right));

        byte[] initialhash = mts.hash(first);

        long serializedSize = MerkleTrees.serializer.serializedSize(mts, MessagingService.current_version);
        DataOutputBuffer out = new DataOutputBuffer();
        MerkleTrees.serializer.serialize(mts, out, MessagingService.current_version);
        byte[] serialized = out.toByteArray();

        assertEquals(serializedSize, serialized.length);

        DataInputBuffer in = new DataInputBuffer(serialized);
        MerkleTrees restored = MerkleTrees.serializer.deserialize(in, MessagingService.current_version);

        assertHashEquals(initialhash, restored.hash(first));
    }

    @Test
    public void testDifference()
    {
        int maxsize = 16;
        mts = new MerkleTrees(partitioner);
        mts.addMerkleTree(32, fullRange());

        MerkleTrees mts2 = new MerkleTrees(partitioner);
        mts2.addMerkleTree(32, fullRange());

        mts.init();
        mts2.init();

        int seed = 123456789;
        // add dummy hashes to both trees
        Random random1 = new Random(seed);
        for (TreeRange range : mts.rangeIterator())
            range.addAll(new HIterator(random1, range.right));

        Random random2 = new Random(seed);
        for (TreeRange range : mts2.rangeIterator())
            range.addAll(new HIterator(random2, range.right));

        TreeRange leftmost = null;
        TreeRange middle = null;

        mts.maxsize(fullRange(), maxsize + 2); // give some room for splitting

        // split the leftmost
        Iterator<TreeRange> ranges = mts.rangeIterator();
        leftmost = ranges.next();
        mts.split(leftmost.right);

        // set the hashes for the leaf of the created split
        middle = mts.get(leftmost.right);
        middle.hash(digest("arbitrary!"));
        mts.get(partitioner.midpoint(leftmost.left, leftmost.right)).hash(digest("even more arbitrary!"));

        // trees should disagree for (leftmost.left, middle.right]
        List<Range<Token>> diffs = MerkleTrees.difference(mts, mts2);
        assertEquals(diffs + " contains wrong number of differences:", 1, diffs.size());
        assertTrue(diffs.contains(new Range<>(leftmost.left, middle.right)));
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
                hash = MerkleTree.xor(hstack.pop(), hash);
                depth = dstack.pop()-1;
            }
            dstack.push(depth);
            hstack.push(hash);
        }
        assert hstack.size() == 1;
        return hstack.pop();
    }

    public static class HIterator extends AbstractIterator<RowHash>
    {
        private final Random random;
        private final Iterator<Token> tokens;

        HIterator(int... tokens)
        {
            this(new Random(), tokens);
        }

        HIterator(Random random, int... tokens)
        {
            List<Token> tlist = new ArrayList<>(tokens.length);
            for (int token : tokens)
                tlist.add(tok(token));
            this.tokens = tlist.iterator();
            this.random = random;
        }

        public HIterator(Token... tokens)
        {
            this(new Random(), tokens);
        }

        HIterator(Random random, Token... tokens)
        {
            this(random, Arrays.asList(tokens).iterator());
        }

        private HIterator(Random random, Iterator<Token> tokens)
        {
            this.random = random;
            this.tokens = tokens;
        }

        public RowHash computeNext()
        {
            if (tokens.hasNext())
            {
                byte[] digest = new byte[32];
                random.nextBytes(digest);
                return new RowHash(tokens.next(), digest, 12345L);
            }
            return endOfData();
        }
    }
}
