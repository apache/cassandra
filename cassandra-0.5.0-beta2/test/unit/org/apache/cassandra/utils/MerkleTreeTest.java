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

import java.io.*;
import java.util.*;
import java.math.BigInteger;

import org.apache.cassandra.dht.*;
import static org.apache.cassandra.utils.MerkleTree.*;

import org.apache.log4j.Logger;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class MerkleTreeTest
{
    private static final Logger logger = Logger.getLogger(MerkleTreeTest.class);

    public static byte[] DUMMY = "blah".getBytes();

    /**
     * If a test assumes that the tree is 8 units wide, then it should set this value
     * to 8.
     */
    public static BigInteger TOKEN_SCALE = new BigInteger("8");

    protected IPartitioner partitioner;
    protected MerkleTree mt;

    @Before
    public void clear()
    {
        TOKEN_SCALE = new BigInteger("8");
        partitioner = new RandomPartitioner();
        mt = new MerkleTree(partitioner, RECOMMENDED_DEPTH, Integer.MAX_VALUE);
    }

    public static void assertHashEquals(final byte[] left, final byte[] right)
    {
        assertHashEquals("", left, right);
    }

    public static void assertHashEquals(String message, final byte[] left, final byte[] right)
    {
        String lstring = left == null ? "null" : FBUtilities.bytesToHex(left);
        String rstring = right == null ? "null" : FBUtilities.bytesToHex(right);
        assertEquals(message, lstring, rstring);
    }

    /**
     * The value returned by this method is affected by TOKEN_SCALE: setting TOKEN_SCALE
     * to 8 means that passing 0 through 8 for this method will return values mapped
     * between 0 and Token.MAX_VALUE.
     */
    public static BigIntegerToken tok(int i)
    {
        BigInteger md5_max = new BigInteger("2").pow(127);
        BigInteger bint = md5_max.divide(TOKEN_SCALE).multiply(new BigInteger(""+i));
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
        assertEquals(new Range(tok(7), tok(0)), mt.get(tok(0)));
        assertEquals(new Range(tok(0), tok(4)), mt.get(tok(3)));
        assertEquals(new Range(tok(0), tok(4)), mt.get(tok(4)));
        assertEquals(new Range(tok(4), tok(6)), mt.get(tok(6)));
        assertEquals(new Range(tok(6), tok(7)), mt.get(tok(7)));

        // check depths
        assertEquals((byte)1, mt.get(tok(4)).depth);
        assertEquals((byte)2, mt.get(tok(6)).depth);
        assertEquals((byte)3, mt.get(tok(7)).depth);
        assertEquals((byte)3, mt.get(tok(0)).depth);

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
        mt = new MerkleTree(partitioner, (byte)2, Integer.MAX_VALUE);

        assertTrue(mt.split(tok(4)));
        assertTrue(mt.split(tok(2)));
        assertEquals(3, mt.size());
        
        // should fail to split below hashdepth
        assertFalse(mt.split(tok(1)));
        assertEquals(3, mt.size());
        assertEquals(new Range(tok(4), tok(0)), mt.get(tok(0)));
        assertEquals(new Range(tok(0), tok(2)), mt.get(tok(2)));
        assertEquals(new Range(tok(2), tok(4)), mt.get(tok(4)));
    }

    @Test
    public void testSplitLimitSize()
    {
        mt = new MerkleTree(partitioner, RECOMMENDED_DEPTH, 2);

        assertTrue(mt.split(tok(4)));
        assertEquals(2, mt.size());
        
        // should fail to split above maxsize
        assertFalse(mt.split(tok(2)));
        assertEquals(2, mt.size());
        assertEquals(new Range(tok(4), tok(0)), mt.get(tok(0)));
        assertEquals(new Range(tok(0), tok(4)), mt.get(tok(4)));
    }

    @Test
    public void testCompact()
    {
        // (zero, one], (one,two], ... (seven, zero]
        mt.split(tok(4));
        mt.split(tok(2)); 
        mt.split(tok(6));
        mt.split(tok(1));
        mt.split(tok(3));
        mt.split(tok(5));
        mt.split(tok(7));

        // compact (zero,two] and then (four,six]
        mt.compact(tok(1));
        mt.compact(tok(5));
        assertEquals(6, mt.size());
        assertEquals(new Range(tok(0), tok(2)), mt.get(tok(2)));
        assertEquals(new Range(tok(2), tok(3)), mt.get(tok(3)));
        assertEquals(new Range(tok(3), tok(4)), mt.get(tok(4)));
        assertEquals(new Range(tok(4), tok(6)), mt.get(tok(5)));
        assertEquals(new Range(tok(6), tok(7)), mt.get(tok(7)));
        assertEquals(new Range(tok(7), tok(0)), mt.get(tok(0)));
        // compacted ranges should be at depth 2, and the rest at 3
        for (int i : new int[]{2,6}){ assertEquals((byte)2, mt.get(tok(i)).depth); }
        for (int i : new int[]{3,4,7,0}){ assertEquals((byte)3, mt.get(tok(i)).depth); }

        // compact (two,four] and then (six,zero]
        mt.compact(tok(3));
        mt.compact(tok(7));
        assertEquals(4, mt.size());
        assertEquals(new Range(tok(0), tok(2)), mt.get(tok(2)));
        assertEquals(new Range(tok(2), tok(4)), mt.get(tok(4)));
        assertEquals(new Range(tok(4), tok(6)), mt.get(tok(5)));
        assertEquals(new Range(tok(6), tok(0)), mt.get(tok(0)));
        for (int i : new int[]{2,4,5,0}){ assertEquals((byte)2, mt.get(tok(i)).depth); }

        // compact (zero,four]
        mt.compact(tok(2));
        assertEquals(3, mt.size());
        assertEquals(new Range(tok(0), tok(4)), mt.get(tok(2)));
        assertEquals(new Range(tok(4), tok(6)), mt.get(tok(6)));
        assertEquals(new Range(tok(6), tok(0)), mt.get(tok(0)));

        // compact (four, zero]
        mt.compact(tok(6));
        assertEquals(2, mt.size());
        assertEquals(new Range(tok(0), tok(4)), mt.get(tok(2)));
        assertEquals(new Range(tok(4), tok(0)), mt.get(tok(6)));
        assertEquals((byte)1, mt.get(tok(2)).depth);
        assertEquals((byte)1, mt.get(tok(6)).depth);

        // compact (zero, zero] (the root)
        mt.compact(tok(4));
        assertEquals(1, mt.size());
        assertEquals(new Range(tok(0), tok(0)), mt.get(tok(0)));
        assertEquals((byte)0, mt.get(tok(0)).depth);
    }

    @Test
    public void testCompactHash()
    {
        byte[] val = DUMMY;
        byte[] valXval = hashed(val, 1, 1);

        // (zero, four], (four,zero]
        mt.split(tok(4));

        // validate both ranges
        mt.get(tok(4)).hash(val);
        mt.get(tok(0)).hash(val);

        // compact (zero, eight]
        mt.compact(tok(4));
        assertHashEquals(valXval, mt.get(tok(0)).hash());
    }

    @Test
    public void testInvalids()
    {
        Iterator<TreeRange> ranges;
        
        // (zero, zero]
        ranges = mt.invalids(new Range(tok(0), tok(0)));
        assertEquals(new Range(tok(0), tok(0)), ranges.next());
        assertFalse(ranges.hasNext());

        // all invalid
        mt.split(tok(4));
        mt.split(tok(2));
        mt.split(tok(6));
        mt.split(tok(3));
        mt.split(tok(5));
        ranges = mt.invalids(new Range(tok(0), tok(0)));
        assertEquals(new Range(tok(0), tok(2)), ranges.next());
        assertEquals(new Range(tok(2), tok(3)), ranges.next());
        assertEquals(new Range(tok(3), tok(4)), ranges.next());
        assertEquals(new Range(tok(4), tok(5)), ranges.next());
        assertEquals(new Range(tok(5), tok(6)), ranges.next());
        assertEquals(new Range(tok(6), tok(0)), ranges.next());
        assertFalse(ranges.hasNext());
        
        // some invalid
        mt.get(tok(2)).hash("non-null!".getBytes());
        mt.get(tok(4)).hash("non-null!".getBytes());
        mt.get(tok(5)).hash("non-null!".getBytes());
        mt.get(tok(0)).hash("non-null!".getBytes());
        ranges = mt.invalids(new Range(tok(0), tok(0)));
        assertEquals(new Range(tok(2), tok(3)), ranges.next());
        assertEquals(new Range(tok(5), tok(6)), ranges.next());
        assertFalse(ranges.hasNext());
        
        // some invalid in left subrange
        ranges = mt.invalids(new Range(tok(0), tok(6)));
        assertEquals(new Range(tok(2), tok(3)), ranges.next());
        assertEquals(new Range(tok(5), tok(6)), ranges.next());
        assertFalse(ranges.hasNext());

        // some invalid in right subrange
        ranges = mt.invalids(new Range(tok(2), tok(0)));
        assertEquals(new Range(tok(2), tok(3)), ranges.next());
        assertEquals(new Range(tok(5), tok(6)), ranges.next());
        assertFalse(ranges.hasNext());
    }

    @Test
    public void testHashFull()
    {
        byte[] val = DUMMY;
        Range range = new Range(tok(0), tok(0));

        // (zero, zero]
        assertNull(mt.hash(range));
        
        // validate the range
        mt.get(tok(0)).hash(val);
        
        assertHashEquals(val, mt.hash(range));
    }

    @Test
    public void testHashPartial()
    {
        byte[] val = DUMMY;
        byte[] leftval = hashed(val, 1, 1);
        byte[] partialval = hashed(val, 1);
        Range left = new Range(tok(0), tok(4));
        Range partial = new Range(tok(2), tok(4));
        Range right = new Range(tok(4), tok(0));
        Range linvalid = new Range(tok(1), tok(4));
        Range rinvalid = new Range(tok(4), tok(6));

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
        mt.get(tok(0)).hash(val);
        
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
        Range full = new Range(tok(0), tok(0));
        Range lchild = new Range(tok(0), tok(4));
        Range rchild = new Range(tok(4), tok(0));
        Range invalid = new Range(tok(1), tok(0));

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
        mt.get(tok(0)).hash(val);
        
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
        Range childfull = new Range(tok(0), tok(4));
        Range full = new Range(tok(0), tok(0));
        Range invalid = new Range(tok(4), tok(0));

        mt = new MerkleTree(partitioner, RECOMMENDED_DEPTH, Integer.MAX_VALUE);
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
        mt.get(tok(0)).hash(val);

        assertHashEquals(fullval, mt.hash(full));
        assertHashEquals(childfullval, mt.hash(childfull));
        assertNull(mt.hash(invalid));
    }

    @Test
    public void testHashRandom()
    {
        int max = 1000000;
        TOKEN_SCALE = new BigInteger("" + max);

        mt = new MerkleTree(partitioner, RECOMMENDED_DEPTH, 32);
        Random random = new Random();
        while (true)
        {
            if (!mt.split(tok(random.nextInt(max))))
                break;
        }

        // validate the tree
        TreeRangeIterator ranges = mt.invalids(new Range(tok(0), tok(0)));
        for (TreeRange range : ranges)
            range.validate(new HIterator(/*empty*/ new int[0]));

        assert null != mt.hash(new Range(tok(0), tok(0))) :
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

        Range full = new Range(tok(0), tok(0));
        Iterator<TreeRange> ranges;
        MerkleTree mt2 = new MerkleTree(partitioner, RECOMMENDED_DEPTH, Integer.MAX_VALUE);

        mt.split(tok(8));
        mt.split(tok(4));
        mt.split(tok(12));
        mt.split(tok(6));
        mt.split(tok(10));
        
        ranges = mt.invalids(full);
        ranges.next().validate(new HIterator(2, 4)); // (0,4]: depth 2
        ranges.next().validate(new HIterator(6)); // (4,6]
        ranges.next().validate(new HIterator(8)); // (6,8]
        ranges.next().validate(new HIterator(/*empty*/ new int[0])); // (8,10]
        ranges.next().validate(new HIterator(12)); // (10,12]
        ranges.next().validate(new HIterator(14, 0)); // (12,0]: depth 2


        mt2.split(tok(8));
        mt2.split(tok(4));
        mt2.split(tok(12));
        mt2.split(tok(2));
        mt2.split(tok(10));
        mt2.split(tok(9));
        mt2.split(tok(11));

        ranges = mt2.invalids(full);
        ranges.next().validate(new HIterator(2)); // (0,2]
        ranges.next().validate(new HIterator(4)); // (2,4]
        ranges.next().validate(new HIterator(6, 8)); // (4,8]: depth 2
        ranges.next().validate(new HIterator(/*empty*/ new int[0])); // (8,9]
        ranges.next().validate(new HIterator(/*empty*/ new int[0])); // (9,10]
        ranges.next().validate(new HIterator(/*empty*/ new int[0])); // (10,11]: depth 4
        ranges.next().validate(new HIterator(12)); // (11,12]: depth 4
        ranges.next().validate(new HIterator(14, 0)); // (12,0]: depth 2

        byte[] mthash = mt.hash(full);
        byte[] mt2hash = mt2.hash(full);
        assertHashEquals("Tree hashes did not match: " + mt + " && " + mt2, mthash, mt2hash);
    }

    @Test
    public void testSerialization() throws Exception
    {
        Range full = new Range(tok(0), tok(0));
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oout = new ObjectOutputStream(bout);
    
        // populate and validate the tree
        mt.maxsize(256);
        mt.init();
        for (TreeRange range : mt.invalids(full))
            range.validate(new HIterator(range.right()));

        byte[] initialhash = mt.hash(full);
        oout.writeObject(mt);
        oout.close();

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        ObjectInputStream oin = new ObjectInputStream(bin);
        MerkleTree restored = (MerkleTree)oin.readObject();
    
        // restore partitioner after serialization
        restored.partitioner(partitioner);

        assertHashEquals(initialhash, restored.hash(full));
    }

    @Test
    public void testDifference()
    {
        Range full = new Range(tok(0), tok(0));
        int maxsize = 16;
        mt.maxsize(maxsize);
        MerkleTree mt2 = new MerkleTree(partitioner, RECOMMENDED_DEPTH, maxsize);
        mt.init();
        mt2.init();

        TreeRange leftmost = null;
        TreeRange middle = null;
        TreeRange rightmost = null;

        // compact the leftmost, and split the rightmost
        Iterator<TreeRange> ranges = mt.invalids(full);
        leftmost = ranges.next();
        rightmost = null;
        while (ranges.hasNext())
            rightmost = ranges.next();
        mt.compact(leftmost.right());
        leftmost = mt.get(leftmost.right()); // leftmost is now a larger range
        mt.split(rightmost.right());
        
        // set the hash for the left neighbor of rightmost
        middle = mt.get(rightmost.left());
        middle.hash("arbitrary!".getBytes());
        byte depth = middle.depth;

        // add dummy hashes to the rest of both trees
        for (TreeRange range : mt.invalids(full))
            range.validate(new HIterator(range.right()));
        for (TreeRange range : mt2.invalids(full))
            range.validate(new HIterator(range.right()));
        
        // trees should disagree for leftmost, (middle.left, rightmost.right]
        List<TreeRange> diffs = MerkleTree.difference(mt, mt2);
        assertEquals(diffs + " contains wrong number of differences:", 2, diffs.size());
        assertTrue(diffs.contains(leftmost));
        assertTrue(diffs.contains(new Range(middle.left(), rightmost.right())));
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
            while (dstack.peek() == depth)
            {
                // consume the stack
                hash = Hashable.binaryHash(hstack.pop(), hash);
                depth = dstack.pop()-1;
            }
            dstack.push(depth);
            hstack.push(hash);
        }
        assert hstack.size() == 1;
        return hstack.pop();
    }

    static class HIterator extends AbstractIterator<RowHash> implements PeekingIterator<RowHash>
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
    
        @Override
        public RowHash computeNext()
        {
            if (tokens.hasNext())
                return new RowHash(tokens.next(), DUMMY);
            return endOfData();
        }
    }
}
