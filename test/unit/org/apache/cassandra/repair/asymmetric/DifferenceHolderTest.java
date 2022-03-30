/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.repair.asymmetric;

import java.net.UnknownHostException;
import java.util.Iterator;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.db.Digest;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.TreeResponse;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.MerkleTreesTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DifferenceHolderTest
{
    private static byte[] digest(String string)
    {
        return Digest.forValidator()
                     .update(string.getBytes(), 0, string.getBytes().length)
                     .digest();
    }

    @Test
    public void testFromEmptyMerkleTrees() throws UnknownHostException
    {
        InetAddressAndPort a1 = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort a2 = InetAddressAndPort.getByName("127.0.0.2");

        MerkleTrees mts1 = new MerkleTrees(Murmur3Partitioner.instance);
        MerkleTrees mts2 = new MerkleTrees(Murmur3Partitioner.instance);
        mts1.init();
        mts2.init();

        TreeResponse tr1 = new TreeResponse(a1, mts1);
        TreeResponse tr2 = new TreeResponse(a2, mts2);

        DifferenceHolder dh = new DifferenceHolder(Lists.newArrayList(tr1, tr2));
        assertTrue(dh.get(a1).get(a2).isEmpty());
    }

    @Test
    public void testFromMismatchedMerkleTrees() throws UnknownHostException
    {
        IPartitioner partitioner = Murmur3Partitioner.instance;
        Range<Token> fullRange = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        int maxsize = 16;
        InetAddressAndPort a1 = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort a2 = InetAddressAndPort.getByName("127.0.0.2");
        // merkle tree building stolen from MerkleTreesTest:
        MerkleTrees mts1 = new MerkleTrees(partitioner);
        MerkleTrees mts2 = new MerkleTrees(partitioner);
        mts1.addMerkleTree(32, fullRange);
        mts2.addMerkleTree(32, fullRange);
        mts1.init();
        mts2.init();
        // add dummy hashes to both trees
        for (MerkleTree.TreeRange range : mts1.rangeIterator())
            range.addAll(new MerkleTreesTest.HIterator(range.right));
        for (MerkleTree.TreeRange range : mts2.rangeIterator())
            range.addAll(new MerkleTreesTest.HIterator(range.right));

        MerkleTree.TreeRange leftmost = null;
        MerkleTree.TreeRange middle = null;

        mts1.maxsize(fullRange, maxsize + 2); // give some room for splitting

        // split the leftmost
        Iterator<MerkleTree.TreeRange> ranges = mts1.rangeIterator();
        leftmost = ranges.next();
        mts1.split(leftmost.right);

        // set the hashes for the leaf of the created split
        middle = mts1.get(leftmost.right);
        middle.hash(digest("arbitrary!"));
        mts1.get(partitioner.midpoint(leftmost.left, leftmost.right)).hash(digest("even more arbitrary!"));

        TreeResponse tr1 = new TreeResponse(a1, mts1);
        TreeResponse tr2 = new TreeResponse(a2, mts2);

        DifferenceHolder dh = new DifferenceHolder(Lists.newArrayList(tr1, tr2));
        assertTrue(dh.get(a1).get(a2).size() == 1);
        assertTrue(dh.hasDifferenceBetween(a1, a2, fullRange));
        // only a1 is added as a key - see comment in dh.keyHosts()
        assertEquals(Sets.newHashSet(a1), dh.keyHosts());
    }
}
