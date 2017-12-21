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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.TreeResponse;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.MerkleTreesTest;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DifferenceHolderTest
{
    @Test
    public void testFromEmptyMerkleTrees() throws UnknownHostException
    {
        InetAddress a1 = InetAddress.getByName("127.0.0.1");
        InetAddress a2 = InetAddress.getByName("127.0.0.2");

        MerkleTrees mt1 = new MerkleTrees(Murmur3Partitioner.instance);
        MerkleTrees mt2 = new MerkleTrees(Murmur3Partitioner.instance);
        mt1.init();
        mt2.init();

        TreeResponse tr1 = new TreeResponse(a1, mt1);
        TreeResponse tr2 = new TreeResponse(a2, mt2);

        DifferenceHolder dh = new DifferenceHolder(Lists.newArrayList(tr1, tr2));
        assertTrue(dh.get(a1).get(a2).isEmpty());
    }

    @Test
    public void testFromMismatchedMerkleTrees() throws UnknownHostException
    {
        IPartitioner partitioner = Murmur3Partitioner.instance;
        Range<Token> fullRange = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        int maxsize = 16;
        InetAddress a1 = InetAddress.getByName("127.0.0.1");
        InetAddress a2 = InetAddress.getByName("127.0.0.2");
        // merkle tree building stolen from MerkleTreesTest:
        MerkleTrees mt1 = new MerkleTrees(partitioner);
        MerkleTrees mt2 = new MerkleTrees(partitioner);
        mt1.addMerkleTree(32, fullRange);
        mt2.addMerkleTree(32, fullRange);
        mt1.init();
        mt2.init();
        // add dummy hashes to both trees
        for (MerkleTree.TreeRange range : mt1.invalids())
            range.addAll(new MerkleTreesTest.HIterator(range.right));
        for (MerkleTree.TreeRange range : mt2.invalids())
            range.addAll(new MerkleTreesTest.HIterator(range.right));

        MerkleTree.TreeRange leftmost = null;
        MerkleTree.TreeRange middle = null;

        mt1.maxsize(fullRange, maxsize + 2); // give some room for splitting

        // split the leftmost
        Iterator<MerkleTree.TreeRange> ranges = mt1.invalids();
        leftmost = ranges.next();
        mt1.split(leftmost.right);

        // set the hashes for the leaf of the created split
        middle = mt1.get(leftmost.right);
        middle.hash("arbitrary!".getBytes());
        mt1.get(partitioner.midpoint(leftmost.left, leftmost.right)).hash("even more arbitrary!".getBytes());

        TreeResponse tr1 = new TreeResponse(a1, mt1);
        TreeResponse tr2 = new TreeResponse(a2, mt2);

        DifferenceHolder dh = new DifferenceHolder(Lists.newArrayList(tr1, tr2));
        assertTrue(dh.get(a1).get(a2).size() == 1);
        assertTrue(dh.hasDifferenceBetween(a1, a2, fullRange));
        // only a1 is added as a key - see comment in dh.keyHosts()
        assertEquals(Sets.newHashSet(a1), dh.keyHosts());
    }
}
