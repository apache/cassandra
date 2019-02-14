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

package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;

import static org.junit.Assert.assertEquals;

public class LocalSyncTaskTest extends SchemaLoader
{
    private static final IPartitioner partirioner = Murmur3Partitioner.instance;
    public static final String KEYSPACE1 = "DifferencerTest";
    public static final String CF_STANDARD = "Standard1";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));
    }

    /**
     * When there is no difference between two, LocalSyncTask should return stats with 0 difference.
     */
    @Test
    public void testNoDifference() throws Throwable
    {
        final InetAddress ep1 = InetAddress.getByName("127.0.0.1");
        final InetAddress ep2 = InetAddress.getByName("127.0.0.1");

        Range<Token> range = new Range<>(partirioner.getMinimumToken(), partirioner.getRandomToken());
        RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), KEYSPACE1, "Standard1", Arrays.asList(range));

        MerkleTrees tree1 = createInitialTree(desc);

        MerkleTrees tree2 = createInitialTree(desc);

        // difference the trees
        // note: we reuse the same endpoint which is bogus in theory but fine here
        TreeResponse r1 = new TreeResponse(ep1, tree1);
        TreeResponse r2 = new TreeResponse(ep2, tree2);
        LocalSyncTask task = new LocalSyncTask(desc, r1.endpoint, r2.endpoint,
                                               MerkleTrees.difference(r1.trees, r2.trees),
                                               ActiveRepairService.UNREPAIRED_SSTABLE, false);
        task.run();

        assertEquals(0, task.get().numberOfDifferences);
    }

    @Test
    public void testDifference() throws Throwable
    {
        Range<Token> range = new Range<>(partirioner.getMinimumToken(), partirioner.getRandomToken());
        UUID parentRepairSession = UUID.randomUUID();
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");

        ActiveRepairService.instance.registerParentRepairSession(parentRepairSession,  FBUtilities.getBroadcastAddress(), Arrays.asList(cfs), Arrays.asList(range), false, System.currentTimeMillis(), false);

        RepairJobDesc desc = new RepairJobDesc(parentRepairSession, UUID.randomUUID(), KEYSPACE1, "Standard1", Arrays.asList(range));

        MerkleTrees tree1 = createInitialTree(desc);

        MerkleTrees tree2 = createInitialTree(desc);

        // change a range in one of the trees
        Token token = partirioner.midpoint(range.left, range.right);
        tree1.invalidate(token);
        MerkleTree.TreeRange changed = tree1.get(token);
        changed.hash("non-empty hash!".getBytes());

        Set<Range<Token>> interesting = new HashSet<>();
        interesting.add(changed);

        // difference the trees
        // note: we reuse the same endpoint which is bogus in theory but fine here
        TreeResponse r1 = new TreeResponse(InetAddress.getByName("127.0.0.1"), tree1);
        TreeResponse r2 = new TreeResponse(InetAddress.getByName("127.0.0.2"), tree2);
        LocalSyncTask task = new LocalSyncTask(desc,  r1.endpoint, r2.endpoint,
                                               MerkleTrees.difference(r1.trees, r2.trees),
                                               ActiveRepairService.UNREPAIRED_SSTABLE, false);
        task.run();

        // ensure that the changed range was recorded
        assertEquals("Wrong differing ranges", interesting.size(), task.getCurrentStat().numberOfDifferences);
    }

    private MerkleTrees createInitialTree(RepairJobDesc desc)
    {
        MerkleTrees tree = new MerkleTrees(partirioner);
        tree.addMerkleTrees((int) Math.pow(2, 15), desc.ranges);
        tree.init();
        for (MerkleTree.TreeRange r : tree.invalids())
        {
            r.ensureHashInitialised();
        }
        return tree;
    }
}
