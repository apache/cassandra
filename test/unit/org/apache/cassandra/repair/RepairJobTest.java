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

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RepairJobTest
{
    private static final IPartitioner PARTITIONER = ByteOrderedPartitioner.instance;

    static InetAddressAndPort addr1;
    static InetAddressAndPort addr2;
    static InetAddressAndPort addr3;
    static InetAddressAndPort addr4;
    static InetAddressAndPort addr5;

    static Range<Token> range1 = range(0, 1);
    static Range<Token> range2 = range(2, 3);
    static Range<Token> range3 = range(4, 5);
    static RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), "ks", "cf", Arrays.asList());

    @After
    public void reset()
    {
        FBUtilities.reset();
        DatabaseDescriptor.setBroadcastAddress(addr1.address);
    }

    static
    {
        try
        {
            addr1 = InetAddressAndPort.getByName("127.0.0.1");
            addr2 = InetAddressAndPort.getByName("127.0.0.2");
            addr3 = InetAddressAndPort.getByName("127.0.0.3");
            addr4 = InetAddressAndPort.getByName("127.0.0.4");
            addr5 = InetAddressAndPort.getByName("127.0.0.5");
            DatabaseDescriptor.setBroadcastAddress(addr1.address);
        }
        catch (UnknownHostException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateStandardSyncTasks()
    {
        testCreateStandardSyncTasks(false);
    }

    @Test
    public void testCreateStandardSyncTasksPullRepair()
    {
        testCreateStandardSyncTasks(true);
    }

    public static void testCreateStandardSyncTasks(boolean pullRepair)
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "same",      range2, "same", range3, "same"),
                                                         treeResponse(addr2, range1, "different", range2, "same", range3, "different"),
                                                         treeResponse(addr3, range1, "same",      range2, "same", range3, "same"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    noTransient(), // transient
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        Assert.assertEquals(2, tasks.size());

        SyncTask task = tasks.get(pair(addr1, addr2));
        Assert.assertTrue(task.isLocal());
        Assert.assertTrue(((LocalSyncTask) task).requestRanges);
        Assert.assertEquals(!pullRepair, ((LocalSyncTask) task).transferRanges);
        Assert.assertEquals(Arrays.asList(range1, range3), task.rangesToSync);

        task = tasks.get(pair(addr2, addr3));
        Assert.assertFalse(task.isLocal());
        Assert.assertTrue(task instanceof SymmetricRemoteSyncTask);
        Assert.assertEquals(Arrays.asList(range1, range3), task.rangesToSync);

        Assert.assertNull(tasks.get(pair(addr1, addr3)));
    }

    @Test
    public void testStandardSyncTransient()
    {
        // Do not stream towards transient nodes
        testStandardSyncTransient(true);
        testStandardSyncTransient(false);
    }

    public void testStandardSyncTransient(boolean pullRepair)
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "same", range2, "same", range3, "same"),
                                                         treeResponse(addr2, range1, "different", range2, "same", range3, "different"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    transientPredicate(addr2),
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        Assert.assertEquals(1, tasks.size());

        SyncTask task = tasks.get(pair(addr1, addr2));
        Assert.assertTrue(task.isLocal());
        Assert.assertTrue(((LocalSyncTask) task).requestRanges);
        Assert.assertFalse(((LocalSyncTask) task).transferRanges);
        Assert.assertEquals(Arrays.asList(range1, range3), task.rangesToSync);
    }

    @Test
    public void testStandardSyncLocalTransient()
    {
        // Do not stream towards transient nodes
        testStandardSyncLocalTransient(true);
        testStandardSyncLocalTransient(false);
    }

    public void testStandardSyncLocalTransient(boolean pullRepair)
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "same", range2, "same", range3, "same"),
                                                         treeResponse(addr2, range1, "different", range2, "same", range3, "different"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    transientPredicate(addr1),
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        if (pullRepair)
        {
            Assert.assertTrue(tasks.isEmpty());
            return;
        }

        Assert.assertEquals(1, tasks.size());

        SyncTask task = tasks.get(pair(addr1, addr2));
        Assert.assertTrue(task.isLocal());
        Assert.assertFalse(((LocalSyncTask) task).requestRanges);
        Assert.assertTrue(((LocalSyncTask) task).transferRanges);
        Assert.assertEquals(Arrays.asList(range1, range3), task.rangesToSync);
    }

    @Test
    public void testEmptyDifference()
    {
        // one of the nodes is a local coordinator
        testEmptyDifference(addr1, noTransient(), true);
        testEmptyDifference(addr1, noTransient(), false);
        testEmptyDifference(addr2, noTransient(), true);
        testEmptyDifference(addr2, noTransient(), false);
        testEmptyDifference(addr1, transientPredicate(addr1), true);
        testEmptyDifference(addr2, transientPredicate(addr1), true);
        testEmptyDifference(addr1, transientPredicate(addr1), false);
        testEmptyDifference(addr2, transientPredicate(addr1), false);
        testEmptyDifference(addr1, transientPredicate(addr2), true);
        testEmptyDifference(addr2, transientPredicate(addr2), true);
        testEmptyDifference(addr1, transientPredicate(addr2), false);
        testEmptyDifference(addr2, transientPredicate(addr2), false);

        // nonlocal coordinator
        testEmptyDifference(addr3, noTransient(), true);
        testEmptyDifference(addr3, noTransient(), false);
        testEmptyDifference(addr3, noTransient(), true);
        testEmptyDifference(addr3, noTransient(), false);
        testEmptyDifference(addr3, transientPredicate(addr1), true);
        testEmptyDifference(addr3, transientPredicate(addr1), true);
        testEmptyDifference(addr3, transientPredicate(addr1), false);
        testEmptyDifference(addr3, transientPredicate(addr1), false);
        testEmptyDifference(addr3, transientPredicate(addr2), true);
        testEmptyDifference(addr3, transientPredicate(addr2), true);
        testEmptyDifference(addr3, transientPredicate(addr2), false);
        testEmptyDifference(addr3, transientPredicate(addr2), false);
    }

    public void testEmptyDifference(InetAddressAndPort local, Predicate<InetAddressAndPort> isTransient, boolean pullRepair)
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "same", range2, "same", range3, "same"),
                                                         treeResponse(addr2, range1, "same", range2, "same", range3, "same"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    local, // local
                                                                                    isTransient,
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        Assert.assertTrue(tasks.isEmpty());
    }

    @Test
    public void testCreateStandardSyncTasksAllDifferent()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "one",   range2, "one",   range3, "one"),
                                                         treeResponse(addr2, range1, "two",   range2, "two",   range3, "two"),
                                                         treeResponse(addr3, range1, "three", range2, "three", range3, "three"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    ep -> ep.equals(addr3), // transient
                                                                                    false,
                                                                                    true,
                                                                                    PreviewKind.ALL));

        Assert.assertEquals(3, tasks.size());
        SyncTask task = tasks.get(pair(addr1, addr2));
        Assert.assertTrue(task.isLocal());
        Assert.assertEquals(Arrays.asList(range1, range2, range3), task.rangesToSync);

        task = tasks.get(pair(addr2, addr3));
        Assert.assertFalse(task.isLocal());
        Assert.assertEquals(Arrays.asList(range1, range2, range3), task.rangesToSync);

        task = tasks.get(pair(addr1, addr3));
        Assert.assertTrue(task.isLocal());
        Assert.assertEquals(Arrays.asList(range1, range2, range3), task.rangesToSync);
    }

    @Test
    public void testCreate5NodeStandardSyncTasksWithTransient()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "one",   range2, "one",   range3, "one"),
                                                         treeResponse(addr2, range1, "two",   range2, "two",   range3, "two"),
                                                         treeResponse(addr3, range1, "three", range2, "three", range3, "three"),
                                                         treeResponse(addr4, range1, "four",  range2, "four",  range3, "four"),
                                                         treeResponse(addr5, range1, "five",  range2, "five",  range3, "five"));

        Predicate<InetAddressAndPort> isTransient = ep -> ep.equals(addr4) || ep.equals(addr5);
        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    isTransient, // transient
                                                                                    false,
                                                                                    true,
                                                                                    PreviewKind.ALL));

        SyncNodePair[] pairs = new SyncNodePair[] {pair(addr1, addr2),
                                                   pair(addr1, addr3),
                                                   pair(addr1, addr4),
                                                   pair(addr1, addr5),
                                                   pair(addr2, addr4),
                                                   pair(addr2, addr4),
                                                   pair(addr2, addr5),
                                                   pair(addr3, addr4),
                                                   pair(addr3, addr5)};

        for (SyncNodePair pair : pairs)
        {
            SyncTask task = tasks.get(pair);
            // Local only if addr1 is a coordinator
            assertEquals(task.isLocal(), pair.coordinator.equals(addr1));

            boolean isRemote = !pair.coordinator.equals(addr1) && !pair.peer.equals(addr1);
            boolean involvesTransient = isTransient.test(pair.coordinator) || isTransient.test(pair.peer);
            assertEquals(String.format("Coordinator: %s\n, Peer: %s\n",pair.coordinator, pair.peer),
                         isRemote && involvesTransient,
                         task instanceof AsymmetricRemoteSyncTask);

            // All ranges to be synchronised
            Assert.assertEquals(Arrays.asList(range1, range2, range3), task.rangesToSync);
        }
    }

    @Test
    public void testLocalSyncWithTransient()
    {
        try
        {
            for (InetAddressAndPort local : new InetAddressAndPort[]{ addr1, addr2, addr3 })
            {
                FBUtilities.reset();
                DatabaseDescriptor.setBroadcastAddress(local.address);
                testLocalSyncWithTransient(local, false);
            }
        }
        finally
        {
            FBUtilities.reset();
            DatabaseDescriptor.setBroadcastAddress(addr1.address);
        }
    }

    @Test
    public void testLocalSyncWithTransientPullRepair()
    {
        try
        {
            for (InetAddressAndPort local : new InetAddressAndPort[]{ addr1, addr2, addr3 })
            {
                FBUtilities.reset();
                DatabaseDescriptor.setBroadcastAddress(local.address);
                testLocalSyncWithTransient(local, true);
            }
        }
        finally
        {
            FBUtilities.reset();
            DatabaseDescriptor.setBroadcastAddress(addr1.address);
        }

    }

    public static void testLocalSyncWithTransient(InetAddressAndPort local, boolean pullRepair)
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "one",   range2, "one",   range3, "one"),
                                                         treeResponse(addr2, range1, "two",   range2, "two",   range3, "two"),
                                                         treeResponse(addr3, range1, "three", range2, "three", range3, "three"),
                                                         treeResponse(addr4, range1, "four",  range2, "four",  range3, "four"),
                                                         treeResponse(addr5, range1, "five",  range2, "five",  range3, "five"));

        Predicate<InetAddressAndPort> isTransient = ep -> ep.equals(addr4) || ep.equals(addr5);
        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    local, // local
                                                                                    isTransient, // transient
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        assertEquals(9, tasks.size());
        for (InetAddressAndPort addr : new InetAddressAndPort[]{ addr1, addr2, addr3 })
        {
            if (local.equals(addr))
                continue;

            LocalSyncTask task = (LocalSyncTask) tasks.get(pair(local, addr));
            assertTrue(task.requestRanges);
            assertEquals(!pullRepair, task.transferRanges);
        }

        LocalSyncTask task = (LocalSyncTask) tasks.get(pair(local, addr4));
        assertTrue(task.requestRanges);
        assertFalse(task.transferRanges);

        task = (LocalSyncTask) tasks.get(pair(local, addr5));
        assertTrue(task.requestRanges);
        assertFalse(task.transferRanges);
    }

    @Test
    public void testLocalAndRemoteTransient()
    {
        testLocalAndRemoteTransient(false);
    }

    @Test
    public void testLocalAndRemoteTransientPullRepair()
    {
        testLocalAndRemoteTransient(true);
    }

    private static void testLocalAndRemoteTransient(boolean pullRepair)
    {
        DatabaseDescriptor.setBroadcastAddress(addr4.address);
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "one", range2, "one", range3, "one"),
                                                         treeResponse(addr2, range1, "two", range2, "two", range3, "two"),
                                                         treeResponse(addr3, range1, "three", range2, "three", range3, "three"),
                                                         treeResponse(addr4, range1, "four", range2, "four", range3, "four"),
                                                         treeResponse(addr5, range1, "five", range2, "five", range3, "five"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    addr4, // local
                                                                                    ep -> ep.equals(addr4) || ep.equals(addr5), // transient
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        assertNull(tasks.get(pair(addr4, addr5)));
    }

    @Test
    public void testOptimizedCreateStandardSyncTasksAllDifferent()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "one",   range2, "one",   range3, "one"),
                                                         treeResponse(addr2, range1, "two",   range2, "two",   range3, "two"),
                                                         treeResponse(addr3, range1, "three", range2, "three", range3, "three"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createOptimisedSyncingSyncTasks(desc,
                                                                                            treeResponses,
                                                                                            addr1, // local
                                                                                            noTransient(),
                                                                                            addr -> "DC1",
                                                                                            false,
                                                                                            PreviewKind.ALL));

        for (SyncNodePair pair : new SyncNodePair[]{ pair(addr1, addr2),
                                                     pair(addr1, addr3),
                                                     pair(addr2, addr1),
                                                     pair(addr2, addr3),
                                                     pair(addr3, addr1),
                                                     pair(addr3, addr2) })
        {
            assertEquals(Arrays.asList(range1, range2, range3), tasks.get(pair).rangesToSync);
        }
    }

    @Test
    public void testOptimizedCreateStandardSyncTasks()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "one",   range2, "one"),
                                                         treeResponse(addr2, range1, "one",   range2, "two"),
                                                         treeResponse(addr3, range1, "three", range2, "two"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createOptimisedSyncingSyncTasks(desc,
                                                                                            treeResponses,
                                                                                            addr4, // local
                                                                                            noTransient(),
                                                                                            addr -> "DC1",
                                                                                            false,
                                                                                            PreviewKind.ALL));

        for (SyncTask task : tasks.values())
            assertTrue(task instanceof AsymmetricRemoteSyncTask);

        assertEquals(Arrays.asList(range1), tasks.get(pair(addr1, addr3)).rangesToSync);
        // addr1 can get range2 from either addr2 or addr3 but not from both
        assertStreamRangeFromEither(tasks, Arrays.asList(range2),
                                    addr1, addr2, addr3);

        assertEquals(Arrays.asList(range1), tasks.get(pair(addr2, addr3)).rangesToSync);
        assertEquals(Arrays.asList(range2), tasks.get(pair(addr2, addr1)).rangesToSync);

        // addr3 can get range1 from either addr1 or addr2 but not from both
        assertStreamRangeFromEither(tasks, Arrays.asList(range1),
                                    addr3, addr2, addr1);
        assertEquals(Arrays.asList(range2), tasks.get(pair(addr3, addr1)).rangesToSync);
    }

    @Test
    public void testOptimizedCreateStandardSyncTasksWithTransient()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "same",      range2, "same", range3, "same"),
                                                         treeResponse(addr2, range1, "different", range2, "same", range3, "different"),
                                                         treeResponse(addr3, range1, "same",      range2, "same", range3, "same"));

        RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), "ks", "cf", Arrays.asList());
        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createOptimisedSyncingSyncTasks(desc,
                                                                                            treeResponses,
                                                                                            addr1, // local
                                                                                            ep -> ep.equals(addr3),
                                                                                            addr -> "DC1",
                                                                                            false,
                                                                                            PreviewKind.ALL));

        assertEquals(3, tasks.size());
        SyncTask task = tasks.get(pair(addr1, addr2));
        assertTrue(task.isLocal());
        assertElementEquals(Arrays.asList(range1, range3), task.rangesToSync);
        assertTrue(((LocalSyncTask)task).requestRanges);
        assertFalse(((LocalSyncTask)task).transferRanges);

        assertStreamRangeFromEither(tasks, Arrays.asList(range3),
                                    addr2, addr1, addr3);

        task = tasks.get(pair(addr2, addr3));
        assertFalse(task.isLocal());
        assertElementEquals(Arrays.asList(range1), task.rangesToSync);
    }

    // Asserts that ranges are streamed from one of the nodes but not from the both
    public static void assertStreamRangeFromEither(Map<SyncNodePair, SyncTask> tasks, List<Range<Token>> ranges,
                                                   InetAddressAndPort target, InetAddressAndPort either, InetAddressAndPort or)
    {
        InetAddressAndPort streamsFrom;
        InetAddressAndPort doesntStreamFrom;
        if (tasks.containsKey(pair(target, either)))
        {
            streamsFrom = either;
            doesntStreamFrom = or;
        }
        else
        {
            doesntStreamFrom = either;
            streamsFrom = or;
        }

        SyncTask task = tasks.get(pair(target, streamsFrom));
        assertTrue(task instanceof AsymmetricRemoteSyncTask);
        assertElementEquals(ranges, task.rangesToSync);
        assertDoesntStreamRangeFrom(tasks, ranges, target, doesntStreamFrom);
    }

    public static void assertDoesntStreamRangeFrom(Map<SyncNodePair, SyncTask> tasks, List<Range<Token>> ranges,
                                                   InetAddressAndPort target, InetAddressAndPort source)
    {
        Set<Range<Token>> rangeSet = new HashSet<>(ranges);
        SyncTask task = tasks.get(pair(target, source));
        if (task == null)
            return; // Doesn't stream anything

        for (Range<Token> range : task.rangesToSync)
        {
            assertFalse(String.format("%s shouldn't stream %s from %s",
                                      target, range, source),
                        rangeSet.contains(range));
        }
    }

    public static <T> void assertElementEquals(Collection<T> col1, Collection<T> col2)
    {
        Set<T> set1 = new HashSet<>(col1);
        Set<T> set2 = new HashSet<>(col2);
        Set<T> difference = Sets.difference(set1, set2);
        assertTrue("Expected empty difference but got: " + difference.toString(),
                   difference.isEmpty());
    }

    public static Token tk(int i)
    {
        return PARTITIONER.getToken(ByteBufferUtil.bytes(i));
    }

    public static Range<Token> range(int from, int to)
    {
        return new Range<>(tk(from), tk(to));
    }

    public static TreeResponse treeResponse(InetAddressAndPort addr, Object... rangesAndHashes)
    {
        MerkleTrees trees = new MerkleTrees(PARTITIONER);
        for (int i = 0; i < rangesAndHashes.length; i += 2)
        {
            Range<Token> range = (Range<Token>) rangesAndHashes[i];
            String hash = (String) rangesAndHashes[i + 1];
            MerkleTree tree = trees.addMerkleTree(2, MerkleTree.RECOMMENDED_DEPTH, range);
            tree.get(range.left).hash(hash.getBytes());
        }

        return new TreeResponse(addr, trees);
    }

    public static SyncNodePair pair(InetAddressAndPort node1, InetAddressAndPort node2)
    {
        return new SyncNodePair(node1, node2);
    }

    public static Map<SyncNodePair, SyncTask> toMap(List<SyncTask> tasks)
    {
        Map<SyncNodePair, SyncTask> map = new HashMap();
        for (SyncTask task : tasks)
        {
            SyncTask oldTask = map.put(task.nodePair, task);
            Assert.assertNull(String.format("\nNode pair: %s\nOld task:  %s\nNew task:  %s\n",
                                            task.nodePair,
                                            oldTask,
                                            task),
                              oldTask);
        }
        return map;
    }

    public static Predicate<InetAddressAndPort> transientPredicate(InetAddressAndPort... transientNodes)
    {
        Set<InetAddressAndPort> set = new HashSet<>();
        for (InetAddressAndPort node : transientNodes)
            set.add(node);

        return set::contains;
    }

    public static Predicate<InetAddressAndPort> noTransient()
    {
        return node -> false;
    }
}
