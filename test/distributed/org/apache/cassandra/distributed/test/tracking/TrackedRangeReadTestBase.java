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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * The cases are split across {@link TrackedRangeReadTest}, {@link TrackedLegacyIndexedRangeReadTest} and
 * {@link TrackedFilteredRangeReadCarryOverTest} because every case leaves behind keyspaces whose tables keep a
 * memtable region and table metrics while the cluster is up, so the heap bounds how many cases one class can hold.
 */
public abstract class TrackedRangeReadTestBase extends TestBaseImpl
{
    protected static final int REPLICAS = 3;

    public enum Mode
    {
        FULL("{'class': 'SimpleStrategy', 'replication_factor': 3}", false),
        WITNESSES("{'class': 'SimpleStrategy', 'replication_factor': '3/1'}", true);

        final String replication;
        final boolean transientReplicationEnabled;

        Mode(String replication, boolean transientReplicationEnabled)
        {
            this.replication = replication;
            this.transientReplicationEnabled = transientReplicationEnabled;
        }
    }

    @Parameterized.Parameter
    public Mode parameter;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> modes()
    {
        return Arrays.asList(new Object[]{ Mode.FULL }, new Object[]{ Mode.WITNESSES });
    }

    protected static Mode mode;

    protected static Cluster cluster;

    /**
     * One cluster per mode, because {@code transient_replication_enabled} is a yaml setting. The Parameterized runner
     * runs every case of one parameter before the next, so each cluster is built once.
     */
    @Before
    public void beforeEach() throws IOException
    {
        if (mode == parameter)
            return;

        closeCluster();
        /*
         * Each case leaves the replicas holding different data, so everything that could make them agree before the
         * read is disabled. The active reconciler's regular priority queue is paused because a write whose target
         * starts with '!' (see write) drops the mutation sent to one node, and TrackedWriteResponseHandler.onFailure
         * schedules a retry of it on that queue. A tracked read's own reconciliation uses the high priority queue,
         * which keeps running.
         */
        cluster = Cluster.build()
                         .withNodes(REPLICAS)
                         // one token per node, so cassandra.dtest.num_tokens cannot change the ranges that the keys
                         // passed to assertReadTogetherFromNode1 were chosen for
                         .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(REPLICAS, 1))
                         .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                               .set("hinted_handoff_enabled", false)
                                               .set("transient_replication_enabled", parameter.transientReplicationEnabled)
                                               .set("mutation_tracking.background_reconciliation_enabled", false))
                         .start();
        cluster.forEach(() -> MutationTrackingService.instance().pauseActiveReconcilerRegularPriority());
        mode = parameter;
    }

    @AfterClass
    public static void teardown()
    {
        closeCluster();
    }

    private static void closeCluster()
    {
        mode = null;
        if (cluster != null)
        {
            cluster.close();
            cluster = null;
        }
    }

    protected static void createTrackedKeyspace(String keyspace)
    {
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " + mode.replication + " AND replication_type='tracked'", keyspace));
    }

    protected static final int UNPAGED = 0;

    /**
     * @param probe given the tracked keyspace and the untracked keyspace's rows, run before the tracked read. It
     *              must read with node local executeInternal; a coordinated tracked read reconciles the replicas.
     */
    protected static String assertTrackedMatchesOracle(String name, String table, String[] writes, String select,
                                                       int pageSize, BiConsumer<String, Object[][]> probe)
    {
        String untracked = createKeyspace(name + "_oracle", table, false);
        write(untracked, writes);
        Object[][] expected = read(untracked, select, pageSize);

        String tracked = createKeyspace(name, table, true);
        write(tracked, writes);
        probe.accept(tracked, expected);

        assertRows(read(tracked, select, pageSize), expected);
        return tracked;
    }

    private static String createKeyspace(String keyspace, String table, boolean tracked)
    {
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = "
                                          + (tracked ? mode.replication + " AND replication_type='tracked'"
                                                     : Mode.FULL.replication), keyspace));
        for (String statement : table.split(";"))
            cluster.schemaChange(withKeyspace(statement, keyspace));
        SAIUtil.waitForIndexQueryable(cluster, keyspace);
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());
        return keyspace;
    }

    /**
     * Every write, node local ones included, goes through {@code Keyspace.applyInternalTracked}, which does not apply
     * to the table a mutation whose token is in none of the node's full local ranges. So under {@link Mode#WITNESSES}
     * a node local read does not see a write to a transient replica.
     */
    private static void write(String keyspace, String[] writes)
    {
        for (String write : writes)
        {
            int colon = write.indexOf(':');
            String target = write.substring(0, colon);
            String cql = withKeyspace(write.substring(colon + 1), keyspace);
            if (target.equals("*"))
                cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL);
            else if (target.charAt(0) == '!')
                writeMissingOn(Integer.parseInt(target.substring(1)), cql);
            else
                cluster.get(Integer.parseInt(target)).executeInternal(cql);
        }
    }

    private static void writeMissingOn(int missing, String cql)
    {
        AssertingLatch mutationDropped = new AssertingLatch("a MUTATION_REQ to node " + missing + " to be dropped");
        IMessageFilters.Filter dropped = cluster.filters().inbound().verbs(Verb.MUTATION_REQ.id).to(missing)
                                               .messagesMatching((from, to, message) -> {
                                                   mutationDropped.countDown();
                                                   return true;
                                               })
                                               .drop();
        try
        {
            cluster.coordinator(missing == 1 ? 2 : 1).execute(cql, ConsistencyLevel.QUORUM);
            // QUORUM can return before the mutation to `missing` arrives; removing the filter then would let it through
            mutationDropped.await();
        }
        finally
        {
            // off() rather than reset(), so a case that installs a filter of its own keeps it
            dropped.off();
        }
    }

    private static Object[][] read(String keyspace, String select, int pageSize)
    {
        String cql = withKeyspace(select, keyspace);
        if (pageSize == UNPAGED)
            return cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL);

        List<Object[]> rows = new ArrayList<>();
        Iterator<Object[]> paged = cluster.coordinator(1).executeWithPaging(cql, ConsistencyLevel.ALL, pageSize);
        while (paged.hasNext())
            rows.add(paged.next());
        return rows.toArray(new Object[0][]);
    }

    protected static Object[][] nodeLocal(String keyspace, int node, String select)
    {
        return cluster.get(node).executeInternal(withKeyspace(select, keyspace));
    }

    /** Only valid for tables keyed on {@code (pk0 int, pk1 text)}. */
    protected static Set<Integer> fullReplicasFor(String keyspace, int pk0, String pk1)
    {
        Set<Integer> full = new TreeSet<>();
        for (int node = 1; node <= REPLICAS; node++)
        {
            boolean isFull = cluster.get(node).callOnInstance(() -> {
                AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
                Token token = DatabaseDescriptor.getPartitioner()
                                                .getToken(CompositeType.build(ByteBufferAccessor.instance,
                                                                              Int32Type.instance.decompose(pk0),
                                                                              UTF8Type.instance.decompose(pk1)));
                for (Range<Token> range : strategy.getLocalRanges(ClusterMetadata.current()).onlyFull().ranges())
                    if (range.contains(token))
                        return true;
                return false;
            });
            if (isFull)
                full.add(node);
        }
        return full;
    }

    protected static Object[][] materializedOn(String keyspace, int node, Object[][] rows)
    {
        List<Object[]> held = new ArrayList<>();
        for (Object[] row : rows)
            if (fullReplicasFor(keyspace, (Integer) row[0], (String) row[1]).contains(node))
                held.add(row);
        return held.toArray(new Object[0][]);
    }

    /**
     * {@code ReplicaPlans.maybeMerge} does not merge adjacent ranges that a shared endpoint replicates differently, so
     * under {@code '3/1'} partitions with different full replicas are read separately.
     */
    protected static void assertReadTogetherFromNode1(String keyspace, Object[]... partitions)
    {
        Set<Integer> shared = null;
        for (Object[] partition : partitions)
        {
            String name = '(' + partition[0].toString() + ",'" + partition[1] + "')";
            Set<Integer> full = fullReplicasFor(keyspace, (Integer) partition[0], (String) partition[1]);
            Assert.assertTrue(name + " is witnessed by node 1, so node 1 neither holds it nor is the replica a read of it is answered from",
                              full.contains(1));
            if (shared == null)
                shared = full;
            else
                Assert.assertEquals(name + " is not replicated by the same nodes as the partitions before it, so it is in a range of its own and is read on its own",
                                    shared, full);
        }
    }

    /**
     * Node 1 is the data replica of every range it is full for: {@link #read} coordinates on node 1 and
     * TrackedRead.start prefers the local replica when it is full.
     */
    protected static void assertDataReplicaCannotAnswerAlone(String keyspace, String select, Object[][] oracle)
    {
        Assert.assertFalse("Not stressed: the data replica answers this query correctly on its own",
                           Arrays.deepEquals(nodeLocal(keyspace, 1, select), oracle));

        if (mode == Mode.WITNESSES)
            assertWitnessesMaterializedNothing(keyspace);
    }

    private static void assertWitnessesMaterializedNothing(String keyspace)
    {
        Set<List<Object>> anywhere = new HashSet<>();
        for (int node = 1; node <= REPLICAS; node++)
        {
            for (Object[] partition : nodeLocal(keyspace, node, "SELECT DISTINCT pk0, pk1 FROM %s.tbl"))
            {
                Assert.assertTrue("node " + node + " materialized (" + partition[0] + ",'" + partition[1] + "') and is only a witness of it",
                                  fullReplicasFor(keyspace, (Integer) partition[0], (String) partition[1]).contains(node));
                anywhere.add(Arrays.asList(partition));
            }
        }
        Assert.assertFalse("Not stressed: no node materialized any partition, so nothing is being witnessed",
                           anywhere.isEmpty());
    }

    protected static void assertEveryReplicaCanAnswerAlone(String keyspace, String select, Object[][] oracle)
    {
        boolean someNodeShort = false;
        for (int node = 1; node <= REPLICAS; node++)
        {
            Object[][] expected = materializedOn(keyspace, node, oracle);
            assertRows(nodeLocal(keyspace, node, select), expected);
            someNodeShort |= expected.length < oracle.length;
        }
        Assert.assertEquals(mode == Mode.FULL ? "A node is short under a mode where every node is a full replica"
                                              : "Not stressed: every replica holds the whole answer, so nothing is being witnessed",
                            mode == Mode.WITNESSES, someNodeShort);
    }

    /** (1,'b') is a static only partition that the index matches and the rest of {@code select} rejects. */
    protected static void staticOnlyPartitionDoesNotMatch(String name, String table, String select)
    {
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 10) USING TIMESTAMP 10",
            "*:UPDATE %s.tbl USING TIMESTAMP 11 SET s = 3 WHERE pk0 = 1 AND pk1 = 'b'"
        };
        assertTrackedMatchesOracle(name, table, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, select, oracle));
    }

    private static final String[] STATIC_ONLY_PARTITIONS =
    {
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 10) USING TIMESTAMP 10",
        "*:UPDATE %s.tbl USING TIMESTAMP 11 SET s = 3 WHERE pk0 = 1 AND pk1 = 'b'",
        "*:UPDATE %s.tbl USING TIMESTAMP 12 SET s = 3 WHERE pk0 = 1 AND pk1 = 'c'",
        "*:UPDATE %s.tbl USING TIMESTAMP 13 SET s = 3 WHERE pk0 = 1 AND pk1 = 'd'"
    };

    /** (1,'b'), (1,'c') and (1,'d') must not count toward the page size or LIMIT. */
    protected static void staticOnlyPartitionsAreDropped(String name, String table, String select, int pageSize)
    {
        assertTrackedMatchesOracle(name, table, STATIC_ONLY_PARTITIONS, select, pageSize,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, select, oracle));
    }

    /**
     * Node 1 lacks (1,'a') and gets it through reconciliation. (1,'a') sorts after the last key node 1's index scan
     * reached, and the read must not treat the partitions between the two as read.
     * <p>
     * Murmur3 orders the keys (1,'n'), (1,'u'), (1,'a'); all match the index on {@code v}. With a page size of one,
     * node 1's index scan stops after (1,'n'), leaving (1,'u') an index match it has not read. (1,'n') fails the
     * {@code w = 1} filter, so the page is not full and the read goes on to (1,'u').
     */
    protected static void indexedRangeReadHandedAKeyPastTheScannedRange(String name, String table)
    {
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'n', 1, 100, 0) USING TIMESTAMP 10",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'u', 1, 100, 1) USING TIMESTAMP 11",
            "!1:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'a', 1, 100, 1) USING TIMESTAMP 12"
        };
        String select = "SELECT pk0, pk1, ck, v, w FROM %s.tbl WHERE v = 100 AND w = 1 ALLOW FILTERING";
        assertTrackedMatchesOracle(name, table, writes, select, 1, (keyspace, oracle) -> {
            assertReadTogetherFromNode1(keyspace, row(1, "n"), row(1, "u"), row(1, "a"));
            assertDataReplicaCannotAnswerAlone(keyspace, select, oracle);
        });
    }

    public static String withKeyspace(String replaceIn, String keyspace)
    {
        return String.format(replaceIn, keyspace);
    }
}
