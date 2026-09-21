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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;

/**
 * A tracked GROUP BY range read whose leading partitions reconciliation removes.
 * <p>
 * A group is as many rows as it likes, so the two counts a GROUP BY read keeps are not interchangeable: the limit
 * counts groups and the read can have gone through far more rows than that to find them. The budget a short read
 * follow up is given is a number of rows, and taking it as the limit minus the rows counted rather than minus the
 * groups counted asks for a negative number of them, which stops the follow up read before its first row exactly as
 * zero would. The answer then stays short by however many groups reconciliation removed from the front of the range,
 * with the rows to fill them sitting unread in the rest of it.
 * <p>
 * This is its own class, and keeps its own cluster, for two reasons. Every case leaves its keyspaces behind for as
 * long as the cluster is up, so how many cases one class holds is bounded by the heap of the one JVM that runs it.
 * And the fixture is built on the whole ring being scanned as one range, which it is only where every replica plan
 * on the ring is the same one: a plan that is full for one range and transient for the next cannot be merged with it,
 * so under transient replication the read the case is about would be three reads of a third of the data.
 */
public class TrackedGroupByRangeReadTest extends TestBaseImpl
{
    private static final int REPLICAS = 3;

    /** The one value of {@code pk0} every partition of the fixture shares; {@code pk1} is what distinguishes them. */
    private static final int PK0 = 1;

    /** How many partitions the fixture writes, which is more than the read under test can return. */
    private static final int PARTITIONS = 6;

    /** The LIMIT the read under test runs at, counted in groups. */
    private static final int GROUPS = 3;

    /**
     * How many partitions the delete reconciliation delivers removes, counted from the front of the scan. It has to
     * be more than one: the scan materializes one partition per group it is allowed plus the one whose arrival tells
     * it the last group is complete, so removing a single leading partition would leave the limit exactly filled and
     * the read would not be short at all.
     */
    private static final int REMOVED = 2;

    /**
     * Rows in each of the partitions the scan materializes, which has to be more than {@link #GROUPS} so that the rows
     * the scan goes through to find the groups it has outnumber the groups it is allowed. That is what makes the limit
     * minus the rows counted negative rather than merely small, and it is the whole difference between the two units:
     * of a limit of three groups, eight rows in and one group closed, what is left is two groups or minus five rows.
     */
    private static final int ROWS_PER_SCANNED_PARTITION = 4;

    private static final String TABLE = "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, PRIMARY KEY ((pk0, pk1), ck))";

    /** The read under test. One group per partition, off a range scan, three of them asked for. */
    private static final String SELECT = "SELECT pk0, pk1, count(v) FROM %s.tbl GROUP BY pk0, pk1 LIMIT " + GROUPS;

    /**
     * The same query with no limit on it, which is how the expected answer is obtained rather than written down. With
     * no limit there is no group budget to come up short against - the pager's internal page size becomes the group
     * limit and no range read reaches it - so the untracked keyspace this is asked of answers it off the merge of its
     * three replicas alone, and the first {@link #GROUPS} rows of that answer are what the read under test has to
     * return.
     */
    private static final String ORACLE_SELECT = "SELECT pk0, pk1, count(v) FROM %s.tbl GROUP BY pk0, pk1";

    /** The partitions each node holds of its own, which is what says the replicas are still divergent at read time. */
    private static final String MATERIALIZED_PARTITIONS = "SELECT DISTINCT pk0, pk1 FROM %s.tbl";

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = Cluster.build()
                         .withNodes(REPLICAS)
                         // this case is built on node 1 still holding partitions the deletes removed when the read
                         // under test runs, so that removing them is something the read has to reconcile; background
                         // reconciliation converges the replicas within a few seconds and would heal that first
                         .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                               .set("mutation_tracking.background_reconciliation_enabled", false))
                         .start();
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testGroupByRangeReadWhoseLeadingPartitionsReconciliationRemoved()
    {
        List<String> keys = partitionKeysInTokenOrder();

        String tracked = createKeyspace("group_by_leading_partitions_removed", true);
        String untracked = createKeyspace("group_by_leading_partitions_removed_oracle", false);
        for (String keyspace : Arrays.asList(tracked, untracked))
            write(keyspace, keys);

        Object[][] wholeAnswer = coordinatorRead(untracked, ORACLE_SELECT);
        Assert.assertEquals("The oracle should hold one group per partition the deletes did not remove",
                            PARTITIONS - REMOVED, wholeAnswer.length);
        Object[][] expected = Arrays.copyOf(wholeAnswer, GROUPS);

        // The unstressed case checks. Node 1 is the data replica of this read, and deterministically so: the read
        // below coordinates on node 1, TrackedRead.start prefers the local replica whenever it is a full one, and at
        // RF=3 on three nodes every node is a full replica of every range. It never received the deletes, so the
        // partitions the answer has to be missing are ones it still holds, and only reconciliation can take them out.
        Assert.assertEquals("Not stressed: the data replica does not hold the partitions the deletes remove",
                            PARTITIONS, nodeLocal(tracked, 1, MATERIALIZED_PARTITIONS).length);
        Assert.assertEquals("The deletes did not land on node 2, so there is nothing to reconcile",
                            PARTITIONS - REMOVED, nodeLocal(tracked, 2, MATERIALIZED_PARTITIONS).length);

        long followUpsBefore = shortReadProtectionRequests(tracked, 1);
        assertRows(coordinatorRead(tracked, SELECT), expected);
        Assert.assertTrue("Not stressed: the read filled its limit without a follow up read",
                          shortReadProtectionRequests(tracked, 1) > followUpsBefore);
    }

    /**
     * The {@code pk1} values in the order a range scan visits them, which is token order and has nothing to do with
     * their own. Which partition the fixture treats as leading is decided from this rather than from a ring the test
     * would have to write down, and the token is read out of the partitioner rather than computed here so that the
     * order cannot drift from the one the scan actually takes.
     */
    private static List<String> partitionKeysInTokenOrder()
    {
        List<String> keys = new ArrayList<>();
        Map<String, Long> tokens = new HashMap<>();
        for (int i = 0; i < PARTITIONS; i++)
        {
            String pk1 = String.valueOf((char) ('a' + i));
            keys.add(pk1);
            tokens.put(pk1, tokenOf(pk1));
        }
        Assert.assertEquals("Two of the fixture's partitions share a token", PARTITIONS, tokens.values().stream().distinct().count());
        keys.sort(Comparator.comparingLong(tokens::get));
        return keys;
    }

    private static long tokenOf(String pk1)
    {
        return cluster.get(1).callOnInstance(() -> DatabaseDescriptor.getPartitioner()
                                                                    .getToken(CompositeType.build(ByteBufferAccessor.instance,
                                                                                                  Int32Type.instance.decompose(PK0),
                                                                                                  UTF8Type.instance.decompose(pk1)))
                                                                    .getLongValue());
    }

    private static String createKeyspace(String keyspace, boolean tracked)
    {
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + REPLICAS + "}"
                                          + (tracked ? " AND replication_type='tracked'" : ""), keyspace));
        cluster.schemaChange(withKeyspace(TABLE, keyspace));
        return keyspace;
    }

    /**
     * {@link #ROWS_PER_SCANNED_PARTITION} rows in each partition the scan materializes and one in each partition past
     * it, so that the follow up read's budget of the groups still missing is a budget of that many whole groups, then
     * a partition delete for the leading {@link #REMOVED} of them applied on node 2 alone.
     * <p>
     * A write prefixed with the coordinator is applied on every replica and one applied with {@code executeInternal}
     * lands on that node alone, which is what leaves the replicas divergent. Neither escapes mutation tracking:
     * {@code Keyspace.applyInternalTracked} sits below the coordinator, so the delete is journaled on node 2 as
     * unreconciled and is available to the read that has to reconcile it.
     */
    private static void write(String keyspace, List<String> keys)
    {
        for (int i = 0; i < keys.size(); i++)
        {
            int rows = i <= GROUPS ? ROWS_PER_SCANNED_PARTITION : 1;
            for (int ck = 0; ck < rows; ck++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (" + PK0 + ", '" + keys.get(i) + "', " + ck + ", " + ck + ") USING TIMESTAMP 10", keyspace),
                                               ConsistencyLevel.ALL);
        }

        for (int i = 0; i < REMOVED; i++)
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 20 WHERE pk0 = " + PK0 + " AND pk1 = '" + keys.get(i) + "'", keyspace));
    }

    private static Object[][] coordinatorRead(String keyspace, String select)
    {
        return cluster.coordinator(1).execute(withKeyspace(select, keyspace), ConsistencyLevel.ALL);
    }

    /** What one node answers on its own, off its own memtables and sstables, reconciling nothing. */
    private static Object[][] nodeLocal(String keyspace, int node, String select)
    {
        return cluster.get(node).executeInternal(withKeyspace(select, keyspace));
    }

    /** How many follow up reads the range reads of this table have asked for, which the case has to be one of. */
    private static long shortReadProtectionRequests(String keyspace, int node)
    {
        return cluster.get(node).callOnInstance(() -> Keyspace.open(keyspace)
                                                             .getColumnFamilyStore("tbl")
                                                             .metric
                                                             .shortReadProtectionRequests
                                                             .getCount());
    }

    private static String withKeyspace(String replaceIn, String keyspace)
    {
        return String.format(replaceIn, keyspace);
    }
}
