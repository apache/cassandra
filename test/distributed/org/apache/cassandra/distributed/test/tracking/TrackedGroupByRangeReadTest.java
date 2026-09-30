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
 * Under GROUP BY, {@code ExtendingCompletedRead.toQuery} must ask for the groups still missing, not
 * {@code count() - rowsCounted()}, which is negative with this data. Not in {@link TrackedRangeReadTestBase}: its
 * replication_factor '3/1' runs read each node's range separately, and this test needs one read of the whole ring.
 */
public class TrackedGroupByRangeReadTest extends TestBaseImpl
{
    private static final int REPLICAS = 3;

    private static final int PK0 = 1;

    private static final int PARTITIONS = 6;

    private static final int GROUPS = 3;

    /**
     * Two, because the data replica reads {@code GROUPS + 1} partitions (one more to find the end of the last group),
     * so deleting one would still leave {@link #GROUPS} groups.
     */
    private static final int REMOVED = 2;

    /** More than {@link #GROUPS}, so that {@code count() - rowsCounted()} is negative. */
    private static final int ROWS_PER_SCANNED_PARTITION = 4;

    private static final String TABLE = "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, PRIMARY KEY ((pk0, pk1), ck))";

    private static final String SELECT = "SELECT pk0, pk1, count(v) FROM %s.tbl GROUP BY pk0, pk1 LIMIT " + GROUPS;

    private static final String ORACLE_SELECT = "SELECT pk0, pk1, count(v) FROM %s.tbl GROUP BY pk0, pk1";

    private static final String MATERIALIZED_PARTITIONS = "SELECT DISTINCT pk0, pk1 FROM %s.tbl";

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = Cluster.build()
                         .withNodes(REPLICAS)
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

        // Node 1 is the data replica: the read coordinates on node 1 and TrackedRead.start prefers a full local
        // replica.
        Assert.assertEquals("Not stressed: the data replica does not hold the partitions the deletes remove",
                            PARTITIONS, nodeLocal(tracked, 1, MATERIALIZED_PARTITIONS).length);
        Assert.assertEquals("The deletes did not land on node 2, so there is nothing to reconcile",
                            PARTITIONS - REMOVED, nodeLocal(tracked, 2, MATERIALIZED_PARTITIONS).length);

        long followUpsBefore = shortReadProtectionRequests(tracked, 1);
        assertRows(coordinatorRead(tracked, SELECT), expected);
        Assert.assertTrue("Not stressed: the read filled its limit without a follow up read",
                          shortReadProtectionRequests(tracked, 1) > followUpsBefore);
    }

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
     * On a tracked keyspace {@code executeInternal} goes through {@code Keyspace.applyInternalTracked}, so the deletes
     * are tracked and exist on node 2 only.
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

    private static Object[][] nodeLocal(String keyspace, int node, String select)
    {
        return cluster.get(node).executeInternal(withKeyspace(select, keyspace));
    }

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
