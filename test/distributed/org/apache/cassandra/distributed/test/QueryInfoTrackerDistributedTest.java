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

package org.apache.cassandra.distributed.test;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.service.QueryInfoTrackerTest;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class QueryInfoTrackerDistributedTest extends TestBaseImpl
{
    private static Cluster cluster;
    private final static String rfOneKs = "rfoneks";

    @BeforeClass
    public static void setupCluster() throws Throwable
    {
        cluster = init(Cluster.build().withNodes(3).start());

        cluster.schemaChange("CREATE KEYSPACE " + rfOneKs +
                             " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    }

    @AfterClass
    public static void close() throws Exception
    {
        cluster.close();
        cluster = null;
    }

    @Test
    public void testTrackingInDataResolverResolve()
    {
        ReadRepairTester tester = new ReadRepairTester(cluster, ReadRepairStrategy.BLOCKING, 1, false, false, false)
        {
            @Override
            ReadRepairTester self()
            {
                return this;
            }
        };

        String keyspace = tester.qualifiedTableName.split("\\.")[0];

        tester.createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        cluster.coordinator(1).execute("INSERT INTO " + tester.qualifiedTableName + " (pk, ck, v) VALUES (1, 1, 1)",
                                       ConsistencyLevel.QUORUM);

        tester.mutate(2, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 2)");

        cluster.get(tester.coordinator).runOnInstance(() -> {
            StorageProxy.instance.registerQueryTracker(new QueryInfoTrackerTest.TestQueryInfoTracker(keyspace));
        });

        tester.assertRowsDistributed("SELECT * FROM %s WHERE pk=1 AND ck=1",
                                     2,
                                     row(1, 1, 2));

        cluster.get(tester.coordinator).runOnInstance(() -> {
            QueryInfoTrackerTest.TestQueryInfoTracker tracker =
            (QueryInfoTrackerTest.TestQueryInfoTracker) StorageProxy.instance.queryTracker();

            Assert.assertEquals(1, tracker.reads.get());
            Assert.assertEquals(1, tracker.readPartitions.get());
            Assert.assertEquals(1, tracker.readRows.get());
            Assert.assertEquals(1, tracker.replicaPlans.get());
        });
    }

    @Test
    public void testTrackingInDigestResolverGetData()
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        cluster.get(1).runOnInstance(() -> {
            StorageProxy.instance.registerQueryTracker(new QueryInfoTrackerTest.TestQueryInfoTracker(KEYSPACE));
        });

        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)",
                                       ConsistencyLevel.QUORUM);
        assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                  ConsistencyLevel.QUORUM),
                   row(1, 1, 1));


        cluster.get(1).runOnInstance(() -> {
            QueryInfoTrackerTest.TestQueryInfoTracker tracker =
            (QueryInfoTrackerTest.TestQueryInfoTracker) StorageProxy.instance.queryTracker();

            Assert.assertEquals(1, tracker.reads.get());
            Assert.assertEquals(1, tracker.readPartitions.get());
            Assert.assertEquals(1, tracker.readRows.get());
            Assert.assertEquals(1, tracker.replicaPlans.get());
        });
    }

    @Test
    public void testTrackingReadsWithEndpointGrouping() throws Throwable
    {
        String table = rfOneKs + ".saiTbl";
        cluster.schemaChange("CREATE TABLE " + table + " (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT)");
        cluster.schemaChange("CREATE CUSTOM INDEX IF NOT EXISTS test_idx ON " + table + " (v1) USING 'StorageAttachedIndex'");

        int rowsCount = 1000;

        for (int i = 0; i < rowsCount; ++i)
        {
            cluster.coordinator(1).execute("INSERT INTO " + table + " (id1, v1, v2) VALUES (?, ?, ?);",
                                           ConsistencyLevel.QUORUM,
                                           String.valueOf(i),
                                           i,
                                           String.valueOf(i));
        }


        cluster.get(1).runOnInstance(() -> {
            StorageProxy.instance.registerQueryTracker(new QueryInfoTrackerTest.TestQueryInfoTracker(rfOneKs));
        });

        cluster.coordinator(1).execute(String.format("SELECT id1 FROM %s WHERE v1>=0", table),
                                       ConsistencyLevel.QUORUM);

        cluster.get(1).runOnInstance(() -> {
            QueryInfoTrackerTest.TestQueryInfoTracker tracker =
            (QueryInfoTrackerTest.TestQueryInfoTracker) StorageProxy.instance.queryTracker();

            Assert.assertEquals(1, tracker.rangeReads.get());
            Assert.assertEquals(rowsCount, tracker.readPartitions.get());
            Assert.assertEquals(rowsCount, tracker.readRows.get());
            Assert.assertEquals(4, tracker.replicaPlans.get());
        });
    }
}
