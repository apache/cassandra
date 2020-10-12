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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.*;
import static org.apache.cassandra.net.OutboundConnections.LARGE_MESSAGE_THRESHOLD;
import static org.apache.cassandra.net.Verb.READ_REPAIR_REQ;
import static org.apache.cassandra.net.Verb.READ_REPAIR_RSP;
import static org.junit.Assert.fail;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class SimpleReadWriteTest extends TestBaseImpl
{
    @Test
    public void coordinatorReadTest() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 2)");
            cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, 3)");

            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                     ConsistencyLevel.ALL,
                                                     1),
                       row(1, 1, 1),
                       row(1, 2, 2),
                       row(1, 3, 3));
        }
    }

    @Test
    public void largeMessageTest() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < LARGE_MESSAGE_THRESHOLD ; i++)
                builder.append('a');
            String s = builder.toString();
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)",
                                           ConsistencyLevel.ALL,
                                           s);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                      ConsistencyLevel.ALL,
                                                      1),
                       row(1, 1, s));
        }
    }

    @Test
    public void coordinatorWriteTest() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='none'");

            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)",
                                          ConsistencyLevel.QUORUM);

            for (int i = 0; i < 3; i++)
            {
                assertRows(cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                           row(1, 1, 1));
            }

            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                     ConsistencyLevel.QUORUM),
                       row(1, 1, 1));
        }
    }

    @Test
    public void readRepairTest() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='blocking'");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));

            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                     ConsistencyLevel.ALL), // ensure node3 in preflist
                       row(1, 1, 1));

            // Verify that data got repaired to the third node
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                       row(1, 1, 1));
        }
    }

    @Test
    public void readRepairTimeoutTest() throws Throwable
    {
        final long reducedReadTimeout = 3000L;
        try (Cluster cluster = (Cluster) init(builder().withNodes(3).start()))
        {
            cluster.forEach(i -> i.runOnInstance(() -> DatabaseDescriptor.setReadRpcTimeout(reducedReadTimeout)));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='blocking'");
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));
            cluster.verbs(READ_REPAIR_RSP).to(1).drop();
            final long start = System.currentTimeMillis();
            try
            {
                cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.ALL);
                fail("Read timeout expected but it did not occur");
            }
            catch (Exception ex)
            {
                // the containing exception class was loaded by another class loader. Comparing the message as a workaround to assert the exception
                Assert.assertTrue(ex.getClass().toString().contains("ReadTimeoutException"));
                long actualTimeTaken = System.currentTimeMillis() - start;
                long magicDelayAmount = 100L; // it might not be the best way to check if the time taken is around the timeout value.
                // Due to the delays, the actual time taken from client perspective is slighly more than the timeout value
                Assert.assertTrue(actualTimeTaken > reducedReadTimeout);
                // But it should not exceed too much
                Assert.assertTrue(actualTimeTaken < reducedReadTimeout + magicDelayAmount);
                assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                           row(1, 1, 1)); // the partition happened when the repaired node sending back ack. The mutation should be in fact applied.
            }
        }
    }

    @Test
    public void failingReadRepairTest() throws Throwable
    {
        // This test makes a explicit assumption about which nodes are read from; that 1 and 2 will be "contacts", and that 3 will be ignored.
        // This is a implementation detail of org.apache.cassandra.locator.ReplicaPlans#contactForRead and
        // org.apache.cassandra.locator.AbstractReplicationStrategy.getNaturalReplicasForToken that may change
        // in a future release; when that happens this test could start to fail but should only fail with the explicit
        // check that detects this behavior has changed.
        // see CASSANDRA-15507
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='blocking'");

            // nodes 1 and 3 are identical
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 43");
            cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 43");

            // node 2 is different because of the timestamp; a repair is needed
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 42");

            // 2 is out of date so needs to be repaired.  This will make sure that the repair does not happen on the node
            // which will trigger the coordinator to write to node 3
            cluster.verbs(READ_REPAIR_REQ).to(2).drop();

            // save the state of the counters so its known if the contacts list changed
            long readRepairRequestsBefore = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric.readRepairRequests.getCount());
            long speculatedWriteBefore = cluster.get(1).callOnInstance(() -> ReadRepairMetrics.speculatedWrite.getCount());

            Object[][] rows = cluster.coordinator(1)
                       .execute("SELECT pk, ck, v, WRITETIME(v) FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.QUORUM);

            // make sure to check counters first, so can detect if read repair executed as expected
            long readRepairRequestsAfter = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric.readRepairRequests.getCount());
            long speculatedWriteAfter = cluster.get(1).callOnInstance(() -> ReadRepairMetrics.speculatedWrite.getCount());

            // defensive checks to make sure the nodes selected are the ones expected
            Assert.assertEquals("number of read repairs after query does not match expected; its possible the nodes involved with the query did not match expected", readRepairRequestsBefore + 1, readRepairRequestsAfter);
            Assert.assertEquals("number of read repairs speculated writes after query does not match expected; its possible the nodes involved with the query did not match expected", speculatedWriteBefore + 1, speculatedWriteAfter);

            // 1 has newer data than 2 so its write timestamp will be used for the result
            assertRows(rows, row(1, 1, 1, 43L));

            // cheack each node's local state
            // 1 and 3 should match quorum response
            assertRows(cluster.get(1).executeInternal("SELECT pk, ck, v, WRITETIME(v) FROM " + KEYSPACE + ".tbl WHERE pk = 1"), row(1, 1, 1, 43L));
            assertRows(cluster.get(3).executeInternal("SELECT pk, ck, v, WRITETIME(v) FROM " + KEYSPACE + ".tbl WHERE pk = 1"), row(1, 1, 1, 43L));

            // 2 was not repaired (message was dropped), so still has old data
            assertRows(cluster.get(2).executeInternal("SELECT pk, ck, v, WRITETIME(v) FROM " + KEYSPACE + ".tbl WHERE pk = 1"), row(1, 1, 1, 42L));
        }
    }

    /**
     * If a node receives a mutation for a column it's not aware of, it should fail, since it can't write the data.
     */
    @Test
    public void writeWithSchemaDisagreement() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withConfig(config -> config.with(NETWORK)).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
            cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");

            // Introduce schema disagreement
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v2 int", 1);

            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (2, 2, 2, 2)",
                                              ConsistencyLevel.ALL);
                fail("Should have failed because of schema disagreement.");
            }
            catch (Exception e)
            {
                // for some reason, we get weird errors when trying to check class directly
                // I suppose it has to do with some classloader manipulation going on
                Assert.assertTrue(e.getClass().toString().contains("WriteFailureException"));
                // we may see 1 or 2 failures in here, because of the fail-fast behavior of AbstractWriteResponseHandler
                Assert.assertTrue(e.getMessage().contains("INCOMPATIBLE_SCHEMA from ") &&
                                  (e.getMessage().contains("/127.0.0.2") || e.getMessage().contains("/127.0.0.3")));

            }
        }
    }

    /**
     * If a node receives a mutation for a column it knows has been dropped, the write should succeed
     */
    @Test
    public void writeWithSchemaDisagreement2() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withConfig(config -> config.with(NETWORK)).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (1, 1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (1, 1, 1, 1)");
            cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (1, 1, 1, 1)");
            cluster.forEach((instance) -> instance.flush(KEYSPACE));

            // Introduce schema disagreement
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl DROP v2", 1);

            // execute a write including the dropped column where the coordinator is not yet aware of the drop
            // all nodes should process this without error
            cluster.coordinator(2).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (2, 2, 2, 2)",
                                               ConsistencyLevel.ALL);
            // and flushing should also be fine
            cluster.forEach((instance) -> instance.flush(KEYSPACE));
            // the results of reads will vary depending on whether the coordinator has seen the schema change
            // note: read repairs will propagate the v2 value to node1, but this is safe and handled correctly
            assertRows(cluster.coordinator(2).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL),
                       rows(row(1,1,1,1), row(2,2,2,2)));
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL),
                       rows(row(1,1,1), row(2,2,2)));
        }
    }

    /**
     * If a node isn't aware of a column, but receives a mutation without that column, the write should succeed
     */
    @Test
    public void writeWithInconsequentialSchemaDisagreement() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(3).withConfig(config -> config.with(NETWORK)).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
            cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");

            // Introduce schema disagreement
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v2 int", 1);

            // this write shouldn't cause any problems because it doesn't write to the new column
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (2, 2, 2)",
                                           ConsistencyLevel.ALL);
        }
    }

    /**
     * If a node receives a read for a column it's not aware of, it shouldn't complain, since it won't have any data for that column
     */
    @Test
    public void readWithSchemaDisagreement() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(3).withConfig(config -> config.with(NETWORK)).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
            cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");

            // Introduce schema disagreement
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v2 int", 1);

            Object[][] expected = new Object[][]{new Object[]{1, 1, 1, null}};
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.ALL), expected);
        }
    }

    @Test
    public void simplePagedReadsTest() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            int size = 100;
            Object[][] results = new Object[size][];
            for (int i = 0; i < size; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                               ConsistencyLevel.QUORUM,
                                               i, i);
                results[i] = new Object[] { 1, i, i};
            }

            // Make sure paged read returns same results with different page sizes
            for (int pageSize : new int[] { 1, 2, 3, 5, 10, 20, 50})
            {
                assertRows(cluster.coordinator(1).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl",
                                                                    ConsistencyLevel.QUORUM,
                                                                    pageSize),
                           results);
            }
        }
    }

    @Test
    public void pagingWithRepairTest() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            int size = 100;
            Object[][] results = new Object[size][];
            for (int i = 0; i < size; i++)
            {
                // Make sure that data lands on different nodes and not coordinator
                cluster.get(i % 2 == 0 ? 2 : 3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                                                i, i);

                results[i] = new Object[] { 1, i, i};
            }

            // Make sure paged read returns same results with different page sizes
            for (int pageSize : new int[] { 1, 2, 3, 5, 10, 20, 50})
            {
                assertRows(cluster.coordinator(1).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl",
                                                                    ConsistencyLevel.ALL,
                                                                    pageSize),
                           results);
            }

            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl"),
                       results);
        }
    }

    @Test
    public void pagingTests() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(3).start());
             ICluster singleNode = init(builder().withNodes(1).withSubnet(1).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            singleNode.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            for (int i = 0; i < 10; i++)
            {
                for (int j = 0; j < 10; j++)
                {
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                                   ConsistencyLevel.QUORUM,
                                                   i, j, i + i);
                    singleNode.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                                      ConsistencyLevel.QUORUM,
                                                      i, j, i + i);
                }
            }

            int[] pageSizes = new int[] { 1, 2, 3, 5, 10, 20, 50};
            String[] statements = new String [] {"SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck >= 5",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 LIMIT 3",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck >= 5 LIMIT 2",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 LIMIT 2",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC LIMIT 3",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC LIMIT 2",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC LIMIT 2",
                                                 "SELECT DISTINCT pk FROM " + KEYSPACE  + ".tbl LIMIT 3",
                                                 "SELECT DISTINCT pk FROM " + KEYSPACE  + ".tbl WHERE pk IN (3,5,8,10)",
                                                 "SELECT DISTINCT pk FROM " + KEYSPACE  + ".tbl WHERE pk IN (3,5,8,10) LIMIT 2"
            };
            for (String statement : statements)
            {
                for (int pageSize : pageSizes)
                {
                    assertRows(cluster.coordinator(1)
                                      .executeWithPaging(statement,
                                                         ConsistencyLevel.QUORUM,  pageSize),
                               singleNode.coordinator(1)
                                         .executeWithPaging(statement,
                                                            ConsistencyLevel.QUORUM,  Integer.MAX_VALUE));
                }
            }

        }
    }

    @Test
    public void metricsCountQueriesTest() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (pk, ck, v) VALUES (?,?,?)", ConsistencyLevel.ALL, i, i, i);

            long readCount1 = readCount(cluster.get(1));
            long readCount2 = readCount(cluster.get(2));
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE pk = ? and ck = ?", ConsistencyLevel.ALL, i, i);

            readCount1 = readCount(cluster.get(1)) - readCount1;
            readCount2 = readCount(cluster.get(2)) - readCount2;
            Assert.assertEquals(readCount1, readCount2);
            Assert.assertEquals(100, readCount1);
        }
    }


    @Test
    public void skippedSSTableWithPartitionDeletionTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY(pk, ck))");
            // insert a partition tombstone on node 1, the deletion timestamp should end up being the sstable's minTimestamp
            cluster.get(1).executeInternal("DELETE FROM " + KEYSPACE + ".tbl USING TIMESTAMP 1 WHERE pk = 0");
            // and a row from a different partition, to provide the sstable's min/max clustering
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 2");
            cluster.get(1).flush(KEYSPACE);
            // expect a single sstable, where minTimestamp equals the timestamp of the partition delete
            cluster.get(1).runOnInstance(() -> {
                Set<SSTableReader> sstables = Keyspace.open(KEYSPACE)
                                                      .getColumnFamilyStore("tbl")
                                                      .getLiveSSTables();
                assertEquals("Expected a single sstable, but found " + sstables.size(), 1, sstables.size());
                long minTimestamp = sstables.iterator().next().getMinTimestamp();
                assertEquals("Expected min timestamp of 1, but was " + minTimestamp, 1, minTimestamp);
            });

            // on node 2, add a row for the deleted partition with an older timestamp than the deletion so it should be shadowed
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 10, 10) USING TIMESTAMP 0");


            Object[][] rows = cluster.coordinator(1)
                                     .execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=0 AND ck > 5",
                                              ConsistencyLevel.ALL);
            assertEquals("Expected 0 rows, but found " + rows.length, 0, rows.length);
        }
    }

    @Test
    public void skippedSSTableWithPartitionDeletionShadowingDataOnAnotherNode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY(pk, ck))");
            // insert a partition tombstone on node 1, the deletion timestamp should end up being the sstable's minTimestamp
            cluster.get(1).executeInternal("DELETE FROM " + KEYSPACE + ".tbl USING TIMESTAMP 1 WHERE pk = 0");
            // and a row from a different partition, to provide the sstable's min/max clustering
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 1");
            cluster.get(1).flush(KEYSPACE);
            // sstable 1 has minTimestamp == maxTimestamp == 1 and is skipped due to its min/max clusterings. Now we
            // insert a row which is not shadowed by the partition delete and flush to a second sstable. Importantly,
            // this sstable's minTimestamp is > than the maxTimestamp of the first sstable. This would cause the first
            // sstable not to be reincluded in the merge input, but we can't really make that decision as we don't
            // know what data and/or tombstones are present on other nodes
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 6, 6) USING TIMESTAMP 2");
            cluster.get(1).flush(KEYSPACE);

            // on node 2, add a row for the deleted partition with an older timestamp than the deletion so it should be shadowed
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 10, 10) USING TIMESTAMP 0");

            Object[][] rows = cluster.coordinator(1)
                                     .execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=0 AND ck > 5",
                                              ConsistencyLevel.ALL);
            // we expect that the row from node 2 (0, 10, 10) was shadowed by the partition delete, but the row from
            // node 1 (0, 6, 6) was not.
            assertRows(rows, new Object[] {0, 6 ,6});
        }
    }

    @Test
    public void skippedSSTableWithPartitionDeletionShadowingDataOnAnotherNode2() throws Throwable
    {
        // don't not add skipped sstables back just because the partition delete ts is < the local min ts

        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY(pk, ck))");
            // insert a partition tombstone on node 1, the deletion timestamp should end up being the sstable's minTimestamp
            cluster.get(1).executeInternal("DELETE FROM " + KEYSPACE + ".tbl USING TIMESTAMP 1 WHERE pk = 0");
            // and a row from a different partition, to provide the sstable's min/max clustering
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 3");
            cluster.get(1).flush(KEYSPACE);
            // sstable 1 has minTimestamp == maxTimestamp == 1 and is skipped due to its min/max clusterings. Now we
            // insert a row which is not shadowed by the partition delete and flush to a second sstable. The first sstable
            // has a maxTimestamp > than the min timestamp of all sstables, so it is a candidate for reinclusion to the
            // merge. Hoever, the second sstable's minTimestamp is > than the partition delete. This would  cause the
            // first sstable not to be reincluded in the merge input, but we can't really make that decision as we don't
            // know what data and/or tombstones are present on other nodes
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 6, 6) USING TIMESTAMP 2");
            cluster.get(1).flush(KEYSPACE);

            // on node 2, add a row for the deleted partition with an older timestamp than the deletion so it should be shadowed
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 10, 10) USING TIMESTAMP 0");

            Object[][] rows = cluster.coordinator(1)
                                     .execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=0 AND ck > 5",
                                              ConsistencyLevel.ALL);
            // we expect that the row from node 2 (0, 10, 10) was shadowed by the partition delete, but the row from
            // node 1 (0, 6, 6) was not.
            assertRows(rows, new Object[] {0, 6 ,6});
        }
    }

    private long readCount(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric.readLatency.latency.getCount());
    }

    private static Object[][] rows(Object[]...rows)
    {
        Object[][] r = new Object[rows.length][];
        System.arraycopy(rows, 0, r, 0, rows.length);
        return r;
    }

}
