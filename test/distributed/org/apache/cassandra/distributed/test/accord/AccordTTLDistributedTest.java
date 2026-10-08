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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test was authored entirely by LLM
 *
 * Verifies that data written through Accord with a TTL, and tombstones written through Accord, are stored with
 * local expiration/deletion times that are relative to when the transaction executed, and are identical on every
 * replica. Each replica applies a transaction's writes itself, so they must all derive these times identically.
 */
public class AccordTTLDistributedTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordTTLDistributedTest.class);

    private static final int DEFAULT_TTL = 10000;
    private static final int EXPLICIT_TTL = 5000;
    // marker used in local time arrays for tombstones
    private static final int TOMBSTONE = -1;

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder, 2);
    }

    @Test
    public void testReplicasAgreeOnLocalExpirationAndDeletionTimes() throws Exception
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, l list<int>, PRIMARY KEY (k, c)) " +
             "WITH " + transactionalMode.asCqlParam() + " AND default_time_to_live = " + DEFAULT_TTL, cluster -> {
            ICoordinator coordinator = cluster.coordinator(1);
            String table = qualifiedAccordTableName;

            long start = FBUtilities.nowInSeconds();
            // 0: transaction, expiring via default_time_to_live
            coordinator.execute(wrapInTxn("INSERT INTO " + table + " (k, c, v) VALUES (0, 0, 0)"), QUORUM);
            // 1: CAS, expiring via an explicit TTL
            coordinator.execute("INSERT INTO " + table + " (k, c, v) VALUES (1, 0, 0) IF NOT EXISTS USING TTL " + EXPLICIT_TTL, QUORUM);
            // 2: plain write, expiring via an explicit TTL
            coordinator.execute("INSERT INTO " + table + " (k, c, v) VALUES (2, 0, 0) USING TTL " + EXPLICIT_TTL, QUORUM);
            // 3: transaction partition deletion
            coordinator.execute("INSERT INTO " + table + " (k, c, v) VALUES (3, 0, 0) USING TTL 0", QUORUM);
            coordinator.execute(wrapInTxn("DELETE FROM " + table + " WHERE k = 3"), QUORUM);
            // 4: CAS row deletion
            coordinator.execute("INSERT INTO " + table + " (k, c, v) VALUES (4, 0, 0) USING TTL 0", QUORUM);
            coordinator.execute("DELETE FROM " + table + " WHERE k = 4 AND c = 0 IF EXISTS", QUORUM);
            // 5: CAS list set by index, which is evaluated on execution, expiring via an explicit TTL
            coordinator.execute("UPDATE " + table + " USING TTL 0 SET v = 0, l = l + [1] WHERE k = 5 AND c = 0", QUORUM);
            coordinator.execute("UPDATE " + table + " USING TTL " + EXPLICIT_TTL + " SET l[0] = 2 WHERE k = 5 AND c = 0 IF v = 0", QUORUM);
            long end = FBUtilities.nowInSeconds();

            // the expiring rows must be visible, with the TTL we asked for
            assertRemainingTTL(coordinator.execute("SELECT ttl(v) FROM " + table + " WHERE k = 0 AND c = 0", QUORUM), DEFAULT_TTL);
            assertRemainingTTL(coordinator.execute("SELECT ttl(v) FROM " + table + " WHERE k = 1 AND c = 0", QUORUM), EXPLICIT_TTL);
            assertRemainingTTL(coordinator.execute("SELECT ttl(v) FROM " + table + " WHERE k = 2 AND c = 0", QUORUM), EXPLICIT_TTL);
            assertEquals(0, coordinator.execute("SELECT * FROM " + table + " WHERE k IN (3, 4)", QUORUM).length);
            assertRows(coordinator.execute("SELECT l FROM " + table + " WHERE k = 5 AND c = 0", QUORUM), row(Collections.singletonList(2)));

            int[] expectedTTLs = { DEFAULT_TTL, EXPLICIT_TTL, EXPLICIT_TTL, TOMBSTONE, TOMBSTONE, EXPLICIT_TTL };
            for (int k = 0; k < expectedTTLs.length; k++)
            {
                int partition = k;
                // a replica may not yet have applied every write, so retry until they all have
                Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).untilAsserted(() -> {
                    long[] firstTimes = null;
                    String firstContents = null;
                    for (IInvokableInstance instance : cluster)
                    {
                        long[] times = localTimes(instance, accordTableName, partition);
                        assertTrue("No expiring data or tombstones found for partition " + partition + " on node " + instance.config().num(), times.length > 0);
                        for (int i = 0; i < times.length; i += 2)
                            assertTime(partition, instance, times[i], (int) times[i + 1], expectedTTLs[partition], start, end);

                        // replicas must agree not only on times, but on everything else (e.g. list cell paths)
                        String contents = localContents(instance, accordTableName, partition);
                        if (firstTimes == null)
                        {
                            firstTimes = times;
                            firstContents = contents;
                        }
                        else
                        {
                            assertArrayEquals("Replicas disagree on the local times of partition " + partition, firstTimes, times);
                            assertEquals("Replicas disagree on the contents of partition " + partition, firstContents, contents);
                        }
                    }
                });
            }
        });
    }

    @Test
    public void testTransactionalDefaultTTLExpiresOnAllReplicas() throws Exception
    {
        int ttl = 2;
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY (k, c)) " +
             "WITH " + transactionalMode.asCqlParam() + " AND default_time_to_live = " + ttl, cluster -> {
            ICoordinator coordinator = cluster.coordinator(1);
            String select = "SELECT v FROM " + qualifiedAccordTableName + " WHERE k = 0 AND c = 0";
            coordinator.execute(wrapInTxn("INSERT INTO " + qualifiedAccordTableName + " (k, c, v) VALUES (0, 0, 0)"), QUORUM);
            assertEquals(1, coordinator.execute(select, QUORUM).length);
            for (IInvokableInstance instance : cluster)
                assertEquals(1, cluster.coordinator(instance.config().num()).execute(wrapInTxn(select), QUORUM).length);

            Awaitility.await().atMost(ttl + 30, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
                      .until(() -> coordinator.execute(select, QUORUM).length == 0);
            for (IInvokableInstance instance : cluster)
                assertEquals(0, cluster.coordinator(instance.config().num()).execute(wrapInTxn(select), QUORUM).length);
        });
    }

    private static void assertRemainingTTL(Object[][] rows, int ttl)
    {
        assertEquals("Expected exactly one row", 1, rows.length);
        int remaining = (Integer) rows[0][0];
        assertTrue("Unexpected remaining TTL " + remaining + " for TTL " + ttl, remaining <= ttl && remaining > ttl - 120);
    }

    private static void assertTime(int k, IInvokableInstance instance, long localTime, int ttl, int expectedTTL, long start, long end)
    {
        String where = "partition " + k + " on node " + instance.config().num();
        assertEquals("Unexpected TTL in " + where, expectedTTL, ttl);
        long offset = ttl == TOMBSTONE ? 0 : ttl;
        assertTrue(String.format("Local %s time %d in %s should be within [%d, %d]",
                                 ttl == TOMBSTONE ? "deletion" : "expiration", localTime, where, start + offset, end + offset),
                   localTime >= start + offset && localTime <= end + offset);
    }

    /**
     * @return pairs of (local deletion or expiration time, ttl or {@link #TOMBSTONE}) for every tombstone and expiring
     * liveness info or cell stored locally for partition {@code k}, in iteration order
     */
    private static long[] localTimes(IInvokableInstance instance, String table, int k)
    {
        return instance.callOnInstance(() -> {
            List<Long> times = new ArrayList<>();
            readLocal(table, k, partition -> {
                addDeletion(times, partition.partitionLevelDeletion());
                addRow(times, partition.staticRow());
                while (partition.hasNext())
                {
                    Unfiltered unfiltered = partition.next();
                    if (unfiltered.isRow())
                        addRow(times, (Row) unfiltered);
                }
            });
            long[] result = new long[times.size()];
            for (int i = 0; i < result.length; i++)
                result[i] = times.get(i);
            return result;
        });
    }

    /**
     * @return a complete description of partition {@code k} as stored locally, including timestamps, TTLs,
     * local deletion times and cell paths
     */
    private static String localContents(IInvokableInstance instance, String table, int k)
    {
        return instance.callOnInstance(() -> {
            StringBuilder contents = new StringBuilder();
            readLocal(table, k, partition -> {
                contents.append(partition.partitionLevelDeletion()).append('\n');
                contents.append(partition.staticRow().toString(partition.metadata(), true)).append('\n');
                while (partition.hasNext())
                    contents.append(partition.next().toString(partition.metadata(), true)).append('\n');
            });
            return contents.toString();
        });
    }

    private static void readLocal(String table, int k, Consumer<UnfilteredRowIterator> consumer)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(k));
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), FBUtilities.nowInSeconds(), key);
        command = command.withTransactionalSettings(false, command.nowInSec());
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    consumer.accept(partition);
                }
            }
        }
    }

    private static void addDeletion(List<Long> times, DeletionTime deletion)
    {
        if (deletion.isLive())
            return;
        times.add(deletion.localDeletionTime());
        times.add((long) TOMBSTONE);
    }

    private static void addRow(List<Long> times, Row row)
    {
        LivenessInfo info = row.primaryKeyLivenessInfo();
        if (info.isExpiring())
        {
            times.add(info.localExpirationTime());
            times.add((long) info.ttl());
        }
        addDeletion(times, row.deletion().time());
        for (ColumnData cd : row)
        {
            if (cd.column().isComplex())
                addDeletion(times, ((ComplexColumnData) cd).complexDeletion());
        }
        for (Cell<?> cell : row.cells())
        {
            if (cell.isExpiring())
            {
                times.add(cell.localDeletionTime());
                times.add((long) cell.ttl());
            }
            else if (cell.isTombstone())
            {
                times.add(cell.localDeletionTime());
                times.add((long) TOMBSTONE);
            }
        }
    }
}
