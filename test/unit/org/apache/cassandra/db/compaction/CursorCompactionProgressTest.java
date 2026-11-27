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

package org.apache.cassandra.db.compaction;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.differential.DifferentialCompactionTester;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertTrue;

/**
 * The cursor path refreshes the bytes scanned every 128 merged unfiltereds, so that
 * {@code nodetool compactionstats} and the compaction rate limiter see a wide partition move
 * while it is being merged, not only when it ends. The total at the end does not show this,
 * because it is the same however often progress is refreshed.
 */
@RunWith(BMUnitRunner.class)
@BMRule(name = "record each progress refresh",
        targetClass = "CursorCompactor",
        targetMethod = "updateTotalBytesRead()",
        targetLocation = "AT EXIT",
        condition = "org.apache.cassandra.db.compaction.CursorCompactionProgressTest.isRecording()",
        action = "org.apache.cassandra.db.compaction.CursorCompactionProgressTest.refreshed($0)")
public class CursorCompactionProgressTest extends DifferentialCompactionTester
{
    private static final long WIDE_PK = 1000;
    private static final int WIDE_ROWS = 1000;
    /** Must equal CursorCompactor.UNFILTERED_TO_UPDATE_PROGRESS. */
    private static final int UNFILTERED_PER_REFRESH = 128;

    /** The bytes scanned at each refresh made while the wide partition was merged. */
    public static final List<Long> wideRefreshes = new CopyOnWriteArrayList<>();
    private static volatile Thread recordingThread;

    public static boolean isRecording()
    {
        return Thread.currentThread() == recordingThread;
    }

    public static void refreshed(CursorCompactor compactor)
    {
        if (ByteBufferUtil.toLong(compactor.partitionDescriptor.key().getKey()) == WIDE_PK)
            wideRefreshes.add(compactor.getTotalBytesScanned());
    }

    @After
    public void tearDown()
    {
        recordingThread = null;
        restoreSelectedFormat();
    }

    @Test
    public void progressMovesInsideWidePartitionBig() throws Throwable
    {
        assertProgressMovesInsideWidePartition("big");
    }

    @Test
    public void progressMovesInsideWidePartitionBti() throws Throwable
    {
        assertProgressMovesInsideWidePartition("bti");
    }

    private void assertProgressMovesInsideWidePartition(String format) throws Throwable
    {
        selectSSTableFormat(format);
        String table = createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        cfs.disableAutoCompaction();
        // three overlapping sstables; the wide partition has the same rows in each, so it merges WIDE_ROWS unfiltereds
        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 20; pk++)
                insert(table, pk, round);
            for (long ck = 0; ck < WIDE_ROWS; ck++)
                insert(table, WIDE_PK, ck, round);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }

        wideRefreshes.clear();
        recordingThread = Thread.currentThread();
        commitThroughFactory(cfs, true, (c, txn, gcBefore) -> new CompactionTask(c, txn, gcBefore, false));
        recordingThread = null;

        // The refresh counter does not reset per partition, so the wide partition's WIDE_ROWS unfiltereds cross at least this many multiples of 128.
        int expected = WIDE_ROWS / UNFILTERED_PER_REFRESH;
        List<Long> refreshes = wideRefreshes;
        assertTrue(format + ": expected at least " + expected + " progress refreshes inside the wide partition, got " + refreshes.size(),
                   refreshes.size() >= expected);
        for (int i = 1; i < refreshes.size(); i++)
            assertTrue(format + ": bytes scanned must grow between refreshes inside the wide partition: " + refreshes,
                       refreshes.get(i) > refreshes.get(i - 1));
    }

    private void insert(String table, long pk, int round)
    {
        insert(table, pk, 0, round);
    }

    private void insert(String table, long pk, long ck, int round)
    {
        execute(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP %d",
                              KEYSPACE, table, 1_000_000L + round),
                pk, ck, "value-" + round + '-' + ck);
    }
}
