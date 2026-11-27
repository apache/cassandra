/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3;

import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.exceptions.QueryCancelledException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A read that skips through data covered by a deletion emits nothing while doing so, so it never reaches the
 * cancellation check that {@link ReadCommand#executeLocally} applies to the merged result. The merge has to check
 * for cancellation itself. Otherwise such a read runs on past its timeout, keeping its read OpOrder group open (and
 * memtable reclaim blocked) for as long as the scan takes.
 * <p>
 * The same goes for a legacy secondary index read going through stale or deleted index entries.
 */
public class ShadowedReadTimeoutTest extends CQLTester
{
    private static final int SSTABLES = 10;
    private static final int ROWS_PER_SSTABLE = 50_000;
    private static final int INDEXED_ROWS = 100_000;
    private static final long TIMEOUT_MS = 10;

    @Test
    public void testSinglePartitionReadOfRangeDeletedRows() throws Throwable
    {
        ColumnFamilyStore cfs = populate();
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = 0 AND ck < ?", (long) SSTABLES + 1, SSTABLES * ROWS_PER_SSTABLE);
        assertSlowAndCancelled(() -> Util.cmd(cfs, 0).build());
    }

    @Test
    public void testRangeReadOfRangeDeletedRows() throws Throwable
    {
        ColumnFamilyStore cfs = populate();
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = 0 AND ck < ?", (long) SSTABLES + 1, SSTABLES * ROWS_PER_SSTABLE);
        assertSlowAndCancelled(() -> Util.cmd(cfs).build());
    }

    @Test
    public void testRangeReadOfDeletedPartition() throws Throwable
    {
        ColumnFamilyStore cfs = populate();
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = 0", (long) SSTABLES + 1);
        assertSlowAndCancelled(() -> Util.cmd(cfs).build());
    }

    @Test
    public void testLegacyIndexReadOfStaleEntries() throws Throwable
    {
        ColumnFamilyStore cfs = populateWithStaleIndexEntries();
        // A read deletes the stale entries it goes through, so the read with a timeout must come first. The read
        // without one then goes through the entries the first read did not get to.
        assertCancelled(Util.cmd(cfs).filterOn("v", Operator.EQ, 1).build());
        assertSlowerThanTimeout(Util.cmd(cfs).filterOn("v", Operator.EQ, 1).build());
    }

    @Test
    public void testLegacyIndexReadOfDeletedEntries() throws Throwable
    {
        ColumnFamilyStore cfs = populateWithStaleIndexEntries();
        // This read deletes all the stale entries.
        assertTrue(Util.getAll(Util.cmd(cfs).filterOn("v", Operator.EQ, 1).build()).isEmpty());
        assertSlowAndCancelled(() -> Util.cmd(cfs).filterOn("v", Operator.EQ, 1).build());
    }

    /**
     * Writes one partition whose rows are spread over several sstables.
     */
    private ColumnFamilyStore populate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int s = 0; s < SSTABLES; ++s)
        {
            UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), 0).withTimestamp(s + 1);
            for (int i = 0; i < ROWS_PER_SSTABLE; ++i)
                builder.newRow(s * ROWS_PER_SSTABLE + i).add("v", i);
            builder.applyUnsafe();
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }
        return cfs;
    }

    /**
     * Writes rows indexed by a legacy secondary index, then changes the indexed value of all of them. The entries for
     * the old value stay, as stale entries, because the old rows had already been flushed when the value changed.
     */
    private ColumnFamilyStore populateWithStaleIndexEntries() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        createIndex("CREATE INDEX ON %s(v)");
        waitForTableIndexesQueryable();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int i = 0; i < INDEXED_ROWS; ++i)
            UpdateBuilder.create(cfs.metadata(), i).withTimestamp(1).newRow().add("v", 1).applyUnsafe();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        for (int i = 0; i < INDEXED_ROWS; ++i)
            UpdateBuilder.create(cfs.metadata(), i).withTimestamp(2).newRow().add("v", 2).applyUnsafe();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        return cfs;
    }

    private void assertSlowAndCancelled(Supplier<ReadCommand> commands)
    {
        assertSlowerThanTimeout(commands.get());
        assertCancelled(commands.get());
    }

    /**
     * Without a timeout, the read goes through all the data and returns nothing. It must take long enough to tell if a
     * read with a timeout is cancelled.
     */
    private void assertSlowerThanTimeout(ReadCommand command)
    {
        long start = System.nanoTime();
        assertTrue(Util.getAll(command).isEmpty());
        long fullScanMs = NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue("Reading the deleted data took only " + fullScanMs + " ms, too fast to tell if it is cancelled.\n" +
                   "Please increase the amount of data this test writes.",
                   fullScanMs >= 5 * TIMEOUT_MS);
    }

    /**
     * With a timeout, executed the way ReadCommandVerbHandler does it, the read must be cancelled.
     */
    private void assertCancelled(ReadCommand command)
    {
        command.setMonitoringTime(approxTime.now(), false, MILLISECONDS.toNanos(TIMEOUT_MS), MILLISECONDS.toNanos(TIMEOUT_MS));
        long start = System.nanoTime();
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            command.createResponse(iterator, controller.getRepairedDataInfo());
            fail(String.format("Read with a %d ms timeout was not cancelled, it took %d ms",
                               TIMEOUT_MS, NANOSECONDS.toMillis(System.nanoTime() - start)));
        }
        catch (QueryCancelledException e)
        {
            logger.info("Read with a {} ms timeout cancelled after {} ms",
                        TIMEOUT_MS, NANOSECONDS.toMillis(System.nanoTime() - start));
        }
        assertTrue(command.isAborted());
        assertFalse(command.complete());
    }
}
