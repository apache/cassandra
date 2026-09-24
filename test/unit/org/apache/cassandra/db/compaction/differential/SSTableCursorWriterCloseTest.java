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

package org.apache.cassandra.db.compaction.differential;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.SSTableCursorWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.test.microbench.sstable.SSTableCursorPipeUtil;
import org.apache.cassandra.tools.JsonTransformer;
import org.apache.cassandra.tools.Util;

import static org.junit.Assert.assertEquals;

/**
 * Correctness gate for {@link SSTableCursorWriter#close()} under the BTI format.
 *
 * <p>{@code close()} is the standalone finish path: it closes the cursor index writer, calls
 * {@code SSTableWriter.finish(false)}, releases the returned reader's Ref, and closes the writer.
 * Production compaction finalizes through {@code CompactionAwareWriter} and never calls it; only the
 * JMH microbench {@code SSTablePipeCursorBench} drives it today. This test runs the same read/copy/close
 * pipe the microbench uses, minus JMH, then reads every row of the finished output back and asserts it
 * is logically identical to the input. If {@code close()}/{@code finish()} left the sstable incomplete
 * or unreadable, the read-back fails.
 */
public class SSTableCursorWriterCloseTest extends CQLTester
{
    /** A few hundred partitions, so the copy pipe runs a realistic partition stream. */
    private static final int PARTITIONS = 300;

    /** Non-static rows per partition. */
    private static final int ROWS_PER_PARTITION = 6;

    /** Every Nth partition carries a static value, so STATIC_ROW_START runs. */
    private static final int STATIC_STRIDE = 3;

    /** Every Nth partition carries a range tombstone, so TOMBSTONE_START runs. */
    private static final int RANGE_TOMBSTONE_STRIDE = 5;

    /** Every Nth partition carries a partition-level deletion, older than the rows so they survive. */
    private static final int PARTITION_DELETE_STRIDE = 7;

    /** Deletion timestamp for the partition-level tombstones: older than the inserted rows. */
    private static final long PARTITION_DELETE_TIMESTAMP = 1L;

    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void selectBti()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("bti");
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    @Test
    public void closeFinishesAReadableIdenticalSSTable() throws Throwable
    {
        ColumnFamilyStore cfs = buildOneInputSSTable();

        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("fixture must flush to exactly one sstable", 1, live.size());
        SSTableReader inputReader = live.iterator().next();
        Descriptor inputDescriptor = inputReader.descriptor;
        assertEquals("fixture input must be BTI", "bti", inputDescriptor.getFormat().name());

        String inputDump = dump(inputReader);

        org.apache.cassandra.io.util.File outDir =
            new org.apache.cassandra.io.util.File(Files.createTempDirectory("cursor-writer-close-out").toFile());

        Descriptor outputDescriptor;
        try (SSTableCursorReader cursorReader = SSTableCursorReader.fromDescriptor(inputDescriptor))
        {
            SortedTableWriter<?, ?> ssTableWriter =
                (SortedTableWriter<?, ?>) CompactionManager.createWriter(cfs, outDir, 0, 0, null, false,
                                                                         inputReader,
                                                                         LifecycleTransaction.offline(OperationType.COMPACTION));
            outputDescriptor = ssTableWriter.descriptor;
            assertEquals("output writer must be BTI so BtiCursorIndexWriter.close() runs",
                         "bti", outputDescriptor.getFormat().name());

            SSTableCursorWriter cursorWriter = new SSTableCursorWriter(ssTableWriter);
            // Copy the whole sstable through the cursor pipe (static rows, rows, range tombstones).
            SSTableCursorPipeUtil.copySSTable(cursorReader, cursorWriter);

            // THE GATE: the standalone finish path this test exists to cover.
            cursorWriter.close();
        }

        SSTableReader outputReader = SSTableReader.open(cfs, outputDescriptor);
        try
        {
            String outputDump = dump(outputReader);
            assertEquals("close()/finish() produced an sstable whose logical content differs from the input",
                         inputDump, outputDump);
        }
        finally
        {
            outputReader.selfRef().release();
            // Best-effort scratch cleanup; never let a cleanup failure mask the assertion above.
            try
            {
                org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(outDir.toString()));
            }
            catch (IOException ignored)
            {
                // scratch dir under build/tmp; harmless if it lingers
            }
        }
    }

    /**
     * Builds a single flushed BTI sstable holding a deterministic mix of partitions with static
     * values, multiple clustering rows, range tombstones and partition-level deletions.
     */
    private ColumnFamilyStore buildOneInputSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s int static, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int pk = 0; pk < PARTITIONS; pk++)
        {
            if (pk % STATIC_STRIDE == 0)
                execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, pk);

            for (int ck = 0; ck < ROWS_PER_PARTITION; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "val" + pk + '_' + ck);

            if (pk % RANGE_TOMBSTONE_STRIDE == 0)
                execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?", pk, 2, 3);

            if (pk % PARTITION_DELETE_STRIDE == 0)
                execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", PARTITION_DELETE_TIMESTAMP, pk);
        }
        flush();
        return cfs;
    }

    /** Canonical logical dump of every partition, with a fixed "now" so it cannot depend on the clock. */
    private static String dump(SSTableReader sstable) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            JsonTransformer.toJsonLines(scanner, Util.iterToStream(scanner), true, false,
                                        sstable.metadata(), 0, baos);
        }
        return baos.toString(StandardCharsets.UTF_8);
    }
}
