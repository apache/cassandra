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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertTrue;

/**
 * Burn counterpart to {@link ParameterizedCompactionDifferentialTest}. It runs the same
 * {@link DifferentialSchemas#minimalCorpus() corpus} at a large scale, under BTI only, so at least
 * {@link #MIN_TOTAL_ROWS} rows flow through the cursor-vs-iterator differential.
 *
 * <p>It is a separate class from the matrix so the matrix stays fast. Three system properties drive it:
 * <ul>
 *   <li>{@link CassandraRelevantProperties#TEST_COMPACTION_BURN_SCALE} sizes each shape (default
 *       {@link #DEFAULT_SCALE}).</li>
 *   <li>{@link CassandraRelevantProperties#TEST_COMPACTION_BURN_MINUTES} is a wall-clock budget. Above zero,
 *       the corpus repeats in rounds until the budget elapses, so the burn can soak for hours. Zero (the
 *       default) runs one round and keeps {@code ant test} fast.</li>
 *   <li>{@link CassandraRelevantProperties#TEST_COMPACTION_BURN_CHECKPOINT} is the path of the progress file
 *       (default {@code build/test/logs/burn-checkpoint.tsv}).</li>
 * </ul>
 *
 * <p>Two design choices let the burn soak for a long time and lose no validated work if it fails:
 * <ul>
 *   <li><b>Flat memory.</b> The run drops each shape's table once its check passes, so the heap holds only
 *       one shape at a time. Scale and duration are then free knobs, not a march toward an out-of-memory
 *       error.</li>
 *   <li><b>Durable checkpoint.</b> The run appends one fsync'd line per shape. If the JVM dies, that file
 *       records which shapes passed and how much data was validated.</li>
 * </ul>
 *
 * <p>To raise scale or duration, run through {@code test-isolated.sh}. It forwards every {@code -Dcassandra.*}
 * argument into the forked test JVM, so {@code -Dcassandra.test.compaction_burn_scale=N} and
 * {@code -Dcassandra.test.compaction_burn_minutes=M} reach the test. On a raw ant line a bare {@code -D} does
 * not reach the fork; pass it inside {@code -Dtest.jvm.args="..."} instead.
 */
public class BurnCompactionDifferentialTest extends DifferentialCompactionTester
{
    /** Default scale, chosen so the corpus yields at least {@link #MIN_TOTAL_ROWS} rows. */
    static final int DEFAULT_SCALE = 80;

    /** The floor the run must clear, proving it is a real burn and not a shrunken matrix. */
    static final long MIN_TOTAL_ROWS = 100_000;

    /** Digest-mode capture: the logical dump is streamed into a SHA-256, so capture memory stays flat. */
    @Override
    protected boolean scaleCapture()
    {
        return true;
    }

    @Before
    public void selectBti()
    {
        selectSSTableFormat("bti");
    }

    @After
    public void restoreFormat()
    {
        restoreSelectedFormat();
    }

    @Test
    public void burnCorpusUnderBti() throws Exception
    {
        int scale = CassandraRelevantProperties.TEST_COMPACTION_BURN_SCALE.getInt(DEFAULT_SCALE);
        assertTrue("burn scale must be positive", scale > 0);
        int budgetMinutes = CassandraRelevantProperties.TEST_COMPACTION_BURN_MINUTES.getInt();
        assertTrue("burn minutes must not be negative", budgetMinutes >= 0);
        long targetRows = CassandraRelevantProperties.TEST_COMPACTION_BURN_TARGET_ROWS.getLong();
        assertTrue("burn target rows must not be negative", targetRows >= 0);

        List<DifferentialSchema> corpus = DifferentialSchemas.minimalCorpus();
        Path checkpointPath = Paths.get(CassandraRelevantProperties.TEST_COMPACTION_BURN_CHECKPOINT.getString());

        long startNanos = System.nanoTime();
        long deadlineNanos = budgetMinutes > 0 ? startNanos + TimeUnit.MINUTES.toNanos(budgetMinutes) : 0;

        long totalRows = 0;
        long totalBytes = 0;
        long totalUncompressed = 0;
        int round = 0;
        try (BurnCheckpoint checkpoint = BurnCheckpoint.open(checkpointPath))
        {
            checkpoint.record(String.format("# burn start scale=%d minutes=%d target_rows=%d shapes=%d",
                                            scale, budgetMinutes, targetRows, corpus.size()));
            logger.info("burn run starting at scale {} for {} minute(s), target {} rows, across {} corpus shapes; checkpoint at {}",
                        scale, budgetMinutes, targetRows, corpus.size(), checkpointPath.toAbsolutePath());

            do
            {
                round++;
                int shapeNum = 0;
                for (DifferentialSchema schema : corpus)
                {
                    shapeNum++;
                    ShapeStats shape = runShape(schema, scale, round);
                    totalRows += shape.rows;
                    totalBytes += shape.bytes;
                    totalUncompressed += shape.uncompressedBytes;
                    long elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000L;

                    // Durable, flushed record BEFORE the next shape starts: a crash after this line still
                    // leaves proof that this shape passed and how much data has been validated.
                    checkpoint.record(String.format("round=%d\tshape=%d/%d\t%s\trows=%d\tbytes=%d\tuncompressed=%d\t" +
                                                    "cumulative_rows=%d\tcumulative_bytes=%d\tcumulative_uncompressed=%d\telapsed_s=%d\tPASS",
                                                    round, shapeNum, corpus.size(), schema.name(),
                                                    shape.rows, shape.bytes, shape.uncompressedBytes,
                                                    totalRows, totalBytes, totalUncompressed, elapsedSec));
                    logger.info("burn progress: round {} shape {}/{} {} compacted {} rows / {} bytes on disk / {} uncompressed " +
                                "(cumulative {} rows, {} bytes, {} uncompressed, {}s elapsed)",
                                round, shapeNum, corpus.size(), schema.name(),
                                shape.rows, shape.bytes, shape.uncompressedBytes,
                                totalRows, totalBytes, totalUncompressed, elapsedSec);
                }
                // Keep going while either budget still wants more rounds: the time budget has not elapsed, or
                // the row target has not been reached. With neither property set, the corpus runs exactly once.
                boolean underTimeBudget = deadlineNanos != 0 && System.nanoTime() < deadlineNanos;
                boolean underRowTarget = targetRows > 0 && totalRows < targetRows;
                if (!underTimeBudget && !underRowTarget)
                    break;
            }
            while (true);

            checkpoint.record(String.format("# burn done rounds=%d cumulative_rows=%d cumulative_bytes=%d cumulative_uncompressed=%d",
                                            round, totalRows, totalBytes, totalUncompressed));
        }

        logger.info("burn run at scale {} processed {} rows / {} bytes on disk / {} uncompressed across {} round(s) of the corpus",
                    scale, totalRows, totalBytes, totalUncompressed, round);
        assertTrue("burn run processed only " + totalRows + " rows at scale " + scale + "; expected at least " +
                   MIN_TOTAL_ROWS + " (raise -D" + CassandraRelevantProperties.TEST_COMPACTION_BURN_SCALE.getKey() + ")",
                   totalRows >= MIN_TOTAL_ROWS);
    }

    /**
     * Writes one shape, asserts the cursor and iterator paths agree, then drops its table so its data does
     * not stay resident into the next shape. Returns the rows and bytes compacted for this shape. The
     * structural guards run only on the first round, because every round regenerates identical data.
     */
    private ShapeStats runShape(DifferentialSchema schema, int scale, int round) throws Exception
    {
        ColumnFamilyStore cfs = writeAndGuardShape(schema, scale, round == 1);
        ShapeStats stats = ShapeStats.measure(cfs);
        assertCursorMatchesIteratorForShape(cfs, schema);
        // Flat memory: release this shape's sstables and memtable before the next shape allocates. The drop
        // must run while currentTable() is still this shape; writeAndGuardShape left it there and the
        // assertion above operates on the passed cfs without creating a new table.
        dropTable("DROP TABLE %s");
        return stats;
    }

    /** The rows and bytes one shape put on disk before compaction: what the differential run validated. */
    private static final class ShapeStats
    {
        final long rows;
        final long bytes;             // compressed bytes on disk
        final long uncompressedBytes; // logical payload before compression

        private ShapeStats(long rows, long bytes, long uncompressedBytes)
        {
            this.rows = rows;
            this.bytes = bytes;
            this.uncompressedBytes = uncompressedBytes;
        }

        /** Sums the three totals across the live set in one pass. */
        static ShapeStats measure(ColumnFamilyStore cfs)
        {
            long rows = 0;
            long bytes = 0;
            long uncompressed = 0;
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                rows += sstable.getTotalRows();
                bytes += sstable.onDiskLength();
                uncompressed += sstable.uncompressedLength();
            }
            return new ShapeStats(rows, bytes, uncompressed);
        }
    }

    /**
     * A tiny append-only progress log that flushes and fsyncs every line, so a JVM crash or kill loses no
     * record of the work already validated. Each line is one shape's PASS plus the running row total.
     */
    private static final class BurnCheckpoint implements AutoCloseable
    {
        private final FileOutputStream out;

        private BurnCheckpoint(FileOutputStream out)
        {
            this.out = out;
        }

        static BurnCheckpoint open(Path path) throws IOException
        {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null)
                Files.createDirectories(parent);
            // APPEND so an earlier round's record is never truncated away by a restart.
            return new BurnCheckpoint(new FileOutputStream(path.toFile(), true));
        }

        void record(String line) throws IOException
        {
            out.write((line + '\n').getBytes(StandardCharsets.UTF_8));
            out.flush();          // push out of the JVM buffer, so the line survives a kill of this process
            out.getFD().sync();   // force to disk, so it also survives an OS crash or power loss
        }

        @Override
        public void close() throws IOException
        {
            out.close();
        }
    }
}
