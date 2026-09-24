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

import static org.junit.Assert.assertTrue;

/**
 * Burn counterpart to {@link ParameterizedCompactionDifferentialTest}: runs the same
 * {@link DifferentialSchemas#minimalCorpus() corpus} at a large scale, under BTI only, so at least
 * {@link #MIN_TOTAL_ROWS} rows flow through the cursor-vs-iterator differential in one run.
 *
 * <p>Kept a separate class from the fast matrix so the matrix stays fast. Three system properties drive it:
 * <ul>
 *   <li>{@link CassandraRelevantProperties#TEST_COMPACTION_BURN_SCALE} sizes each shape (default
 *       {@link #DEFAULT_SCALE}).</li>
 *   <li>{@link CassandraRelevantProperties#TEST_COMPACTION_BURN_MINUTES} is a wall-clock budget. When it is
 *       greater than zero, the corpus repeats in rounds until the budget elapses, so the burn can soak for
 *       hours. Zero (the default) runs exactly one round, which keeps the normal {@code ant test} fast.</li>
 *   <li>{@link CassandraRelevantProperties#TEST_COMPACTION_BURN_CHECKPOINT} is the path of a durable progress
 *       file (default {@code build/test/logs/burn-checkpoint.tsv}).</li>
 * </ul>
 *
 * <p>Two properties let the burn run for a long time without failing, and lose no validated work if it does
 * fail:
 * <ul>
 *   <li><b>Flat memory.</b> Each shape's table is dropped as soon as its differential check passes, so heap
 *       does not grow across shapes or rounds. The run holds only one shape's data at a time, so scale and
 *       duration are free knobs rather than a slow march toward an out-of-memory error.</li>
 *   <li><b>Durable checkpoint.</b> After every shape the run appends one flushed, fsync'd line to the
 *       checkpoint file. If the JVM dies mid-run, that file is the preserved record of exactly which shapes
 *       passed and how many rows were validated.</li>
 * </ul>
 *
 * <p>To raise the scale or duration, run through the {@code test-isolated.sh} harness, which forwards every
 * {@code -Dcassandra.*} argument into the forked test JVM: {@code -Dcassandra.test.compaction_burn_scale=N}
 * and {@code -Dcassandra.test.compaction_burn_minutes=M} then reach the test. A bare {@code -D} on a raw ant
 * line does NOT reach the fork (the junit fork takes only an explicit jvmarg allowlist plus
 * {@code test.jvm.args}); outside the harness, pass them inside {@code -Dtest.jvm.args="..."}.
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

    /** Every corpus shape must leave a merged sstable to compare; an empty-vs-empty pass proves nothing. */
    @Override
    protected boolean requireNonEmptyOutput()
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
            checkpoint.record(String.format("# burn start scale=%d minutes=%d shapes=%d", scale, budgetMinutes, corpus.size()));
            logger.info("burn run starting at scale {} for {} minute(s) across {} corpus shapes; checkpoint at {}",
                        scale, budgetMinutes, corpus.size(), checkpointPath.toAbsolutePath());

            do
            {
                round++;
                int shapeNum = 0;
                for (DifferentialSchema schema : corpus)
                {
                    shapeNum++;
                    ShapeStats shape = runShape(schema, scale);
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
            }
            while (deadlineNanos != 0 && System.nanoTime() < deadlineNanos);

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
     * not stay resident into the next shape. Returns the rows and bytes compacted for this shape.
     */
    private ShapeStats runShape(DifferentialSchema schema, int scale) throws Exception
    {
        ColumnFamilyStore cfs = writeAndGuardShape(schema, scale);
        ShapeStats stats = new ShapeStats(rowsOnDisk(cfs), bytesOnDisk(cfs), uncompressedBytesOnDisk(cfs));
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

        ShapeStats(long rows, long bytes, long uncompressedBytes)
        {
            this.rows = rows;
            this.bytes = bytes;
            this.uncompressedBytes = uncompressedBytes;
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
