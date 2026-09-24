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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Audit Gap 6: cursor compaction that reads one sstable format and writes the other.
 *
 * <p>{@link org.apache.cassandra.db.compaction.CursorCompactor#isSupported} gates only on the selected
 * (output) format; it never inspects the input sstables' formats. Every other differential test sets the
 * selected format in {@code @Before} before flushing inputs, so inputs and output always share a format.
 * An operator can instead flush inputs, then change {@code sstable.selected_format}, then compact — so
 * BTI-in/BIG-out and BIG-in/BTI-out are real, otherwise-unexercised paths.
 *
 * <p>Each scenario flushes overlapping inputs under one format, switches the selected format, then compacts.
 * The differential tests assert the cursor and iterator paths agree byte-for-byte and that the output is the
 * newly selected format. The read-back tests additionally re-read every merged partition through an absolute
 * point read. The key-cache test drives the BIG-output / BTI-input direction with key-cache migration on, so
 * {@link org.apache.cassandra.io.sstable.format.big.BigTableWriter}'s shouldCacheKey sees non-KeyCacheSupport
 * (BTI) originals.
 */
public class CrossFormatCursorCompactionTest extends DifferentialCompactionTester
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void rememberFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    /** Flushes two overlapping inputs under {@code inputFormat}, verifying they landed in that format. */
    private ColumnFamilyStore twoOverlappingInputs(String inputFormat) throws Exception
    {
        DatabaseDescriptor.setSelectedSSTableFormat(inputFormat);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // first sstable: pk 0..9
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 20; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, "v" + ck);
        flush();

        // second, newer sstable: pk 5..14 overwrites the overlap and adds new partitions
        for (long pk = 5; pk < 15; pk++)
            for (long ck = 0; ck < 20; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck + 1000, "updated" + ck);
        flush();

        assertEquals("scenario needs two overlapping sstables to merge", 2, cfs.getLiveSSTables().size());
        for (SSTableReader in : cfs.getLiveSSTables())
            assertEquals("input sstable was not flushed in the intended format " + in.descriptor,
                         inputFormat, in.descriptor.getFormat().name());
        return cfs;
    }

    /**
     * BTI inputs, BIG output. Runs both compaction paths and asserts they agree byte-for-byte, and that the
     * output is BIG (the newly selected format), not a silent fall-through to the input's BTI format.
     */
    @Test
    public void btiInputsBigOutputDifferential() throws Exception
    {
        ColumnFamilyStore cfs = twoOverlappingInputs("bti");
        DatabaseDescriptor.setSelectedSSTableFormat("big");

        CapturedOutput out = assertCursorMatchesIterator(cfs);
        assertEquals("expected a single merged output", 1, out.sstables.size());
    }

    /**
     * BIG inputs, BTI output. Runs both compaction paths and asserts they agree byte-for-byte, and that the
     * output is BTI (the newly selected format), not a silent fall-through to the input's BIG format.
     */
    @Test
    public void bigInputsBtiOutputDifferential() throws Exception
    {
        ColumnFamilyStore cfs = twoOverlappingInputs("big");
        DatabaseDescriptor.setSelectedSSTableFormat("bti");

        CapturedOutput out = assertCursorMatchesIterator(cfs);
        assertEquals("expected a single merged output", 1, out.sstables.size());
    }

    /** BTI inputs, BIG output: commit a real cursor compaction and read every merged row back absolutely. */
    @Test
    public void btiInputsBigOutputReadBack() throws Exception
    {
        assertCrossFormatReadBack("bti", "big");
    }

    /** BIG inputs, BTI output: commit a real cursor compaction and read every merged row back absolutely. */
    @Test
    public void bigInputsBtiOutputReadBack() throws Exception
    {
        assertCrossFormatReadBack("big", "bti");
    }

    /**
     * Flushes inputs under {@code inputFormat}, selects {@code outputFormat}, commits a cursor-only compaction,
     * asserts the output is {@code outputFormat}, and reads every merged partition/row back through an absolute
     * point read comparing the exact merged values the two overlapping inputs imply.
     */
    private void assertCrossFormatReadBack(String inputFormat, String outputFormat) throws Exception
    {
        ColumnFamilyStore cfs = twoOverlappingInputs(inputFormat);
        DatabaseDescriptor.setSelectedSSTableFormat(outputFormat);

        // expected merged state: pk 0..4 from first input, pk 5..14 from the newer input
        Map<Long, Long> expectedV1 = new HashMap<>();
        Map<Long, String> expectedV2 = new HashMap<>();
        for (long pk = 0; pk < 15; pk++)
            for (long ck = 0; ck < 20; ck++)
            {
                long key = pk * 100 + ck;
                if (pk >= 5)
                {
                    expectedV1.put(key, ck + 1000);
                    expectedV2.put(key, "updated" + ck);
                }
                else
                {
                    expectedV1.put(key, ck);
                    expectedV2.put(key, "v" + ck);
                }
            }

        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        commitCompaction(cfs, cfs.getLiveSSTables(), true, gcBefore);

        assertEquals("cursor compaction should have produced a single merged output",
                     1, cfs.getLiveSSTables().size());
        SSTableReader output = cfs.getLiveSSTables().iterator().next();
        assertEquals("cursor output was written in the wrong format: expected the newly selected " +
                     outputFormat + " but got " + output.descriptor.getFormat().name() +
                     " (silent fall-through to the input format?)",
                     outputFormat, output.descriptor.getFormat().name());
        assertOutputFormatIsSelected(output);

        TableMetadata metadata = output.metadata();
        ColumnMetadata v1 = metadata.getColumn(ByteBuffer.wrap("v1".getBytes(StandardCharsets.UTF_8)));
        ColumnMetadata v2 = metadata.getColumn(ByteBuffer.wrap("v2".getBytes(StandardCharsets.UTF_8)));
        assertNotNull("schema is missing column v1", v1);
        assertNotNull("schema is missing column v2", v2);
        ColumnFilter fetchAll = ColumnFilter.all(metadata);

        int rowsChecked = 0;
        for (long pk = 0; pk < 16; pk++)
        {
            DecoratedKey key = cfs.decorateKey(LongType.instance.decompose(pk));
            List<Row> rows = readPartitionRows(output, key, fetchAll);

            if (pk >= 15)
            {
                assertTrue("point read of pk " + pk + " returned rows, but the merge holds no such " +
                           "partition in " + output.descriptor, rows.isEmpty());
                continue;
            }
            assertEquals("wrong row count for pk " + pk + " in merged " + outputFormat + " output", 20, rows.size());
            for (Row row : rows)
            {
                long ck = LongType.instance.compose(row.clustering().bufferAt(0));
                long ekey = pk * 100 + ck;
                Cell<?> cellV1 = row.getCell(v1);
                Cell<?> cellV2 = row.getCell(v2);
                assertNotNull("pk " + pk + " ck " + ck + ": v1 cell missing in merged " + outputFormat + " output", cellV1);
                assertNotNull("pk " + pk + " ck " + ck + ": v2 cell missing in merged " + outputFormat + " output", cellV2);
                assertEquals("pk " + pk + " ck " + ck + ": merged v1 wrong in " + outputFormat + " point read",
                             expectedV1.get(ekey), (Long) LongType.instance.compose(cellV1.buffer()));
                assertEquals("pk " + pk + " ck " + ck + ": merged v2 wrong in " + outputFormat + " point read",
                             expectedV2.get(ekey), UTF8Type.instance.compose(cellV2.buffer()));
                rowsChecked++;
            }
        }
        assertEquals("scenario must have read back every merged row", 15 * 20, rowsChecked);
    }

    /**
     * BIG output built from BTI (non-KeyCacheSupport) inputs, with key-cache migration ON. Exercises
     * {@link org.apache.cassandra.io.sstable.format.big.BigTableWriter}'s shouldCacheKey with originals that
     * are not KeyCacheSupport: it must not crash, must migrate nothing (BTI originals hold no cached
     * positions), and the BIG output must round-trip.
     */
    @Test
    public void bigOutputFromBtiInputsWithKeyCacheMigration() throws Exception
    {
        boolean originalMigrate = DatabaseDescriptor.shouldMigrateKeycacheOnCompaction();
        long originalKeyCacheCapacity = CacheService.instance.keyCache.getCapacity();
        try
        {
            DatabaseDescriptor.setSelectedSSTableFormat("bti");
            createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                        "WITH compression = {'enabled': 'false'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();

            int partitions = 100;
            String padding = "v".repeat(200);
            for (int round = 0; round < 2; round++)
            {
                for (int pk = 0; pk < partitions; pk++)
                    for (int ck = 0; ck < 8; ck++)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck + round * 8, padding);
                flush();
            }
            assertTrue("fixture needs at least two BTI inputs", cfs.getLiveSSTables().size() >= 2);
            for (SSTableReader in : cfs.getLiveSSTables())
            {
                assertEquals("input must be BTI for this scenario", "bti", in.descriptor.getFormat().name());
                assertFalse("BTI inputs must not be KeyCacheSupport, which is the whole point of this scenario",
                            in instanceof KeyCacheSupport<?>);
            }

            // migration ON and an enabled key cache
            DatabaseDescriptor.setMigrateKeycacheOnCompaction(true);
            if (originalKeyCacheCapacity == 0)
                CacheService.instance.keyCache.setCapacity(1L << 20);
            CacheService.instance.invalidateKeyCache();

            // warm reads: for BTI inputs these cache nothing, but they are the reads shouldCacheKey would consult
            for (int pk = 0; pk < partitions; pk++)
                execute("SELECT * FROM %s WHERE pk = ?", pk);

            // now switch the OUTPUT to BIG and commit a cursor compaction
            DatabaseDescriptor.setSelectedSSTableFormat("big");
            long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
            commitCompaction(cfs, cfs.getLiveSSTables(), true, gcBefore);

            assertEquals("expected a single merged output", 1, cfs.getLiveSSTables().size());
            SSTableReader output = cfs.getLiveSSTables().iterator().next();
            assertEquals("output must be BIG", "big", output.descriptor.getFormat().name());
            assertOutputFormatIsSelected(output);

            // BTI originals held nothing in the key cache, so nothing should have migrated, but the compaction
            // must have completed without a crash and every partition must read back.
            int migrated = 0;
            ColumnFilter fetchAll = ColumnFilter.all(output.metadata());
            for (int pk = 0; pk < partitions; pk++)
            {
                DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(pk));
                if (((KeyCacheSupport<?>) output).getCachedPosition(key, false) != null)
                    migrated++;
                List<Row> rows = readPartitionRows(output, key, fetchAll);
                assertEquals("pk " + pk + " lost rows through the cross-format BIG output", 16, rows.size());
            }
            assertEquals("no BTI original is KeyCacheSupport, so shouldCacheKey must have migrated nothing",
                         0, migrated);
        }
        finally
        {
            DatabaseDescriptor.setMigrateKeycacheOnCompaction(originalMigrate);
            CacheService.instance.keyCache.setCapacity(originalKeyCacheCapacity);
            CacheService.instance.invalidateKeyCache();
        }
    }

    /** Reads one partition of {@code sstable} through a full-partition point read; returns its rows in order. */
    private static List<Row> readPartitionRows(SSTableReader sstable, DecoratedKey key, ColumnFilter fetchAll)
    {
        List<Row> rows = new ArrayList<>();
        try (UnfilteredRowIterator partition = sstable.rowIterator(key, Slices.ALL, fetchAll, false,
                                                                   SSTableReadsListener.NOOP_LISTENER))
        {
            while (partition.hasNext())
            {
                Unfiltered unfiltered = partition.next();
                if (unfiltered.isRow())
                    rows.add((Row) unfiltered);
            }
        }
        return rows;
    }
}
