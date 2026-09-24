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
import java.util.HashMap;
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
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Runs the whole {@link BasicDifferentialCompactionTest} corpus with the BTI format selected, so the
 * clustering-free table shape ({@link BasicDifferentialCompactionTest#noClusteringColumns}) is compacted
 * through the cursor path under BTI rather than only under the default BIG format.
 *
 * <p>The differential harness's slice read-back returns early for a zero-clustering table (its index has
 * no seek to route), so this class also reads the clustering-free cursor output back per partition through
 * an absolute point read and asserts the merged value, to flush out any latent bug in the clustering-free
 * BTI write/read path.
 */
public class BtiBasicDifferentialCompactionTest extends BasicDifferentialCompactionTest
{
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

    /** One partition's merged regular-column values, or {@code null} where the partition should not exist. */
    private static final class Expected
    {
        final long v1;
        final String v2;

        Expected(long v1, String v2)
        {
            this.v1 = v1;
            this.v2 = v2;
        }
    }

    /**
     * Compacts a clustering-free table through the cursor path under BTI, then reads every partition of the
     * single merged output back through an absolute point read (not the differential harness's slice
     * read-back, which skips this shape) and asserts each merged value is exactly what the two overlapping
     * inputs imply.
     */
    @Test
    public void clusteringFreeReadBackUnderBti() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint PRIMARY KEY, v1 bigint, v2 text)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        Map<Long, Expected> expected = new HashMap<>();

        // first sstable: pk 0..19
        for (long pk = 0; pk < 20; pk++)
        {
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", pk, pk, "v" + pk);
            expected.put(pk, new Expected(pk, "v" + pk));
        }
        flush();

        // second, newer sstable: pk 10..29 overwrites the overlap and adds pk 20..29
        for (long pk = 10; pk < 30; pk++)
        {
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", pk, pk + 100, "updated" + pk);
            expected.put(pk, new Expected(pk + 100, "updated" + pk));
        }
        flush();

        assertEquals("the scenario needs two overlapping sstables to merge", 2, cfs.getLiveSSTables().size());

        // commit a real cursor compaction under BTI; the live set becomes the single merged output
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        commitCompaction(cfs, cfs.getLiveSSTables(), true, gcBefore);

        assertEquals("cursor compaction should have produced a single output sstable",
                     1, cfs.getLiveSSTables().size());
        SSTableReader out = cfs.getLiveSSTables().iterator().next();
        // guards against a silent format fallback: the point read below must exercise BTI, not BIG
        assertOutputFormatIsSelected(out);

        TableMetadata metadata = out.metadata();
        ColumnMetadata v1 = metadata.getColumn(ByteBuffer.wrap("v1".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        ColumnMetadata v2 = metadata.getColumn(ByteBuffer.wrap("v2".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertNotNull("schema is missing column v1", v1);
        assertNotNull("schema is missing column v2", v2);

        ColumnFilter fetchAll = ColumnFilter.all(metadata);

        // absolute point read for every key that could exist, including the two just past the end that must not
        for (long pk = 0; pk < 32; pk++)
        {
            DecoratedKey key = cfs.decorateKey(LongType.instance.decompose(pk));
            Row row = readOnlyRow(out, key, fetchAll);
            Expected want = expected.get(pk);

            if (want == null)
            {
                assertNull("point read of pk " + pk + " returned a row, but the merge should hold no such partition " +
                           "in " + out.descriptor, row);
                continue;
            }

            assertNotNull("point read of pk " + pk + " returned no row from the merged BTI output " + out.descriptor,
                          row);

            Cell<?> cellV1 = row.getCell(v1);
            Cell<?> cellV2 = row.getCell(v2);
            assertNotNull("pk " + pk + ": v1 cell missing in merged BTI output", cellV1);
            assertNotNull("pk " + pk + ": v2 cell missing in merged BTI output", cellV2);

            assertEquals("pk " + pk + ": merged v1 wrong in BTI point read",
                         Long.valueOf(want.v1), LongType.instance.compose(cellV1.buffer()));
            assertEquals("pk " + pk + ": merged v2 wrong in BTI point read",
                         want.v2, UTF8Type.instance.compose(cellV2.buffer()));
        }
    }

    /** Reads exactly one partition of {@code sstable} through a full-partition point read; returns its lone row or null. */
    private static Row readOnlyRow(SSTableReader sstable, DecoratedKey key, ColumnFilter fetchAll)
    {
        try (UnfilteredRowIterator partition = sstable.rowIterator(key, Slices.ALL, fetchAll, false,
                                                                   SSTableReadsListener.NOOP_LISTENER))
        {
            Row found = null;
            int rows = 0;
            while (partition.hasNext())
            {
                Unfiltered unfiltered = partition.next();
                if (!unfiltered.isRow())
                    continue;
                rows++;
                found = (Row) unfiltered;
            }
            assertEquals("a clustering-free partition must hold exactly one row on a hit; pk read " + key +
                         " in " + sstable.descriptor, found == null ? 0 : 1, rows);
            return found;
        }
    }
}
