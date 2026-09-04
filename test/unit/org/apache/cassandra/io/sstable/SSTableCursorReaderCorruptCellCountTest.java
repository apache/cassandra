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
package org.apache.cassandra.io.sstable;

import java.io.RandomAccessFile;
import java.lang.reflect.Field;

import org.junit.Test;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@code CellCursor.openNextColumnRun} reads a complex column's cell count with a silent
 * narrowing cast:
 * <pre>remainingCellsInColumn = (int) dataReader.readUnsignedVInt();</pre>
 * The reference ({@code UnfilteredSerializer.readComplexColumn}) and this same method's own cell
 * path length read (a few lines below) both use the checked {@code readUnsignedVInt32}, which
 * throws {@link VIntCoding.VIntOutOfRangeException} when a decoded value
 * does not fit in 32 bits. A count vint whose value has bit 31 set casts to a negative int, which
 * defeats the {@code while (remainingCellsInColumn == 0)} loop guard that clears the
 * present-column bit: no bit is ever cleared, {@code hasNext()} stays true forever, and the
 * column walk runs past the row body instead of failing at the damaged field.
 *
 * This test patches the on-disk cell-count vint of a real complex column to encode
 * {@code 0x80000000L} (5 bytes; casts to {@code Integer.MIN_VALUE}) and asserts that a cursor
 * read reports {@link CorruptSSTableException} whose cause is the range error, at the damaged
 * field, matching the reference's failure mode on the same kind of corruption.
 */
public class SSTableCursorReaderCorruptCellCountTest extends CQLTester
{
    /** The exact value ADV-001 uses: fits in 32 unsigned bits, but casts to a negative int. */
    private static final long MALICIOUS_COUNT = 0x80000000L;

    @Test
    public void corruptCellCountIsReportedAtTheDamagedField() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // A partial update of one element writes a cell and no complex deletion (see
        // ComplexColumnCursorReadTest.multiCellCollections), which keeps the row-body layout
        // simple: the column bitmap is followed immediately by the cell count vint, with no
        // deletion-time field ahead of it.
        execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", "k", 1L, 1L, 1L);
        flush();
        assertEquals("expected exactly one sstable", 1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        long cellCountOffset = findCellCountOffset(sstable);

        byte[] malicious = encodeMaliciousCount();
        // Sanity check on the encoding itself: this is the premise of the whole test.
        assertEquals("expected a 5-byte vint for a value whose low 32 bits have bit 31 set",
                     5, malicious.length);

        patchFile(sstable, cellCountOffset, malicious);

        CorruptSSTableException tripped = null;
        try
        {
            walkFully(sstable);
        }
        catch (CorruptSSTableException e)
        {
            tripped = e;
        }
        assertNotNull("a corrupt cell count must trip CorruptSSTableException", tripped);
        assertNotNull("the range-check diagnostic must be carried as the cause", tripped.getCause());
        assertTrue("expected VIntOutOfRangeException as the cause, got: " + tripped.getCause(),
                   tripped.getCause() instanceof VIntCoding.VIntOutOfRangeException);
    }

    /**
     * Drives a raw cursor up to {@code CELL_HEADER_START} for the row's sole column and returns
     * the file offset there. With no row-level complex deletion (guaranteed by the scenario
     * above), that offset is exactly where the cell-count vint of the complex column starts:
     * {@code readRowHeader} sets {@code unfilteredCellsStart} to this position right after the
     * column-presence bitmap and before any per-column data.
     */
    private static long findCellCountOffset(SSTableReader sstable) throws Exception
    {
        try (SSTableCursorReader cursor = new SSTableCursorReader(sstable))
        {
            PartitionDescriptor pHeader = new PartitionDescriptor(sstable.getPartitioner().createReusableKey(0));
            UnfilteredDescriptor rHeader = new UnfilteredDescriptor(sstable.header.clusteringTypes().toArray(AbstractType[]::new));

            int state = cursor.readPartitionHeader(pHeader);
            assertEquals("expected the single row to open directly", ROW_START, state);
            state = cursor.readRowHeader(rHeader);
            assertEquals("expected the row's sole column to be ready to read", CELL_HEADER_START, state);

            Field dataReaderField = SSTableCursorReader.class.getDeclaredField("dataReader");
            dataReaderField.setAccessible(true);
            RandomAccessReader dataReader = (RandomAccessReader) dataReaderField.get(cursor);
            return dataReader.getPosition();
        }
    }

    private static byte[] encodeMaliciousCount() throws Exception
    {
        try (DataOutputBuffer buf = new DataOutputBuffer())
        {
            VIntCoding.writeUnsignedVInt(MALICIOUS_COUNT, buf);
            return buf.toByteArray();
        }
    }

    private static void patchFile(SSTableReader sstable, long offset, byte[] replacement) throws Exception
    {
        try (RandomAccessFile file = new RandomAccessFile(sstable.getDataChannel().file().toJavaIOFile(), "rw"))
        {
            file.seek(offset);
            file.write(replacement);
        }
        if (ChunkCache.instance != null)
            ChunkCache.instance.invalidateFile(sstable.getDataChannel().file().toString());
    }

    /** Walks every cell of every partition, driving the reader exactly as a merge consumer does. */
    private static void walkFully(SSTableReader sstable) throws Exception
    {
        try (SSTableCursorReader cursor = new SSTableCursorReader(sstable))
        {
            PartitionDescriptor pHeader = new PartitionDescriptor(sstable.getPartitioner().createReusableKey(0));
            UnfilteredDescriptor rHeader = new UnfilteredDescriptor(sstable.header.clusteringTypes().toArray(AbstractType[]::new));
            byte[] transfer = new byte[4096];

            int state = cursor.readPartitionHeader(pHeader);
            while (state != SSTableCursorReader.State.DONE)
            {
                while (state != SSTableCursorReader.State.PARTITION_END)
                {
                    switch (state)
                    {
                        case SSTableCursorReader.State.STATIC_ROW_START: state = cursor.readStaticRowHeader(rHeader); break;
                        case SSTableCursorReader.State.ROW_START: state = cursor.readRowHeader(rHeader); break;
                        case SSTableCursorReader.State.TOMBSTONE_START: state = cursor.readTombstoneMarker(rHeader); break;
                        default: fail("unexpected state " + state);
                    }
                    while (state != SSTableCursorReader.State.UNFILTERED_END && state != SSTableCursorReader.State.PARTITION_END)
                    {
                        if (state == SSTableCursorReader.State.CELL_END) { state = cursor.continueReading(); continue; }
                        if (state != CELL_HEADER_START) break;
                        state = cursor.readCellHeader();
                        if (state == SSTableCursorReader.State.CELL_VALUE_START)
                        {
                            try (DataOutputBuffer sink = new DataOutputBuffer())
                            {
                                state = cursor.copyCellValue(sink, transfer);
                            }
                        }
                    }
                    if (state == SSTableCursorReader.State.UNFILTERED_END)
                        state = cursor.continueReading();
                }
                state = cursor.continueReading();
                if (state != SSTableCursorReader.State.DONE)
                    state = cursor.readPartitionHeader(pHeader);
            }
        }
    }
}
