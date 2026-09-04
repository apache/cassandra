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

import java.io.EOFException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Covers both value-copy paths of {@code SSTableCursorReader.copyCellContents}, which
 * {@link SSTableCursorReader#copyCellValue} calls.
 * <p>
 * Which path runs depends on the writer. A heap-backed {@code DataOutputBuffer} takes the
 * {@link DataOutputBuffer#readFully} fast path, which reads the value straight into the writer's
 * array. Anything else — another {@code DataOutputPlus}, or a {@code DataOutputBuffer} whose
 * buffer is direct and so has no array — takes the transfer-buffer loop below it. The
 * {@code hasArray()} half of that choice matters because {@code readFully} dereferences
 * {@code array()}/{@code arrayOffset()} unguarded, so a direct-backed writer on the fast path
 * would throw instead of copying. No production writer is direct-backed today (CursorCompactor's
 * temp cell buffers use the heap-backed {@code new DataOutputBuffer()}), so the loop is reached
 * only by a future consumer, or by these tests.
 * <p>
 * The two paths must also agree on failure. Both convert an {@code IOException} from the input
 * read into {@link CorruptSSTableException} and mark the sstable suspect, and neither may convert
 * anything else: a failure to grow the OUTPUT buffer is a defect in this process rather than
 * damaged data, and must not condemn a healthy sstable.
 */
public class SSTableCursorReaderCopyCellValueTest extends CQLTester
{
    /**
     * Long enough that dropping half of it off the end of the data file lands inside the value
     * bytes, rather than in one of the short fields that trail them.
     */
    private static final int LONG_VALUE_LENGTH = 4000;

    /** A DataOutputBuffer whose backing buffer is always direct, regardless of allocate_type. */
    private static class DirectDataOutputBuffer extends DataOutputBuffer
    {
        DirectDataOutputBuffer(int size)
        {
            super(size);
        }

        @Override
        protected ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocateDirect(size);
        }
    }

    @Test
    public void copyCellValueSucceedsOnDirectBackedWriter() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String expectedValue = "the value bytes that must survive the copy";
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 1L, expectedValue);
        flush();
        assertEquals("expected exactly one sstable", 1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        byte[] copied;
        try (DataOutputBuffer directWriter = new DirectDataOutputBuffer(16))
        {
            // This is the call under test: a direct-backed writer must not throw.
            copied = walkCopyingCellValues(sstable, directWriter);
        }

        // The cursor writes a vint length prefix ahead of a variable-length value (see
        // copyCellContents), the same format AbstractType.writeValue produces for the
        // iterator path.
        try (DataOutputBuffer expectedDob = new DataOutputBuffer())
        {
            UTF8Type.instance.writeValue(ByteBufferUtil.bytes(expectedValue), ByteBufferAccessor.instance, expectedDob);
            assertArrayEquals(expectedDob.toByteArray(), copied);
        }
    }

    /**
     * The {@code readFully} fast path: a value read that runs off the end of a truncated file is
     * damaged data, so it must surface as {@link CorruptSSTableException} AND leave the sstable
     * marked suspect, which is what keeps later compactions and reads away from it.
     */
    @Test
    public void truncatedValueIsCorruptionOnTheHeapWriterFastPath() throws Exception
    {
        SSTableReader sstable = writeOneLongValueThenTruncateInsideIt();
        try (DataOutputBuffer heapWriter = new DataOutputBuffer())
        {
            assertTrue("a plain DataOutputBuffer must be heap-backed, or this test drives the loop "
                       + "instead of the readFully fast path it names",
                       heapWriter.hasArray());
            assertWalkReportsCorruption(sstable, heapWriter);
        }
    }

    /**
     * The transfer-buffer loop, reached here through a direct-backed writer. Its failure contract
     * is the fast path's: the same {@code IOException}, the same
     * {@link CorruptSSTableException}, the same suspect mark. Only the copy mechanism differs.
     */
    @Test
    public void truncatedValueIsCorruptionOnTheTransferBufferLoop() throws Exception
    {
        SSTableReader sstable = writeOneLongValueThenTruncateInsideIt();
        try (DataOutputBuffer directWriter = new DirectDataOutputBuffer(16))
        {
            assertFalse("a direct-backed DataOutputBuffer must have no array, or this test drives "
                        + "the fast path instead of the loop it names",
                        directWriter.hasArray());
            assertWalkReportsCorruption(sstable, directWriter);
        }
    }

    private static void assertWalkReportsCorruption(SSTableReader sstable, DataOutputBuffer writer) throws Exception
    {
        assertFalse("the sstable must start clean, or the suspect assertion below proves nothing",
                    sstable.isMarkedSuspect());

        CorruptSSTableException tripped = null;
        try
        {
            walkCopyingCellValues(sstable, writer);
        }
        catch (CorruptSSTableException e)
        {
            tripped = e;
        }
        assertNotNull("a value read past the end of the file must trip CorruptSSTableException", tripped);
        // EOFException, not merely IOException: it pins that the SHORT READ of the value is what
        // failed. A structural or checksum failure elsewhere in the walk would satisfy a looser
        // assertion while covering neither copy path.
        assertTrue("expected the short read of the value as the cause, got: " + tripped.getCause(),
                   tripped.getCause() instanceof EOFException);
        assertTrue("corruptSSTable must mark the sstable suspect, or later compactions and reads "
                   + "keep selecting a file that cannot be read",
                   sstable.isMarkedSuspect());
    }

    /**
     * One row whose value is {@link #LONG_VALUE_LENGTH} bytes, flushed, then shortened by half
     * that. Compression is off so the data file holds the value bytes verbatim and the truncation
     * lands where it is aimed. The row header and cell header are ahead of the cut, so the walk
     * reaches the value read before it fails.
     */
    private SSTableReader writeOneLongValueThenTruncateInsideIt() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 1L, "v".repeat(LONG_VALUE_LENGTH));
        flush();
        assertEquals("expected exactly one sstable", 1, cfs.getLiveSSTables().size());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        truncateDataFile(sstable, LONG_VALUE_LENGTH / 2);
        return sstable;
    }

    private static void truncateDataFile(SSTableReader sstable, int bytesToDrop) throws Exception
    {
        try (RandomAccessFile file = new RandomAccessFile(sstable.getDataChannel().file().toJavaIOFile(), "rw"))
        {
            long shortened = file.length() - bytesToDrop;
            assertTrue("the cut must leave the row and cell headers on disk", shortened > 0);
            file.setLength(shortened);
        }
        if (ChunkCache.instance != null)
            ChunkCache.instance.invalidateFile(sstable.getDataChannel().file().toString());
    }

    /**
     * Drives the cursor over every cell of {@code sstable}, copying each value into {@code writer},
     * and returns the bytes of the last value copied. This is the same walk a merge consumer runs.
     */
    private static byte[] walkCopyingCellValues(SSTableReader sstable, DataOutputBuffer writer) throws Exception
    {
        try (SSTableCursorReader cursor = new SSTableCursorReader(sstable))
        {
            byte[] transfer = new byte[4096];
            PartitionDescriptor pHeader = new PartitionDescriptor(sstable.getPartitioner().createReusableKey(0));
            UnfilteredDescriptor rHeader = new UnfilteredDescriptor(sstable.header.clusteringTypes().toArray(AbstractType[]::new));

            int state = cursor.readPartitionHeader(pHeader);
            byte[] copied = null;
            while (state != DONE)
            {
                while (state != PARTITION_END)
                {
                    switch (state)
                    {
                        case STATIC_ROW_START: state = cursor.readStaticRowHeader(rHeader); break;
                        case ROW_START: state = cursor.readRowHeader(rHeader); break;
                        case TOMBSTONE_START: state = cursor.readTombstoneMarker(rHeader); break;
                        default: throw new IllegalStateException("state " + state);
                    }
                    while (state != UNFILTERED_END && state != PARTITION_END)
                    {
                        if (state == CELL_END) { state = cursor.continueReading(); continue; }
                        if (state != CELL_HEADER_START) break;
                        state = cursor.readCellHeader();
                        if (state == CELL_VALUE_START)
                        {
                            writer.clear();
                            state = cursor.copyCellValue(writer, transfer);
                            copied = writer.toByteArray();
                        }
                    }
                    if (state == UNFILTERED_END)
                        state = cursor.continueReading();
                }
                state = cursor.continueReading();
                if (state != DONE)
                    state = cursor.readPartitionHeader(pHeader);
            }
            return copied;
        }
    }
}
