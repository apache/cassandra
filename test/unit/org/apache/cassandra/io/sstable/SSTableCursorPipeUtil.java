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

import java.io.IOException;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ReusableCellLivenessInfo;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;

public class SSTableCursorPipeUtil
{

    public static void copySSTable(SSTableCursorReader reader, SSTableCursorWriter writer) throws Throwable
    {
        PartitionDescriptor pHeader = new PartitionDescriptor(reader.ssTableReader().getPartitioner().createReusableKey(0));
        UnfilteredDescriptor unfilteredDescriptor = new UnfilteredDescriptor(reader.ssTableReader().header.clusteringTypes());
        ClusteringDescriptor lastWritten = new ClusteringDescriptor(reader.ssTableReader().header.clusteringTypes());
        int readerState = PARTITION_START;
        boolean first = true;

        while (readerState != DONE)
        {
            readerState = reader.readPartitionHeader(pHeader);
            if (first)
            {
                first = false;
                writer.setFirst(pHeader.keyBuffer());
            }
            readerState = copyPartition(reader, writer, pHeader, unfilteredDescriptor, lastWritten, readerState);
        }
    }

    /**
     * @param lastWritten holds the clustering of the last row or marker written. The reader can skip a
     * row the dropped-column filter leaves empty, and the descriptor then still holds the skipped row.
     * If the filter leaves every row of a partition empty, the partition is still written, with no rows; the
     * iterator path writes no partition then.
     */
    public static int copyPartition(SSTableCursorReader reader, SSTableCursorWriter writer, PartitionDescriptor pHeader, UnfilteredDescriptor unfilteredDescriptor, ClusteringDescriptor lastWritten, int readerState) throws IOException
    {
        if (readerState != STATIC_ROW_START &&
            readerState != ROW_START &&
            readerState != TOMBSTONE_START &&
            readerState != PARTITION_END)
            throw new IllegalStateException();

        final byte[] keyBytes = pHeader.keyBytes();
        final int keyLength = pHeader.keyLength();
        final DeletionTime pDeletionTime = pHeader.deletionTime();

        int headerLength = writer.writePartitionStart(keyBytes, keyLength, pDeletionTime);
        int unfilteredCounter = 0;
        while (readerState != PARTITION_END)
        {
            switch (readerState)
            {
                case STATIC_ROW_START:
                    readerState = copyStaticRow(reader, writer, unfilteredDescriptor);
                    headerLength = (int) (writer.getPosition() - writer.getPartitionStart());
                    break;
                case ROW_START:
                    long rowStart = writer.getPosition();
                    readerState = copyRow(reader, writer, unfilteredDescriptor, unfilteredCounter);
                    // count the row only if it was written, not if the reader skipped it
                    if (writer.getPosition() != rowStart)
                    {
                        unfilteredCounter++;
                        lastWritten.copy(unfilteredDescriptor);
                    }
                    break;
                case TOMBSTONE_START:
                    readerState = copyRangeTombstone(reader, writer, unfilteredDescriptor, unfilteredCounter++);
                    lastWritten.copy(unfilteredDescriptor);
            }
        }
        // the last clustering is recorded before the end of partition is written, as the iterator path does
        if (unfilteredCounter > 1)
            writer.updateClusteringMetadata(lastWritten);
        writer.writePartitionEnd(pHeader.key(), pDeletionTime, headerLength,
                                 unfilteredCounter > 0 ? lastWritten : null);
        return reader.continueReading();
    }

    public static int copyStaticRow(SSTableCursorReader reader, SSTableCursorWriter writer, UnfilteredDescriptor unfilteredDescriptor) throws IOException
    {
        int readerState = reader.readStaticRowHeader(unfilteredDescriptor);
        return copyRowAfterDescriptor(reader, writer, unfilteredDescriptor, readerState, true, false);
    }

    public static int copyRow(SSTableCursorReader reader, SSTableCursorWriter writer, UnfilteredDescriptor unfilteredDescriptor, int unfilteredIndex) throws IOException
    {
        int readerState = reader.readRowHeader(unfilteredDescriptor);
        // the reader skipped a row the dropped-column filter left empty
        if (readerState != CELL_HEADER_START && readerState != UNFILTERED_END)
            return readerState;
        return copyRowAfterDescriptor(reader, writer, unfilteredDescriptor, readerState, false, unfilteredIndex == 0);
    }

    public static int copyRangeTombstone(SSTableCursorReader reader, SSTableCursorWriter writer, UnfilteredDescriptor unfilteredDescriptor, int unfilteredIndex) throws IOException
    {
        reader.readTombstoneMarker(unfilteredDescriptor);
        writer.writeRangeTombstone(unfilteredDescriptor, unfilteredIndex == 0);
        return reader.continueReading();
    }

    private final static byte[] copyColumnValueBuffer = new byte[4096]; // used to copy cell contents (maybe piecemeal if very large, since we don't have a direct read option)

    public static int copyRowAfterDescriptor(SSTableCursorReader reader, SSTableCursorWriter writer, UnfilteredDescriptor unfilteredDescriptor, int readerState, boolean isStatic, boolean updateClusteringMetadata) throws IOException
    {
        writer.writeRowStart(unfilteredDescriptor.livenessInfo(), unfilteredDescriptor.deletionTime(),
                             unfilteredDescriptor.isShadowableDeletion(), isStatic);

        // Copy cells
        while (readerState != UNFILTERED_END)
        {
            if (readerState != CELL_HEADER_START)
                throw new IllegalStateException("Unexpected reader state: " + readerState);
            readerState = reader.readCellHeader();
            SSTableCursorReader.CellCursor cellCursor = reader.cellCursor();

            /**
             * {@link Cell.Serializer#serialize}
             */
            int cellFlags = cellCursor.cellFlags;
            ReusableCellLivenessInfo cellLiveness = cellCursor.cellLiveness;
            writer.writeCellHeader(cellFlags, cellLiveness, cellCursor.cellColumn);
            if (readerState == CELL_VALUE_START)
            {
                readerState = writer.writeCellValue(reader, copyColumnValueBuffer);
            }
            else if (Cell.Serializer.hasValue(cellFlags))
            {
                throw new IllegalStateException("Flags and state contradict");
            }
            if (readerState != CELL_END)
                throw new IllegalStateException("Expect CELL_END after cell read. State: " + readerState);

            readerState = reader.continueReading();
        }

        writer.writeRowEnd(unfilteredDescriptor, updateClusteringMetadata);

        return reader.continueReading();
    }
}
