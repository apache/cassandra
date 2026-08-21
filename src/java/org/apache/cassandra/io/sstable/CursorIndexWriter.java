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

/**
 * Format-specific partition/row index production for {@link SSTableCursorWriter}.
 *
 * The seam is EVENT-shaped because {@link UnfilteredDescriptor}s are transient (reused per
 * row): implementations that need block-boundary clusterings must capture them at
 * {@link #rowWritten} time, or take them from a descriptor the write side owns, rather than
 * expecting one to still exist at a block boundary. The cursor writer owns all data-file
 * serialization and the open range-tombstone marker; implementations own everything about
 * their index format (BIG: promoted index blocks + Index.db entries + bloom filter/summary;
 * other formats own their own index structures accordingly).
 */
public abstract class CursorIndexWriter
{
    protected long partitionStart;
    // Offset within the current partition where the current index block begins. MUST be a
    // long, like the reference SortedTablePartitionWriter.indexBlockStartOffset: partitions
    // can exceed 2GiB, and an int here wraps negative past Integer.MAX_VALUE — the block-cut
    // arithmetic then cuts every row and the serialized offsets corrupt the promoted index
    // (pinned by CursorIndexWriterOffsetWidthTest, and exercised at scale by the giant-partition
    // boundary run, ck_stride = rows_per_sstable).
    protected long indexBlockStartOffset;

    public final void startPartition(long partitionStartPosition, long positionAfterHeader)
    {
        this.partitionStart = partitionStartPosition;
        reset();
        notePosition(positionAfterHeader);
    }

    /** Static rows reset the block clock without participating in row index blocks. */
    public final void staticRowWritten(long position)
    {
        notePosition(position);
    }

    public final long indexBlockStartOffset()
    {
        return indexBlockStartOffset;
    }

    protected final void notePosition(long endOfRowPosition)
    {
        indexBlockStartOffset = endOfRowPosition - partitionStart;
    }

    protected final long currentOffsetInPartition(long position)
    {
        return position - partitionStart;
    }

    /** Clear per-partition state. Called by {@link #startPartition}. */
    protected abstract void reset();

    /**
     * A non-static row or range tombstone marker was written to [rowStart, rowEnd).
     * The descriptor's clustering describes it; openMarker is the range deletion open at
     * the END of this unfiltered (LIVE if none).
     */
    public abstract void rowWritten(UnfilteredDescriptor descriptor, long rowStart, long rowEnd,
                                    DeletionTime openMarker) throws IOException;

    /**
     * The partition (including its end-of-partition marker) ends at partitionEnd.
     *
     * @param lastName the clustering of the last non-static unfiltered written to this partition, which a
     *                 trailing index block needs as its last name; null if the partition wrote none, in which
     *                 case there is no trailing block to cut.
     */
    public abstract void endPartition(byte[] key, int keyLength, int headerLength,
                                      DeletionTime partitionDeletionTime, long partitionEnd,
                                      ClusteringDescriptor lastName) throws IOException;
}
