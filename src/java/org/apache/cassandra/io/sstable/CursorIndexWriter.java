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
 * An {@link UnfilteredDescriptor} stays valid only during its own row. An implementation that
 * needs a clustering at a block boundary must copy that clustering during {@link #rowWritten}.
 *
 * The cursor writer owns data-file serialization and the open range-tombstone marker. An
 * implementation owns its index format.
 */
public abstract class CursorIndexWriter
{
    protected long partitionStart;
    // Offset of the current index block from the start of the current partition. Keep this a
    // long: a partition can exceed 2GiB. CursorIndexWriterOffsetWidthTest pins the width.
    protected long indexBlockStartOffset;

    public final void startPartition(long partitionStartPosition, long positionAfterHeader)
    {
        this.partitionStart = partitionStartPosition;
        reset();
        notePosition(positionAfterHeader);
    }

    /** No row index block holds a static row. The next block still starts at {@code position}. */
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

    /** Clear per-partition state. */
    protected abstract void reset();

    /**
     * The writer wrote a non-static row or a range tombstone marker to [rowStart, rowEnd).
     *
     * @param openMarker the range deletion open at the end of this unfiltered, or
     *                   {@link DeletionTime#LIVE} if no range deletion is open.
     */
    public abstract void rowWritten(UnfilteredDescriptor descriptor, long rowStart, long rowEnd,
                                    DeletionTime openMarker) throws IOException;

    /**
     * The partition ends at partitionEnd, which includes the end-of-partition marker.
     *
     * @param lastName the clustering of the last non-static unfiltered in this partition. A
     *                 trailing index block uses it as the block's last name. Null if the
     *                 partition wrote no non-static unfiltered, which leaves no trailing block
     *                 to cut.
     */
    public abstract void endPartition(byte[] key, int keyLength, int headerLength,
                                      DeletionTime partitionDeletionTime, long partitionEnd,
                                      ClusteringDescriptor lastName) throws IOException;
}
