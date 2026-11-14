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
import java.util.function.BiFunction;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.dht.ReusableDecoratedKey;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;

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
    /** Builds the index writer for one table writer. See {@link org.apache.cassandra.io.sstable.format.SSTableFormat#cursorIndexWriterFactory}. */
    public interface Factory
    {
        /**
         * The class of table writer that {@link #create} takes. Cursor compaction falls back to the
         * iterator path, before it opens any cursor, unless this is a {@link SortedTableWriter}.
         */
        Class<? extends SSTableWriter> writerClass();

        CursorIndexWriter create(SortedTableWriter<?, ?> writer, SerializationHeader header);

        /**
         * A factory for table writers of class {@code writerClass}. {@link #create} checks the
         * writer's class with {@link #checkWriterType} before it calls {@code create}.
         *
         * @param format the name of the format, for the message if the writer has another class
         */
        static <W extends SortedTableWriter<?, ?>> Factory of(String format, Class<W> writerClass,
                                                              BiFunction<W, SerializationHeader, CursorIndexWriter> create)
        {
            return new Factory()
            {
                @Override
                public Class<W> writerClass()
                {
                    return writerClass;
                }

                @Override
                public CursorIndexWriter create(SortedTableWriter<?, ?> writer, SerializationHeader header)
                {
                    return create.apply(checkWriterType(format, writer, writerClass), header);
                }
            };
        }
    }

    /**
     * Returns {@code writer} as a {@code type}, or throws if it is not one. Cursor compaction calls this
     * once per sstable writer, so a format whose writer class does not match fails with a clear message.
     *
     * @param format the name of the format whose cursor compaction needs the {@code type}
     */
    public static <T> T checkWriterType(String format, SSTableWriter writer, Class<T> type)
    {
        if (!type.isInstance(writer))
            throw new IllegalStateException(String.format("cursor compaction for format %s needs a %s, but the writer is a %s",
                                                          format, type.getName(), writer.getClass().getName()));
        return type.cast(writer);
    }

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
     * The partition's data ends at partitionEnd, which is past its end-of-partition marker. Writes what is
     * left of the partition's row index. The cursor writer calls this before it adds the partition to the
     * metadata, as {@link SortedTableWriter} does, because a BTI early open can run from inside this write.
     * This does nothing for a format that keeps the row index in memory until {@link #endPartition}.
     *
     * @param lastName the clustering of the last non-static unfiltered written to this
     *                 partition, or null if the partition wrote none.
     */
    public void finishRowIndex(long partitionEnd, ClusteringDescriptor lastName) throws IOException
    {
    }

    /**
     * The partition ends at partitionEnd, which is past its end-of-partition marker. Called after
     * {@link #finishRowIndex}, once the partition is in the metadata.
     *
     * @param key the partition key; a reusable instance, whose bytes and token the next partition
     *            overwrites. Anything that keeps it past this call, such as the index summary's
     *            readable boundary, the key cache or the BTI partition index, takes retainable().
     * @param lastName the clustering of the last non-static unfiltered written to this
     *                 partition, or null if the partition wrote none.
     */
    public abstract void endPartition(ReusableDecoratedKey key, byte[] keyBytes, int keyLength,
                                      int headerLength, DeletionTime partitionDeletionTime,
                                      long partitionEnd, ClusteringDescriptor lastName) throws IOException;

    /**
     * Releases the per-instance index state. The table writer that built this calls it in its
     * transaction cleanup, on commit and on abort. The underlying file writers belong to the table
     * writer, so an implementation must not close them.
     */
    public void close()
    {
    }
}
