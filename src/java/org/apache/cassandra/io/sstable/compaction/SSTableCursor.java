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

package org.apache.cassandra.io.sstable.compaction;

import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * An sstable cursor is an iterator-like object that is used to enumerate and merge the content of sstables.
 * It produces a stream of cells, broken up with row and partition boundaries -- doing this allows the merging to be
 * done using a single container and merger instead of the hierarchy used in UnfilteredPartitionIterator-
 * UnfilteredRowIterator-Row-ComplexColumn-Cell.
 *
 * There are two other important differences done to improve merging performance:
 *   - static rows are not special and are specified as normal rows with STATIC_CLUSTERING
 *   - cursors track the currently active range deletion
 *
 * More details about the design, functionality and performance of cursors can be found in the included cursors.md file.
 */
public interface SSTableCursor extends AutoCloseable
{
    /**
     * Enumeration of the type of item at the current position: either a cell, a range tombstone marker, a header of
     * some upper level of the hierarchy, or an end-of-stream.
     * This combines information about the object seen with information about its level in the logical hierarchy
     * (specified in the "level" field). The latter is key for comparing the position of two cursors which are iterated
     * together in a merge: if a cursor is positioned on a lower level in the hierarchy than another, it is listing
     * content in a group that is either exhausted or not opened in the other cursor, and in both cases the other
     * cursor's position must have a bigger higher-level key. That is, a cursor with a smaller level is always before
     * a cursor with a higher one (see {@link SSTableCursorMerger#mergeComparator}).
     */
    enum Type
    {
        COMPLEX_COLUMN_CELL(0),
        COMPLEX_COLUMN(1),
        SIMPLE_COLUMN(1),
        ROW(2),
        RANGE_TOMBSTONE(2),
        PARTITION(3),
        EXHAUSTED(4),
        UNINITIALIZED(-1);

        /** The actual level, used for comparisons (some types e.g. ROW and RANGE_TOMBSTONE share a level). */
        final int level;

        Type(int level)
        {
            this.level = level;
        }
    }

    /**
     * Advance the cursor and return the level of the next element. The returned level is the same as what level()
     * returns next.
     * Any errors during read should be converted to CorruptSSTableException.
     */
    Type advance();

    /**
     * The current level. UNINITIALIZED if iteration has not started, otherwise what the last advance() returned.
     */
    Type type();

    /**
     * Current partition key. Only valid if a partition is in effect, i.e. if level() <= PARTITION.
     */
    DecoratedKey partitionKey();

    /**
     * Partition level deletion. Only valid in a partition.
     */
    DeletionTime partitionLevelDeletion();

    /**
     * Current clustering key. Only valid if positioned within/on a row/unfiltered, i.e. if level() <= ROW.
     * For rows, this will be Clustering, and range tombstone markers will use prefix.
     */
    ClusteringPrefix<?> clusteringKey();

    /**
     * Liveness info for the current row's clustering key. Only valid within a row.
     */
    LivenessInfo clusteringKeyLivenessInfo();

    /**
     * Row level deletion. Only valid within a row or on a range deletion. In the latter case, reports the new
     * deletion being set.
     */
    DeletionTime rowLevelDeletion();

    /**
     * Currently open range deletion. This tracks the last set range deletion, LIVE if none has been seen.
     * If positioned on a range tombstone marker, this will report the _previous_ deletion.
     */
    DeletionTime activeRangeDeletion();

    /**
     * Metadata for the current column.  Only valid within/on a column, i.e. if
     * level() <= SIMPLE/COMPLEX_COLUMN.
     * In a merged complex column this may be different from cell().column() because it contains the most up-to-date
     * version while individual cell sources will report their own.
     */
    ColumnMetadata column();

    /**
     * Deletion of the current complex column. Only valid within/on a complex column, i.e. if
     * level() == COMPLEX_COLUMN[_CELL].
     */
    DeletionTime complexColumnDeletion();

    /**
     * Current cell. This may be a column or a cell within a complex column. Only valid if positioned on a cell,
     * which may be a simple column or a cell in a complex one, i.e. level() == SIMPLE_COLUMN or COMPLEX_COLUMN_CELL.
     */
    Cell<?> cell();

    /**
     * @return number of bytes processed. This should be used as progress indication together with bytesTotal.
     */
    long bytesProcessed();
    /**
     * @return number of bytes total. This should be used as progress indication together with bytesProcessed.
     */
    long bytesTotal();

    void close();

    static SSTableCursor empty()
    {
        return new SSTableCursor()
        {
            boolean initialized = false;

            public Type advance()
            {
                initialized = true;
                return Type.EXHAUSTED;
            }

            public Type type()
            {
                return initialized ? Type.EXHAUSTED : Type.UNINITIALIZED;
            }

            public DecoratedKey partitionKey()
            {
                return null;
            }

            public DeletionTime partitionLevelDeletion()
            {
                return null;
            }

            public ClusteringPrefix clusteringKey()
            {
                return null;
            }

            public LivenessInfo clusteringKeyLivenessInfo()
            {
                return null;
            }

            public DeletionTime rowLevelDeletion()
            {
                return null;
            }

            public DeletionTime activeRangeDeletion()
            {
                return null;
            }

            public DeletionTime complexColumnDeletion()
            {
                return null;
            }

            public ColumnMetadata column()
            {
                return null;
            }

            public Cell cell()
            {
                return null;
            }

            public long bytesProcessed()
            {
                return 0;
            }

            public long bytesTotal()
            {
                return 0;
            }

            public void close()
            {
                // nothing
            }
        };
    }
}
