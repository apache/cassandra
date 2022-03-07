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
 * Wrapper that skips empty data. This is done by only reporting a header (complex column/row/partition) with no
 * deletion or timestamp after valid content (cell, range tombstone, header with deletion/timestamp) is found; if no
 * such content is found, the source is advanced to the next header without reporting the empty block to the consumer.
 *
 * In other words, in response to a single advance() call, this will take multiple steps in the source cursor until it
 * reaches content, and when it does it reports the highest-level header it has had to go through to reach that content.
 * On the next advance() call it will descend one level in the logical hierarchy and report the next header. This
 * repeats until it reaches the level of the content. After reporting the level of the non-empty content, the next
 * advance() call will repeat the procedure, starting with searching the input for non-empty content.
 *
 * Here's a sample evolution of input and output:
 *                  [input]               [output]
 *                  UNINITIALIZED         UNINITIALIZED
 *                  PARTITION Pa
 *                  ROW Raa
 *                  SIMPLE_COLUMN Caa     PARTITION Pa
 *                                        ROW Raa
 *                                        SIMPLE_COLUMN Caa
 *                  ROW Rab
 *                  SIMPLE_COLUMN Cab     ROW Rab
 *                                        SIMPLE_COLUMN Cab
 *                  PARTITION Pb
 *                  ROW Rba
 *                  PARTITION Pc
 *                  RANGE_TOMBSTONE Tca   PARTITION Pc
 *                                        RANGE_TOMBSTONE Tca
 *                  RANGE_TOMBSTONE Tcb   RANGE_TOMBSTONE Tcb
 *                  EXHAUSTED             EXHAUSTED
 *
 * To report a header it is sufficient to just issue its level and pass on the data from the wrapped cursor, because
 * cursors always make the upper-level data (e.g. partition key) available while they advance within that level.
 */
public class SkipEmptyDataCursor implements SSTableCursor
{
    private final SSTableCursor wrapped;
    private Type type = Type.UNINITIALIZED;

    public SkipEmptyDataCursor(SSTableCursor wrapped)
    {
        this.wrapped = wrapped;
    }

    public Type advance()
    {
        Type current = wrapped.type();
        if (current != type)
            return type = advanceOurLevel(current);

        type = wrapped.advance();
        while (true)
        {
            switch (type)
            {
                case EXHAUSTED:
                    return type;
                case SIMPLE_COLUMN:
                case COMPLEX_COLUMN_CELL:
                case RANGE_TOMBSTONE:
                    // we are good, we have content
                    return type;
                case COMPLEX_COLUMN:
                    if (!complexColumnDeletion().isLive())
                        return type;   // we have to report this column even without any cells
                    if (advanceToComplexCell())
                        return type; // we found a cell and should now report the column
                    // There is no cell of this complex column. We may have advance to another column, row or partition.
                    break;
                case ROW:
                    if (!rowLevelDeletion().isLive() || !clusteringKeyLivenessInfo().isEmpty())
                        return type;   // we have to report this row even without any columns
                    if (advanceToColumn())
                        return type;   // we have reached a cell, but we must still report the row
                    // There is no cell. We may have advanced to a new row or new partition.
                    break;
                case PARTITION:
                    if (!partitionLevelDeletion().isLive())
                        return type;   // we have to report this partition even without any content
                    if (advanceToNonEmptyRow())
                        return type;   // We have reached a cell (or RT). We must report the partition, then the row.
                                       // The wrapped cursor still returns their information (pkey, ckey etc.)
                    // No rows or all empty. We must have advanced to new partition or exhausted.
                    break;
                default:
                    throw new AssertionError();
            }
            type = wrapped.type();
        }
    }

    /**
     * Called to report content that has been advanced to, but not yet reported. This will descend one level in the
     * logical hierarchy towards the target and return the resulting position.
     * For example, on seeing a row header we first advance to a non-empty cell and report the header, and on the
     * following advance() call report the cell we have advanced to. This method takes care of the latter part.
     */
    private Type advanceOurLevel(Type target)
    {
        switch (type)
        {
            case PARTITION:
                switch (target)
                {
                    case COMPLEX_COLUMN_CELL:
                    case SIMPLE_COLUMN:
                    case COMPLEX_COLUMN:
                        return Type.ROW;
                    case ROW:
                    case RANGE_TOMBSTONE:
                        return target;
                    default:
                        throw new AssertionError();
                }
            case ROW:
                switch (target)
                {
                    case COMPLEX_COLUMN_CELL:
                        return Type.COMPLEX_COLUMN;
                    case SIMPLE_COLUMN:
                    case COMPLEX_COLUMN:
                        return target;
                    default:
                        throw new AssertionError();
                }
            case COMPLEX_COLUMN:
                switch (target)
                {
                    case COMPLEX_COLUMN_CELL:
                        return target;
                    default:
                        throw new AssertionError();
                }

            default:
                // can't have any differences in any other case
                throw new AssertionError();
        }
    }

    private boolean advanceToComplexCell()
    {
        Type current = wrapped.advance();
        switch (current)
        {
            case COMPLEX_COLUMN_CELL:
                return true;
            case SIMPLE_COLUMN:
            case COMPLEX_COLUMN:
            case ROW:
            case RANGE_TOMBSTONE:
            case PARTITION:
            case EXHAUSTED:
                return false;
            default:
                throw new AssertionError();
        }
    }

    private boolean advanceToColumn()
    {
        Type current = wrapped.advance();
        while (true)
        {
            switch (current)
            {
                case SIMPLE_COLUMN:
                    return true;
                case COMPLEX_COLUMN:
                    if (!complexColumnDeletion().isLive())
                        return true;
                    if (advanceToComplexCell())
                        return true;
                    // There is no cell, skip this complex column. We may have a new column, or a new partition or row.
                    break;
                case ROW:
                case RANGE_TOMBSTONE:
                case PARTITION:
                case EXHAUSTED:
                    return false;
                case COMPLEX_COLUMN_CELL:
                    // can't jump directly to cell without going through COMPLEX_COLUMN
                default:
                    throw new AssertionError();
            }
            current = wrapped.type();
        }
    }

    private boolean advanceToNonEmptyRow()
    {
        Type current = wrapped.advance();
        while (true)
        {
            switch (current)
            {
                case RANGE_TOMBSTONE:
                    // we have content
                    return true;
                case ROW:
                    if (!rowLevelDeletion().isLive() || !clusteringKeyLivenessInfo().isEmpty())
                        return true;   // we have to report this row even without any cells
                    if (advanceToColumn())
                        return true;
                    // There is no column. We may have advanced to a new row or new partition.
                    break;
                case PARTITION:
                case EXHAUSTED:
                    return false;
                case SIMPLE_COLUMN:
                case COMPLEX_COLUMN:
                case COMPLEX_COLUMN_CELL:
                    // Can't jump directly from partition to cell.
                default:
                    throw new AssertionError();
            }
            current = wrapped.type();
        }
    }

    public Type type()
    {
        return type;
    }

    public DecoratedKey partitionKey()
    {
        return wrapped.partitionKey();
    }

    public DeletionTime partitionLevelDeletion()
    {
        return wrapped.partitionLevelDeletion();
    }

    public ClusteringPrefix clusteringKey()
    {
        return wrapped.clusteringKey();
    }

    public LivenessInfo clusteringKeyLivenessInfo()
    {
        return wrapped.clusteringKeyLivenessInfo();
    }

    public DeletionTime rowLevelDeletion()
    {
        return wrapped.rowLevelDeletion();
    }

    public DeletionTime activeRangeDeletion()
    {
        return wrapped.activeRangeDeletion();
    }

    public DeletionTime complexColumnDeletion()
    {
        return wrapped.complexColumnDeletion();
    }

    public ColumnMetadata column()
    {
        return wrapped.column();
    }

    public Cell cell()
    {
        return wrapped.cell();
    }

    public long bytesProcessed()
    {
        return wrapped.bytesProcessed();
    }

    public long bytesTotal()
    {
        return wrapped.bytesTotal();
    }

    public void close()
    {
        wrapped.close();
    }
}
