/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.util.NoSuchElementException;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;

public class EmptyIterators
{

    private static class EmptyBasePartitionIterator<R extends BaseRowIterator<?>> implements BasePartitionIterator<R>
    {
        EmptyBasePartitionIterator()
        {
        }

        public void close()
        {
        }

        public boolean hasNext()
        {
            return false;
        }

        public R next()
        {
            throw new NoSuchElementException();
        }
    }

    private static class EmptyUnfilteredPartitionIterator extends EmptyBasePartitionIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        final TableMetadata metadata;

        public EmptyUnfilteredPartitionIterator(TableMetadata metadata)
        {
            this.metadata = metadata;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }
    }

    private static class EmptyPartitionIterator extends EmptyBasePartitionIterator<RowIterator> implements PartitionIterator
    {
        public static final EmptyPartitionIterator instance = new EmptyPartitionIterator();
        private EmptyPartitionIterator()
        {
            super();
        }
    }

    private static class EmptyBaseRowIterator<U extends Unfiltered> implements BaseRowIterator<U>
    {
        final RegularAndStaticColumns columns;
        final TableMetadata metadata;
        final DecoratedKey partitionKey;
        final boolean isReverseOrder;
        final Row staticRow;

        EmptyBaseRowIterator(RegularAndStaticColumns columns, TableMetadata metadata, DecoratedKey partitionKey, boolean isReverseOrder, Row staticRow)
        {
            this.columns = columns;
            this.metadata = metadata;
            this.partitionKey = partitionKey;
            this.isReverseOrder = isReverseOrder;
            this.staticRow = staticRow;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public boolean isReverseOrder()
        {
            return isReverseOrder;
        }

        public RegularAndStaticColumns columns()
        {
            return columns;
        }

        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        public void close()
        {
        }

        public boolean isEmpty()
        {
            return staticRow == Rows.EMPTY_STATIC_ROW;
        }

        public boolean hasNext()
        {
            return false;
        }

        public U next()
        {
            throw new NoSuchElementException();
        }
    }

    private static class EmptyUnfilteredRowIterator extends EmptyBaseRowIterator<Unfiltered> implements UnfilteredRowIterator
    {
        final DeletionTime partitionLevelDeletion;
        public EmptyUnfilteredRowIterator(RegularAndStaticColumns columns, TableMetadata metadata, DecoratedKey partitionKey,
                                          boolean isReverseOrder, Row staticRow, DeletionTime partitionLevelDeletion)
        {
            super(columns, metadata, partitionKey, isReverseOrder, staticRow);
            this.partitionLevelDeletion = partitionLevelDeletion;
        }

        public boolean isEmpty()
        {
            return partitionLevelDeletion == DeletionTime.LIVE && super.isEmpty();
        }

        public DeletionTime partitionLevelDeletion()
        {
            return partitionLevelDeletion;
        }

        public EncodingStats stats()
        {
            return EncodingStats.NO_STATS;
        }
    }

    private static class EmptyRowIterator extends EmptyBaseRowIterator<Row> implements RowIterator
    {
        public EmptyRowIterator(TableMetadata metadata, DecoratedKey partitionKey, boolean isReverseOrder, Row staticRow)
        {
            super(RegularAndStaticColumns.NONE, metadata, partitionKey, isReverseOrder, staticRow);
        }
    }

    public static UnfilteredPartitionIterator unfilteredPartition(TableMetadata metadata)
    {
        return new EmptyUnfilteredPartitionIterator(metadata);
    }

    public static PartitionIterator partition()
    {
        return EmptyPartitionIterator.instance;
    }

    // this method is the only one that can return a non-empty iterator, but it still has no rows, so it seems cleanest to keep it here
    public static UnfilteredRowIterator unfilteredRow(TableMetadata metadata, DecoratedKey partitionKey, boolean isReverseOrder, Row staticRow, DeletionTime partitionDeletion)
    {
        RegularAndStaticColumns columns = RegularAndStaticColumns.NONE;
        if (!staticRow.isEmpty())
            columns = new RegularAndStaticColumns(Columns.from(staticRow), Columns.NONE);
        else
            staticRow = Rows.EMPTY_STATIC_ROW;

        if (partitionDeletion.isLive())
            partitionDeletion = DeletionTime.LIVE;

        return new EmptyUnfilteredRowIterator(columns, metadata, partitionKey, isReverseOrder, staticRow, partitionDeletion);
    }

    public static UnfilteredRowIterator unfilteredRow(TableMetadata metadata, DecoratedKey partitionKey, boolean isReverseOrder)
    {
        return new EmptyUnfilteredRowIterator(RegularAndStaticColumns.NONE, metadata, partitionKey, isReverseOrder, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE);
    }

    public static RowIterator row(TableMetadata metadata, DecoratedKey partitionKey, boolean isReverseOrder)
    {
        return new EmptyRowIterator(metadata, partitionKey, isReverseOrder, Rows.EMPTY_STATIC_ROW);
    }
}
