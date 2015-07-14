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
package org.apache.cassandra.db.partitions;

import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;

public class FilteredPartition extends AbstractThreadUnsafePartition
{
    private final Row staticRow;

    private FilteredPartition(CFMetaData metadata,
                              DecoratedKey partitionKey,
                              PartitionColumns columns,
                              Row staticRow,
                              List<Row> rows)
    {
        super(metadata, partitionKey, columns, rows);
        this.staticRow = staticRow;
    }

    /**
     * Create a FilteredPartition holding all the rows of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     */
    public static FilteredPartition create(RowIterator iterator)
    {
        CFMetaData metadata = iterator.metadata();
        boolean reversed = iterator.isReverseOrder();

        List<Row> rows = new ArrayList<>();

        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            if (unfiltered.isRow())
                rows.add((Row)unfiltered);
        }

        if (reversed)
            Collections.reverse(rows);

        return new FilteredPartition(metadata, iterator.partitionKey(), iterator.columns(), iterator.staticRow(), rows);
    }

    protected boolean canHaveShadowedData()
    {
        // We only create instances from RowIterator that don't have shadowed data (nor deletion info really)
        return false;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public DeletionInfo deletionInfo()
    {
        return DeletionInfo.LIVE;
    }

    public EncodingStats stats()
    {
        return EncodingStats.NO_STATS;
    }

    public RowIterator rowIterator()
    {
        final Iterator<Row> iter = iterator();
        return new RowIterator()
        {
            public CFMetaData metadata()
            {
                return metadata;
            }

            public boolean isReverseOrder()
            {
                return false;
            }

            public PartitionColumns columns()
            {
                return columns;
            }

            public DecoratedKey partitionKey()
            {
                return key;
            }

            public Row staticRow()
            {
                return FilteredPartition.this.staticRow();
            }

            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public Row next()
            {
                return iter.next();
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
            }
        };
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("[%s.%s] key=%s columns=%s",
                    metadata.ksName,
                    metadata.cfName,
                    metadata.getKeyValidator().getString(partitionKey().getKey()),
                    columns));

        if (staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("\n    ").append(staticRow().toString(metadata));

        for (Row row : this)
            sb.append("\n    ").append(row.toString(metadata));

        return sb.toString();
    }
}
