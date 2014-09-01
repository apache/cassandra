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

import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;

public class FilteredPartition extends AbstractPartitionData implements Iterable<Row>
{
    private FilteredPartition(CFMetaData metadata,
                              DecoratedKey partitionKey,
                              PartitionColumns columns,
                              int initialRowCapacity,
                              boolean sortable)
    {
        super(metadata, partitionKey, DeletionTime.LIVE, columns, initialRowCapacity, sortable);
    }

    /**
     * Create a FilteredPartition holding all the rows of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     */
    public static FilteredPartition create(RowIterator iterator)
    {
        FilteredPartition partition = new FilteredPartition(iterator.metadata(),
                                                            iterator.partitionKey(),
                                                            iterator.columns(),
                                                            4,
                                                            iterator.isReverseOrder());

        partition.staticRow = iterator.staticRow().takeAlias();

        Writer writer = partition.new Writer(true);

        while (iterator.hasNext())
            iterator.next().copyTo(writer);

        // A Partition (or more precisely AbstractPartitionData) always assumes that its data is in clustering
        // order. So if we've just added them in reverse clustering order, reverse them.
        if (iterator.isReverseOrder())
            partition.reverse();

        return partition;
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
                return staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
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
        try (RowIterator iterator = rowIterator())
        {
            StringBuilder sb = new StringBuilder();
            CFMetaData metadata = iterator.metadata();
            PartitionColumns columns = iterator.columns();

            sb.append(String.format("[%s.%s] key=%s columns=%s reversed=%b",
                                    metadata.ksName,
                                    metadata.cfName,
                                    metadata.getKeyValidator().getString(iterator.partitionKey().getKey()),
                                    columns,
                                    iterator.isReverseOrder()));

            if (iterator.staticRow() != Rows.EMPTY_STATIC_ROW)
                sb.append("\n    ").append(iterator.staticRow().toString(metadata));

            while (iterator.hasNext())
                sb.append("\n    ").append(iterator.next().toString(metadata));

            return sb.toString();
        }
    }
}
