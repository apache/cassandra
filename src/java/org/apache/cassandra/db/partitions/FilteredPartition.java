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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.rows.*;

public class FilteredPartition extends ImmutableBTreePartition
{
    public FilteredPartition(RowIterator rows)
    {
        super(rows.metadata(), rows.partitionKey(), build(rows, DeletionInfo.LIVE, false, 16));
    }

    /**
     * Create a FilteredPartition holding all the rows of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     */
    public static FilteredPartition create(RowIterator iterator)
    {
        return new FilteredPartition(iterator);
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
                return FilteredPartition.this.columns();
            }

            public DecoratedKey partitionKey()
            {
                return FilteredPartition.this.partitionKey();
            }

            public Row staticRow()
            {
                return FilteredPartition.this.staticRow();
            }

            public void close() {}

            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public Row next()
            {
                return iter.next();
            }

            public boolean isEmpty()
            {
                return staticRow().isEmpty() && !hasRows();
            }
        };
    }
}
