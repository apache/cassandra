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

package org.apache.cassandra.service.accord.txn;

import java.util.function.Supplier;

import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.utils.ObjectSizes;

public class TxnRangeReadResult implements TxnResult
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnRangeReadResult(null));

    public final Supplier<PartitionIterator> partitions;

    public TxnRangeReadResult(Supplier<PartitionIterator> partitions)
    {
        this.partitions = partitions;
    }

    @Override
    public Kind kind()
    {
        return Kind.range_read;
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        PartitionIterator iterator = partitions.get();
        while (iterator.hasNext())
        {
            RowIterator rowIterator = iterator.next();
            Row staticRow = rowIterator.staticRow();
            if (staticRow != null)
                size += staticRow.unsharedHeapSize();
            while (rowIterator.hasNext())
                size += rowIterator.next().unsharedHeapSize();
        }
        // TODO: Include the other parts of FilteredPartition after we rebase to pull in BTreePartitionData?
        return size;
    }
}
