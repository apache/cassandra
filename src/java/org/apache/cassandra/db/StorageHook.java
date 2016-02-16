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

package org.apache.cassandra.db;

import java.util.UUID;

import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

public interface StorageHook
{
    public static final StorageHook instance = createHook();

    public void reportWrite(UUID cfid, PartitionUpdate partitionUpdate);
    public void reportRead(UUID cfid, DecoratedKey key);
    public UnfilteredRowIteratorWithLowerBound makeRowIteratorWithLowerBound(ColumnFamilyStore cfs,
                                                                      DecoratedKey partitionKey,
                                                                      SSTableReader sstable,
                                                                      ClusteringIndexFilter filter,
                                                                      ColumnFilter selectedColumns,
                                                                      boolean isForThrift,
                                                                      int nowInSec,
                                                                      boolean applyThriftTransformation);
    public UnfilteredRowIterator makeRowIterator(ColumnFamilyStore cfs,
                                                 SSTableReader sstable,
                                                 DecoratedKey key,
                                                 Slices slices,
                                                 ColumnFilter selectedColumns,
                                                 boolean reversed,
                                                 boolean isForThrift);

    static StorageHook createHook()
    {
        String className =  System.getProperty("cassandra.storage_hook");
        if (className != null)
        {
            return FBUtilities.construct(className, StorageHook.class.getSimpleName());
        }
        else
        {
            return new StorageHook()
            {
                public void reportWrite(UUID cfid, PartitionUpdate partitionUpdate) {}

                public void reportRead(UUID cfid, DecoratedKey key) {}

                public UnfilteredRowIteratorWithLowerBound makeRowIteratorWithLowerBound(ColumnFamilyStore cfs, DecoratedKey partitionKey, SSTableReader sstable, ClusteringIndexFilter filter, ColumnFilter selectedColumns, boolean isForThrift, int nowInSec, boolean applyThriftTransformation)
                {
                    return new UnfilteredRowIteratorWithLowerBound(partitionKey,
                                                                   sstable,
                                                                   filter,
                                                                   selectedColumns,
                                                                   isForThrift,
                                                                   nowInSec,
                                                                   applyThriftTransformation);
                }

                public UnfilteredRowIterator makeRowIterator(ColumnFamilyStore cfs, SSTableReader sstable, DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, boolean isForThrift)
                {
                    return sstable.iterator(key, slices, selectedColumns, reversed, isForThrift);
                }
            };
        }
    }
}
