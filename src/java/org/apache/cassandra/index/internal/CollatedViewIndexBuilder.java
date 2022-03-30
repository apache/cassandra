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
package org.apache.cassandra.index.internal;

import java.util.Collection;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * Manages building an entire index from column family data. Runs on to compaction manager.
 */
public class CollatedViewIndexBuilder extends SecondaryIndexBuilder
{
    private final ColumnFamilyStore cfs;
    private final Set<Index> indexers;
    private final ReducingKeyIterator iter;
    private final TimeUUID compactionId;
    private final Collection<SSTableReader> sstables;

    public CollatedViewIndexBuilder(ColumnFamilyStore cfs, Set<Index> indexers, ReducingKeyIterator iter, Collection<SSTableReader> sstables)
    {
        this.cfs = cfs;
        this.indexers = indexers;
        this.iter = iter;
        this.compactionId = nextTimeUUID();
        this.sstables = sstables;
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(cfs.metadata(),
                                  OperationType.INDEX_BUILD,
                                  iter.getBytesRead(),
                                  iter.getTotalBytes(),
                                  compactionId,
                                  sstables);
    }

    public void build()
    {
        try
        {
            int pageSize = cfs.indexManager.calculateIndexingPageSize();
            RegularAndStaticColumns targetPartitionColumns = extractIndexedColumns();
            
            while (iter.hasNext())
            {
                if (isStopRequested())
                    throw new CompactionInterruptedException(getCompactionInfo());
                DecoratedKey key = iter.next();
                cfs.indexManager.indexPartition(key, indexers, pageSize, targetPartitionColumns);
            }
        }
        finally
        {
            iter.close();
        }
    }

    private RegularAndStaticColumns extractIndexedColumns()
    {
        RegularAndStaticColumns.Builder builder = RegularAndStaticColumns.builder();
        
        for (Index index : indexers)
        {
            boolean isPartitionIndex = true;
            
            for (ColumnMetadata column : cfs.metadata().regularAndStaticColumns())
            {
                if (index.dependsOn(column))
                {
                    builder.add(column);
                    isPartitionIndex = false;
                }
            }

            // if any index declares no dependency on any column, it is a full partition index
            // so we can use the base partition columns as the input source
            if (isPartitionIndex)
                return cfs.metadata().regularAndStaticColumns();
        }
        
        return builder.build();
    }
}
