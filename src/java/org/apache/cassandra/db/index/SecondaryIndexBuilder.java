/**
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
package org.apache.cassandra.db.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedSet;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;

/**
 * Manages building an entire index from column family data. Runs on to compaction manager.
 */
public class SecondaryIndexBuilder implements  CompactionInfo.Holder
{
    
    private final ColumnFamilyStore cfs;
    private final SortedSet<ByteBuffer> columns;
    private final ReducingKeyIterator iter;
    
    public SecondaryIndexBuilder(ColumnFamilyStore cfs, SortedSet<ByteBuffer> columns, ReducingKeyIterator iter)
    {
        this.cfs = cfs;
        this.columns = columns;
        this.iter = iter;
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(this.hashCode(),
                                  cfs.table.name,
                                  cfs.columnFamily,
                                  OperationType.INDEX_BUILD,
                                  iter.getBytesRead(),
                                  iter.getTotalBytes());
    }

    public void build()
    {
        while (iter.hasNext())
        {
            DecoratedKey<?> key = iter.next();
            Table.indexRow(key, cfs, columns);
        }

        try
        {
            iter.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
