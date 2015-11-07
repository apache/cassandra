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
package org.apache.cassandra.db.compaction.writers;

import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

public class MaxSSTableSizeWriter extends CompactionAwareWriter
{
    private final long estimatedTotalKeys;
    private final long expectedWriteSize;
    private final long maxSSTableSize;
    private final int level;
    private final long estimatedSSTables;
    private final Set<SSTableReader> allSSTables;

    public MaxSSTableSizeWriter(ColumnFamilyStore cfs,
                                Directories directories,
                                LifecycleTransaction txn,
                                Set<SSTableReader> nonExpiredSSTables,
                                long maxSSTableSize,
                                int level)
    {
        this(cfs, directories, txn, nonExpiredSSTables, maxSSTableSize, level, false, false);
    }

    @SuppressWarnings("resource")
    public MaxSSTableSizeWriter(ColumnFamilyStore cfs,
                                Directories directories,
                                LifecycleTransaction txn,
                                Set<SSTableReader> nonExpiredSSTables,
                                long maxSSTableSize,
                                int level,
                                boolean offline,
                                boolean keepOriginals)
    {
        super(cfs, directories, txn, nonExpiredSSTables, offline, keepOriginals);
        this.allSSTables = txn.originals();
        this.level = level;
        this.maxSSTableSize = maxSSTableSize;
        long totalSize = cfs.getExpectedCompactedFileSize(nonExpiredSSTables, txn.opType());
        expectedWriteSize = Math.min(maxSSTableSize, totalSize);
        estimatedTotalKeys = SSTableReader.getApproximateKeyCount(nonExpiredSSTables);
        estimatedSSTables = Math.max(1, estimatedTotalKeys / maxSSTableSize);
    }

    @Override
    public boolean realAppend(UnfilteredRowIterator partition)
    {
        RowIndexEntry rie = sstableWriter.append(partition);
        if (sstableWriter.currentWriter().getOnDiskFilePointer() > maxSSTableSize)
            switchCompactionLocation(getWriteDirectory(expectedWriteSize));
        return rie != null;
    }

    public void switchCompactionLocation(Directories.DataDirectory location)
    {
        @SuppressWarnings("resource")
        SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(cfs.getSSTablePath(getDirectories().getLocationForDisk(location))),
                                                    estimatedTotalKeys / estimatedSSTables,
                                                    minRepairedAt,
                                                    cfs.metadata,
                                                    new MetadataCollector(allSSTables, cfs.metadata.comparator, level),
                                                    SerializationHeader.make(cfs.metadata, nonExpiredSSTables),
                                                    cfs.indexManager.listIndexes(),
                                                    txn);

        sstableWriter.switchWriter(writer);

    }

    @Override
    public long estimatedKeys()
    {
        return estimatedTotalKeys;
    }
}
