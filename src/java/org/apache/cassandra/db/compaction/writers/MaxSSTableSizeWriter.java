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
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

public class MaxSSTableSizeWriter extends CompactionAwareWriter
{
    private final long maxSSTableSize;
    private final int level;
    private final long estimatedSSTables;
    private final Set<SSTableReader> allSSTables;
    private Directories.DataDirectory sstableDirectory;

    public MaxSSTableSizeWriter(ColumnFamilyStore cfs,
                                Directories directories,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9978
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7066
                                LifecycleTransaction txn,
                                Set<SSTableReader> nonExpiredSSTables,
                                long maxSSTableSize,
                                int level)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11148
        this(cfs, directories, txn, nonExpiredSSTables, maxSSTableSize, level, false);
    }

    @Deprecated
    public MaxSSTableSizeWriter(ColumnFamilyStore cfs,
                                Directories directories,
                                LifecycleTransaction txn,
                                Set<SSTableReader> nonExpiredSSTables,
                                long maxSSTableSize,
                                int level,
                                boolean offline,
                                boolean keepOriginals)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11148
        this(cfs, directories, txn, nonExpiredSSTables, maxSSTableSize, level, keepOriginals);
    }

    public MaxSSTableSizeWriter(ColumnFamilyStore cfs,
                                Directories directories,
                                LifecycleTransaction txn,
                                Set<SSTableReader> nonExpiredSSTables,
                                long maxSSTableSize,
                                int level,
                                boolean keepOriginals)
    {
        super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
        this.allSSTables = txn.originals();
        this.level = level;
        this.maxSSTableSize = maxSSTableSize;

        long totalSize = getTotalWriteSize(nonExpiredSSTables, estimatedTotalKeys, cfs, txn.opType());
        estimatedSSTables = Math.max(1, totalSize / maxSSTableSize);
    }

    /**
     * Gets the estimated total amount of data to write during compaction
     */
    private static long getTotalWriteSize(Iterable<SSTableReader> nonExpiredSSTables, long estimatedTotalKeys, ColumnFamilyStore cfs, OperationType compactionType)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11344
        long estimatedKeysBeforeCompaction = 0;
        for (SSTableReader sstable : nonExpiredSSTables)
            estimatedKeysBeforeCompaction += sstable.estimatedKeys();
        estimatedKeysBeforeCompaction = Math.max(1, estimatedKeysBeforeCompaction);
        double estimatedCompactionRatio = (double) estimatedTotalKeys / estimatedKeysBeforeCompaction;

        return Math.round(estimatedCompactionRatio * cfs.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType));
    }

    protected boolean realAppend(UnfilteredRowIterator partition)
    {
        RowIndexEntry rie = sstableWriter.append(partition);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11623
        if (sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten() > maxSSTableSize)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6696
            switchCompactionLocation(sstableDirectory);
        }
        return rie != null;
    }

    @Override
    public void switchCompactionLocation(Directories.DataDirectory location)
    {
        sstableDirectory = location;
        @SuppressWarnings("resource")
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12716
        SSTableWriter writer = SSTableWriter.create(cfs.newSSTableDescriptor(getDirectories().getLocationForDisk(sstableDirectory)),
                                                    estimatedTotalKeys / estimatedSSTables,
                                                    minRepairedAt,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9143
                                                    pendingRepair,
                                                    isTransient,
                                                    cfs.metadata,
                                                    new MetadataCollector(allSSTables, cfs.metadata().comparator, level),
                                                    SerializationHeader.make(cfs.metadata(), nonExpiredSSTables),
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10678
                                                    cfs.indexManager.listIndexes(),
                                                    txn);

        sstableWriter.switchWriter(writer);
    }
}
