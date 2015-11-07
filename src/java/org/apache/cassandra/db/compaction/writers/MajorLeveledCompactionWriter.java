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

import java.io.File;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

public class MajorLeveledCompactionWriter extends CompactionAwareWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MajorLeveledCompactionWriter.class);
    private final long maxSSTableSize;
    private final long expectedWriteSize;
    private final Set<SSTableReader> allSSTables;
    private int currentLevel = 1;
    private long averageEstimatedKeysPerSSTable;
    private long partitionsWritten = 0;
    private long totalWrittenInLevel = 0;
    private int sstablesWritten = 0;

    public MajorLeveledCompactionWriter(ColumnFamilyStore cfs,
                                        Directories directories,
                                        LifecycleTransaction txn,
                                        Set<SSTableReader> nonExpiredSSTables,
                                        long maxSSTableSize)
    {
        this(cfs, directories, txn, nonExpiredSSTables, maxSSTableSize, false, false);
    }

    @SuppressWarnings("resource")
    public MajorLeveledCompactionWriter(ColumnFamilyStore cfs,
                                        Directories directories,
                                        LifecycleTransaction txn,
                                        Set<SSTableReader> nonExpiredSSTables,
                                        long maxSSTableSize,
                                        boolean offline,
                                        boolean keepOriginals)
    {
        super(cfs, directories, txn, nonExpiredSSTables, offline, keepOriginals);
        this.maxSSTableSize = maxSSTableSize;
        this.allSSTables = txn.originals();
        expectedWriteSize = Math.min(maxSSTableSize, cfs.getExpectedCompactedFileSize(nonExpiredSSTables, txn.opType()));
    }

    @Override
    @SuppressWarnings("resource")
    public boolean realAppend(UnfilteredRowIterator partition)
    {
        long posBefore = sstableWriter.currentWriter().getOnDiskFilePointer();
        RowIndexEntry rie = sstableWriter.append(partition);
        totalWrittenInLevel += sstableWriter.currentWriter().getOnDiskFilePointer() - posBefore;
        partitionsWritten++;
        if (sstableWriter.currentWriter().getOnDiskFilePointer() > maxSSTableSize)
        {
            if (totalWrittenInLevel > LeveledManifest.maxBytesForLevel(currentLevel, maxSSTableSize))
            {
                totalWrittenInLevel = 0;
                currentLevel++;
            }

            averageEstimatedKeysPerSSTable = Math.round(((double) averageEstimatedKeysPerSSTable * sstablesWritten + partitionsWritten) / (sstablesWritten + 1));
            switchCompactionLocation(getWriteDirectory(expectedWriteSize));
            partitionsWritten = 0;
            sstablesWritten++;
        }
        return rie != null;

    }

    public void switchCompactionLocation(Directories.DataDirectory directory)
    {
        File sstableDirectory = getDirectories().getLocationForDisk(directory);
        @SuppressWarnings("resource")
        SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(cfs.getSSTablePath(sstableDirectory)),
                                                    averageEstimatedKeysPerSSTable,
                                                    minRepairedAt,
                                                    cfs.metadata,
                                                    new MetadataCollector(allSSTables, cfs.metadata.comparator, currentLevel),
                                                    SerializationHeader.make(cfs.metadata, nonExpiredSSTables),
                                                    cfs.indexManager.listIndexes(),
                                                    txn);
        sstableWriter.switchWriter(writer);
    }
}
