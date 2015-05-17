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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
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
    private final boolean skipAncestors;

    public MajorLeveledCompactionWriter(ColumnFamilyStore cfs, Set<SSTableReader> allSSTables, Set<SSTableReader> nonExpiredSSTables, long maxSSTableSize, boolean offline, OperationType compactionType)
    {
        super(cfs, allSSTables, nonExpiredSSTables, offline);
        this.maxSSTableSize = maxSSTableSize;
        this.allSSTables = allSSTables;
        expectedWriteSize = Math.min(maxSSTableSize, cfs.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType));
        long estimatedSSTables = Math.max(1, SSTableReader.getTotalBytes(nonExpiredSSTables) / maxSSTableSize);
        long keysPerSSTable = estimatedTotalKeys / estimatedSSTables;
        File sstableDirectory = cfs.directories.getLocationForDisk(getWriteDirectory(expectedWriteSize));
        skipAncestors = estimatedSSTables * allSSTables.size() > 200000; // magic number, avoid storing too much ancestor information since allSSTables are ancestors to *all* resulting sstables

        if (skipAncestors)
            logger.warn("Many sstables involved in compaction, skipping storing ancestor information to avoid running out of memory");

        SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(cfs.getTempSSTablePath(sstableDirectory)),
                                                    keysPerSSTable,
                                                    minRepairedAt,
                                                    cfs.metadata,
                                                    cfs.partitioner,
                                                    new MetadataCollector(allSSTables, cfs.metadata.comparator, currentLevel, skipAncestors));
        sstableWriter.switchWriter(writer);
    }

    @Override
    public boolean append(AbstractCompactedRow row)
    {
        long posBefore = sstableWriter.currentWriter().getOnDiskFilePointer();
        RowIndexEntry rie = sstableWriter.append(row);
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
            File sstableDirectory = cfs.directories.getLocationForDisk(getWriteDirectory(expectedWriteSize));
            SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(cfs.getTempSSTablePath(sstableDirectory)),
                                                        averageEstimatedKeysPerSSTable,
                                                        minRepairedAt,
                                                        cfs.metadata,
                                                        cfs.partitioner,
                                                        new MetadataCollector(allSSTables, cfs.metadata.comparator, currentLevel, skipAncestors));
            sstableWriter.switchWriter(writer);
            partitionsWritten = 0;
            sstablesWritten++;
        }
        return rie != null;

    }
}