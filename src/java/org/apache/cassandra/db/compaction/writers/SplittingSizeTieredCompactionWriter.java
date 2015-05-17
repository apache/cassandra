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
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

/**
 * CompactionAwareWriter that splits input in differently sized sstables
 *
 * Biggest sstable will be total_compaction_size / 2, second biggest total_compaction_size / 4 etc until
 * the result would be sub 50MB, all those are put in the same
 */
public class SplittingSizeTieredCompactionWriter extends CompactionAwareWriter
{
    private static final Logger logger = LoggerFactory.getLogger(SplittingSizeTieredCompactionWriter.class);

    public static final long DEFAULT_SMALLEST_SSTABLE_BYTES = 50_000_000;
    private final double[] ratios;
    private final long totalSize;
    private final Set<SSTableReader> allSSTables;
    private long currentBytesToWrite;
    private int currentRatioIndex = 0;

    public SplittingSizeTieredCompactionWriter(ColumnFamilyStore cfs, Set<SSTableReader> allSSTables, Set<SSTableReader> nonExpiredSSTables, OperationType compactionType)
    {
        this(cfs, allSSTables, nonExpiredSSTables, compactionType, DEFAULT_SMALLEST_SSTABLE_BYTES);
    }

    public SplittingSizeTieredCompactionWriter(ColumnFamilyStore cfs, Set<SSTableReader> allSSTables, Set<SSTableReader> nonExpiredSSTables, OperationType compactionType, long smallestSSTable)
    {
        super(cfs, allSSTables, nonExpiredSSTables, false);
        this.allSSTables = allSSTables;
        totalSize = cfs.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType);
        double[] potentialRatios = new double[20];
        double currentRatio = 1;
        for (int i = 0; i < potentialRatios.length; i++)
        {
            currentRatio /= 2;
            potentialRatios[i] = currentRatio;
        }

        int noPointIndex = 0;
        // find how many sstables we should create - 50MB min sstable size
        for (double ratio : potentialRatios)
        {
            noPointIndex++;
            if (ratio * totalSize < smallestSSTable)
            {
                break;
            }
        }
        ratios = Arrays.copyOfRange(potentialRatios, 0, noPointIndex);
        File sstableDirectory = cfs.directories.getLocationForDisk(getWriteDirectory(Math.round(totalSize * ratios[currentRatioIndex])));
        long currentPartitionsToWrite = Math.round(estimatedTotalKeys * ratios[currentRatioIndex]);
        currentBytesToWrite = Math.round(totalSize * ratios[currentRatioIndex]);
        SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(cfs.getTempSSTablePath(sstableDirectory)),
                                                                            currentPartitionsToWrite,
                                                                            minRepairedAt,
                                                                            cfs.metadata,
                                                                            cfs.partitioner,
                                                                            new MetadataCollector(allSSTables, cfs.metadata.comparator, 0));

        sstableWriter.switchWriter(writer);
        logger.debug("Ratios={}, expectedKeys = {}, totalSize = {}, currentPartitionsToWrite = {}, currentBytesToWrite = {}", ratios, estimatedTotalKeys, totalSize, currentPartitionsToWrite, currentBytesToWrite);
    }

    @Override
    public boolean append(AbstractCompactedRow row)
    {
        RowIndexEntry rie = sstableWriter.append(row);
        if (sstableWriter.currentWriter().getOnDiskFilePointer() > currentBytesToWrite && currentRatioIndex < ratios.length - 1) // if we underestimate how many keys we have, the last sstable might get more than we expect
        {
            currentRatioIndex++;
            currentBytesToWrite = Math.round(totalSize * ratios[currentRatioIndex]);
            long currentPartitionsToWrite = Math.round(ratios[currentRatioIndex] * estimatedTotalKeys);
            File sstableDirectory = cfs.directories.getLocationForDisk(getWriteDirectory(Math.round(totalSize * ratios[currentRatioIndex])));
            SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(cfs.getTempSSTablePath(sstableDirectory)),
                                                                                currentPartitionsToWrite,
                                                                                minRepairedAt,
                                                                                cfs.metadata,
                                                                                cfs.partitioner,
                                                                                new MetadataCollector(allSSTables, cfs.metadata.comparator, 0));
            sstableWriter.switchWriter(writer);
            logger.debug("Switching writer, currentPartitionsToWrite = {}", currentPartitionsToWrite);
        }
        return rie != null;
    }
}