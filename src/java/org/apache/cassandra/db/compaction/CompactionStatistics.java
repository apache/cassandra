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

package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.UUID;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A container for the statistics of a single compaction event:
 * input and output size, duration, before and after partition and row counters, etc.
 */
public class CompactionStatistics
{
    private final CompactionStrategyManager strategyManager;

    public final TableMetadata metadata;
    public final OperationType tasktype;
    public final UUID compactionId;

    public final Collection<SSTableReader> startSstables;
    public final long startSizeBytes;

    public long estInputSizeBytes;
    public long totalSourceRows;
    public long totalSourcePartitions;

    public long[] mergedPartitionCounts;
    public long[] mergedRowsCounts;

    public Collection<SSTableReader> endSstables;
    public long endSizeBytes;

    public long bytesRead;
    public long bytesWritten;

    public long durationInNanos;
    public boolean stopRequested;

    CompactionStatistics(ColumnFamilyStore cfs, OperationType tasktype, UUID compactionId, Collection<SSTableReader> startSstables)
    {
        this.strategyManager = cfs.getCompactionStrategyManager();

        this.metadata = cfs.metadata();
        this.tasktype = tasktype;
        this.compactionId = compactionId;
        this.startSstables = startSstables;
        this.startSizeBytes = SSTableReader.getTotalBytes(startSstables);
        this.estInputSizeBytes = this.startSizeBytes;

        this.totalSourceRows = 0;
        this.totalSourcePartitions = 0;
        this.mergedPartitionCounts = new long[0];
        this.mergedRowsCounts = new long[0];
        this.endSizeBytes = 0;
        this.bytesRead = 0;
        this.bytesWritten = 0;
        this.durationInNanos = 0;
        this.stopRequested = false;
    }

    double sizeRatio()
    {
        if (estInputSizeBytes > 0)
            return endSizeBytes / (double) estInputSizeBytes;

        // this is a valid case, when there are no sstables to actually compact
        // the previous code would return a NaN that would be logged as zero
        return 0;
    }

    void setEndSstables(Collection<SSTableReader> endSstables)
    {
        this.endSstables = endSstables;
        this.endSizeBytes = SSTableReader.getTotalBytes(endSstables);
    }

    public AbstractCompactionStrategy getStrategyFor(SSTableReader ssTableReader)
    {
        return strategyManager.getCompactionStrategyFor(ssTableReader);
    }
}